package eventsource

import (
	"context"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/encoding"
	"github.com/c-kruse/vanflow/messaging"
	"github.com/c-kruse/vanflow/store"
	"github.com/cenkalti/backoff/v4"
)

type ManagerConfig struct {
	Source Info

	Stores []store.Interface

	// HeartbeatInterval defaults to 2 seconds
	HeartbeatInterval time.Duration
	// BeaconInterval defaults to 10 seconds
	BeaconInterval time.Duration

	// UseAlternateHeartbeatAddress indicates that the manager should send
	// heartbeat messages to the source address with the `.heartbeats` suffix.
	UseAlternateHeartbeatAddress bool

	// FlushDelay is the amount of time to wait after receiving an initial
	// flush message before beginning to send updates.
	FlushDelay time.Duration
	// FlushBatchSize is the maximum number of records that will be sent in a
	// single record message on flush.
	FlushBatchSize int

	// UpdateBufferTime is the amount of time to wait for a full batch of
	// record updates before sending a partial batch.
	UpdateBufferTime time.Duration
	// UpdateBatchSize is the maximum number of record updates that will be
	// sent in a single record message. Defaults to 1.
	UpdateBatchSize int
}

type RecordUpdate struct {
	Prev vanflow.Record
	Curr vanflow.Record
}

type Manager struct {
	ManagerConfig
	factory messaging.SessionFactory

	flushQueue  chan struct{}
	changeQueue chan RecordUpdate
	sendQueue   chan vanflow.RecordMessage
}

func NewManager(factory messaging.SessionFactory, cfg ManagerConfig) *Manager {
	return &Manager{
		factory:       factory,
		ManagerConfig: cfg,
		flushQueue:    make(chan struct{}, 8),
		changeQueue:   make(chan RecordUpdate, 256),
		sendQueue:     make(chan vanflow.RecordMessage, 256),
	}
}

func (m *Manager) PublishUpdate(update RecordUpdate) {
	m.changeQueue <- update
}

func (m *Manager) Run(ctx context.Context) {
	go m.listenFlushes(ctx)
	go m.sendKeepalives(ctx)
	go m.sendRecords(ctx)
	m.serve(ctx)
}

func (m *Manager) sendRecords(ctx context.Context) {
	b := defaultBackOff(ctx)
	backoff.Retry(func() error {
		conn, err := m.factory.Create(ctx)
		if err != nil {
			slog.Error("could not establish connection for event source records", slog.Any("error", err))
			return err
		}
		sender, err := conn.Sender(ctx, m.Source.Address, nil)
		if err != nil {
			slog.Error("could not open sender for event source records", slog.Any("error", err))
			return err
		}
		defer sender.Close(ctx)

		for {
			select {
			case <-ctx.Done():
				return nil
			case record := <-m.sendQueue:
				msg, err := record.Encode()
				if err != nil {
					slog.Error("skipping record message after encoding error:", slog.Any("error", err))
					continue
				}
				if err := sender.Send(ctx, msg, nil); err != nil {
					slog.Error("error sending event source record", slog.Any("error", err))
					return err
				}
				b.Reset() // message sent so connection is happy
			}
		}
	}, b)
}

func diffRecord(d RecordUpdate) (vanflow.Record, bool) {
	if d.Prev == nil {
		return d.Curr, true
	}
	prev, err := encoding.Encode(d.Prev)
	if err != nil {
		slog.Error("record update diff error encoding prev", slog.Any("error", err))
		return nil, false
	}
	next, err := encoding.Encode(d.Curr)
	if err != nil {
		slog.Error("record update diff error encoding curr", slog.Any("error", err))
		return nil, false
	}
	var isDiff bool
	delta := make(map[any]any)
	for k := range next {
		pv, ok := prev[k]
		if !ok {
			isDiff = true
			delta[k] = next[k]
			continue
		}
		if next[k] != pv {
			isDiff = true
			delta[k] = next[k]
		}
		kCodePoint, ok := k.(uint32)
		if ok && kCodePoint < 2 { // hack to keep identifying info in the record
			delta[k] = next[k]
		}
	}
	if !isDiff {
		return nil, false
	}
	deltaRecord, err := encoding.Decode(delta)
	if err != nil {
		slog.Error("record update diff error decoding delta", slog.Any("error", err))
		return nil, false
	}
	return deltaRecord.(vanflow.Record), true
}

func (m *Manager) serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case update := <-m.changeQueue:
			var buffer []RecordUpdate
			if m.UpdateBufferTime > 0 && m.UpdateBatchSize > 1 {
				bufferCtx, cancelBuffer := context.WithTimeout(ctx, m.UpdateBufferTime)
				buffer = nextN(bufferCtx, m.changeQueue, m.UpdateBatchSize-1)
				cancelBuffer()
				if ctx.Err() != nil {
					return
				}
			}
			buffer = append([]RecordUpdate{update}, buffer...)

			var record vanflow.RecordMessage
			record.Records = make([]vanflow.Record, 0, len(buffer))
			for _, update := range buffer {
				delta, changed := diffRecord(update)
				if !changed {
					continue
				}
				record.Records = append(record.Records, delta)
			}
			if len(record.Records) == 0 {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case m.sendQueue <- record:
			}

		case <-m.flushQueue:
			// handle a flush
			if m.FlushDelay > 0 {
				drainCtx, cancelDrain := context.WithTimeout(ctx, m.FlushDelay)
				nextN(drainCtx, m.flushQueue, 2048)
				cancelDrain()
				if ctx.Err() != nil {
					return
				}
			}
			slog.Info("servicing flush", slog.String("source", m.Source.ID))
			flushStoreQ := make([]store.Interface, len(m.Stores))
			copy(flushStoreQ, m.Stores)
			selector := &store.Selector{
				Limit: m.FlushBatchSize,
			}
			for len(flushStoreQ) > 0 {
				stor := flushStoreQ[0]
				result, err := stor.List(ctx, selector)
				if err != nil {
					if errors.Is(err, ctx.Err()) {
						return
					}
					slog.Error("flush error listing resources", slog.Any("error", err))
					continue // probably bad.
				}
				selector.Continue = result.Continue
				if result.Continue == "" {
					flushStoreQ = flushStoreQ[1:]
				}

				if len(result.Entries) == 0 {
					continue
				}
				var record vanflow.RecordMessage
				record.Records = make([]vanflow.Record, len(result.Entries))
				for i := range result.Entries {
					record.Records[i] = result.Entries[i].Record
				}
				select {
				case <-ctx.Done():
					return
				case m.sendQueue <- record:
				}
			}
		}
	}
}

func (m *Manager) sendKeepalives(ctx context.Context) {
	beaconInterval := m.BeaconInterval
	if beaconInterval <= 0 {
		beaconInterval = 10 * time.Second
	}
	heartbeatInterval := m.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 2 * time.Second
	}

	beaconTimer := time.NewTicker(beaconInterval)
	heartbeatTimer := time.NewTicker(heartbeatInterval)

	beaconMessage := vanflow.BeaconMessage{
		Version:    uint32(m.Source.Version),
		SourceType: m.Source.Type,
		Address:    m.Source.Address,
		Direct:     m.Source.Direct,
		Identity:   m.Source.ID,
	}
	beaconMessage.To = beaconAddress
	heartbeatMessage := vanflow.HeartbeatMessage{
		Version:  uint32(m.Source.Version),
		Identity: m.Source.ID,
	}

	heartbeatAddr := m.Source.Address
	if m.UseAlternateHeartbeatAddress {
		heartbeatAddr = heartbeatAddr + sourceSuffixHeartbeats
	}
	sendSettled := &amqp.SenderOptions{SettlementMode: amqp.SenderSettleModeSettled.Ptr()}
	b := defaultBackOff(ctx)
	backoff.Retry(func() error {
		conn, err := m.factory.Create(ctx)
		if err != nil {
			slog.Error("could not establish connection for event source keepalive", slog.Any("error", err))
			return err
		}
		beaconSender, err := conn.Sender(ctx, beaconAddress, sendSettled)
		if err != nil {
			slog.Error("could not open sender for event source beacon", slog.Any("error", err))
			return err
		}
		defer beaconSender.Close(ctx)
		heartbeatSender, err := conn.Sender(ctx, heartbeatAddr, sendSettled)
		if err != nil {
			slog.Error("could not open sender for event source heartbeat", slog.Any("error", err))
			return err
		}
		defer heartbeatSender.Close(ctx)

		// start with sending beacon
		if err := sendWithTimeout(ctx, beaconInterval, beaconSender, beaconMessage.Encode()); err != nil {
			slog.Error("error sending initial beacon message", slog.Any("error", err))
			return err
		}
		hearbeatTimeouts := 0
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-beaconTimer.C:
				msg := beaconMessage.Encode()
				if err := sendWithTimeout(ctx, beaconInterval, beaconSender, msg); err != nil {
					slog.Error("error sending event source beacon", slog.Any("error", err))
					return err
				}
			case <-heartbeatTimer.C:
				heartbeatMessage.Now = uint64(time.Now().UnixMicro())
				msg := heartbeatMessage.Encode()
				if err := sendWithTimeout(ctx, heartbeatInterval, heartbeatSender, msg); err != nil {
					// skupper router will block messages sent multicast
					// without a listener by default. It needs to register a
					// beacon first before heartbeats will pass.
					if errors.Is(err, errSendTimeoutExceeded) && hearbeatTimeouts < 3 {
						hearbeatTimeouts++
						slog.Info("heartbeat message send timed out")
						continue
					}
					slog.Error("error sending event source heartbeat", slog.Any("error", err))
					return err
				}
			}
		}
	}, b)
}

func (m *Manager) listenFlushes(ctx context.Context) {
	flushMessages := listen(ctx, m.factory, m.Source.Direct, 256)
	for {
		select {
		case <-ctx.Done():
			return
		case <-flushMessages:
			select {
			case m.flushQueue <- struct{}{}:
			default: // drop flush if queue is full
			}
		}
	}
}

var errSendTimeoutExceeded = errors.New("send timed out")

func sendWithTimeout(ctx context.Context, timeout time.Duration, sender messaging.Sender, msg *amqp.Message) error {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := sender.Send(ctx, msg, nil)
	if err != nil {
		if errors.Is(err, requestCtx.Err()) && ctx.Err() == nil {
			return errSendTimeoutExceeded
		}
	}
	return err
}

// pulls next N items from a channel
func nextN[T any](ctx context.Context, c <-chan T, n int) []T {
	var out []T
	if n < 1 {
		return out
	}
	for {
		select {
		case <-ctx.Done():
			return out
		case t := <-c:
			out = append(out, t)
			if len(out) == n {
				return out
			}
		}
	}
}

func defaultBackOff(ctx context.Context) backoff.BackOffContext {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = 100 * time.Millisecond
	exp.MaxInterval = 30 * time.Second
	exp.MaxElapsedTime = 0
	return backoff.WithContext(exp, ctx)
}

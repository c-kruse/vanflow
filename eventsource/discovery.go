package eventsource

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/messaging"
)

const beaconAddress = "mc/sfe.all"

// Discovery manages a collection of known event sources
type Discovery struct {
	factory    messaging.SessionFactory
	lock       sync.Mutex
	state      map[string]Info
	discovered chan Info
	forgotten  chan Info
}

type DiscoveryHandlers struct {
	Discovered func(source Info)
	Forgotten  func(source Info)
}

func NewDiscovery(factory messaging.SessionFactory) *Discovery {
	return &Discovery{
		factory:    factory,
		state:      make(map[string]Info),
		discovered: make(chan Info, 32),
		forgotten:  make(chan Info, 32),
	}
}

// Run event source discovery until the context is cancelled
func (d *Discovery) Run(ctx context.Context, handlers DiscoveryHandlers) error {
	beaconMsgs := listen(ctx, d.factory, beaconAddress, 256)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-beaconMsgs:
			if !ok {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				slog.Info("restarting beacon listener")
				beaconMsgs = listen(ctx, d.factory, beaconAddress, 256)
			}
			if *msg.Properties.Subject != "BEACON" {
				slog.Info("received non-beacon from beacon source", slog.Any("subject", msg.Properties.Subject), slog.Any("source", msg.Properties.To))
				continue
			}
			d.observe(vanflow.DecodeBeacon(msg))
		case info := <-d.discovered:
			if handlers.Discovered != nil {
				handlers.Discovered(info)
			}
		case info := <-d.forgotten:
			if handlers.Forgotten != nil {
				handlers.Forgotten(info)
			}
		}
	}
}

// Get an EventSource by ID
func (d *Discovery) Get(id string) (source Info, ok bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	state, ok := d.state[id]
	if !ok {
		return source, false
	}
	return state, true
}

// Forget an EventSource
func (d *Discovery) Forget(id string) bool {
	var (
		state  Info
		forget bool
	)
	d.lock.Lock()
	// unlock then call onForget
	defer func() {
		d.lock.Unlock()
		if forget {
			d.forgotten <- state
		}
	}()
	state, forget = d.state[id]
	delete(d.state, id)
	return forget
}

// List all known EventSources
func (d *Discovery) List() []Info {
	d.lock.Lock()
	defer d.lock.Unlock()
	results := make([]Info, 0, len(d.state))
	for _, state := range d.state {
		results = append(results, state)
	}
	return results
}

type WatchConfig struct {
	// ID of the event source to watch
	ID string
	// Timeout for client activity. When set, timeout is the duration that
	// discovery will wait after the most recent client activity before
	// forgetting the source.
	Timeout time.Duration

	// GracePeriod is the time to wait for client activity before enforcing
	// the Timeout.
	GracePeriod time.Duration
}

// NewWatchClient creates a client for a given event source and uses that client
// to keep the source LastHeard time up to date.
func (d *Discovery) NewWatchClient(ctx context.Context, cfg WatchConfig) (*Client, error) {
	info, ok := d.Get(cfg.ID)
	if !ok {
		return nil, fmt.Errorf("unknown event source %s", cfg.ID)
	}

	c := NewClient(d.factory, info)
	w := newWatch(d, c)
	go w.run(ctx, cfg)
	return c, nil
}

type watch struct {
	lastSeen  atomic.Pointer[time.Time]
	discovery *Discovery
	client    *Client
}

func newWatch(discovery *Discovery, client *Client) *watch {
	w := watch{
		discovery: discovery,
		client:    client,
	}
	client.OnHeartbeat(func(vanflow.HeartbeatMessage) {
		ts := time.Now()
		w.lastSeen.Store(&ts)
	})

	client.OnRecord(func(vanflow.RecordMessage) {
		ts := time.Now()
		w.lastSeen.Store(&ts)
	})
	return &w
}

func (w *watch) run(ctx context.Context, cfg WatchConfig) {
	var (
		watchTimer  *time.Timer
		watchTimerC <-chan time.Time
	)

	if cfg.Timeout > 0 {
		watchTimer = time.NewTimer(cfg.GracePeriod + cfg.Timeout)
		watchTimerC = watchTimer.C
		defer watchTimer.Stop()
	}

	// only update discovery data once per second to keep
	// lock contention reasonable.
	advanceTicker := time.NewTicker(time.Second)
	defer advanceTicker.Stop()
	defer w.client.Close()

	var prevObserved time.Time
	prevObserved, _ = w.advanceLastSeen(prevObserved)
	for {
		select {
		case <-ctx.Done():
			return
		case <-watchTimerC:
			// Watch Timeout has been reached. Check one last time for watch
			// activity before forgetting the event source.
			next, ok := w.advanceLastSeen(prevObserved)
			if !ok {
				w.discovery.Forget(cfg.ID)
				return
			}
			w.discovery.lastSeen(cfg.ID, next)
			watchTimer.Reset(cfg.Timeout)
			prevObserved = next
		case <-advanceTicker.C:
			next, ok := w.advanceLastSeen(prevObserved)
			if !ok {
				continue
			}

			w.discovery.lastSeen(cfg.ID, next)
			if watchTimer != nil {
				if !watchTimer.Stop() {
					<-watchTimer.C
				}
				watchTimer.Reset(cfg.Timeout)
			}
			prevObserved = next
		}
	}
}

func (w *watch) advanceLastSeen(prev time.Time) (time.Time, bool) {
	currentObserved := w.lastSeen.Load()
	if currentObserved == nil || !currentObserved.After(prev) {
		return prev, false
	}
	return *currentObserved, true
}

func (d *Discovery) observe(beacon vanflow.BeaconMessage) {
	var (
		state      Info
		discovered bool
	)
	d.lock.Lock()
	// release lock before calling onDiscover
	defer func() {
		d.lock.Unlock()
		if discovered {
			d.discovered <- state
		}
	}()
	tObserved := time.Now()
	state, ok := d.state[beacon.Identity]
	if !ok {
		state = Info{
			ID:       beacon.Identity,
			Version:  int(beacon.Version),
			Type:     beacon.SourceType,
			Address:  beacon.Address,
			Direct:   beacon.Direct,
			LastSeen: tObserved,
		}
		discovered = true
	}
	state.LastSeen = tObserved
	d.state[beacon.Identity] = state
}

func (d *Discovery) lastSeen(id string, latest time.Time) {
	d.lock.Lock()
	defer d.lock.Unlock()
	state, ok := d.state[id]
	if !ok {
		return
	}
	if latest.After(state.LastSeen) {
		state.LastSeen = latest
		d.state[id] = state
	}
}

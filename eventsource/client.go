package eventsource

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/Azure/go-amqp"
	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/messaging"
	"github.com/cenkalti/backoff/v4"
)

const (
	sourceSuffixFlows      = ".flows"
	sourceSuffixLogs       = ".logs"
	sourceSuffixHeartbeats = ".heartbeats"
)

// Client is responsible for interacting with Event Sources
//
// Allows the caller to register message handler callbacks to react to received
// messages and to control exactly what sources the client listens to.
type Client struct {
	factory     messaging.SessionFactory
	eventSource Info

	lock              sync.Mutex
	cleanup           []func()
	heartbeatHandlers []HeartbeatMessageHandler
	recordHandlers    []RecordMessageHandler
	updateHandlers    chan struct{}
	handlerWorkers    int

	wg         sync.WaitGroup
	records    chan vanflow.RecordMessage
	heartbeats chan vanflow.HeartbeatMessage
}

type ClientConfig struct {
	// Source of vanflow events
	Source Info
}

func NewClient(factory messaging.SessionFactory, cfg ClientConfig) *Client {
	c := &Client{
		factory:        factory,
		eventSource:    cfg.Source,
		updateHandlers: make(chan struct{}, 1),
		records:        make(chan vanflow.RecordMessage),
		heartbeats:     make(chan vanflow.HeartbeatMessage),
		handlerWorkers: 1,
	}
	c.start()
	return c
}

func (c *Client) start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	done := make(chan struct{})
	c.cleanup = append(c.cleanup, func() { close(done) })

	for i := 0; i < c.handlerWorkers; i++ {
		c.wg.Add(1)
		go c.handleMessages(done)
	}
}

func (c *Client) handleMessages(done chan struct{}) {
	defer c.wg.Done()
	var ( // local copies of record message handlers
		recordHandlers     []RecordMessageHandler
		heartbeatHandlers  []HeartbeatMessageHandler
		invalidateHandlers chan struct{} // trigger to reload handlers
	)
	reloadHandlers := func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		recordHandlers = make([]RecordMessageHandler, len(c.recordHandlers))
		heartbeatHandlers = make([]HeartbeatMessageHandler, len(c.heartbeatHandlers))
		copy(recordHandlers, c.recordHandlers)
		copy(heartbeatHandlers, c.heartbeatHandlers)
		invalidateHandlers = c.updateHandlers
	}
	reloadHandlers()

	for {
		select {
		case <-done:
			return
		case <-invalidateHandlers:
			reloadHandlers()
		case message := <-c.records:
			for _, handler := range c.recordHandlers {
				handler(message)
			}
		case message := <-c.heartbeats:
			for _, handler := range c.heartbeatHandlers {
				handler(message)
			}
		}
	}
}

type HeartbeatMessageHandler func(vanflow.HeartbeatMessage)
type RecordMessageHandler func(vanflow.RecordMessage)

// OnHeartbeat registers a callback handler for HeartbeatMessages.
func (c *Client) OnHeartbeat(handler HeartbeatMessageHandler) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.heartbeatHandlers = append(c.heartbeatHandlers, handler)
	c.signalHandlerReload()
}

// OnRecord registers a callback handler for RecordMessages.
func (c *Client) OnRecord(handler RecordMessageHandler) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.recordHandlers = append(c.recordHandlers, handler)
	c.signalHandlerReload()
}

func (c *Client) signalHandlerReload() {
	close(c.updateHandlers)
	c.updateHandlers = make(chan struct{})
}

// Listen instructs the Client to listen to an event source using the specified
// listener configuration until the context is cancelled or client.Close() is
// called.
func (c *Client) Listen(ctx context.Context, attributes ListenerConfigProvider) error {
	c.wg.Add(1)
	listenerCtx, listenerCancel := context.WithCancel(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()
	c.cleanup = append(c.cleanup, listenerCancel)

	go func(ctx context.Context) {
		defer c.wg.Done()
		cfg := attributes.Get(c.eventSource)
		msgs := listen(ctx, c.factory, cfg.Address, uint32(cfg.Credit))
		for {
			select {
			case <-ctx.Done():
				return
			case amqpMsg, ok := <-msgs:
				if !ok {
					return
				}
				decoded, err := vanflow.Decode(amqpMsg)
				if err != nil {
					slog.Error("skipping message that could not be decoded", slog.Any("error", err))
					continue
				}
				switch message := decoded.(type) {
				case vanflow.RecordMessage:
					c.records <- message
				case vanflow.HeartbeatMessage:
					c.heartbeats <- message
				}
			}
		}
	}(listenerCtx)
	return nil
}

// Close stops all listeners
func (c *Client) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, cancel := range c.cleanup {
		cancel()
	}
	c.wg.Wait()
}

// SendFlush sends a FlushMessage to the Event Source
func (c *Client) SendFlush(ctx context.Context) error {
	var flush vanflow.FlushMessage
	flush.To = c.eventSource.Direct
	msg := flush.Encode()
	conn, err := c.factory.Create(ctx)
	if err != nil {
		return fmt.Errorf("could not establish connection: %s", err)
	}

	sender, err := conn.Sender(ctx, c.eventSource.Direct, nil)
	if err != nil {
		return fmt.Errorf("could not start sender: %s", err)
	}
	defer sender.Close(ctx)
	if err := sender.Send(ctx, msg, nil); err != nil {
		return fmt.Errorf("failed to send flush message: %w", err)
	}
	return nil
}

type ListenerConfig struct {
	Address string
	Credit  int
}

type ListenerConfigProvider interface {
	Get(Info) ListenerConfig
}

type addresser func(i Info) string

func (fn addresser) Get(info Info) ListenerConfig {
	return ListenerConfig{Address: fn(info), Credit: 256}
}

func FromSourceAddress() ListenerConfigProvider {
	return addresser(func(i Info) string { return i.Address })
}

func FromSourceAddressLogs() ListenerConfigProvider {
	return addresser(func(i Info) string { return i.Address + sourceSuffixLogs })
}

func FromSourceAddressFlows() ListenerConfigProvider {
	return addresser(func(i Info) string { return i.Address + sourceSuffixFlows })
}

func FromSourceAddressHeartbeats() ListenerConfigProvider {
	return addresser(func(i Info) string { return i.Address + sourceSuffixHeartbeats })
}

func listen(ctx context.Context, factory messaging.SessionFactory, address string, credits uint32) <-chan *amqp.Message {
	msgs := make(chan *amqp.Message)
	go func() {
		defer close(msgs)
		b := backoffRetryForever(ctx)
		backoff.Retry(func() error {
			err := func() error {
				if err := ctx.Err(); err != nil {
					return err
				}
				conn, err := factory.Create(ctx)
				if err != nil {
					return fmt.Errorf("could not establish connection: %w", err)
				}

				recv, err := conn.Receiver(ctx, address, &amqp.ReceiverOptions{Credit: 256})
				if err != nil {
					return fmt.Errorf("could not start receiver: %w", err)
				}
				defer recv.Close(ctx)
				for {
					msg, err := recv.Receive(ctx, nil)
					if err != nil {
						return fmt.Errorf("error receiving beacon message: %w", err)
					}
					msgs <- msg
					err = recv.AcceptMessage(ctx, msg)
					if err != nil {
						return fmt.Errorf("error accepting beacon message: %w", err)
					}
					b.Reset()
				}
			}()
			if err := ctx.Err(); err != nil {
				return nil
			}
			slog.Error("tearing down connection due to error", slog.Any("error", err), slog.String("address", address))
			return err
		}, b)

	}()
	return msgs
}

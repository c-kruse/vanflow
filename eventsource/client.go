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

// Client is responsible for interracting with Event Sources
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

	wg sync.WaitGroup
}

func NewClient(factory messaging.SessionFactory, info Info) *Client {
	c := &Client{
		factory:     factory,
		eventSource: info,
	}
	return c
}

type HeartbeatMessageHandler func(vanflow.HeartbeatMessage)
type RecordMessageHandler func(vanflow.RecordMessage)

// OnHeartbeat registers a callback handler for HeartbeatMessages.
func (c *Client) OnHeartbeat(handler HeartbeatMessageHandler) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.heartbeatHandlers = append(c.heartbeatHandlers, handler)
}

// OnRecord registers a callback handler for RecordMessages.
func (c *Client) OnRecord(handler RecordMessageHandler) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.recordHandlers = append(c.recordHandlers, handler)
}

// Listen instructs the Client to listen to an event source using the specified
// listener attributes until the context is cancelled or client.Close() is
// called.
func (c *Client) Listen(ctx context.Context, attributes ListenerAttributeFactory) error {
	c.wg.Add(1)
	listenerCtx, listenerCancel := context.WithCancel(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()
	c.cleanup = append(c.cleanup, listenerCancel)
	var recordHandlers []RecordMessageHandler
	var heartbeatHandlers []HeartbeatMessageHandler
	recordHandlers = append(recordHandlers, c.recordHandlers...)
	heartbeatHandlers = append(heartbeatHandlers, c.heartbeatHandlers...)

	go func(ctx context.Context) {
		defer c.wg.Done()
		address, credits := attributes.Get(c.eventSource)
		msgs := listen(ctx, c.factory, address, credits)
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
					slog.Error("could not decode message. skipping", slog.Any("error", err))
					continue
				}
				switch message := decoded.(type) {
				case vanflow.RecordMessage:
					for _, handler := range recordHandlers {
						handler(message)
					}
				case vanflow.HeartbeatMessage:
					for _, handler := range heartbeatHandlers {
						handler(message)
					}
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

type ListenerAttributeFactory interface {
	Get(Info) (address string, credit uint32)
}

type addresser func(i Info) string

func (fn addresser) Get(info Info) (string, uint32) {
	return fn(info), 250
}

func FromSourceAddress() ListenerAttributeFactory {
	return addresser(func(i Info) string { return i.Address })
}

func FromSourceAddressLogs() ListenerAttributeFactory {
	return addresser(func(i Info) string { return i.Address + sourceSuffixLogs })
}

func FromSourceAddressFlows() ListenerAttributeFactory {
	return addresser(func(i Info) string { return i.Address + sourceSuffixFlows })
}

func FromSourceAddressHeartbeats() ListenerAttributeFactory {
	return addresser(func(i Info) string { return i.Address + sourceSuffixHeartbeats })
}

func listen(ctx context.Context, factory messaging.SessionFactory, address string, credits uint32) <-chan *amqp.Message {
	msgs := make(chan *amqp.Message, 32)
	go func() {
		defer close(msgs)
		b := defaultBackOff(ctx)
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
					err = recv.AcceptMessage(ctx, msg)
					if err != nil {
						return fmt.Errorf("error accepting beacon message: %w", err)
					}
					b.Reset()
					msgs <- msg
				}
			}()

			slog.Error("tearing down connection due to error", slog.Any("error", err), slog.String("address", address))
			return err
		}, b)

	}()
	return msgs
}

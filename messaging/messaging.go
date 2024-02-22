// package messaging wraps github.com/Azure/go-aqmp with interfaces
// so that a stub may be inserted for testing.
//
// Closely follows skupper's pkg/messaging interface, but chooses to use the
// Azure vcabbage fork instead.
package messaging

import (
	"context"
	"fmt"

	amqp "github.com/Azure/go-amqp"
)

type SessionFactory interface {
	Create(context.Context) (Session, error)
}

type Session interface {
	Sender(ctx context.Context, address string, opts *amqp.SenderOptions) (Sender, error)
	Receiver(ctx context.Context, address string, opts *amqp.ReceiverOptions) (Receiver, error)
	Close(context.Context) error
}

type Sender interface {
	Send(context.Context, *amqp.Message, *amqp.SendOptions) error
	Close(context.Context) error
}

type Receiver interface {
	Receive(context.Context, *amqp.ReceiveOptions) (*amqp.Message, error)
	AcceptMessage(context.Context, *amqp.Message) error
	Close(context.Context) error
}

type Config struct {
	Conn *amqp.ConnOptions
}

var (
	defaultConnOptions = &amqp.ConnOptions{
		MaxFrameSize: 1<<32 - 1,
	}
	defaultReceiverOptions = &amqp.ReceiverOptions{
		Credit: 256,
	}
)

func NewSessionFactory(address string, config Config) SessionFactory {
	if config.Conn == nil {
		config.Conn = defaultConnOptions
	}
	return factory{address: address, Config: config}
}

type factory struct {
	Config  Config
	address string
}

func (f factory) Create(ctx context.Context) (Session, error) {
	conn, err := amqp.Dial(ctx, f.address, f.Config.Conn)
	if err != nil {
		return nil, fmt.Errorf("dial error for %s: %s", f.address, err)
	}
	s, err := conn.NewSession(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("session create error for %s: %s", f.address, err)
	}
	return &session{
		conn:    conn,
		session: s,
		cfg:     f.Config,
	}, nil
}

type session struct {
	conn    *amqp.Conn
	session *amqp.Session
	cfg     Config
}

func (c session) Close(ctx context.Context) error {
	if err := c.session.Close(ctx); err != nil {
		return err
	}
	return c.conn.Close()
}

func (c *session) Sender(ctx context.Context, address string, opts *amqp.SenderOptions) (Sender, error) {
	return c.session.NewSender(ctx, address, opts)
}

func (c *session) Receiver(ctx context.Context, address string, opts *amqp.ReceiverOptions) (Receiver, error) {
	if opts == nil {
		opts = defaultReceiverOptions
	}
	return c.session.NewReceiver(ctx, address, opts)
}

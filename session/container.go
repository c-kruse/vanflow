package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/cenkalti/backoff/v4"
)

type ReceiverOptions struct {
	Credit int
}

func (o ReceiverOptions) get() amqp.ReceiverOptions {
	var result amqp.ReceiverOptions
	if o.Credit <= 0 {
		result.Credit = int32(o.Credit)
	}
	return result
}

type SenderOptions struct {
	SendPresettled bool
}

func (o SenderOptions) get() amqp.SenderOptions {
	var result amqp.SenderOptions
	if o.SendPresettled {
		result.SettlementMode = amqp.SenderSettleModeSettled.Ptr()
	}
	return result
}

type Container interface {
	Start(context.Context)
	NewReceiver(address string, opts ReceiverOptions) Receiver
	NewSender(address string, opts SenderOptions) Sender
}

type Receiver interface {
	Next(context.Context) (*amqp.Message, error)
	Accept(context.Context, *amqp.Message) error
	Close(context.Context) error
}

type Sender interface {
	Send(context.Context, *amqp.Message) error
	Close(context.Context) error
}

type ContainerConfig struct {
	Conn *amqp.ConnOptions
	// BackOff strategy to use when reestablishing a connection defaults to an
	// exponential backoff capped at 30 second intervals with no set retry
	// limit.
	BackOff backoff.BackOff
}

func NewContainer(address string, config ContainerConfig) Container {
	c := &container{
		address:       address,
		config:        config,
		hasNext:       make(chan struct{}),
		sessionErrors: make(chan sessionErr, 32),
		notifyOK:      make(chan int, 32),
	}
	return c
}

type container struct {
	address string
	config  ContainerConfig

	mu      sync.Mutex
	sess    *amqp.Session
	gen     int
	hasNext chan struct{}

	sessionErrors chan sessionErr
	notifyOK      chan int
}

func (c *container) awaitNextSession(ctx context.Context, prev int) (session *amqp.Session, gen int, err error) {
	c.mu.Lock()
	session, gen, hasNext := c.sess, c.gen, c.hasNext
	c.mu.Unlock()
	if prev != gen {
		return session, gen, nil
	}
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-hasNext:
		return c.awaitNextSession(ctx, prev)
	}
}

type sessionErr struct {
	Generation int
	Err        error
}

func (e sessionErr) Error() string {
	return e.Err.Error()
}

// Start the container. It will run until the context is cancelled or until the
// backoff strategy chosen finishes.
func (c *container) Start(ctx context.Context) {
	if c.config.BackOff == nil {
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = time.Millisecond * 250
		b.MaxInterval = time.Second * 30
		b.MaxElapsedTime = 0
		b.Reset()
		c.config.BackOff = b
	}
	go func() {
		var generation int
		var prevSessionTeardown func() = func() {}
		b := backoff.WithContext(c.config.BackOff, ctx)
		err := backoff.RetryNotify(
			func() error {
				conn, err := amqp.Dial(ctx, c.address, c.config.Conn)
				if err != nil {
					return fmt.Errorf("dial error: %s", err)
				}
				sess, err := conn.NewSession(ctx, nil)
				if err != nil {
					return fmt.Errorf("session create error: %s", err)
				}
				generation++

				c.mu.Lock()
				close(c.hasNext)
				c.sess, c.gen, c.hasNext = sess, generation, make(chan struct{})
				c.mu.Unlock()

				prevSessionTeardown()
				prevSessionTeardown = func() {
					sess.Close(ctx)
					conn.Close()
				}

				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case gen := <-c.notifyOK:
						if gen == generation {
							b.Reset()
						}
					case recvErr := <-c.sessionErrors:
						if recvErr.Generation == generation {
							return fmt.Errorf("session receiver error: %s", recvErr)
						}
					}
				}
			},
			b,
			func(err error, d time.Duration) {
				slog.Error("session error triggered restart", slog.String("delay", d.String()), slog.Any("error", err))
			},
		)
		defer prevSessionTeardown()
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return
			}
			slog.Error("container stopping", slog.Any("error", err))
		}
	}()
}

func (s *container) NewReceiver(address string, opts ReceiverOptions) Receiver {
	return s.newLink(address, opts, SenderOptions{})
}

func (s *container) NewSender(address string, opts SenderOptions) Sender {
	return s.newLink(address, ReceiverOptions{}, opts)
}

func (c *container) newLink(address string, r ReceiverOptions, s SenderOptions) *link {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := &link{
		address:       address,
		container:     c,
		receiverOpts:  r.get(),
		senderOpts:    s.get(),
		sessionErrors: c.sessionErrors,
		reportOK:      c.notifyOK,
		curr:          c.sess,
		currGen:       c.gen,
	}
	return l
}

type link struct {
	address      string
	receiverOpts amqp.ReceiverOptions
	senderOpts   amqp.SenderOptions

	sessionErrors chan<- sessionErr
	reportOK      chan<- int

	container *container

	mu      sync.Mutex
	closed  bool
	currGen int
	curr    *amqp.Session
	rcvGen  int
	rcv     *amqp.Receiver
	sndGen  int
	snd     *amqp.Sender
}

var errLinkClosed = errors.New("link closed")

func (r *link) awaitSession(ctx context.Context) error {
	r.mu.Lock()
	curr, currGen, closed := r.curr, r.currGen, r.closed
	r.mu.Unlock()
	if closed {
		return errLinkClosed
	}
	if curr != nil {
		return nil
	}
	next, nextGen, err := r.container.awaitNextSession(ctx, currGen)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.curr, r.currGen = next, nextGen
	r.mu.Unlock()
	return nil
}

func (r *link) withReceiver(ctx context.Context, fn func(receiver *amqp.Receiver, generation int) error) error {
	if err := r.awaitSession(ctx); err != nil {
		return r.handleError(ctx, fmt.Errorf("session await error: %w", err))
	}
	rcv, currGen, err := r.getReceiver(ctx)
	if err != nil {
		return r.handleError(ctx, fmt.Errorf("receiver create error: %w", err))
	}
	if err := fn(rcv, currGen); err != nil {
		return r.handleError(ctx, err)
	}
	return nil
}

func (r *link) withSender(ctx context.Context, fn func(sender *amqp.Sender, generation int) error) error {
	if err := r.awaitSession(ctx); err != nil {
		return r.handleError(ctx, fmt.Errorf("session await error: %w", err))
	}
	snd, currGen, err := r.getSender(ctx)
	if err != nil {
		return r.handleError(ctx, fmt.Errorf("sender create error: %w", err))
	}
	if err := fn(snd, currGen); err != nil {
		return r.handleError(ctx, err)
	}
	return nil
}

func (r *link) handleError(ctx context.Context, err error) error {
	if errors.Is(err, ctx.Err()) {
		return err
	}
	if errors.Is(err, errLinkClosed) {
		return err
	}
	r.sessionErrors <- sessionErr{
		Generation: r.currGen,
		Err:        err,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.curr, r.rcv, r.snd = nil, nil, nil
	return err
}

func (r *link) getReceiver(ctx context.Context) (*amqp.Receiver, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rcv != nil && r.rcvGen == r.currGen {
		return r.rcv, r.rcvGen, nil
	}
	rcv, err := r.curr.NewReceiver(ctx, r.address, &amqp.ReceiverOptions{Credit: int32(r.receiverOpts.Credit)})
	if err != nil {
		return nil, 0, err
	}
	r.rcv, r.rcvGen = rcv, r.currGen
	return r.rcv, r.rcvGen, nil
}

func (r *link) getSender(ctx context.Context) (*amqp.Sender, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snd != nil && r.sndGen == r.currGen {
		return r.snd, r.sndGen, nil
	}
	snd, err := r.curr.NewSender(ctx, r.address, &r.senderOpts)
	if err != nil {
		return nil, 0, err
	}
	r.snd, r.sndGen = snd, r.currGen
	return r.snd, r.sndGen, nil
}

func (r *link) Next(ctx context.Context) (*amqp.Message, error) {
	var (
		result *amqp.Message
	)
	err := r.withReceiver(ctx, func(rcv *amqp.Receiver, _ int) error {
		msg, err := rcv.Receive(ctx, nil)
		if err != nil {
			return fmt.Errorf("receive error: %w", err)
		}
		result = msg
		return nil
	})
	if err != nil && ctx.Err() == nil {
		return r.Next(ctx)
	}
	return result, err
}

func (r *link) Accept(ctx context.Context, msg *amqp.Message) error {
	var (
		acceptErr  error
		generation int
	)
	err := r.withReceiver(ctx, func(rcv *amqp.Receiver, gen int) error {
		generation = gen
		acceptErr = rcv.AcceptMessage(ctx, msg)
		// accepting messages is a stateful operation, so don't report the error back
		// to the container and signal a connection teardown
		return nil
	})
	if acceptErr != nil {
		return acceptErr
	}
	if err == nil {
		select {
		case r.reportOK <- generation:
		default:
		}
	}
	return err
}

func (r *link) Send(ctx context.Context, msg *amqp.Message) error {
	var generation int
	err := r.withSender(ctx, func(snd *amqp.Sender, gen int) error {
		generation = gen
		err := snd.Send(ctx, msg, nil)
		if err != nil {
			return fmt.Errorf("send error: %w", err)
		}
		return nil
	})
	if err == nil {
		select {
		case r.reportOK <- generation:
		default:
		}
	}
	return err
}

func (r *link) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	rcv := r.rcv
	r.curr, r.rcv, r.snd = nil, nil, nil
	if rcv != nil {
		return rcv.Close(ctx)
	}
	return nil
}

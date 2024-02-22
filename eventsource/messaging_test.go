package eventsource

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/c-kruse/vanflow/messaging"
)

type MockConnectionFactory struct {
	URL    string
	Broker *MockBroker
}

// NewMockConnectionFactory creates an in-memory message brokering stub.
// Messages will be sent multicast and are buffered for all unclosed receivers.
func NewMockConnectionFactory(t *testing.T, url string) *MockConnectionFactory {
	return &MockConnectionFactory{
		URL:    url,
		Broker: NewBroker(),
	}
}

func (f *MockConnectionFactory) Create(context.Context) (messaging.Session, error) {
	c := &mockConnection{broker: f.Broker, done: make(chan struct{})}
	return c, nil
}

func (f *MockConnectionFactory) Url() string {
	return f.URL
}

type MockBroker struct {
	mu     sync.Mutex
	topics map[string]*multicast
}

func NewBroker() *MockBroker {
	return &MockBroker{
		topics: map[string]*multicast{},
	}
}
func (b *MockBroker) AwaitReceivers(address string, n int) {
	topic := b.get(address)
	ct := func() int {
		topic.mu.Lock()
		defer topic.mu.Unlock()
		return len(topic.receivers)
	}
	for n > ct() {
		time.Sleep(time.Millisecond * 5)
	}
}

func (b *MockBroker) subscribe(address string, r *mockReceiver) {
	b.get(address).subscribe(r)
}

func (b *MockBroker) send(address string, msg *amqp.Message) {
	b.get(address).send(msg)
}

func (b *MockBroker) get(address string) *multicast {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[address]
	if !ok {
		t = newMulticast()
		b.topics[address] = t
	}
	return t
}

type multicast struct {
	mu        sync.Mutex
	receivers []*mockReceiver
}

func newMulticast() *multicast {
	return &multicast{}
}

func (m *multicast) send(msg *amqp.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.receivers {
		r.send(msg)
	}
}

func (m *multicast) subscribe(r *mockReceiver) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receivers = append(m.receivers, r)
}

type mockConnection struct {
	broker    *MockBroker
	closeOnce sync.Once
	done      chan struct{}
}

func (c *mockConnection) Sender(_ context.Context, address string, _ *amqp.SenderOptions) (messaging.Sender, error) {
	return &mockSender{address: address, connection: c, done: make(chan struct{})}, nil
}

func (c *mockConnection) Receiver(_ context.Context, address string, opts *amqp.ReceiverOptions) (messaging.Receiver, error) {
	if opts == nil {
		opts = &amqp.ReceiverOptions{Credit: 256}
	}
	r := &mockReceiver{connection: c, channel: make(chan *amqp.Message, opts.Credit), done: make(chan struct{})}
	c.broker.subscribe(address, r)
	return r, nil
}

func (c *mockConnection) Close(context.Context) error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func (c *mockConnection) send(address string, msg *amqp.Message) error {
	select {
	case <-c.done:
		return fmt.Errorf("channel closed")
	default:
		c.broker.send(address, msg)
	}
	return nil
}

func (c *mockConnection) receive(r *mockReceiver) (*amqp.Message, error) {
	select {
	case msg := <-r.channel:
		if msg == nil {
			return nil, fmt.Errorf("Failed to receive")
		}
		return msg, nil
	case <-c.done:
		return nil, fmt.Errorf("connection closed")
	}
}

type mockSender struct {
	connection *mockConnection
	address    string
	closeOnce  sync.Once
	done       chan struct{}
}

func (s *mockSender) Send(_ context.Context, msg *amqp.Message, _ *amqp.SendOptions) error {
	select {
	case <-s.done:
		return fmt.Errorf("sender closed")
	default:
		return s.connection.send(s.address, msg)
	}
}

func (s *mockSender) Close(context.Context) error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

type mockReceiver struct {
	connection *mockConnection
	channel    chan *amqp.Message
	closeOnce  sync.Once
	done       chan struct{}
}

func (r *mockReceiver) send(msg *amqp.Message) {
	select {
	case <-r.done:
		return
	default:
		r.channel <- msg
	}
}

func (r *mockReceiver) AcceptMessage(context.Context, *amqp.Message) error {
	return nil
}

func (r *mockReceiver) Receive(context.Context, *amqp.ReceiveOptions) (*amqp.Message, error) {
	return r.connection.receive(r)
}

func (r *mockReceiver) Close(context.Context) error {
	r.closeOnce.Do(func() { close(r.done) })
	return nil
}

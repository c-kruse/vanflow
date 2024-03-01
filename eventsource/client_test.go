package eventsource

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/messaging"
	"gotest.tools/assert"
)

func TestClient(t *testing.T) {
	t.Parallel()
	tstCtx, tstCancel := context.WithCancel(context.Background())
	defer tstCancel()
	factory := NewMockConnectionFactory(t, "mockamqp://local")

	client := NewClient(factory, ClientConfig{Source: Info{
		ID:      "test",
		Address: "mc/sfe.test",
	}})
	heartbeats := make(chan vanflow.HeartbeatMessage, 8)
	records := make(chan vanflow.RecordMessage, 8)
	client.OnHeartbeat(func(m vanflow.HeartbeatMessage) { heartbeats <- m })
	client.OnRecord(func(m vanflow.RecordMessage) { records <- m })

	tstConn, _ := factory.Create(tstCtx)
	sender, _ := tstConn.Sender(tstCtx, "mc/sfe.test", nil)
	assert.Check(t, client.Listen(tstCtx, FromSourceAddress()))
	factory.Broker.AwaitReceivers("mc/sfe.test", 1)

	heartbeat := vanflow.HeartbeatMessage{
		Identity: "test", Version: 1, Now: 22,
		MessageProps: vanflow.MessageProps{
			To:      "mc/sfe.test",
			Subject: "HEARTBEAT",
		},
	}
	for i := 0; i < 10; i++ {
		sender.Send(tstCtx, heartbeat.Encode(), nil)
		actual := <-heartbeats
		assert.DeepEqual(t, actual, heartbeat)
		heartbeat.Now++
	}
	record := vanflow.RecordMessage{
		MessageProps: vanflow.MessageProps{
			To:      "mc/sfe.test",
			Subject: "RECORD",
		},
	}
	for i := 0; i < 10; i++ {
		msg, err := record.Encode()
		assert.Check(t, err)
		sender.Send(tstCtx, msg, nil)
		actual := <-records
		assert.DeepEqual(t, actual, record)
		name := fmt.Sprintf("router-%d", i)
		record.Records = append(record.Records, &vanflow.RouterRecord{BaseRecord: vanflow.BaseRecord{ID: name}})
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		client.Close()
	}()
	select {
	case <-closed: //okay
	case <-time.After(500 * time.Millisecond):
		t.Error("expected client.Close() to promptly return")
	}

	msg, err := record.Encode()
	assert.Check(t, err)
	sender.Send(tstCtx, msg, nil)
	select {
	case <-time.After(100 * time.Millisecond): //okay
	case <-records:
		t.Error("expected client to stop handling records after close called")
	}

}

func TestClientFlush(t *testing.T) {
	factory := NewMockConnectionFactory(t, "mockamqp://local")

	testCases := []struct {
		ClientName string
		When       func(t *testing.T, ctx context.Context, client *Client)
		Expect     func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message)
	}{
		{
			ClientName: "flush",
			When: func(t *testing.T, ctx context.Context, client *Client) {
				assert.Check(t, client.SendFlush(ctx))
			},
			Expect: func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message) {
				select {
				case <-time.After(time.Millisecond * 500):
					t.Errorf("expected flush message")
				case msg := <-flushMsg:
					assert.Equal(t, *msg.Properties.Subject, "FLUSH")
				}
			},
		}, {
			ClientName: "noop",
			When: func(t *testing.T, ctx context.Context, client *Client) {
			},
			Expect: func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message) {
				select {
				case <-time.After(time.Millisecond * 200):
				case msg := <-flushMsg:
					t.Errorf("unexpected flush message: %v", msg)
				}
			},
		}, {
			ClientName: "flush-on-first-message",
			When: func(t *testing.T, ctx context.Context, client *Client) {
				tstConn, _ := factory.Create(ctx)
				sender, _ := tstConn.Sender(ctx, "mc/sfe.flush-on-first-message", nil)
				go sendHeartbeatMessagesTo(t, ctx, sender)
				assert.Check(t, client.Listen(ctx, FromSourceAddress()))
				assert.Check(t, FlushOnFirstMessage(ctx, client))
			},
			Expect: func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message) {
				select {
				case <-time.After(time.Millisecond * 100):
					t.Errorf("expected flush message")
				case msg := <-flushMsg:
					assert.Equal(t, *msg.Properties.Subject, "FLUSH")
				}
			},
		}, {
			ClientName: "flush-on-first-message-timeout",
			When: func(t *testing.T, ctx context.Context, client *Client) {
				flushCtx, cancel := context.WithTimeout(ctx, time.Millisecond*10)
				defer cancel()
				err := FlushOnFirstMessage(flushCtx, client)
				assert.Assert(t, err != nil)
			},
			Expect: func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message) {
				select {
				case <-time.After(time.Millisecond * 200):
				case msg := <-flushMsg:
					t.Errorf("unexpected flush message: %v", msg)
				}
			},
		},
	}
	for _, _tc := range testCases {
		tc := _tc
		t.Run(tc.ClientName, func(t *testing.T) {
			t.Parallel()
			tstCtx, tstCancel := context.WithCancel(context.Background())
			defer tstCancel()
			client := NewClient(factory, ClientConfig{Source: Info{
				ID:      tc.ClientName,
				Address: "mc/sfe." + tc.ClientName,
				Direct:  "sfe." + tc.ClientName,
			}})
			tstConn, _ := factory.Create(tstCtx)
			receiver, _ := tstConn.Receiver(tstCtx, "sfe."+tc.ClientName, &amqp.ReceiverOptions{Credit: 1})

			flush := make(chan *amqp.Message)
			go func() {
				for {
					msg, err := receiver.Receive(tstCtx, nil)
					assert.Check(t, err)
					flush <- msg
				}
			}()
			tc.When(t, tstCtx, client)
			tc.Expect(t, tstCtx, flush)
		})
	}

}

func sendHeartbeatMessagesTo(t *testing.T, ctx context.Context, sender messaging.Sender) {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
			assert.Check(t, sender.Send(ctx, vanflow.HeartbeatMessage{}.Encode(), nil))
		}
	}
}

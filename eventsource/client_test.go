package eventsource

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/session"
	"gotest.tools/assert"
)

func TestClient(t *testing.T) {
	t.Parallel()
	tstCtx, tstCancel := context.WithCancel(context.Background())
	defer tstCancel()
	factory := requireContainers(t)
	ctr, tstCtr := factory.Create(), factory.Create()
	ctr.Start(tstCtx)
	tstCtr.Start(tstCtx)

	clientID := uniqueSuffix("test")
	client := NewClient(ctr, ClientOptions{Source: Info{
		ID:      clientID,
		Address: mcsfe(clientID),
	}})
	heartbeats := make(chan vanflow.HeartbeatMessage, 8)
	records := make(chan vanflow.RecordMessage, 8)
	client.OnHeartbeat(func(m vanflow.HeartbeatMessage) { heartbeats <- m })
	client.OnRecord(func(m vanflow.RecordMessage) { records <- m })

	sender := tstCtr.NewSender(mcsfe(clientID), session.SenderOptions{})
	assert.Check(t, client.Listen(tstCtx, FromSourceAddress()))

	heartbeat := vanflow.HeartbeatMessage{
		Identity: clientID, Version: 1, Now: 22,
		MessageProps: vanflow.MessageProps{
			To:      mcsfe(clientID),
			Subject: "HEARTBEAT",
		},
	}
	initRetryTimer := time.After(250 * time.Millisecond)
	for i := 0; i < 10; i++ {
		sender.Send(tstCtx, heartbeat.Encode())
		select {
		case actual := <-heartbeats:
			initRetryTimer = nil
			assert.DeepEqual(t, actual, heartbeat)
		case <-initRetryTimer:
			t.Log("retrying heartbeat")
			initRetryTimer = nil
		}
		heartbeat.Now++
	}
	record := vanflow.RecordMessage{
		MessageProps: vanflow.MessageProps{
			To:      mcsfe(clientID),
			Subject: "RECORD",
		},
	}
	for i := 0; i < 10; i++ {
		msg, err := record.Encode()
		assert.Check(t, err)
		sender.Send(tstCtx, msg)
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
	sender.Send(tstCtx, msg)
	select {
	case <-time.After(100 * time.Millisecond): //okay
	case <-records:
		t.Error("expected client to stop handling records after close called")
	}

}

func TestClientFlush(t *testing.T) {
	factory := requireContainers(t)
	ctr, tstCtr := factory.Create(), factory.Create()
	ctr.Start(context.Background())
	tstCtr.Start(context.Background())

	testSuffix := uniqueSuffix("")
	testCases := []struct {
		ClientName string
		When       func(t *testing.T, ctx context.Context, client *Client)
		Expect     func(t *testing.T, ctx context.Context, flushMsg <-chan *amqp.Message)
	}{
		{
			ClientName: "flush" + testSuffix,
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
			ClientName: "flush-on-first-message" + testSuffix,
			When: func(t *testing.T, ctx context.Context, client *Client) {
				sender := tstCtr.NewSender(mcsfe("flush-on-first-message")+testSuffix, session.SenderOptions{})
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
			ClientName: "flush-on-first-message-timeout" + testSuffix,
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
			client := NewClient(ctr, ClientOptions{Source: Info{
				ID:      tc.ClientName,
				Address: mcsfe(tc.ClientName),
				Direct:  sfe(tc.ClientName),
			}})
			receiver := tstCtr.NewReceiver(sfe(tc.ClientName), session.ReceiverOptions{})

			flush := make(chan *amqp.Message)
			go func() {
				for {
					msg, err := receiver.Next(tstCtx)
					if tstCtx.Err() != nil {
						return
					}
					assert.Check(t, err)
					receiver.Accept(tstCtx, msg)
					flush <- msg
				}
			}()
			tc.When(t, tstCtx, client)
			tc.Expect(t, tstCtx, flush)
		})
	}

}

func sendHeartbeatMessagesTo(t *testing.T, ctx context.Context, sender session.Sender) {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
			err := sender.Send(ctx, vanflow.HeartbeatMessage{}.Encode())
			if ctx.Err() != nil {
				return
			}
			assert.Check(t, err)
		}
	}
}

func requireContainers(t *testing.T) session.ContainerFactory {
	t.Helper()
	var (
		factory   session.ContainerFactory
		errNotSet = errors.New("QDR_ENDPOINT environment variable not present")
	)
	err := func() error {
		qdr := os.Getenv("QDR_ENDPOINT")
		if qdr == "" {
			return errNotSet
		}
		setupCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		setupCtr := session.NewContainer(qdr, session.ContainerConfig{})
		setupCtr.Start(setupCtx)
		pingAddr := uniqueSuffix("ping")
		ping := setupCtr.NewSender(pingAddr, session.SenderOptions{})
		pong := setupCtr.NewReceiver(pingAddr, session.ReceiverOptions{})

		sendDone := make(chan error)
		go func() {
			defer close(sendDone)
			sendDone <- ping.Send(setupCtx, amqp.NewMessage([]byte("PING")))
		}()
		msg, err := pong.Next(setupCtx)
		if err != nil {
			return fmt.Errorf("qdr receive failed: %s: %s", qdr, err)
		}
		pong.Accept(setupCtx, msg)
		if err := <-sendDone; err != nil {
			return fmt.Errorf("qdr send failed: %s: %s", qdr, err)
		}
		ping.Close(setupCtx)
		pong.Close(setupCtx)

		factory = session.NewContainerFactory(qdr,
			session.ContainerConfig{
				ContainerID: uniqueSuffix("eventsourcetest"),
			})
		return nil
	}()
	if err != nil {
		if testing.Short() || err == errNotSet {
			return session.NewMockContainerFactory()
		}
		t.Fatalf("failed to setup tests: %v", err)
	}

	return factory
}

func uniqueSuffix(prefix string) string {
	var salt [8]byte
	io.ReadFull(rand.Reader, salt[:])
	out := bytes.NewBuffer([]byte(prefix))
	out.WriteByte('-')
	hex.NewEncoder(out).Write(salt[:])
	return out.String()
}

func mcsfe(id string) string {
	return fmt.Sprintf("mc/sfe.%s", id)
}
func sfe(id string) string {
	return fmt.Sprintf("sfe.%s", id)
}

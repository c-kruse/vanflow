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
	factory := messaging.NewMockConnectionFactory(t, "mockamqp://local")

	client := NewClient(factory, Info{
		ID:      "test",
		Address: "mc/sfe.test",
	})
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
	t.Parallel()
	tstCtx, tstCancel := context.WithCancel(context.Background())
	defer tstCancel()
	factory := messaging.NewMockConnectionFactory(t, "mockamqp://local")

	client := NewClient(factory, Info{
		ID:      "test",
		Address: "mc/sfe.test",
		Direct:  "sfe.test",
	})
	tstConn, _ := factory.Create(tstCtx)
	receiver, _ := tstConn.Receiver(tstCtx, "sfe.test", &amqp.ReceiverOptions{Credit: 64})

	flush := make(chan *amqp.Message, 1)
	go func() {
		msg, err := receiver.Receive(tstCtx, nil)
		assert.Check(t, err)
		flush <- msg
	}()

	assert.Check(t, client.SendFlush(tstCtx))

	select {
	case <-time.After(time.Millisecond * 500):
		t.Errorf("expected flush message")
	case msg := <-flush:
		assert.Equal(t, *msg.Properties.Subject, "FLUSH")
	}

}

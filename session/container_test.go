package session

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"gotest.tools/assert"
)

func TestMockContainerPing(t *testing.T) {
	channel := uniqueSuffix("ex")

	ctx := context.Background()

	rtr := NewMockRouter()
	cs := NewMockContainer(rtr)
	cr := NewMockContainer(rtr)
	cs.Start(ctx)
	cr.Start(ctx)
	s := cs.NewSender(channel, SenderOptions{})
	r := cr.NewReceiver(channel, ReceiverOptions{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 64; i++ {
			func() {
				sctx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				err := s.Send(sctx, amqp.NewMessage(
					[]byte(fmt.Sprintf("ping-%d", i))),
				)
				if err != nil {
					t.Error(err)
				}
			}()
		}
	}()

	for i := 0; i < 64; i++ {
		msg, err := r.Next(ctx)
		if err != nil {
			t.Error(err)
		}
		if err := r.Accept(ctx, msg); err != nil {
			t.Error(err)
		}
	}
	select {
	case <-time.After(time.Second * 2):
		t.Fatal("send timed out")
	case <-done: // pass
	}

	assert.Check(t, s.Close(ctx))
	err := s.Send(ctx, amqp.NewMessage([]byte("closed")))
	assert.ErrorContains(t, err, "closed")
}
func TestContainerPing(t *testing.T) {
	testCases := []struct {
		Name       string
		Containers func(t *testing.T) (a, b Container)
	}{
		{
			Name: "mock",
			Containers: func(t *testing.T) (a, b Container) {
				rtr := NewMockRouter()
				return NewMockContainer(rtr), NewMockContainer(rtr)
			},
		}, {
			Name:       "qdr",
			Containers: containersFromEnv,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			cs, cr := tc.Containers(t)

			channel := uniqueSuffix("ex")
			ctx := context.Background()
			cs.Start(ctx)
			cr.Start(ctx)
			s := cs.NewSender(channel, SenderOptions{})
			r := cr.NewReceiver(channel, ReceiverOptions{})

			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < 64; i++ {
					func() {
						sctx, cancel := context.WithTimeout(ctx, time.Second)
						defer cancel()
						err := s.Send(sctx, amqp.NewMessage(
							[]byte(fmt.Sprintf("ping-%d", i))),
						)
						if err != nil {
							t.Error(err)
						}
					}()
				}
			}()

			for i := 0; i < 64; i++ {
				msg, err := r.Next(ctx)
				if err != nil {
					t.Error(err)
				}
				if err := r.Accept(ctx, msg); err != nil {
					t.Error(err)
				}
			}
			select {
			case <-time.After(time.Second * 2):
				t.Fatal("send timed out")
			case <-done: // pass
			}

			assert.Check(t, s.Close(ctx))
			err := s.Send(ctx, amqp.NewMessage([]byte("closed")))
			assert.ErrorContains(t, err, "closed")
		})
	}

}

func containersFromEnv(t *testing.T) (app, test Container) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping test that requires external router")
	}
	err := func() error {
		qdr := os.Getenv("QDR_ENDPOINT")
		if qdr == "" {
			return fmt.Errorf("QDR_ENDPOINT environment variable not present")
		}
		setupCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		conn, err := amqp.Dial(setupCtx, qdr, nil)
		if err != nil {
			return fmt.Errorf("could not establish connection to router: %v", err)
		}
		conn.Close()
		app = NewContainer(qdr, ContainerConfig{})
		test = NewContainer(qdr, ContainerConfig{})
		return nil
	}()
	if err != nil {
		t.Fatalf("failed to setup tests: %v", err)
	}

	return app, test
}

func uniqueSuffix(prefix string) string {
	var salt [8]byte
	io.ReadFull(rand.Reader, salt[:])
	out := bytes.NewBuffer([]byte(prefix))
	out.WriteByte('-')
	hex.NewEncoder(out).Write(salt[:])
	return out.String()
}

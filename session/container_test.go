package session

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"gotest.tools/assert"
)

func TestContainerPing(t *testing.T) {
	testCases := []struct {
		Name    string
		Factory func(t *testing.T) ContainerFactory
	}{
		{
			Name: "mock",
			Factory: func(t *testing.T) ContainerFactory {
				return NewMockContainerFactory()
			},
		}, {
			Name: "qdr",
			Factory: func(t *testing.T) ContainerFactory {
				return containersFromEnv(t)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			factory := tc.Factory(t)
			cs := factory.Create()
			cr := factory.Create()

			channel := "ex/" + randomID()
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

func containersFromEnv(t *testing.T) ContainerFactory {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping test that requires external router")
	}
	var factory ContainerFactory
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
		factory = NewContainerFactory(qdr, ContainerConfig{
			ContainerID: "tc/" + randomID(),
		})
		return nil
	}()
	if err != nil {
		t.Fatalf("failed to setup tests: %v", err)
	}

	return factory
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/skupperproject/skupper/pkg/vanflow"
	"github.com/skupperproject/skupper/pkg/vanflow/session"
	"golang.org/x/time/rate"
)

var (
	address   string
	container string

	limit int
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprint(flag.CommandLine.Output(), "Adds amqp listeners to participate in the vanflow protocol. Is not particularily quick about it.\n")
		flag.PrintDefaults()
	}
	flag.StringVar(&address, "router-address", "amqp://127.0.0.1:5672", "AMQP endpoint")
	flag.StringVar(&container, "router-container-id", "slowboi", "amqp container id")

	flag.IntVar(&limit, "limit", 60, "Rate limit. AMQP Messages accepted per 60 seconds")
	flag.Parse()

	var (
		limiter *rate.Limiter = rate.NewLimiter(rate.Inf, 1)
	)

	if limit > 0 {
		mps := time.Minute / time.Duration(limit)
		limiter = rate.NewLimiter(rate.Every(mps), 1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	connectionFactory := session.NewContainerFactory(address,
		session.ContainerConfig{
			ContainerID: container,
			BackOff:     backoff.NewConstantBackOff(time.Millisecond * 500),
		})
	ctr := connectionFactory.Create()
	ctr.OnSessionError(func(err error) {
		log.Printf("amqp session error: %v", err)
	})
	ctr.Start(ctx)

	var wg sync.WaitGroup

	listenAddress := func(address string) {
		defer wg.Done()
		messages := ctr.NewReceiver(address, session.ReceiverOptions{Credit: 10})
		log.Printf("Started listening for vanflow messages: %q", address)
		for {
			if ctx.Err() != nil {
				return
			}
			msg, err := messages.Next(ctx)
			if err != nil {
				log.Printf("vanflow source %q receive error: %s", address, err)
				continue
			}
			limiter.Wait(ctx)
			messages.Accept(ctx, msg)
			vm, err := vanflow.Decode(msg)
			log.Printf("Message %T: address: %s body: %v", vm, address, vm)

		}

	}
	beacons := ctr.NewReceiver("mc/sfe.all", session.ReceiverOptions{Credit: 10})
	wg.Add(1)
	go func() {
		defer wg.Done()
		addresses := make(map[string]struct{})
		log.Println("Started listening for beacon messages")
		for {
			if ctx.Err() != nil {
				return
			}
			msg, err := beacons.Next(ctx)
			if err != nil {
				log.Printf("Beacon receive error: %s", err)
				continue
			}
			limiter.Wait(ctx)
			beacons.Accept(ctx, msg)
			b := vanflow.DecodeBeacon(msg)
			log.Printf("BEACON: %v %s", b.Address, b.SourceType)
			if _, ok := addresses[b.Address]; !ok {
				addresses[b.Address] = struct{}{}
				wg.Add(1)
				go listenAddress(b.Address)
				if b.SourceType == "ROUTER" {
					wg.Add(1)
					go listenAddress(b.Address + ".flows")
				}
			}
		}
	}()
	wg.Wait()
}

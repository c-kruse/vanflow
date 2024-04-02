package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/session"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-cmp/cmp"
)

var (
	flags         *flag.FlagSet = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	SourceAmqp    string
	CollectorAmqp string
	Debug         bool
)

func init() {
	flags.StringVar(&SourceAmqp, "source-server", "amqp://127.0.0.1:5672", "AMQP server for the event source")
	flags.StringVar(&CollectorAmqp, "collector-server", "amqp://127.0.0.1:5672", "AMQP server for the collector")
	flags.BoolVar(&Debug, "debug", false, "print all messages observed")
	flags.Parse(os.Args[1:])
}

func main() {
	if err := run(); err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	sourceFactory := session.NewContainerFactory(SourceAmqp, session.ContainerConfig{ContainerID: "source", BackOff: backoff.NewConstantBackOff(time.Millisecond * 500)})
	collectorFactory := session.NewContainerFactory(CollectorAmqp, session.ContainerConfig{ContainerID: "collector", BackOff: backoff.NewConstantBackOff(time.Millisecond * 500)})

	interrupt := make(chan os.Signal, 2)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	actual := make(chan map[string]*vanflow.ProcessRecord, 1)
	go func() {
		actual <- runSource(ctx, sourceFactory, done)
	}()
	go func() {
		readFromSource(ctx, sourceFactory)
	}()
	observed := make(chan map[string]*vanflow.ProcessRecord, 1)
	go func() {
		observed <- runCollector(ctx, collectorFactory)
	}()

	<-interrupt
	close(done)
	log.Print("Event source stopped. Awaiting second interrupt to cancel collector...")
	<-interrupt
	log.Print("Collector stopped. Awaiting results")
	cancel()
	a := <-actual
	o := <-observed
	if !cmp.Equal(a, o) {
		log.Fatalf("expected and actual were different: %s", cmp.Diff(a, o))
	}
	log.Print("OKAY!")

	return nil
}

func readFromSource(ctx context.Context, factory session.ContainerFactory) {
	container := factory.Create()
	container.OnSessionError(func(err error) { log.Print(err) })
	container.Start(ctx)
	rcv := container.NewReceiver("mc/sfe.testsource", session.ReceiverOptions{Credit: 256})
	enc := json.NewEncoder(os.Stdout)
	for {
		msg, err := rcv.Next(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				log.Printf("unexpected error receiving message: %s", err)
			}
			continue
		}
		rcv.Accept(ctx, msg)
		if Debug {
			vmsg, err := vanflow.Decode(msg)
			if err != nil {
				log.Printf("unexpected error decoding message: %s", err)
				continue
			}
			enc.Encode(vmsg)
		}
	}
}

func runSource(ctx context.Context, factory session.ContainerFactory, done <-chan struct{}) map[string]*vanflow.ProcessRecord {
	type sequenceInfo struct {
		Finalized time.Time
		Sequence  int64
	}
	recordState := make(map[string]*vanflow.ProcessRecord)
	lastUpdatedState := make(map[string]sequenceInfo)

	container := factory.Create()
	container.OnSessionError(func(err error) { log.Print(err) })
	container.Start(ctx)
	flushRcv := container.NewReceiver("sfe.testsource", session.ReceiverOptions{Credit: 256})
	recordSnd := container.NewSender("mc/sfe.testsource", session.SenderOptions{})

	flushes := make(chan vanflow.FlushMessage, 1)
	go func() {
		for {
			next, err := flushRcv.Next(ctx)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					panic(err)
				}
				return
			}
			flushRcv.Accept(ctx, next)
			msg, err := vanflow.Decode(next)
			flush, ok := msg.(vanflow.FlushMessage)
			if !ok {
				continue
			}
			flushes <- flush
		}
	}()

	var (
		nRecords        int
		currentSequence int64
		interval        time.Duration = time.Millisecond * 100
		jitter          float64       = 2.5
	)
	nextRecordID := func() string {
		nRecords++
		return fmt.Sprintf("testsource:%d", nRecords)
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))
	nextDelay := func() time.Duration {
		return time.Duration(float64(interval) * (r.Float64() * jitter))
	}

	randomRecord := func() string {
		first := r.Intn(len(recordState))
		for key := range recordState {
			if first == 0 {
				return key
			}
			first--
		}
		return ""
	}

	for i := 0; i < 128; i++ {
		id := nextRecordID()
		recordState[id] = &vanflow.ProcessRecord{BaseRecord: vanflow.NewBase(id, time.Now().Truncate(time.Microsecond))}
	}

	tAction := time.NewTimer(nextDelay())
	defer tAction.Stop()
	heartbeatTimer := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-ctx.Done():
			return recordState
		case <-done:
			done = nil
			if !tAction.Stop() {
				<-tAction.C
			}
		case f := <-flushes:
			var keys []string
			if f.Head == 0 {
				keys = make([]string, 0, len(recordState))
				for key := range recordState {
					keys = append(keys, key)
				}
			} else {
				for key, s := range lastUpdatedState {
					if s.Sequence > f.Head {
						keys = append(keys, key)
					}
				}
			}
			for len(keys) > 0 {
				batch := keys
				if len(batch) > 10 {
					batch = batch[:10]
				}
				records := make([]vanflow.Record, len(batch))
				for i, key := range batch {
					record, ok := recordState[key]
					if !ok {
						record = &vanflow.ProcessRecord{BaseRecord: vanflow.NewBase(key, time.Now().Add(-1*time.Minute), lastUpdatedState[key].Finalized)}
					}
					records[i] = record
				}
				currentSequence++
				recordMsg := vanflow.RecordMessage{
					Records:  records,
					Sequence: currentSequence,
				}
				msg, err := recordMsg.Encode()
				if err != nil {
					panic(err)
				}
				if err := recordSnd.Send(ctx, msg); err != nil {
					panic(err)
				}
				for _, key := range batch {
					lastUpdatedState[key] = sequenceInfo{Sequence: currentSequence}
				}
				keys = keys[len(batch):]
			}
			// send bookmark heartbeat to indicate flush is over
			if err := recordSnd.Send(ctx, vanflow.HeartbeatMessage{
				Identity: "testsource",
				Now:      uint64(time.Now().UnixMicro()),
				Head:     currentSequence,
			}.Encode()); err != nil {
				log.Printf("source send error: %s", err)
			}
			log.Printf("SOURCE: flushed from %d to %d", f.Head, currentSequence)
		case <-heartbeatTimer.C:
			if err := recordSnd.Send(ctx, vanflow.HeartbeatMessage{
				Identity: "testsource",
				Now:      uint64(time.Now().UnixMicro()),
				Head:     currentSequence,
			}.Encode()); err != nil {
				log.Printf("source send error: %s", err)
			}
		case <-tAction.C:
			tAction.Reset(nextDelay())
			n := r.Float64() * 100
			switch {
			case n < 25: // terminate a record
				key := randomRecord()
				record := recordState[key]
				delete(recordState, key)
				record.EndTime = &vanflow.Time{Time: time.Now()}
				currentSequence++
				msg, err := vanflow.RecordMessage{
					Sequence: currentSequence,
					Records:  []vanflow.Record{record},
				}.Encode()
				if err != nil {
					panic(err)
				}
				if err := recordSnd.Send(ctx, msg); err != nil {
					log.Printf("source send error: %s", err)
				}
				lastUpdatedState[key] = sequenceInfo{Sequence: currentSequence, Finalized: time.Now()}
			case n < 55: // add a new record
				id := nextRecordID()
				recordState[id] = &vanflow.ProcessRecord{BaseRecord: vanflow.NewBase(id, time.Now().Truncate(time.Microsecond))}
				currentSequence++
				msg, err := vanflow.RecordMessage{
					Sequence: currentSequence,
					Records:  []vanflow.Record{recordState[id]},
				}.Encode()
				if err != nil {
					panic(err)
				}
				if err := recordSnd.Send(ctx, msg); err != nil {
					log.Printf("source send error: %s", err)
				}
				lastUpdatedState[id] = sequenceInfo{Sequence: currentSequence}
			default: // update a single record
				key := randomRecord()
				name := fmt.Sprintf("recordname-%d", time.Now().Unix())
				recordState[key].Name = &name
				currentSequence++
				msg, err := vanflow.RecordMessage{
					Sequence: currentSequence,
					Records:  []vanflow.Record{recordState[key]},
				}.Encode()
				if err != nil {
					panic(err)
				}
				if err := recordSnd.Send(ctx, msg); err != nil {
					log.Printf("source send error: %s", err)
				}
				lastUpdatedState[key] = sequenceInfo{Sequence: currentSequence}
			}
		}
	}
}

func runCollector(ctx context.Context, factory session.ContainerFactory) map[string]*vanflow.ProcessRecord {
	recordState := make(map[string]*vanflow.ProcessRecord)

	container := factory.Create()
	container.OnSessionError(func(err error) { log.Print(err) })
	container.Start(ctx)
	rcv := container.NewReceiver("mc/sfe.testsource", session.ReceiverOptions{Credit: 256})
	flushSnd := container.NewSender("sfe.testsource", session.SenderOptions{})

	var (
		currentSequence int64
	)

	msgs := make(chan any, 256)
	go func() {
		defer rcv.Close(ctx)
		for {
			msg, err := rcv.Next(ctx)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					log.Printf("unexpected error receiving message: %s", err)
				}
				continue
			}
			rcv.Accept(ctx, msg)
			vmsg, err := vanflow.Decode(msg)
			if err != nil {
				panic(err)
			}
			msgs <- vmsg
		}

	}()

	flushAndSync := func(head int64) {
		var recordCount int
		defer func() {
			log.Printf("COLLECTOR: flush from sequence %d to %d. Got %d records", head, currentSequence, recordCount)
		}()
		if err := flushSnd.Send(ctx, vanflow.FlushMessage{
			Head: head,
		}.Encode()); err != nil {
			log.Printf("unexpected error sending flush: %s", err)
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case recordMsg := <-msgs:
				switch m := recordMsg.(type) {
				case vanflow.HeartbeatMessage:
					currentSequence = m.Head
					return
				case vanflow.RecordMessage:
					for _, record := range m.Records {
						recordCount++
						pr := record.(*vanflow.ProcessRecord)
						if pr.EndTime != nil {
							delete(recordState, pr.ID)
						} else {
							recordState[pr.ID] = pr
						}
					}
				}
			}
		}
	}

	<-msgs
	flushAndSync(0)
	for {
		select {
		case <-ctx.Done():
			return recordState
		case recordMsg := <-msgs:
			switch m := recordMsg.(type) {
			case vanflow.HeartbeatMessage:
				if m.Head != currentSequence {
					flushAndSync(currentSequence)
					continue
				}
			case vanflow.RecordMessage:
				if m.Sequence != currentSequence+1 {
					flushAndSync(currentSequence)
					continue
				}
				currentSequence = m.Sequence
				for _, record := range m.Records {
					pr := record.(*vanflow.ProcessRecord)
					if pr.Terminated() {
						delete(recordState, pr.ID)
					} else {
						recordState[pr.ID] = pr
					}
				}
			}
		}
	}
}

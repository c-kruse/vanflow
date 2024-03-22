package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/eventsource"
	"github.com/c-kruse/vanflow/session"
	"github.com/c-kruse/vanflow/store"
	"github.com/gorilla/handlers"
)

func serveFixture(ctx context.Context, factory session.ContainerFactory) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fixture := &fixtureServer{
		factory: factory,
		status:  "pending fixture data",
		updates: make(chan map[store.SourceRef]store.Interface),
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/", fixture.ServeFixture)
	mux.HandleFunc("/flows", fixture.ServeFlows)

	fixture.Start(ctx)

	slog.Info("Starting server on :9080")
	srv := &http.Server{
		Addr:    ":9080",
		Handler: handlers.LoggingHandler(os.Stdout, mux),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("server error", slog.Any("error", err))
		}
	}()
	container := factory.Create()
	container.Start(ctx)
	container.OnSessionError(func(err error) {
		if _, ok := err.(session.RetryableError); !ok {
			slog.Error("starting shutdown due to non-retryable container error", slog.Any("error", err))
			cancel()
			return
		}
		slog.Error("container session error", slog.Any("error", err))
	})
	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	wg.Wait()
}

type fixtureServer struct {
	factory        session.ContainerFactory
	lock           sync.Mutex
	status         string
	managers       map[string]*eventsource.Manager
	flowsProcessed int

	updates chan map[store.SourceRef]store.Interface
}

func (f *fixtureServer) Start(ctx context.Context) {

	go func() {
		var managers map[string]*eventsource.Manager
		var (
			manageCtx    context.Context
			manageCancel context.CancelFunc
		)
		for {
			select {
			case <-ctx.Done():
				return
			case config := <-f.updates:
				slog.Info("reconfigure received. stopping managers.")
				if manageCancel != nil {
					manageCancel()
				}
				f.lock.Lock()
				f.status = "reconfiguring"
				f.managers = nil
				f.lock.Unlock()
				manageCtx, manageCancel = context.WithCancel(ctx)
				defer manageCancel()
				managers = map[string]*eventsource.Manager{}
				for src, stor := range config {
					container := f.factory.Create()
					container.Start(ctx)
					container.OnSessionError(func(err error) {
						if _, ok := err.(session.RetryableError); !ok {
							slog.Error("manager container non-retryable container error", slog.Any("error", err), slog.String("manager", src.Name))
							return
						}
						slog.Error("manager container session error", slog.Any("error", err), slog.String("manager", src.Name))
					})
					isController := src.Type == "CONTROLLER"
					managers[src.Name] = eventsource.NewManager(container, eventsource.ManagerConfig{
						Source: eventsource.Info{
							ID:      src.Name,
							Version: 1,
							Type:    src.Type,
							Address: "mc/sfe." + src.Name,
							Direct:  "sfe." + src.Name,
						},
						UseAlternateHeartbeatAddress: isController,
						Stores:                       []store.Interface{stor},
						FlushDelay:                   time.Second,
						FlushBatchSize:               10,
					})
				}

				for i := range managers {
					time.Sleep(time.Millisecond * 500)
					all, _ := managers[i].Stores[0].List(context.TODO(), nil)
					slog.Info("starting new manager", slog.String("source", managers[i].Source.ID), slog.Int("records", len(all.Entries)))
					go managers[i].Run(manageCtx)
				}
				f.lock.Lock()
				f.status = "running"
				f.managers = managers
				f.lock.Unlock()
			}
		}
	}()
}

func (f *fixtureServer) ServeFixture(rw http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(rw)
	var response ReplaceResponse
	f.lock.Lock()
	response.Status = f.status
	f.lock.Unlock()
	if r.Method != http.MethodPost {
		enc.Encode(response)
		return
	}

	dec := json.NewDecoder(r.Body)
	defer r.Body.Close()
	var request ReplaceRequest
	if err := dec.Decode(&request); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		response.Error = err.Error()
		enc.Encode(response)
		return
	}

	entriesBySource := groupEntriesBySource(request)
	sourceStores := map[store.SourceRef]store.Interface{}
	for src, entries := range entriesBySource {
		if len(entries) == 0 {
			continue
		}
		stor := newFixtureStore()
		if err := stor.Replace(context.TODO(), entries); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			response.Error = err.Error()
			enc.Encode(response)
			return
		}
		sourceStores[src] = stor
	}
	f.updates <- sourceStores
	enc.Encode(response)
}

type flowPair struct {
	Server store.Entry
	Client store.Entry
}

func (f *fixtureServer) ServeFlows(rw http.ResponseWriter, r *http.Request) {
	stores := map[string]store.Interface{}
	f.lock.Lock()
	defer f.lock.Unlock()
	for id, manager := range f.managers {
		stores[id] = manager.Stores[0]
	}
	flowEntries := make(map[string]store.Entry)
	flowPairsIDs := make(map[string]string)
	for _, stor := range stores {
		result, err := stor.Index(r.Context(), "ByType", store.Entry{TypeMeta: vanflow.FlowRecord{}.GetTypeMeta()}, nil)
		if err != nil {
			slog.Error("error querying flow records", slog.Any("error", err))
			rw.WriteHeader(http.StatusInternalServerError)
		}
		for _, result := range result.Entries {
			f := result.Record.(*vanflow.FlowRecord)
			flowEntries[f.ID] = result
			if f.Counterflow != nil && *f.Counterflow != "" {
				flowPairsIDs[f.ID] = *f.Counterflow
			}
		}
	}
	var flowPairs []flowPair
	for serverID, clientID := range flowPairsIDs {
		flowPairs = append(flowPairs, flowPair{
			Server: flowEntries[serverID],
			Client: flowEntries[clientID],
		})
	}
	if r.Method == http.MethodPost {
		var request struct {
			Flows    int `json:"flows"`
			Duration int `json:"duration"`
		}
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(rw, "Bad request: %s\n", err)
			fmt.Fprintln(rw, "")
			fmt.Fprintln(rw, "expected:")
			request.Flows = 100
			request.Duration = 3
			json.NewEncoder(rw).Encode(request)
			return
		}

		for _, pair := range flowPairs {
			go f.fakeFlow(pair, f.managers[pair.Server.Meta.Source.Name], f.managers[pair.Client.Meta.Source.Name], request.Flows, time.Second*time.Duration(request.Duration), 4)
		}
	}
	var response = struct {
		FlowPairs     []flowPair `json:"flowPairs"`
		FlowsReplayed int        `json:"fixtureFlowsResent"`
	}{
		FlowPairs:     flowPairs,
		FlowsReplayed: f.flowsProcessed,
	}
	if err := json.NewEncoder(rw).Encode(response); err != nil {
		slog.Error("error writing response", slog.Any("error", err))
		rw.WriteHeader(http.StatusInternalServerError)
	}
}

func (f *fixtureServer) fakeFlow(og flowPair, srvManager *eventsource.Manager, cliManager *eventsource.Manager, flows int, duration time.Duration, increments int) {
	defer func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		f.flowsProcessed += flows
	}()
	start := time.Now()
	goldenS := og.Server.Record.(*vanflow.FlowRecord)
	goldenC := og.Client.Record.(*vanflow.FlowRecord)
	var bytes [16]byte
	io.ReadFull(rand.Reader, bytes[:])
	srvID, cliID := hex.EncodeToString(bytes[:8]), hex.EncodeToString(bytes[8:])

	slog.Info("starting fixture flows",
		slog.Any("collectorFlow", goldenS.ID),
		slog.Any("listenerFlow", goldenC.ID),
		slog.Int("flows", flows),
		slog.String("duration", fmt.Sprint(duration)),
		slog.String("executionID", srvID),
	)
	srvFlows := make([]*vanflow.FlowRecord, flows)
	cliFlows := make([]*vanflow.FlowRecord, flows)
	for i := 0; i < flows; i++ {
		var (
			octS     uint64
			latencyS uint64
			octC     uint64
			latencyC uint64
		)
		if goldenS.Octets != nil {
			octS = *goldenS.Octets
		}
		if goldenC.Octets != nil {
			octC = *goldenC.Octets
		}
		if goldenS.Latency != nil {
			latencyS = *goldenS.Latency / uint64(increments)
		}
		if goldenC.Latency != nil {
			latencyC = *goldenC.Latency / uint64(increments)
		}
		cliflowid := fmt.Sprintf("%s:%d", cliID, i)
		srvFlows[i] = &vanflow.FlowRecord{BaseRecord: vanflow.NewBase(fmt.Sprintf("%s:%d", srvID, i), time.Now()),
			Counterflow: &cliflowid,
			Parent:      goldenS.Parent,
			SourceHost:  goldenS.SourceHost,
			SourcePort:  goldenS.SourcePort,
			Octets:      &octS,
			Latency:     &latencyS,
			Trace:       goldenS.Trace,
		}
		cliFlows[i] = &vanflow.FlowRecord{BaseRecord: vanflow.NewBase(cliflowid, time.Now()),
			Parent:     goldenC.Parent,
			SourceHost: goldenC.SourceHost,
			SourcePort: goldenC.SourcePort,
			Octets:     &octC,
			Latency:    &latencyC,
		}
	}

	for _, srvFlow := range srvFlows {
		srvManager.PublishUpdate(eventsource.RecordUpdate{Curr: srvFlow})
	}
	for _, cliFlow := range cliFlows {
		cliManager.PublishUpdate(eventsource.RecordUpdate{Curr: cliFlow})
	}
	for i := 0; i < increments; i++ {
		var endTime *vanflow.Time
		if i+1 == increments {
			endTime = &vanflow.Time{Time: time.Now()}
		}
		time.Sleep(duration / time.Duration(increments))
		for i := range srvFlows {
			prev := srvFlows[i]
			next := *prev
			oct, lat := *prev.Octets, *prev.Latency
			oct = oct + (oct / uint64(i+1))
			lat = lat + (lat / uint64(i+1))
			next.Octets = &oct
			next.Latency = &lat
			next.EndTime = endTime

			srvManager.PublishUpdate(eventsource.RecordUpdate{Prev: prev, Curr: &next})
			srvFlows[i] = &next
		}
		for i := range cliFlows {
			prev := cliFlows[i]
			next := *prev
			oct, lat := *prev.Octets, *prev.Latency
			oct = oct + (oct / uint64(i+1))
			lat = lat + (lat / uint64(i+1))
			next.Octets = &oct
			next.Latency = &lat
			next.EndTime = endTime

			cliManager.PublishUpdate(eventsource.RecordUpdate{Prev: prev, Curr: &next})
			cliFlows[i] = &next
		}
	}
	slog.Info("finished fixture flows",
		slog.String("actualDuration", fmt.Sprint(time.Since(start))),
		slog.String("duration", fmt.Sprint(duration)),
		slog.String("executionID", srvID),
	)
}

func groupEntriesBySource(request ReplaceRequest) map[store.SourceRef][]store.Entry {
	recordTypes := getRecordTypes()
	recordTypeMap := map[vanflow.TypeMeta]reflect.Type{}
	for _, r := range recordTypes {
		recordTypeMap[r.GetTypeMeta()] = reflect.TypeOf(r)
	}

	entries := map[store.SourceRef][]store.Entry{}
	for _, partial := range request.Data {
		src := partial.Meta.Source
		rtype, ok := recordTypeMap[partial.TypeMeta]
		if !ok {
			slog.Warn("ignoring record type not configured", "TypeMeta", partial.TypeMeta, "Record", string(partial.Record))
			continue
		}
		record := reflect.New(rtype).Interface()
		if err := json.Unmarshal(partial.Record, record); err != nil {
			slog.Error("failed to unmarshal record", "error", err)
			continue
		}
		entry := store.Entry{
			Meta:     partial.Meta,
			TypeMeta: partial.TypeMeta,
			Record:   record.(vanflow.Record),
		}
		entries[src] = append(entries[src], entry)
	}
	return entries
}

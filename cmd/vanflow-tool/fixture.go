package main

import (
	"context"
	"encoding/json"
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
	var statusMu sync.Mutex
	status := "pending records"

	// notification that the fixture received a set of event sources to
	// immitate
	reconfigure := make(chan map[store.SourceRef]store.Interface)

	http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		enc := json.NewEncoder(rw)
		var response ReplaceResponse
		statusMu.Lock()
		response.Status = status
		statusMu.Unlock()
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
		reconfigure <- sourceStores
		enc.Encode(response)
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var managers []*eventsource.Manager
		var (
			manageCtx    context.Context
			manageCancel context.CancelFunc
		)
		for {
			select {
			case <-ctx.Done():
				return
			case config := <-reconfigure:
				slog.Info("reconfigure received. stopping managers.")
				if manageCancel != nil {
					manageCancel()
				}
				statusMu.Lock()
				status = "reconfiguring"
				statusMu.Unlock()
				time.Sleep(time.Second * 1)
				manageCtx, manageCancel = context.WithCancel(ctx)
				defer manageCancel()
				managers = managers[0:0]
				for src, stor := range config {
					container := factory.Create()
					container.Start(ctx)
					isController := src.Type == "CONTROLLER"
					managers = append(managers, eventsource.NewManager(container, eventsource.ManagerConfig{
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
					}))
				}

				for i := range managers {
					time.Sleep(time.Millisecond * 500)
					all, _ := managers[i].Stores[0].List(context.TODO(), nil)
					slog.Info("starting new manager", slog.String("source", managers[i].Source.ID), slog.Int("records", len(all.Entries)))
					go managers[i].Run(manageCtx)
				}
				statusMu.Lock()
				status = "running"
				statusMu.Unlock()
			}
		}
	}()

	loggingHandler := handlers.LoggingHandler(os.Stdout, http.DefaultServeMux)
	slog.Info("Starting server on :9080")
	srv := &http.Server{
		Addr:    ":9080",
		Handler: loggingHandler,
	}
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

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
	"github.com/c-kruse/vanflow/messaging"
	"github.com/c-kruse/vanflow/store"
	"github.com/gorilla/handlers"
)

func serveFixture(ctx context.Context, factory messaging.SessionFactory) {
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

	go func() {
		var managers []*eventsource.Manager
		manageCtx, manageCancel := context.WithCancel(ctx)
		defer manageCancel()
		for {
			select {
			case <-ctx.Done():
				return
			case config := <-reconfigure:
				slog.Info("reconfigure received. stopping managers.")
				manageCancel()
				statusMu.Lock()
				status = "reconfiguring"
				statusMu.Unlock()
				time.Sleep(time.Second * 1)
				manageCtx, manageCancel = context.WithCancel(ctx)
				defer manageCancel()
				managers = managers[0:0]
				for src, stor := range config {
					isController := src.Type == "CONTROLLER"
					managers = append(managers, eventsource.NewManager(factory, eventsource.ManagerConfig{
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
	http.ListenAndServe(":9080", loggingHandler)
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

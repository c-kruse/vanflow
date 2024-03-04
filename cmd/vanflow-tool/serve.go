package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/eventsource"
	"github.com/c-kruse/vanflow/store"
	"github.com/gorilla/handlers"
)

func getRecordTypes() []vanflow.Record {
	recordTypes := []vanflow.Record{
		vanflow.SiteRecord{},
		vanflow.LinkRecord{},
		vanflow.RouterRecord{},
		vanflow.ListenerRecord{},
		vanflow.ConnectorRecord{},
		vanflow.ProcessRecord{},
	}
	if EnableRecordsFlow {
		recordTypes = append(recordTypes, vanflow.FlowRecord{})
	}
	if EnableRecordsLogs {
		recordTypes = append(recordTypes, vanflow.LogRecord{})
	}
	return recordTypes
}

func serveRecords(ctx context.Context, factory ContainerFactory) {
	slog.Debug("Starting to discover event sources")

	recordTypes := getRecordTypes()
	stor := newFixtureStore()
	dispatchReg := &store.DispatchRegistry{}
	for _, record := range recordTypes {
		dispatchReg.RegisterStore(record, stor)
	}

	refs := []string{
		"/",
		"/sources/",
	}

	http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		var response RecordsResponse
		enc := json.NewEncoder(rw)

		response.Refs = refs
		if r.URL.Path != "/" {
			rw.WriteHeader(http.StatusNotFound)
			response.Error = fmt.Sprintf("no route to path %q", r.URL.Path)
			enc.Encode(response)
			return
		}

		out, err := stor.List(r.Context(), nil)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			response.Error = err.Error()
		}
		response.Data = out.Entries
		enc.Encode(response)
	})

	sources := &sync.Map{}
	http.HandleFunc("/sources/", func(rw http.ResponseWriter, r *http.Request) {
		var response RecordsResponse
		enc := json.NewEncoder(rw)
		sources.Range(func(k, v any) bool {
			response.Data = append(response.Data, store.Entry{
				Meta: store.Metadata{
					Source: v.(store.SourceRef),
				},
			})
			return true
		})
		enc.Encode(response)
	})

	for _, record := range recordTypes {
		exemplar := store.Entry{
			TypeMeta: record.GetTypeMeta(),
		}
		path := "/" + record.GetTypeMeta().String() + "/"
		refs = append(refs, path)
		http.HandleFunc(path, func(rw http.ResponseWriter, r *http.Request) {
			var response RecordsResponse
			enc := json.NewEncoder(rw)
			out, err := stor.Index(r.Context(), "ByType", exemplar, nil)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				response.Error = err.Error()
			}
			response.Data = out.Entries
			enc.Encode(response)
		})
	}

	container := factory()
	container.Start(ctx)
	discovery := eventsource.NewDiscovery(container, eventsource.DiscoveryOptions{})
	go discovery.Run(ctx, eventsource.DiscoveryHandlers{
		Discovered: func(source eventsource.Info) {
			slog.Debug("source discovered", slog.Any("source", source))
			client := eventsource.NewClient(container, eventsource.ClientOptions{Source: source})
			err := discovery.NewWatchClient(ctx, eventsource.WatchConfig{Client: client, ID: source.ID, Timeout: time.Second * 30})
			if err != nil {
				slog.Error("discovery error", slog.Any("error", err))
				return
			}

			sourceRef := store.SourceRef{
				APIVersion: fmt.Sprint(source.Version),
				Type:       source.Type,
				Name:       source.ID,
			}
			sources.Store(source.ID, sourceRef)
			dispatcher := dispatchReg.NewDispatcher(sourceRef)

			client.OnRecord(dispatcher.Dispatch)
			if err = client.Listen(ctx, eventsource.FromSourceAddress()); err != nil {
				slog.Error("error starting listener", slog.Any("error", err))
			}
			if EnableRecordsFlow {
				if err = client.Listen(ctx, eventsource.FromSourceAddressFlows()); err != nil {
					slog.Error("error starting flows listener", slog.Any("error", err))
				}
			}
			if EnableRecordsLogs {
				if err = client.Listen(ctx, eventsource.FromSourceAddressLogs()); err != nil {
					slog.Error("error starting logs listener", slog.Any("error", err))
				}
			}
			if source.Type == "CONTROLLER" {
				if err = client.Listen(ctx, eventsource.FromSourceAddressHeartbeats()); err != nil {
					slog.Error("error starting controller heartbeat listener", slog.Any("error", err))
				}
			}
			go func() {
				flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				err := eventsource.FlushOnFirstMessage(flushCtx, client)
				if err != nil && errors.Is(err, flushCtx.Err()) {
					err = client.SendFlush(ctx)
				}
				if err != nil {
					slog.Error("failed to send FLUSH to source", slog.Any("error", err))
					return
				}
				slog.Debug("flush sent", slog.Any("source", source))
			}()
		},
		Forgotten: func(source eventsource.Info) {
			slog.Debug("source forgotten", slog.Any("source", source))
			sources.Delete(source.ID)
		},
	})
	loggingHandler := handlers.LoggingHandler(os.Stdout, http.DefaultServeMux)
	slog.Info("Starting server on :9090")
	http.ListenAndServe(":9090", loggingHandler)
}

func newFixtureStore() store.Interface {
	keyByTypeAndID := func(obj store.Entry) (string, error) {
		return fmt.Sprintf("%s/%s", obj.TypeMeta.String(), obj.Record.Identity()), nil
	}
	return store.NewDefaultCachingStore(
		store.CacheConfig{
			Key: keyByTypeAndID,
			Indexers: map[string]store.CacheIndexer{
				"ByType": func(entry store.Entry) []string {
					return []string{entry.TypeMeta.String()}
				},
				"BySource": func(entry store.Entry) []string {
					return []string{entry.Meta.Source.String()}
				},
			},
		})
}

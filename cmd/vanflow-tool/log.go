package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/eventsource"
	"github.com/c-kruse/vanflow/messaging"
)

func logOnly(ctx context.Context, factory messaging.SessionFactory) {
	outputstream := make(chan Message, 64)

	heartbeatHandler := func(m vanflow.HeartbeatMessage) {
		outputstream <- Message{
			Subject: m.Subject,
			To:      m.To,
			V:       fmt.Sprint(m.Version),
			Now:     fmt.Sprint(m.Now),
			ID:      m.Identity,
		}
	}
	recordHandler := func(m vanflow.RecordMessage) {
		records := make([]RecordData, len(m.Records))
		for i := range m.Records {
			records[i] = RecordData{
				Type: fmt.Sprintf("%T", m.Records[i]),
				Data: m.Records[i],
			}
		}
		outputstream <- Message{Subject: m.Subject, To: m.To, Data: records}
	}

	slog.Debug("Starting to discover event sources")

	discovery := eventsource.NewDiscovery(factory)
	go discovery.Run(ctx, eventsource.DiscoveryHandlers{
		Discovered: func(source eventsource.Info) {
			slog.Debug("source discovered", "source", source)
			client, err := discovery.NewWatchClient(ctx, eventsource.WatchConfig{ID: source.ID, Timeout: time.Second * 30})
			if err != nil {
				slog.Error("discovery error", "error", err)
				return
			}
			if EnableHeartbeat {
				client.OnHeartbeat(heartbeatHandler)
			}
			if EnableRecords {
				client.OnRecord(recordHandler)
			}
			if FlushOnDiscover {
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
			}
			if err = client.Listen(ctx, eventsource.FromSourceAddress()); err != nil {
				slog.Error("error starting listener", "error", err)
			}
			if EnableRecordsFlow {
				if err = client.Listen(ctx, eventsource.FromSourceAddressFlows()); err != nil {
					slog.Error("error starting flows listener", "error", err)
				}
			}
			if EnableRecordsLogs {
				if err = client.Listen(ctx, eventsource.FromSourceAddressLogs()); err != nil {
					slog.Error("error starting logs listener", "error", err)
				}
			}
			if source.Type == "CONTROLLER" {
				if err = client.Listen(ctx, eventsource.FromSourceAddressHeartbeats()); err != nil {
					slog.Error("error starting controller heartbeat listener", "error", err)
				}
			}
		},
		Forgotten: func(source eventsource.Info) {
			slog.Debug("source forgotten", "source", source)
		},
	})
	enc := json.NewEncoder(os.Stdout)
	for {
		select {
		case <-ctx.Done():
			return
		case out := <-outputstream:
			if err := enc.Encode(out); err != nil {
				slog.Error("error writing output stream", "error", err, "obj", out)
			}
		}
	}
}

type Message struct {
	To         string `json:"to"`
	Subject    string `json:"subject"`
	SourceType string `json:"sourceType,omitempty"`
	Address    string `json:"address,omitempty"`
	Direct     string `json:"direct,omitempty"`
	V          string `json:"v,omitempty"`
	Now        string `json:"now,omitempty"`
	ID         string `json:"id,omitempty"`
	Data       any    `json:"data,omitempty"`
}
type RecordData struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type Record struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

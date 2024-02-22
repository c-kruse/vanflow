package store

import (
	"log/slog"

	"github.com/c-kruse/vanflow"
)

var logger = slog.With("logger", "pkg.flow.store")

// DispatchRegistry serves as a registry where store implementations are
// registered by record type. Used to create a dispatcher for each event source
// so that updates can be dispatched to the appropriate store.
type DispatchRegistry struct {
	reg map[string]Interface
}

// RegisterStore includes a store for the given record type in the registry.
// Subsequent Dispatchers will route records matching that record type to this
// store.
func (d *DispatchRegistry) RegisterStore(record vanflow.Record, stor Interface) {
	if d.reg == nil {
		d.reg = make(map[string]Interface)
	}
	d.reg[record.GetTypeMeta().String()] = stor
}

// NewDispatcher creates a new dispatcher for that source
func (d DispatchRegistry) NewDispatcher(source SourceRef) Dispatcher {
	accum := make(map[string]RecordAccumulator, len(d.reg))
	for k, v := range d.reg {
		accum[k] = v
	}
	return Dispatcher{
		source:       source,
		accumulators: accum,
	}
}

// Dispatcher provides the means to dispatch Records from a given RecordMessage
// to multiple configured stores based on the record's type.
type Dispatcher struct {
	source       SourceRef
	accumulators map[string]RecordAccumulator
}

// Dispatch each record in the message to the configured store.
func (d Dispatcher) Dispatch(msg vanflow.RecordMessage) {
	for _, record := range msg.Records {
		typ := record.GetTypeMeta().String()
		acc, found := d.accumulators[typ]
		if !found {
			logger.Debug("dispatcher not configured with store for record type", "type", typ)
			continue
		}
		if err := acc.Accumulate(d.source, record); err != nil {
			logger.Error("dispatcher encountered error handling record", "type", typ, "record", record, "error", err)
			continue
		}
	}
}

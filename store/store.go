// store contains an interface for accumulating and accessing vanflow record
// state, as well as an in memory cache implementation that stores records in a
// synchronized map.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/c-kruse/vanflow"
)

// Metadata about a Entry
type Metadata struct {
	// Source of the Entry
	Source SourceRef
	// UpdatedAt last time the entry was updated
	UpdatedAt time.Time
	// Annotations that describe the entry
	Annotations map[string]string
}

// SourceRef describes the source of an entry
type SourceRef struct {
	APIVersion string
	Type       string
	Name       string
}

func (ref SourceRef) String() string {
	return fmt.Sprintf("%s.%s.%s", ref.APIVersion, ref.Type, ref.Name)
}

// Entry is the unit of storage in the store. It wraps a Record with additional
// context about that record that may not be a part of the record itself.
type Entry struct {
	Meta     Metadata
	TypeMeta vanflow.TypeMeta
	Record   vanflow.Record
}

// Single wraps an Entry with additional store context
type Single struct {
	Entry

	Found bool
	ETag  string
}

// Set wraps a collection of Entries with additional store context
type Set struct {
	Entries []Entry

	Continue string
}

type Selector struct {
	Offset   int
	Limit    int
	Continue string

	Ordering Ordering
	// SortKey  any
	// Filters  any
}

type Ordering int

const (
	Ascending  Ordering = 0
	Descending Ordering = 1
)

// Interface (externally store.Interface) descibes the methods for interracting
// with a store
type Interface interface {
	RecordAccumulator

	Get(ctx context.Context, obj Entry) (Single, error)
	Add(ctx context.Context, obj Entry) error
	Update(ctx context.Context, obj Entry) error
	Delete(ctx context.Context, obj Entry) error

	List(ctx context.Context, opts *Selector) (Set, error)
	Index(ctx context.Context, idx string, obj Entry, opts *Selector) (Set, error)

	Replace(context.Context, []Entry) error
}

type RecordAccumulator interface {
	// Accumulate new partial state from a Record
	Accumulate(source SourceRef, record vanflow.Record) error
}

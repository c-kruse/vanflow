package store

import (
	"context"
	"errors"

	"github.com/c-kruse/vanflow"
)

func Get[R vanflow.Record](ctx context.Context, stor Interface, exemplar R) (R, error) {
	entry, err := stor.Get(ctx, Entry{TypeMeta: exemplar.GetTypeMeta(), Record: exemplar})
	if err != nil {
		return exemplar, err
	}
	if !entry.Found {
		return exemplar, errors.New("not found")
	}
	return entry.Record.(R), nil
}

func Index[R vanflow.Record](ctx context.Context, stor Interface, index string, exemplar R) ([]R, error) {
	var results []R
	entries, err := stor.Index(ctx, index, Entry{TypeMeta: exemplar.GetTypeMeta(), Record: exemplar}, nil)
	if err != nil || len(entries.Entries) == 0 {
		return results, err
	}

	results = make([]R, 0, len(entries.Entries))
	for _, entry := range entries.Entries {
		record, ok := entry.Record.(R)
		if !ok {
			continue
		}
		results = append(results, record)
	}
	return results, err
}

package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/c-kruse/vanflow"
	"github.com/c-kruse/vanflow/store/internal"
)

const (
	IndexBySource = "source"
	IndexBySite   = "site"

	siteAnnotationKey = "skupper.io/flow/site-id"
	siteUnknownValue  = "unknown"
)

// KeyFunc returns the identity of the entry so that the cache knows how to
// store it.
type KeyFunc func(entry Entry) (string, error)

// CacheIndexer returns a list of index buckets that an entry belongs to. For
// example, a "IndexBySite" index might have a Link entry that belongs to the
// bucket for the two sites it connects.
type CacheIndexer func(Entry) []string

type objectCache struct {
	storage *internal.SyncIndexedMap[Entry]

	keyFunc func(obj Entry) (string, error)

	eventHandlers EventHandlerFuncs
}

type CacheConfig struct {
	Key      KeyFunc
	Indexers map[string]CacheIndexer

	EventHandlers EventHandlerFuncs
}

type EventHandlerFuncs struct {
	OnAdd    func(entry Entry)
	OnChange func(prev, curr Entry)
	OnDelete func(entry Entry)
}

func NewDefaultCachingStore(cfg CacheConfig) Interface {
	if cfg.Key == nil {
		cfg.Key = keyByRecordID
	}
	if cfg.Indexers == nil {
		cfg.Indexers = defaultIndexers()
	}
	cacheIndexers := make(map[string]func(interface{}) []string, len(cfg.Indexers))
	for index, fn := range cfg.Indexers {
		cacheIndexers[index] = func(obj interface{}) []string {
			return fn(obj.(Entry))
		}
	}
	gCacheIndexers := make(map[string]internal.Indexer[Entry], len(cfg.Indexers))
	for index, fn := range cfg.Indexers {
		gCacheIndexers[index] = internal.Indexer[Entry](fn)
	}
	cache := &objectCache{
		keyFunc:       cfg.Key,
		storage:       internal.NewSyncIndexedMap(gCacheIndexers),
		eventHandlers: cfg.EventHandlers,
	}
	return cache
}

func (c *objectCache) Accumulate(source SourceRef, partial vanflow.Record) error {
	key, err := c.keyFunc(Entry{Record: partial})
	if err != nil {
		return fmt.Errorf("error getting key for accumulate: %w", err)
	}
	d, err := c.storage.Patch(key, recordPatcher{Source: source, Record: partial})
	if errors.Is(err, errNoChange) { // okay
		return nil
	}

	if d.Prev != nil && c.eventHandlers.OnChange != nil {
		c.eventHandlers.OnChange(*d.Prev, d.Next)
	} else if d.Prev == nil && c.eventHandlers.OnAdd != nil {
		c.eventHandlers.OnAdd(d.Next)
	}
	return err
}

func (c *objectCache) Get(ctx context.Context, exemplar Entry) (result Single, error error) {
	key, err := c.keyFunc(exemplar)
	if err != nil {
		return result, fmt.Errorf("error getting key for add: %w", err)
	}
	cc, ok := c.storage.Get(key)
	if ok {
		result.Found = true
		result.Entry = cc.Resource
		result.ETag = getETag(cc)
	}
	return result, nil
}

func (c *objectCache) applySelector(result Set, sel Selector) (Set, error) {
	// default ordering by keyfunc
	sort.Slice(result.Entries, func(i, j int) bool {
		ki, _ := c.keyFunc(result.Entries[i])
		kj, _ := c.keyFunc(result.Entries[j])
		delta := strings.Compare(ki, kj)
		if sel.Ordering == Descending {
			return delta >= 0
		}
		return delta < 0
	})
	offset := sel.Offset
	if sel.Continue != "" {
		var err error
		offset, err = decodeCacheContinue(sel.Continue)
		if err != nil {
			return result, err
		}
	}

	if offset >= len(result.Entries) {
		result.Entries = result.Entries[0:0] // empty resultset
	} else {
		result.Entries = result.Entries[offset:]
	}

	if sel.Limit > 0 && len(result.Entries) > sel.Limit {
		result.Entries = result.Entries[:sel.Limit]
		result.Continue = encodeCacheContinue(offset + sel.Limit)
	}
	return result, nil
}

func (c *objectCache) List(ctx context.Context, opts *Selector) (Set, error) {
	var result Set
	all := c.storage.List()
	result.Entries = make([]Entry, len(all))
	for i := range all {
		result.Entries[i] = all[i].Resource
	}
	if opts != nil {
		return c.applySelector(result, *opts)
	}
	return result, nil
}

func (c *objectCache) IndexValues(ctx context.Context, idx string) ([]string, error) {
	return c.storage.IndexValues(idx)
}

func (c *objectCache) Index(ctx context.Context, idx string, obj Entry, opts *Selector) (Set, error) {
	var result Set
	all, err := c.storage.Index(idx, obj)
	if err != nil {
		return result, fmt.Errorf("error retrieving underlying cache index: %w", err)
	}
	result.Entries = make([]Entry, len(all))
	for i := range all {
		result.Entries[i] = all[i].Resource
	}
	if opts != nil {
		return c.applySelector(result, *opts)
	}
	return result, nil
}

func (c *objectCache) Add(ctx context.Context, obj Entry) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return fmt.Errorf("error getting key for add: %w", err)
	}
	obj.Meta.UpdatedAt = time.Now()
	d, err := c.storage.Update(key, obj, internal.WhenNotExists[Entry]())
	if err != nil {
		return err
	}
	if c.eventHandlers.OnAdd != nil {
		c.eventHandlers.OnAdd(d.Next)
	}
	return err
}

func (c *objectCache) Update(ctx context.Context, obj Entry) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return fmt.Errorf("error getting key for update: %w", err)
	}
	obj.Meta.UpdatedAt = time.Now()

	cond, err := getConditional(ctx)
	if err != nil {
		return fmt.Errorf("error extracting conditional from context: %s", err)
	}

	var d internal.Delta[Entry]
	if cond != nil {
		d, err = c.storage.Update(key, obj, cond)
	} else {
		d, err = c.storage.Update(key, obj)
	}

	if err != nil {
		return err
	}
	if d.Prev != nil && c.eventHandlers.OnChange != nil {
		c.eventHandlers.OnChange(*d.Prev, d.Next)
	} else if d.Prev == nil && c.eventHandlers.OnAdd != nil {
		c.eventHandlers.OnAdd(d.Next)
	}
	return nil
}

func (c *objectCache) Delete(ctx context.Context, obj Entry) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return fmt.Errorf("error getting key for update: %w", err)
	}
	deleted, ok := c.storage.Delete(key)
	if ok && c.eventHandlers.OnDelete != nil {
		c.eventHandlers.OnDelete(deleted.Resource)
	}
	return nil
}

func (c *objectCache) Replace(_ context.Context, entries []Entry) error {
	items := make(map[string]Entry, len(entries))
	for _, obj := range entries {
		key, err := c.keyFunc(obj)
		if err != nil {
			return fmt.Errorf("error getting key for replace: %w", err)
		}
		items[key] = obj
	}
	c.storage.Replace(items)
	return nil
}

func keyByRecordID(obj Entry) (string, error) {
	return obj.Record.Identity(), nil
}

func defaultIndexers() map[string]CacheIndexer {
	return map[string]CacheIndexer{
		IndexBySource: func(entry Entry) []string {
			return []string{entry.Meta.Source.String()}
		},
		IndexBySite: func(entry Entry) []string {
			site, ok := entry.Meta.Annotations[siteAnnotationKey]
			if !ok {
				site = siteUnknownValue
			}
			return []string{site}
		},
	}
}

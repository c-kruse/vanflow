package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/c-kruse/vanflow"
	"gotest.tools/assert"
)

func TestCacheAccumulate(t *testing.T) {
	cache := NewDefaultCachingStore(CacheConfig{})
	source := SourceRef{Name: "testing"}

	pointsInTime := []struct {
		Desc         string
		ToAccumulate vanflow.Record
		ExpectState  func(*testing.T, []Entry)
	}{
		{
			Desc: "empty",
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 0)
			},
		}, {
			Desc: "record 1",
			ToAccumulate: &vanflow.SiteRecord{
				BaseRecord: vanflow.NewBase("1", time.UnixMicro(100)),
				Location:   ptrTo("location 1"),
			},
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 1)
				assert.DeepEqual(t, entries[0].Record, &vanflow.SiteRecord{
					BaseRecord: vanflow.NewBase("1", time.UnixMicro(100)),
					Location:   ptrTo("location 1"),
				})
			},
		}, {
			Desc: "record 1 unchanged",
			ToAccumulate: &vanflow.SiteRecord{
				BaseRecord: vanflow.NewBase("1"),
				Location:   ptrTo("location 1"),
			},
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 1)
				assert.DeepEqual(t, entries[0].Record, &vanflow.SiteRecord{
					BaseRecord: vanflow.NewBase("1", time.UnixMicro(100)),
					Location:   ptrTo("location 1"),
				})
			},
		}, {
			Desc: "record 2",
			ToAccumulate: &vanflow.SiteRecord{
				BaseRecord: vanflow.NewBase("2", time.UnixMicro(0)),
			},
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 2)
			},
		}, {
			Desc: "record 1 updated",
			ToAccumulate: &vanflow.SiteRecord{
				BaseRecord: vanflow.NewBase("1"),
				Location:   ptrTo("*location 1*"),
			},
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 2)
				if entries[0].Record.(*vanflow.SiteRecord).BaseRecord.ID != "1" {
					entries[0], entries[1] = entries[1], entries[0]
				}
				assert.DeepEqual(t, entries[0].Record, &vanflow.SiteRecord{
					BaseRecord: vanflow.NewBase("1", time.UnixMicro(100)),
					Location:   ptrTo("*location 1*"),
				})
			},
		}, {
			Desc: "record 2 end time",
			ToAccumulate: &vanflow.SiteRecord{
				BaseRecord: vanflow.NewBase("2", time.UnixMicro(0), time.UnixMicro(9000)),
			},
			ExpectState: func(t *testing.T, entries []Entry) {
				assert.Assert(t, len(entries) == 2)
				if entries[0].Record.(*vanflow.SiteRecord).BaseRecord.ID != "2" {
					entries[0], entries[1] = entries[1], entries[0]
				}
				assert.DeepEqual(t, entries[0].Record, &vanflow.SiteRecord{
					BaseRecord: vanflow.NewBase("2", time.UnixMicro(0), time.UnixMicro(9000)),
				})
			},
		},
	}
	for _, pit := range pointsInTime {
		if pit.ToAccumulate != nil {
			assert.Check(t, cache.Accumulate(source, pit.ToAccumulate))
		}
		if pit.ExpectState != nil {
			set, err := cache.List(context.TODO(), nil)
			assert.Check(t, err)
			pit.ExpectState(t, set.Entries)
		}
	}
}

func TestReplace(t *testing.T) {
	cache := NewDefaultCachingStore(CacheConfig{})
	source := SourceRef{Name: "testing"}

	entries := []Entry{
		{
			Meta: Metadata{
				Source: source,
			},
			Record: vanflow.SiteRecord{BaseRecord: vanflow.NewBase("1")},
		},
		{
			Meta: Metadata{
				Source: source,
			},
			Record: vanflow.SiteRecord{BaseRecord: vanflow.NewBase("2", time.UnixMicro(0))},
		},
	}
	err := cache.Replace(context.TODO(), entries)
	assert.Check(t, err)
	all, err := cache.List(context.TODO(), &Selector{})
	assert.Check(t, err)
	assert.DeepEqual(t, all.Entries, entries)
}

func TestList(t *testing.T) {
	cache := NewDefaultCachingStore(CacheConfig{})
	source := SourceRef{Name: "testing"}

	entries := make([]Entry, 0, 100)
	for i := 0; i < 100; i++ {
		entries = append(entries, Entry{
			Meta:   Metadata{Source: source},
			Record: vanflow.SiteRecord{BaseRecord: vanflow.NewBase(fmt.Sprint(i))},
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return strings.Compare(entries[i].Record.Identity(), entries[j].Record.Identity()) < 0
	})

	err := cache.Replace(context.TODO(), entries)
	assert.Check(t, err)
	all, err := cache.List(context.TODO(), &Selector{})
	assert.Check(t, err)
	assert.DeepEqual(t, all.Entries, entries)

	var response Set
	var paged []Entry
	for more := true; more; more = (response.Continue != "") {
		response, err = cache.List(context.TODO(), &Selector{Limit: 12, Continue: response.Continue})
		assert.Check(t, err)
		paged = append(paged, response.Entries...)
		if response.Continue != "" {
			assert.Equal(t, len(response.Entries), 12)
		}
	}
	assert.Equal(t, len(all.Entries), len(paged))
	assert.DeepEqual(t, all.Entries, paged)
}

func TestConditionalUpdate(t *testing.T) {
	cache := NewDefaultCachingStore(CacheConfig{})
	ctx := context.Background()

	baseEntry := Entry{Record: vanflow.LinkRecord{BaseRecord: vanflow.NewBase("A")}}
	err := cache.Add(ctx, baseEntry)
	assert.Check(t, err)

	resp, err := cache.Get(ctx, baseEntry)
	assert.Check(t, err)
	assert.Assert(t, len(resp.ETag) > 10)

	ctx = WithIfMatch(ctx, resp.ETag)
	entry := resp.Entry
	entry.Meta.Annotations = map[string]string{"A": "Z"}
	err = cache.Update(ctx, entry)
	assert.Check(t, err)

	entry.Meta.Annotations["X"] = "Y"
	err = cache.Update(ctx, entry)
	assert.ErrorContains(t, err, "update condition error")
}

func ptrTo[T any](direct T) *T {
	return &direct
}

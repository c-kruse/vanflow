package internal

import (
	"fmt"
	"sync"
)

type VersionContainer[T any] struct {
	Resource T
	Version  int64
}

type Delta[T any] struct {
	Prev *T
	Next T
}

type Patcher[T any] interface {
	// Patch returns an updated copy the current object or an error if it
	// should not be updated.
	Patch(curr *T) (next T, err error)
}

type SyncIndexedMap[T any] struct {
	mu    sync.RWMutex
	items map[string]VersionContainer[T]

	indexers map[string]Indexer[T]
	indicies map[string]Index
}

func NewSyncIndexedMap[T any](indexers map[string]Indexer[T]) *SyncIndexedMap[T] {
	return &SyncIndexedMap[T]{
		items:    make(map[string]VersionContainer[T]),
		indexers: indexers,
		indicies: make(map[string]Index),
	}
}

func (m *SyncIndexedMap[T]) Patch(key string, patcher Patcher[T], cond ...Cond[T]) (Delta[T], error) {
	var delta Delta[T]
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.items[key]
	for _, condititoinal := range cond {
		if err := condititoinal(cc); err != nil {
			return delta, fmt.Errorf("patch condition error: %w", err)
		}
	}
	var prev *T
	if ok {
		prev = &cc.Resource
	}
	obj, err := patcher.Patch(prev)
	if err != nil {
		return delta, fmt.Errorf("patch apply error: %w", err)
	}
	cc.Resource = obj
	cc.Version++
	m.items[key] = cc
	m.reindex(key, prev, obj)
	delta.Prev = prev
	delta.Next = obj
	return delta, nil
}

func (m *SyncIndexedMap[T]) Update(key string, obj T, cond ...Cond[T]) (Delta[T], error) {
	var delta Delta[T]
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.items[key]
	for _, condititoinal := range cond {
		if err := condititoinal(cc); err != nil {
			return delta, fmt.Errorf("update condition error: %w", err)
		}
	}
	var prev *T
	if ok {
		prev = &cc.Resource
	}
	cc.Resource = obj
	cc.Version++
	m.items[key] = cc
	m.reindex(key, prev, obj)
	delta.Prev = prev
	delta.Next = obj
	return delta, nil
}

func (m *SyncIndexedMap[T]) Delete(key string) (deleted VersionContainer[T], exists bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	deleted, exists = m.items[key]
	if exists {
		delete(m.items, key)
		m.unindex(key, deleted.Resource)
	}
	return deleted, exists
}

func (m *SyncIndexedMap[T]) Get(key string) (obj VersionContainer[T], exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cc, exists := m.items[key]
	return cc, exists
}

func (m *SyncIndexedMap[T]) Replace(items map[string]T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	itemcc := make(map[string]VersionContainer[T], len(items))
	for k, v := range items {
		prevcc := m.items[k]
		prevcc.Resource = v
		itemcc[k] = prevcc
	}
	m.items = itemcc

	m.indicies = make(map[string]Index)
	for key, item := range items {
		m.reindex(key, nil, item)
	}
}

func (m *SyncIndexedMap[T]) List() []VersionContainer[T] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]VersionContainer[T], 0, len(m.items))
	for _, item := range m.items {
		list = append(list, item)
	}
	return list
}

func (m *SyncIndexedMap[T]) IndexValues(indexName string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	indexer := m.indexers[indexName]
	if indexer == nil {
		return nil, fmt.Errorf("Index %q does not exist", indexName)
	}
	index := m.indicies[indexName]
	values := make([]string, 0, len(index))
	for value := range index {
		values = append(values, value)
	}
	return values, nil
}

func (m *SyncIndexedMap[T]) Index(indexName string, obj T) ([]VersionContainer[T], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	indexer := m.indexers[indexName]
	if indexer == nil {
		return nil, fmt.Errorf("Index %q does not exist", indexName)
	}
	index := m.indicies[indexName]

	indexVals := indexer(obj)
	indexedKeys := make(KeySet)
	for _, indexVal := range indexVals {
		for key := range index[indexVal] {
			indexedKeys.Add(key)
		}
	}
	list := make([]VersionContainer[T], 0, len(indexedKeys))
	for key := range indexedKeys {
		list = append(list, m.items[key])
	}
	return list, nil
}

func (m *SyncIndexedMap[T]) reindex(key string, prev *T, curr T) {
	if prev != nil {
		m.unindex(key, *prev)
	}
	for name, indexer := range m.indexers {
		indexVals := indexer(curr)

		index := m.indicies[name]
		if index == nil {
			index = Index{}
			m.indicies[name] = index
		}
		for _, indexVal := range indexVals {
			set := index[indexVal]
			if set == nil {
				set = KeySet{}
				index[indexVal] = set
			}
			set.Add(key)
		}
	}

}

func (m *SyncIndexedMap[T]) unindex(key string, obj T) {
	for name, indexer := range m.indexers {
		indexVals := indexer(obj)

		index := m.indicies[name]
		if index == nil {
			continue
		}
		for _, indexVal := range indexVals {
			set := index[indexVal]
			if set != nil {
				set.Remove(key)
				if len(set) == 0 {
					delete(index, indexVal)
				}
			}
		}
	}
}

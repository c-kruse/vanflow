package internal

type Indexer[T any] func(T) []string
type Index map[string]KeySet
type KeySet map[string]struct{}

func (k KeySet) Add(s string) {
	k[s] = struct{}{}
}

func (k KeySet) Remove(s string) bool {
	_, exists := k[s]
	if exists {
		delete(k, s)
	}
	return exists
}

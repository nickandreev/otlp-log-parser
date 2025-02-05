package aggregator

import (
	"sync"
)

type Aggregator interface {
	AddToKey(key string, val int)
	SnapshotAndReset() map[string]int
}

type simpleSyncCounterAggregator struct {
	mu     sync.Mutex
	values map[string]int
}

// NewSimpleSyncCounterAggregator creates a new simple counter aggregator that is thread-safe
func NewSimpleSyncCounterAggregator() Aggregator {
	return &simpleSyncCounterAggregator{
		values: make(map[string]int),
	}
}

func (a *simpleSyncCounterAggregator) AddToKey(key string, val int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.values[key] += val
}

func (a *simpleSyncCounterAggregator) SnapshotAndReset() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot := make(map[string]int, len(a.values))
	for k, v := range a.values {
		snapshot[k] = v
	}

	// Reset the values map while still holding the lock
	a.values = make(map[string]int)

	return snapshot
}


package aggregator

import (
	"sync"
)

type Aggregator interface {
	AddToKey(key string, val int)
	ResetAll()
	Snapshot() map[string]int
}

type simpleSyncCounterAggregator struct {
	mu     sync.Mutex
	values map[string]int
}

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

func (a *simpleSyncCounterAggregator) Snapshot() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot := make(map[string]int, len(a.values))
	for k, v := range a.values {
		snapshot[k] = v
	}
	return snapshot
}

func (a *simpleSyncCounterAggregator) ResetAll() {
	a.mu.Lock()
	a.values = make(map[string]int)
	a.mu.Unlock()
}

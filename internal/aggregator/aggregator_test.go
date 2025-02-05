package aggregator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleAggregator_AddToKey(t *testing.T) {
	tests := []struct {
		name string
		adds []struct {
			key   string
			value int
		}
		expected map[string]int
	}{
		{
			name: "single value",
			adds: []struct {
				key   string
				value int
			}{
				{key: "foo", value: 1},
			},
			expected: map[string]int{
				"foo": 1,
			},
		},
		{
			name: "multiple values same key",
			adds: []struct {
				key   string
				value int
			}{
				{key: "foo", value: 1},
				{key: "foo", value: 2},
				{key: "foo", value: 3},
			},
			expected: map[string]int{
				"foo": 6,
			},
		},
		{
			name: "multiple keys",
			adds: []struct {
				key   string
				value int
			}{
				{key: "foo", value: 1},
				{key: "bar", value: 2},
				{key: "baz", value: 3},
			},
			expected: map[string]int{
				"foo": 1,
				"bar": 2,
				"baz": 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := NewSimpleSyncCounterAggregator()
			for _, add := range tt.adds {
				agg.AddToKey(add.key, add.value)
			}
			snapshot := agg.SnapshotAndReset()
			assert.Equal(t, tt.expected, snapshot)

			// Verify reset happened
			emptySnapshot := agg.SnapshotAndReset()
			assert.Empty(t, emptySnapshot)
		})
	}
}

func TestSimpleAggregator_SnapshotAndReset(t *testing.T) {
	agg := NewSimpleSyncCounterAggregator()

	// Add some values
	agg.AddToKey("foo", 1)
	agg.AddToKey("bar", 2)

	// Take snapshot and reset
	snapshot := agg.SnapshotAndReset()
	assert.Equal(t, map[string]int{
		"foo": 1,
		"bar": 2,
	}, snapshot)

	// Verify values were reset
	emptySnapshot := agg.SnapshotAndReset()
	assert.Empty(t, emptySnapshot)

	// Add new values after reset
	agg.AddToKey("baz", 3)
	newSnapshot := agg.SnapshotAndReset()
	assert.Equal(t, map[string]int{
		"baz": 3,
	}, newSnapshot)
}

func TestSimpleAggregator_Concurrent(t *testing.T) {
	agg := NewSimpleSyncCounterAggregator()
	var wg sync.WaitGroup

	// Launch multiple goroutines to add values concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agg.AddToKey("concurrent", 1)
		}()
	}

	wg.Wait()

	// Verify the final count
	snapshot := agg.SnapshotAndReset()
	assert.Equal(t, 100, snapshot["concurrent"])
}

func TestSimpleAggregator_ConcurrentSnapshotAndReset(t *testing.T) {
	agg := NewSimpleSyncCounterAggregator()
	var wg sync.WaitGroup

	// Start goroutine to continuously add values
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			agg.AddToKey("key", 1)
		}
	}()

	// Take snapshots while values are being added
	var totalCount int
	for i := 0; i < 10; i++ {
		snapshot := agg.SnapshotAndReset()
		totalCount += snapshot["key"]
	}

	wg.Wait()

	// One final snapshot to get any remaining values
	finalSnapshot := agg.SnapshotAndReset()
	totalCount += finalSnapshot["key"]

	// Verify we counted all 1000 additions
	assert.Equal(t, 1000, totalCount)
}

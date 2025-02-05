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
			assert.Equal(t, tt.expected, agg.Snapshot())
		})
	}
}

func TestSimpleAggregator_ResetAll(t *testing.T) {
	agg := NewSimpleSyncCounterAggregator()

	// Add some values
	agg.AddToKey("foo", 1)
	agg.AddToKey("bar", 2)

	// Verify values were added
	snapshot := agg.Snapshot()
	assert.Equal(t, 1, snapshot["foo"])
	assert.Equal(t, 2, snapshot["bar"])

	// Reset all values
	agg.ResetAll()

	// Verify all values are zero
	snapshot = agg.Snapshot()
	assert.Equal(t, 0, snapshot["foo"])
	assert.Equal(t, 0, snapshot["bar"])
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
	snapshot := agg.Snapshot()
	assert.Equal(t, 100, snapshot["concurrent"])
}

func TestSimpleAggregator_Snapshot(t *testing.T) {
	agg := NewSimpleSyncCounterAggregator()

	// Add initial values
	agg.AddToKey("foo", 1)
	agg.AddToKey("bar", 2)

	// Take snapshot
	snapshot1 := agg.Snapshot()

	// Modify aggregator after snapshot
	agg.AddToKey("foo", 10)

	// Verify snapshot remains unchanged
	assert.Equal(t, 1, snapshot1["foo"])
	assert.Equal(t, 2, snapshot1["bar"])

	// Verify aggregator has new values
	snapshot2 := agg.Snapshot()
	assert.Equal(t, 11, snapshot2["foo"])
	assert.Equal(t, 2, snapshot2["bar"])
}

// Copyright (c) 2021 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger/internal/storage/v1/api/samplingstore/model"
)

func withPopulatedSamplingStore(f func(samplingStore *SamplingStore, baseTime time.Time)) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	millisAfter := baseTime.Add(time.Millisecond * time.Duration(100))
	secondsAfter := baseTime.Add(time.Second * time.Duration(2))
	samplingStore := NewSamplingStore(3)
	// Buckets are inserted oldest-first, as InsertThroughput does at runtime.
	samplingStore.throughputs.add(&storedThroughput{
		throughput: []*model.Throughput{{Service: "svc-1", Operation: "op-1", Count: 1}},
		time:       baseTime,
	})
	samplingStore.throughputs.add(&storedThroughput{
		throughput: []*model.Throughput{{Service: "svc-1", Operation: "op-2", Count: 1}},
		time:       millisAfter,
	})
	samplingStore.throughputs.add(&storedThroughput{
		throughput: []*model.Throughput{{Service: "svc-2", Operation: "op-3", Count: 1}},
		time:       secondsAfter,
	})
	pQPS := &storedServiceOperationProbabilitiesAndQPS{
		hostname: "guntur38ab8928", probabilities: model.ServiceOperationProbabilities{"svc-1": {"op-1": 0.01}}, qps: model.ServiceOperationQPS{"svc-1": {"op-1": 10.0}}, time: baseTime,
	}
	samplingStore.probabilitiesAndQPS = pQPS
	f(samplingStore, baseTime)
}

func withMemorySamplingStore(f func(samplingStore *SamplingStore)) {
	f(NewSamplingStore(5))
}

func operations(throughputs []*model.Throughput) []string {
	ops := make([]string, len(throughputs))
	for i, t := range throughputs {
		ops[i] = t.Operation
	}
	return ops
}

func services(throughputs []*model.Throughput) []string {
	svcs := make([]string, len(throughputs))
	for i, t := range throughputs {
		svcs[i] = t.Service
	}
	return svcs
}

func TestInsertThroughtput(t *testing.T) {
	withMemorySamplingStore(func(samplingStore *SamplingStore) {
		start := time.Now().Add(-time.Millisecond) // ensure stored time is After(start)
		throughputs := []*model.Throughput{
			{Service: "my-svc", Operation: "op"},
			{Service: "our-svc", Operation: "op2"},
		}
		require.NoError(t, samplingStore.InsertThroughput(throughputs))
		// GetThroughput uses an exclusive lower bound, so on a coarse clock the
		// inserted entry can land exactly on start and get dropped — widen the window.
		ret, err := samplingStore.GetThroughput(start.Add(-time.Second), start.Add(time.Second))
		require.NoError(t, err)
		assert.Len(t, ret, 2)

		for i := range 10 {
			in := []*model.Throughput{
				{Service: fmt.Sprint("svc-", i), Operation: fmt.Sprint("op-", i)},
			}
			require.NoError(t, samplingStore.InsertThroughput(in))
		}
		assert.Equal(t, 5, samplingStore.throughputs.len())
	})
}

func TestGetThroughput(t *testing.T) {
	withPopulatedSamplingStore(func(samplingStore *SamplingStore, baseTime time.Time) {
		start := baseTime
		// Interval is (start, end]: op-2 at baseTime+100ms matches, op-1 at start
		// is excluded and op-3 past end is excluded.
		ret, err := samplingStore.GetThroughput(start, start.Add(time.Second))
		require.NoError(t, err)
		assert.Equal(t, []string{"op-2"}, operations(ret))

		// An empty (start == end) interval is rejected instead of returning empty.
		ret1, err := samplingStore.GetThroughput(start, start)
		require.ErrorIs(t, err, errThroughputRangeEmpty)
		assert.Nil(t, ret1)

		// Buckets come back newest-first: op-3 then op-2.
		ret2, err := samplingStore.GetThroughput(start, start.Add(time.Hour))
		require.NoError(t, err)
		assert.Equal(t, []string{"op-3", "op-2"}, operations(ret2))
	})
}

func TestGetThroughputRangeBoundaries(t *testing.T) {
	withPopulatedSamplingStore(func(store *SamplingStore, baseTime time.Time) {
		// End is inclusive: the bucket exactly at baseTime is returned.
		ret, err := store.GetThroughput(baseTime.Add(-time.Nanosecond), baseTime)
		require.NoError(t, err)
		assert.Equal(t, []string{"op-1"}, operations(ret))

		// Start is exclusive and end inclusive: bucket at baseTime dropped,
		// bucket at baseTime+2s (== end) kept, in newest-first order.
		ret, err = store.GetThroughput(baseTime, baseTime.Add(2*time.Second))
		require.NoError(t, err)
		assert.Equal(t, []string{"op-3", "op-2"}, operations(ret))

		// Window entirely after every bucket: no match, no error.
		ret, err = store.GetThroughput(baseTime.Add(time.Hour), baseTime.Add(2*time.Hour))
		require.NoError(t, err)
		assert.Empty(t, ret)

		// Window entirely before every bucket: no match, no error.
		ret, err = store.GetThroughput(baseTime.Add(-2*time.Hour), baseTime.Add(-time.Hour))
		require.NoError(t, err)
		assert.Empty(t, ret)
	})
}

func TestGetThroughputInvalidRange(t *testing.T) {
	withPopulatedSamplingStore(func(store *SamplingStore, baseTime time.Time) {
		ret, err := store.GetThroughput(baseTime.Add(time.Hour), baseTime)
		require.ErrorIs(t, err, errThroughputRangeReversed)
		assert.Nil(t, ret)

		ret, err = store.GetThroughput(baseTime, baseTime)
		require.ErrorIs(t, err, errThroughputRangeEmpty)
		assert.Nil(t, ret)
	})
}

func TestThroughputRingEvictionOrder(t *testing.T) {
	const capacity = 5
	store := NewSamplingStore(capacity)
	before := time.Now().Add(-time.Second)
	const inserts = 8
	for i := range inserts {
		in := []*model.Throughput{{Service: fmt.Sprintf("svc-%d", i), Operation: "op"}}
		require.NoError(t, store.InsertThroughput(in))
	}
	after := time.Now().Add(time.Second)

	assert.Equal(t, capacity, store.throughputs.len())
	// The oldest three buckets must be evicted and the surviving ones returned
	// newest-first.
	ret, err := store.GetThroughput(before, after)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc-7", "svc-6", "svc-5", "svc-4", "svc-3"}, services(ret))
}

func TestThroughputRingWrapAround(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ring := newThroughputRing(3)
	labels := []string{"a", "b", "c", "d", "e"}
	for i, label := range labels {
		ring.add(&storedThroughput{
			throughput: []*model.Throughput{{Service: label, Operation: "op"}},
			time:       baseTime.Add(time.Duration(i) * time.Minute),
		})
	}
	require.Equal(t, 3, ring.len())

	var got []string
	// Range covers every bucket; only the three newest survive, newest-first,
	// even though head has wrapped around the backing array more than once.
	ring.forEachInRange(baseTime.Add(-time.Second), baseTime.Add(time.Hour), func(bucket *storedThroughput) {
		got = append(got, bucket.throughput[0].Service)
	})
	assert.Equal(t, []string{"e", "d", "c"}, got)

	// A query ending at the oldest surviving bucket: end is inclusive and
	// older evicted buckets are not observable.
	got = got[:0]
	oldestSurviving := baseTime.Add(2 * time.Minute) // label "c"
	ring.forEachInRange(baseTime.Add(-time.Second), oldestSurviving, func(bucket *storedThroughput) {
		got = append(got, bucket.throughput[0].Service)
	})
	assert.Equal(t, []string{"c"}, got)
}

func TestThroughputRingZeroCapacity(t *testing.T) {
	store := NewSamplingStore(0)
	require.NoError(t, store.InsertThroughput([]*model.Throughput{{Service: "svc", Operation: "op"}}))
	assert.Equal(t, 0, store.throughputs.len())

	// Negative capacity is treated as zero instead of panicking on insert.
	store = NewSamplingStore(-1)
	require.NotPanics(t, func() {
		require.NoError(t, store.InsertThroughput([]*model.Throughput{{Service: "svc", Operation: "op"}}))
	})
	assert.Equal(t, 0, store.throughputs.len())

	now := time.Now()
	ret, err := store.GetThroughput(now.Add(-time.Second), now.Add(time.Second))
	require.NoError(t, err)
	assert.Empty(t, ret)

	// Invalid ranges are still rejected.
	_, err = store.GetThroughput(now.Add(time.Second), now.Add(-time.Second))
	require.ErrorIs(t, err, errThroughputRangeReversed)
	_, err = store.GetThroughput(now, now)
	require.ErrorIs(t, err, errThroughputRangeEmpty)
}

func TestInsertGetThroughputConcurrent(t *testing.T) {
	const capacity = 50
	store := NewSamplingStore(capacity)
	stop := make(chan struct{})
	var writers, readers sync.WaitGroup

	for range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if err := store.InsertThroughput([]*model.Throughput{{Service: "svc", Operation: "op", Count: 1}}); err != nil {
						t.Errorf("unexpected InsertThroughput error: %v", err)
						return
					}
				}
			}
		}()
	}

	var unexpectedErrors atomic.Int64
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := range 500 {
				if i%2 == 0 {
					now := time.Now()
					ret, err := store.GetThroughput(now.Add(-time.Hour), now.Add(time.Hour))
					if err != nil || len(ret) > capacity {
						unexpectedErrors.Add(1)
					}
				} else {
					// Invalid ranges must keep returning a clear error under contention.
					now := time.Now()
					var err error
					if i%4 == 1 {
						_, err = store.GetThroughput(now.Add(time.Hour), now.Add(-time.Hour))
					} else {
						_, err = store.GetThroughput(now, now)
					}
					if err == nil {
						unexpectedErrors.Add(1)
					}
				}
			}
		}()
	}

	// Wait for the bounded readers to finish, then stop the writers.
	readers.Wait()
	close(stop)
	writers.Wait()

	assert.Zero(t, unexpectedErrors.Load())
	assert.Equal(t, capacity, store.throughputs.len())
}

func TestInsertProbabilitiesAndQPS(t *testing.T) {
	withMemorySamplingStore(func(samplingStore *SamplingStore) {
		require.NoError(t, samplingStore.InsertProbabilitiesAndQPS("dell11eg843d", model.ServiceOperationProbabilities{"new-srv": {"op": 0.1}}, model.ServiceOperationQPS{"new-srv": {"op": 4}}))
		assert.NotEmpty(t, 1, samplingStore.probabilitiesAndQPS)
		// Only latest one is kept in memory
		require.NoError(t, samplingStore.InsertProbabilitiesAndQPS("lncol73", model.ServiceOperationProbabilities{"my-app": {"hello": 0.3}}, model.ServiceOperationQPS{"new-srv": {"op": 7}}))
		assert.InDelta(t, 0.3, samplingStore.probabilitiesAndQPS.probabilities["my-app"]["hello"], 0.01)
	})
}

func TestGetLatestProbability(t *testing.T) {
	withMemorySamplingStore(func(samplingStore *SamplingStore) {
		// No priod data
		ret, err := samplingStore.GetLatestProbabilities()
		require.NoError(t, err)
		assert.Empty(t, ret)
	})

	withPopulatedSamplingStore(func(samplingStore *SamplingStore, _ time.Time) {
		// With some pregenerated data
		ret, err := samplingStore.GetLatestProbabilities()
		require.NoError(t, err)
		assert.Equal(t, model.ServiceOperationProbabilities{"svc-1": {"op-1": 0.01}}, ret)
		require.NoError(t, samplingStore.InsertProbabilitiesAndQPS("utfhyolf", model.ServiceOperationProbabilities{"another-service": {"hello": 0.009}}, model.ServiceOperationQPS{"new-srv": {"op": 5}}))
		ret, _ = samplingStore.GetLatestProbabilities()
		assert.NotEqual(t, model.ServiceOperationProbabilities{"svc-1": {"op-1": 0.01}}, ret)
	})
}

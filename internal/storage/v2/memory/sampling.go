// Copyright (c) 2021 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"errors"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/internal/storage/v1/api/samplingstore/model"
)

var (
	errThroughputRangeReversed = errors.New("throughput time range is invalid: start is after end")
	errThroughputRangeEmpty    = errors.New("throughput time range is invalid: start equals end")
)

// SamplingStore is an in-memory store for sampling data
type SamplingStore struct {
	mu                  sync.RWMutex
	throughputs         throughputRing
	probabilitiesAndQPS *storedServiceOperationProbabilitiesAndQPS
}

type storedThroughput struct {
	throughput []*model.Throughput
	time       time.Time
}

type storedServiceOperationProbabilitiesAndQPS struct {
	hostname      string
	probabilities model.ServiceOperationProbabilities
	qps           model.ServiceOperationQPS
	time          time.Time
}

// NewSamplingStore creates an in-memory sampling store.
func NewSamplingStore(maxBuckets int) *SamplingStore {
	return &SamplingStore{throughputs: newThroughputRing(maxBuckets)}
}

// InsertThroughput implements samplingstore.Store#InsertThroughput.
func (ss *SamplingStore) InsertThroughput(throughput []*model.Throughput) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.throughputs.add(&storedThroughput{throughput: throughput, time: time.Now()})
	return nil
}

// GetThroughput implements samplingstore.Store#GetThroughput. The interval is
// half-open: buckets with start < t <= end are returned, in newest-first order.
func (ss *SamplingStore) GetThroughput(start, end time.Time) ([]*model.Throughput, error) {
	switch {
	case start.After(end):
		return nil, errThroughputRangeReversed
	case start.Equal(end):
		return nil, errThroughputRangeEmpty
	}
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	var retSlice []*model.Throughput
	ss.throughputs.forEachInRange(start, end, func(bucket *storedThroughput) {
		retSlice = append(retSlice, bucket.throughput...)
	})
	return retSlice, nil
}

// InsertProbabilitiesAndQPS implements samplingstore.Store#InsertProbabilitiesAndQPS.
func (ss *SamplingStore) InsertProbabilitiesAndQPS(
	hostname string,
	probabilities model.ServiceOperationProbabilities,
	qps model.ServiceOperationQPS,
) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.probabilitiesAndQPS = &storedServiceOperationProbabilitiesAndQPS{hostname, probabilities, qps, time.Now()}
	return nil
}

// GetLatestProbabilities implements samplingstore.Store#GetLatestProbabilities.
func (ss *SamplingStore) GetLatestProbabilities() (model.ServiceOperationProbabilities, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if ss.probabilitiesAndQPS != nil {
		return ss.probabilitiesAndQPS.probabilities, nil
	}
	return model.ServiceOperationProbabilities{}, nil
}

// throughputRing is a fixed-capacity ring buffer of throughput buckets. The
// newest bucket is at head and buckets are visited newest-first; insertion
// writes a single slot and never moves the stored buckets. Once the ring is
// full, each insertion overwrites the oldest bucket in place.
type throughputRing struct {
	buckets []*storedThroughput
	// head is the index of the newest bucket.
	head int
	// size is the number of buckets currently held, up to len(buckets).
	size int
}

func newThroughputRing(capacity int) throughputRing {
	if capacity < 0 {
		capacity = 0
	}
	return throughputRing{buckets: make([]*storedThroughput, capacity)}
}

func (r *throughputRing) len() int {
	return r.size
}

// add inserts bucket as the newest entry. When the ring is full the oldest
// bucket is evicted by being overwritten in place.
func (r *throughputRing) add(bucket *storedThroughput) {
	if len(r.buckets) == 0 {
		return
	}
	if r.size > 0 {
		r.head = (r.head - 1 + len(r.buckets)) % len(r.buckets)
	}
	r.buckets[r.head] = bucket
	if r.size < len(r.buckets) {
		r.size++
	}
}

// forEachInRange invokes fn for each bucket whose time falls in the half-open
// interval (start, end], visiting buckets newest-first.
//
// Buckets are inserted in non-decreasing time order, so once a bucket is not
// after start every remaining (older) bucket is out of range as well and the
// scan stops.
func (r *throughputRing) forEachInRange(start, end time.Time, fn func(*storedThroughput)) {
	for i := range r.size {
		bucket := r.buckets[(r.head+i)%len(r.buckets)]
		if !bucket.time.After(start) {
			return
		}
		// end is inclusive.
		if !bucket.time.After(end) {
			fn(bucket)
		}
	}
}

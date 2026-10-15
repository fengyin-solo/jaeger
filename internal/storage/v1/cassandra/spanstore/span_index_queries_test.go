// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package spanstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger/internal/storage/cassandra/mocks"
	"github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
)

func traceID(b byte) dbmodel.TraceID {
	return dbmodel.TraceID{15: b}
}

// tagScanQuery mocks a query whose iterator scans the given (trace, span) identity rows.
func tagScanQuery(t *testing.T, rows []SpanIdentity, closeErr error) *mocks.Query {
	t.Helper()
	iter := &mocks.Iterator{}
	remaining := append([]SpanIdentity(nil), rows...)
	for range rows {
		iter.On("Scan", mock.Anything).Run(func(args mock.Arguments) {
			require.NotEmpty(t, remaining)
			dests := args.Get(0).([]any)
			*dests[0].(*dbmodel.TraceID) = remaining[0].TraceID
			*dests[1].(*int64) = remaining[0].SpanID
			remaining = remaining[1:]
		}).Return(true).Once()
	}
	iter.On("Scan", mock.Anything).Return(false)
	iter.On("Close").Return(closeErr)
	query := &mocks.Query{}
	query.On("PageSize", 0).Return(query)
	query.On("Iter").Return(iter)
	query.On("String").Return("queryString")
	return query
}

func traceIDScanQuery(ids []dbmodel.TraceID, closeErr error) *mocks.Query {
	iter := &mocks.Iterator{}
	remaining := append([]dbmodel.TraceID(nil), ids...)
	for range ids {
		iter.On("Scan", mock.Anything).Run(func(args mock.Arguments) {
			dests := args.Get(0).([]any)
			*dests[0].(*dbmodel.TraceID) = remaining[0]
			remaining = remaining[1:]
		}).Return(true).Once()
	}
	iter.On("Scan", mock.Anything).Return(false)
	iter.On("Close").Return(closeErr)
	query := &mocks.Query{}
	query.On("PageSize", 0).Return(query)
	query.On("Iter").Return(iter)
	query.On("String").Return("queryString")
	return query
}

func emptyTraceIDQuery() *mocks.Query {
	iter := &mocks.Iterator{}
	iter.On("Scan", mock.Anything).Return(false)
	iter.On("Close").Return(nil)
	query := &mocks.Query{}
	query.On("PageSize", 0).Return(query)
	query.On("Iter").Return(iter)
	query.On("String").Return("queryString")
	return query
}

func TestFindSpanIDsByTag(t *testing.T) {
	t.Run("complete scan", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			rows := []SpanIdentity{
				{TraceID: traceID(1), SpanID: 10},
				{TraceID: traceID(2), SpanID: 20},
			}
			r.session.On("Query", querySpanIDsByTag, mock.Anything).
				Return(tagScanQuery(t, rows, nil))
			got, reached, err := r.reader.FindSpanIDsByTag(context.Background(), TagIndexQuery{
				ServiceName:  "svc",
				TagKey:       "k",
				TagValue:     "v",
				StartTimeMin: time.Unix(100, 0),
				StartTimeMax: time.Unix(200, 0),
				Limit:        3,
			})
			require.NoError(t, err)
			assert.False(t, reached)
			assert.Equal(t, rows, got)
		})
	})

	t.Run("scan cut at limit", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			rows := []SpanIdentity{
				{TraceID: traceID(1), SpanID: 10},
				{TraceID: traceID(2), SpanID: 20},
			}
			r.session.On("Query", querySpanIDsByTag, mock.Anything).
				Return(tagScanQuery(t, rows, nil))
			got, reached, err := r.reader.FindSpanIDsByTag(context.Background(), TagIndexQuery{
				ServiceName:  "svc",
				TagKey:       "k",
				TagValue:     "v",
				StartTimeMin: time.Unix(100, 0),
				StartTimeMax: time.Unix(200, 0),
				Limit:        1,
			})
			require.NoError(t, err)
			assert.True(t, reached)
			assert.Equal(t, []SpanIdentity{rows[0]}, got)
		})
	})

	t.Run("scan error", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			r.session.On("Query", querySpanIDsByTag, mock.Anything).
				Return(tagScanQuery(t, nil, errors.New("boom")))
			got, reached, err := r.reader.FindSpanIDsByTag(context.Background(), TagIndexQuery{
				ServiceName:  "svc",
				TagKey:       "k",
				TagValue:     "v",
				StartTimeMin: time.Unix(100, 0),
				StartTimeMax: time.Unix(200, 0),
				Limit:        1,
			})
			require.ErrorContains(t, err, "boom")
			assert.Nil(t, got)
			assert.False(t, reached)
		})
	})
}

func TestFindTraceIDsByServiceIndex(t *testing.T) {
	t.Run("operation index complete", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			r.session.On("Query", queryByServiceAndOperationName, mock.Anything).
				Return(traceIDScanQuery([]dbmodel.TraceID{traceID(1), traceID(2)}, nil))
			got, reached, err := r.reader.FindTraceIDsByServiceIndex(context.Background(), ServiceIndexQuery{
				ServiceName:   "svc",
				OperationName: "op",
				StartTimeMin:  time.Unix(100, 0),
				StartTimeMax:  time.Unix(200, 0),
				Limit:         3,
			})
			require.NoError(t, err)
			assert.False(t, reached)
			assert.Equal(t, []dbmodel.TraceID{traceID(1), traceID(2)}, got)
		})
	})

	t.Run("operation index cut at limit", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			r.session.On("Query", queryByServiceAndOperationName, mock.Anything).
				Return(traceIDScanQuery([]dbmodel.TraceID{traceID(1), traceID(2)}, nil))
			got, reached, err := r.reader.FindTraceIDsByServiceIndex(context.Background(), ServiceIndexQuery{
				ServiceName:   "svc",
				OperationName: "op",
				StartTimeMin:  time.Unix(100, 0),
				StartTimeMax:  time.Unix(200, 0),
				Limit:         1,
			})
			require.NoError(t, err)
			assert.True(t, reached)
			assert.Len(t, got, 1)
		})
	})

	t.Run("service name index merges buckets", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			for bucket := range defaultNumBuckets {
				var ids []dbmodel.TraceID
				switch bucket {
				case 0:
					ids = []dbmodel.TraceID{traceID(1), traceID(2)}
				case 7:
					ids = []dbmodel.TraceID{traceID(2), traceID(3)}
				default:
					// empty bucket
				}
				r.session.On("Query", queryTraceIDsByServiceNameBucket, bucketMatcher(bucket)).
					Return(traceIDScanQuery(ids, nil)).Once()
			}
			got, reached, err := r.reader.FindTraceIDsByServiceIndex(context.Background(), ServiceIndexQuery{
				ServiceName:  "svc",
				StartTimeMin: time.Unix(100, 0),
				StartTimeMax: time.Unix(200, 0),
				Limit:        10,
			})
			require.NoError(t, err)
			assert.False(t, reached)
			assert.Equal(t, []dbmodel.TraceID{traceID(1), traceID(2), traceID(3)}, got)
		})
	})

	t.Run("service name index cut at limit", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			for bucket := range defaultNumBuckets {
				var q *mocks.Query
				switch bucket {
				case 3:
					// One bucket returns more rows than the limit, which the union must report.
					q = traceIDScanQuery([]dbmodel.TraceID{traceID(1), traceID(2)}, nil)
				default:
					q = emptyTraceIDQuery()
				}
				r.session.On("Query", queryTraceIDsByServiceNameBucket, bucketMatcher(bucket)).
					Return(q).Once()
			}
			got, reached, err := r.reader.FindTraceIDsByServiceIndex(context.Background(), ServiceIndexQuery{
				ServiceName:  "svc",
				StartTimeMin: time.Unix(100, 0),
				StartTimeMax: time.Unix(200, 0),
				Limit:        1,
			})
			require.NoError(t, err)
			assert.True(t, reached)
			assert.Len(t, got, 1)
		})
	})

	t.Run("operation index error", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			r.session.On("Query", queryByServiceAndOperationName, mock.Anything).
				Return(traceIDScanQuery(nil, errors.New("boom")))
			_, _, err := r.reader.FindTraceIDsByServiceIndex(context.Background(), ServiceIndexQuery{
				ServiceName:   "svc",
				OperationName: "op",
				StartTimeMin:  time.Unix(100, 0),
				StartTimeMax:  time.Unix(200, 0),
				Limit:         1,
			})
			require.ErrorContains(t, err, "boom")
		})
	})
}

func TestFindTraceIDsByDurationIndex(t *testing.T) {
	query := DurationIndexQuery{
		ServiceName:  "svc",
		StartTimeMin: time.Unix(3600, 0),
		StartTimeMax: time.Unix(3600*2+60, 0),
		DurationMin:  time.Second,
		DurationMax:  3 * time.Second,
		Limit:        5,
	}

	t.Run("complete scan over hourly buckets", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			buckets := []time.Time{time.Unix(3600*2, 0), time.Unix(3600, 0)}
			rows := map[time.Time][]dbmodel.TraceID{
				buckets[0]: {traceID(1)},
				buckets[1]: {traceID(1), traceID(2)},
			}
			for _, b := range buckets {
				r.session.On("Query", queryTraceIDsByDurationForSpans, timeBucketMatcher(b)).
					Return(traceIDScanQuery(rows[b], nil)).Once()
			}
			got, reached, err := r.reader.FindTraceIDsByDurationIndex(context.Background(), query)
			require.NoError(t, err)
			assert.False(t, reached)
			assert.Equal(t, []dbmodel.TraceID{traceID(1), traceID(2)}, got)
		})
	})

	t.Run("bucket cut at limit", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			buckets := []time.Time{time.Unix(3600*2, 0), time.Unix(3600, 0)}
			r.session.On("Query", queryTraceIDsByDurationForSpans, timeBucketMatcher(buckets[0])).
				Return(traceIDScanQuery([]dbmodel.TraceID{traceID(1)}, nil)).Once()
			// Six rows in the older bucket cut a limit of five.
			r.session.On("Query", queryTraceIDsByDurationForSpans, timeBucketMatcher(buckets[1])).
				Return(traceIDScanQuery([]dbmodel.TraceID{
					traceID(1), traceID(2), traceID(3), traceID(4), traceID(5), traceID(6),
				}, nil)).Once()
			got, reached, err := r.reader.FindTraceIDsByDurationIndex(context.Background(), query)
			require.NoError(t, err)
			assert.True(t, reached)
			assert.Len(t, got, 5)
		})
	})

	t.Run("bucket error", func(t *testing.T) {
		withSpanReader(t, func(r *spanReaderTest) {
			bucket := time.Unix(3600*2, 0)
			r.session.On("Query", queryTraceIDsByDurationForSpans, timeBucketMatcher(bucket)).
				Return(traceIDScanQuery(nil, errors.New("boom"))).Once()
			_, _, err := r.reader.FindTraceIDsByDurationIndex(context.Background(), query)
			require.ErrorContains(t, err, "boom")
		})
	})
}

// bucketMatcher matches the bound args of one service_name_index bucket query.
func bucketMatcher(bucket int) any {
	return mock.MatchedBy(func(args []any) bool {
		b, ok := args[0].(int)
		return ok && b == bucket
	})
}

// timeBucketMatcher matches the bound args of one duration_index bucket query.
func timeBucketMatcher(bucket time.Time) any {
	return mock.MatchedBy(func(args []any) bool {
		b, ok := args[0].(time.Time)
		return ok && b.Equal(bucket)
	})
}

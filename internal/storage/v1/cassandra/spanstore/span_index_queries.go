// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package spanstore

import (
	"context"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	"github.com/jaegertracing/jaeger/internal/storage/cassandra"
	casmetrics "github.com/jaegertracing/jaeger/internal/storage/cassandra/metrics"
	"github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
)

// Span-level index reads used by the v2 FindSpans implementation. Unlike FindTraceIDs, whose
// answer is a set of traces, these scans report whether their LIMIT cut the scan, so a caller can
// refuse a query rather than read the truncated answer as the whole one (RFC 0016 §9).

const (
	querySpanIDsByTag = `
		SELECT trace_id, span_id
		FROM tag_index
		WHERE service_name = ? AND tag_key = ? AND tag_value = ? and start_time > ? and start_time < ?
		ORDER BY start_time DESC
		LIMIT ?`

	queryTraceIDsByServiceNameBucket = `
		SELECT trace_id
		FROM service_name_index
		WHERE bucket = ? AND service_name = ? AND start_time > ? AND start_time < ?
		ORDER BY start_time DESC
		LIMIT ?`

	queryTraceIDsByDurationForSpans = `
		SELECT trace_id
		FROM duration_index
		WHERE bucket = ? AND service_name = ? AND operation_name = ? AND duration >= ? AND duration <= ?
		LIMIT ?`
)

// SpanIdentity is one (trace, span) pair the tag index resolves a tag predicate to.
type SpanIdentity struct {
	TraceID dbmodel.TraceID
	SpanID  int64
}

// TagIndexQuery selects span identities carrying one exact tag of one service
// within a time range.
type TagIndexQuery struct {
	ServiceName  string
	TagKey       string
	TagValue     string
	StartTimeMin time.Time
	StartTimeMax time.Time
	// Limit bounds the number of rows returned. The scan asks for one row more to tell a
	// truncated scan from one that ended exactly at the limit.
	Limit int
}

// FindSpanIDsByTag reads tag_index for one exact tag. The tag index records the span each row
// belongs to, so the answer has span granularity (RFC 0016 §9). limitReached reports that the
// scan was cut at Limit and the result may be incomplete.
func (s *SpanReader) FindSpanIDsByTag(ctx context.Context, q TagIndexQuery) ([]SpanIdentity, bool, error) {
	_, span := s.tracer.Start(ctx, "FindSpanIDsByTag")
	defer span.End()
	span.SetAttributes(
		attribute.Key("tag.key").String(q.TagKey),
		attribute.Key("tag.value").String(q.TagValue),
	)

	query := s.session.Query(
		querySpanIDsByTag,
		q.ServiceName,
		q.TagKey,
		q.TagValue,
		model.TimeAsEpochMicroseconds(q.StartTimeMin),
		model.TimeAsEpochMicroseconds(q.StartTimeMax),
		q.Limit+1,
	).PageSize(0)

	ids, limitReached, err := scanSpanIdentities(query, q.Limit)
	if err != nil {
		logErrorToSpan(span, err)
		s.logger.Error("Failed to exec query", zap.Error(err), zap.String("query", query.String()))
		return nil, false, err
	}
	return ids, limitReached, nil
}

// ServiceIndexQuery selects trace IDs of a service, optionally of one operation,
// within a time range.
type ServiceIndexQuery struct {
	ServiceName   string
	OperationName string
	StartTimeMin  time.Time
	StartTimeMax  time.Time
	// Limit bounds the distinct trace IDs returned.
	Limit int
}

// FindTraceIDsByServiceIndex scans the service index (service_name_index across every bucket,
// or service_operation_index when the query names an operation). limitReached reports that the
// LIMIT cut one of the partitions or that the distinct set is larger than Limit, in which case
// the answer may be incomplete.
func (s *SpanReader) FindTraceIDsByServiceIndex(ctx context.Context, q ServiceIndexQuery) ([]dbmodel.TraceID, bool, error) {
	if q.OperationName != "" {
		return s.findTraceIDsByServiceAndOperationIndex(ctx, q)
	}
	return s.findTraceIDsByServiceNameIndex(ctx, q)
}

func (s *SpanReader) findTraceIDsByServiceAndOperationIndex(ctx context.Context, q ServiceIndexQuery) ([]dbmodel.TraceID, bool, error) {
	_, sp := s.startSpanForQuery(ctx, "findTraceIDsByServiceAndOperationIndex", queryByServiceAndOperationName)
	defer sp.End()
	query := s.session.Query(
		queryByServiceAndOperationName,
		q.ServiceName,
		q.OperationName,
		model.TimeAsEpochMicroseconds(q.StartTimeMin),
		model.TimeAsEpochMicroseconds(q.StartTimeMax),
		q.Limit+1,
	).PageSize(0)
	ids, reached, err := scanTraceIDQuery(sp, query, s.metrics.queryServiceOperationIndex, q.Limit, s.logger)
	if err != nil {
		return nil, false, err
	}
	return ids, reached, nil
}

func (s *SpanReader) findTraceIDsByServiceNameIndex(ctx context.Context, q ServiceIndexQuery) ([]dbmodel.TraceID, bool, error) {
	_, sp := s.startSpanForQuery(ctx, "findTraceIDsByServiceNameIndex", queryTraceIDsByServiceNameBucket)
	defer sp.End()
	// The service index spreads a service's rows over defaultNumBuckets partitions, so every
	// bucket is scanned with its own limit. One truncated bucket, or a distinct set larger than
	// the limit, makes the union incomplete.
	var ids []dbmodel.TraceID
	limitReached := false
	for bucket := range defaultNumBuckets {
		query := s.session.Query(
			queryTraceIDsByServiceNameBucket,
			bucket,
			q.ServiceName,
			model.TimeAsEpochMicroseconds(q.StartTimeMin),
			model.TimeAsEpochMicroseconds(q.StartTimeMax),
			q.Limit+1,
		).PageSize(0)
		bucketIDs, bucketReached, err := scanTraceIDs(query, q.Limit)
		if err != nil {
			logErrorToSpan(sp, err)
			s.logger.Error("Failed to exec query", zap.Error(err), zap.String("query", query.String()))
			return nil, false, err
		}
		ids = append(ids, bucketIDs...)
		limitReached = limitReached || bucketReached
	}
	unique := dbmodel.UniqueTraceIDsFromList(ids)
	if len(unique) > q.Limit {
		limitReached = true
	}
	return traceIDsFromSet(unique, q.Limit), limitReached, nil
}

// DurationIndexQuery selects trace IDs of a service, optionally of one operation,
// whose duration falls within the inclusive bounds and whose start time falls in the range.
type DurationIndexQuery struct {
	ServiceName   string
	OperationName string
	StartTimeMin  time.Time
	StartTimeMax  time.Time
	DurationMin   time.Duration
	DurationMax   time.Duration
	Limit         int
}

// FindTraceIDsByDurationIndex scans duration_index over each hourly time bucket the range
// touches, the same walk queryByDuration does, but with an explicit completeness signal: a
// truncated bucket or a distinct set larger than Limit reports limitReached.
func (s *SpanReader) FindTraceIDsByDurationIndex(ctx context.Context, q DurationIndexQuery) ([]dbmodel.TraceID, bool, error) {
	_, sp := s.startSpanForQuery(ctx, "FindTraceIDsByDurationIndex", queryTraceIDsByDurationForSpans)
	defer sp.End()

	minDurationMicros := q.DurationMin.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	maxDurationMicros := (time.Hour * 24).Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	if q.DurationMax != 0 {
		maxDurationMicros = q.DurationMax.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	}

	startTimeByHour := q.StartTimeMin.Round(durationBucketSize)
	endTimeByHour := q.StartTimeMax.Round(durationBucketSize)

	var ids []dbmodel.TraceID
	limitReached := false
	for timeBucket := endTimeByHour; timeBucket.After(startTimeByHour) || timeBucket.Equal(startTimeByHour); timeBucket = timeBucket.Add(-durationBucketSize) {
		query := s.session.Query(
			queryTraceIDsByDurationForSpans,
			timeBucket,
			q.ServiceName,
			q.OperationName,
			minDurationMicros,
			maxDurationMicros,
			q.Limit+1,
		)
		bucketIDs, bucketReached, err := scanTraceIDs(query, q.Limit)
		if err != nil {
			logErrorToSpan(sp, err)
			s.logger.Error("Failed to exec query", zap.Error(err), zap.String("query", query.String()))
			return nil, false, err
		}
		ids = append(ids, bucketIDs...)
		limitReached = limitReached || bucketReached
	}
	unique := dbmodel.UniqueTraceIDsFromList(ids)
	if len(unique) > q.Limit {
		limitReached = true
	}
	return traceIDsFromSet(unique, q.Limit), limitReached, nil
}

// scanSpanIdentities scans (trace_id, span_id) rows. The query is expected to have asked for
// limit+1 rows: reaching that count says the scan was cut, and that extra row is dropped.
func scanSpanIdentities(query cassandra.Query, limit int) ([]SpanIdentity, bool, error) {
	iter := query.Iter()
	var ids []SpanIdentity
	var traceID dbmodel.TraceID
	var spanID int64
	for iter.Scan(&traceID, &spanID) {
		ids = append(ids, SpanIdentity{TraceID: traceID, SpanID: spanID})
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	reached := len(ids) > limit
	if reached {
		ids = ids[:limit]
	}
	return ids, reached, nil
}

// scanTraceIDs scans trace_id rows with the same limit+1 convention scanSpanIdentities uses.
func scanTraceIDs(query cassandra.Query, limit int) ([]dbmodel.TraceID, bool, error) {
	iter := query.Iter()
	var ids []dbmodel.TraceID
	var traceID dbmodel.TraceID
	for iter.Scan(&traceID) {
		ids = append(ids, traceID)
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	reached := len(ids) > limit
	if reached {
		ids = ids[:limit]
	}
	return ids, reached, nil
}

// scanTraceIDQuery is scanTraceIDs with the table metrics and error logging executeQuery uses.
func scanTraceIDQuery(
	sp trace.Span,
	query cassandra.Query,
	tableMetrics *casmetrics.Table,
	limit int,
	logger *zap.Logger,
) ([]dbmodel.TraceID, bool, error) {
	start := time.Now()
	ids, reached, err := scanTraceIDs(query, limit)
	tableMetrics.Emit(err, time.Since(start))
	if err != nil {
		logErrorToSpan(sp, err)
		logger.Error("Failed to exec query", zap.Error(err), zap.String("query", query.String()))
		return nil, false, err
	}
	return ids, reached, nil
}

// traceIDsFromSet returns at most limit trace IDs from a set, in a stable order.
func traceIDsFromSet(set dbmodel.UniqueTraceIDs, limit int) []dbmodel.TraceID {
	ids := make([]dbmodel.TraceID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b dbmodel.TraceID) int {
		if a.String() < b.String() {
			return -1
		}
		if a.String() > b.String() {
			return 1
		}
		return 0
	})
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	cspanstore "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore"
	cassdbmodel "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

var (
	errStartAndEndTimeNotSet      = errors.New("start and End Time must be set")
	errStartTimeMinGreaterThanMax = errors.New("start Time Minimum is above Maximum")
)

// Read bounds: a single FindSpans call never reads the whole keyspace. The index scans ask for
// at most maxIndexRowsPerRead rows, point-read at most maxIdentityTraces traces, and every other
// plan point-reads at most maxTracesPerRead distinct traces. A scan that would exceed a bound is
// refused with errors.ErrUnsupported instead of returning the capped answer as complete.
const (
	maxIndexRowsPerRead = int(tracestore.MaxSearchDepth)
	maxTracesPerRead    = int(tracestore.MaxSearchDepth)
	// An identity filter names its traces explicitly; RFC 0016 §14 Q1 leaves a filter-size bound
	// to the query boundary, and this bounds the point reads one such filter turns into.
	maxIdentityTraces = 100
	// maxTagPredicates bounds the tag index reads one query intersects.
	maxTagPredicates = 8
	// maxCursorBytes bounds the cursor wrapped in a page token.
	maxCursorBytes = 4096
)

// spanIndexReader is the span-granular index surface the v1 Cassandra reader exposes for
// FindSpans. Every method reports whether its LIMIT cut the scan, because this reader must
// refuse a query whose index answer may be incomplete rather than narrow it silently.
type spanIndexReader interface {
	FindSpanIDsByTag(ctx context.Context, q cspanstore.TagIndexQuery) ([]cspanstore.SpanIdentity, bool, error)
	FindTraceIDsByServiceIndex(ctx context.Context, q cspanstore.ServiceIndexQuery) ([]cassdbmodel.TraceID, bool, error)
	FindTraceIDsByDurationIndex(ctx context.Context, q cspanstore.DurationIndexQuery) ([]cassdbmodel.TraceID, bool, error)
}

// matchedSpan is a span that passed the full filter with its sort key and its own
// ResourceSpans envelope (FromDBModel gives every db span its own envelope).
type matchedSpan struct {
	key spanSortingKey
	rs  ptrace.ResourceSpans
}

// FindSpans returns one page of the spans matching query, in the effective order, with the
// token that resumes the search (RFC 0016).
//
// The Cassandra indexes select a bounded set of candidates with CQL, and the complete filter is
// evaluated against each candidate span read back, so an index the tag index shares across span,
// resource and event levels can over-fetch but can never answer a different question. A filter no
// index can bound, or a scan past the read caps, is refused with errors.ErrUnsupported so the
// caller falls back rather than reading a narrowed result.
func (r *TraceReader) FindSpans(
	ctx context.Context,
	query tracestore.SpanQueryParams,
) iter.Seq2[tracestore.PageChunk[ptrace.Traces], error] {
	return func(yield func(tracestore.PageChunk[ptrace.Traces], error) bool) {
		if r.finder == nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, fmt.Errorf(
				"this storage backend does not support span search: %w", errors.ErrUnsupported))
			return
		}
		order, err := tracestore.EffectiveSpanOrder(query.OrderBy)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}
		query.OrderBy = order
		fingerprint, err := query.Fingerprint()
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}
		var after *spanCursor
		if query.Pagination.PageToken != "" {
			raw, err := query.Pagination.PageToken.Cursor(fingerprint)
			if err != nil {
				yield(tracestore.PageChunk[ptrace.Traces]{}, err)
				return
			}
			if len(raw) > maxCursorBytes {
				yield(tracestore.PageChunk[ptrace.Traces]{}, fmt.Errorf(
					"%w: page token cursor exceeds %d bytes", tracestore.ErrPaginationInvalid, maxCursorBytes))
				return
			}
			cursor, err := decodeSpanCursor(raw, len(order))
			if err != nil {
				yield(tracestore.PageChunk[ptrace.Traces]{}, err)
				return
			}
			after = &cursor
		}

		prepared, err := prepareFilter(query.Filter)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}
		plan, err := planSpanSearch(prepared.call, query.StartTimeMin, query.StartTimeMax, r.tagFilter)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}

		matched, err := r.collectMatchingSpans(ctx, plan, prepared, query, order)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}
		compare := compareSpanSortingKeys(order)
		slices.SortStableFunc(matched, func(a, b matchedSpan) int { return compare(a.key, b.key) })

		pageSize := query.Pagination.PageSize
		if pageSize > tracestore.MaxPageSize {
			pageSize = tracestore.MaxPageSize
		}
		pageSpansList, next := pageSpans(matched, compare, after, pageSize)
		result := ptrace.NewTraces()
		for _, m := range pageSpansList {
			m.rs.MoveTo(result.ResourceSpans().AppendEmpty())
		}
		chunk := tracestore.PageChunk[ptrace.Traces]{Results: result}
		if next != nil {
			// The effective order holds four fixed-width intrinsic fields, so this cursor is
			// always well under maxCursorBytes by construction.
			token, err := tracestore.NewPageToken(fingerprint, next.encode())
			if err != nil {
				yield(tracestore.PageChunk[ptrace.Traces]{}, err)
				return
			}
			chunk.NextPageToken = token
		}
		yield(chunk, nil)
	}
}

// collectMatchingSpans executes the plan's index scan, reads the candidate traces, and evaluates
// the full filter against every candidate span in its time window.
func (r *TraceReader) collectMatchingSpans(
	ctx context.Context,
	plan spanPlan,
	prepared preparedFilter,
	query tracestore.SpanQueryParams,
	order []tracestore.SpanSortOrder,
) ([]matchedSpan, error) {
	switch plan.kind {
	case planEmpty:
		// a contradiction selects nothing
		return []matchedSpan{}, nil
	case planIdentity:
		if len(plan.traceIDs) > maxIdentityTraces {
			return nil, fmt.Errorf("%w: an identity filter names at most %d traces",
				tracestore.ErrFilterUnsupported, maxIdentityTraces)
		}
		return r.spansFromTraces(ctx, sortedTraceIDs(plan.traceIDs), nil, prepared, query, order)
	case planTag:
		pairs, err := r.scanTagIndex(ctx, plan, query)
		if err != nil {
			return nil, err
		}
		return r.spansFromPairs(ctx, pairs, prepared, query, order)
	case planDuration, planService:
		// Both indexes stop at trace granularity; they differ only in which scan runs.
		var (
			ids     []cassdbmodel.TraceID
			reached bool
			err     error
			index   string
		)
		if plan.kind == planDuration {
			index = "duration index"
			ids, reached, err = r.finder.FindTraceIDsByDurationIndex(ctx, cspanstore.DurationIndexQuery{
				ServiceName:   plan.service,
				OperationName: plan.operation,
				StartTimeMin:  query.StartTimeMin,
				StartTimeMax:  query.StartTimeMax,
				DurationMin:   plan.durationMin,
				DurationMax:   plan.durationMax,
				Limit:         maxIndexRowsPerRead,
			})
		} else {
			index = "service index"
			ids, reached, err = r.finder.FindTraceIDsByServiceIndex(ctx, cspanstore.ServiceIndexQuery{
				ServiceName:   plan.service,
				OperationName: plan.operation,
				StartTimeMin:  query.StartTimeMin,
				StartTimeMax:  query.StartTimeMax,
				Limit:         maxIndexRowsPerRead,
			})
		}
		if err != nil {
			return nil, err
		}
		if reached || len(ids) > maxTracesPerRead {
			return nil, errSearchWindowTooLarge(index)
		}
		return r.spansFromTraces(ctx, ids, nil, prepared, query, order)
	default:
		// planEmpty is handled above, and the planner produces no other plan kind.
		return nil, nil
	}
}

// scanTagIndex intersects the tag index reads of the plan's tag drivers on (trace ID, span ID)
// pairs, the same-span intersection RFC 0016 §9 describes.
func (r *TraceReader) scanTagIndex(
	ctx context.Context,
	plan spanPlan,
	query tracestore.SpanQueryParams,
) (map[cspanstore.SpanIdentity]struct{}, error) {
	drivers := dedupeTagDrivers(plan.tags)
	if len(drivers) > maxTagPredicates {
		return nil, fmt.Errorf("%w: at most %d tag predicates can be intersected",
			tracestore.ErrFilterUnsupported, maxTagPredicates)
	}
	var intersection map[cspanstore.SpanIdentity]struct{}
	for _, driver := range drivers {
		ids, reached, err := r.finder.FindSpanIDsByTag(ctx, cspanstore.TagIndexQuery{
			ServiceName:  plan.service,
			TagKey:       driver.key,
			TagValue:     driver.value,
			StartTimeMin: query.StartTimeMin,
			StartTimeMax: query.StartTimeMax,
			Limit:        maxIndexRowsPerRead,
		})
		if err != nil {
			return nil, err
		}
		if reached {
			return nil, errSearchWindowTooLarge("tag index")
		}
		set := make(map[cspanstore.SpanIdentity]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		if intersection == nil {
			intersection = set
			continue
		}
		for pair := range intersection {
			if _, ok := set[pair]; !ok {
				delete(intersection, pair)
			}
		}
		if len(intersection) == 0 {
			return intersection, nil
		}
	}
	return intersection, nil
}

// spansFromPairs loads the traces of the intersected tag-index pairs and evaluates the complete
// filter against exactly the spans the pairs name, at span granularity.
func (r *TraceReader) spansFromPairs(
	ctx context.Context,
	pairs map[cspanstore.SpanIdentity]struct{},
	prepared preparedFilter,
	query tracestore.SpanQueryParams,
	order []tracestore.SpanSortOrder,
) ([]matchedSpan, error) {
	byTrace := make(map[cassdbmodel.TraceID]map[int64]struct{})
	for pair := range pairs {
		spans, ok := byTrace[pair.TraceID]
		if !ok {
			spans = map[int64]struct{}{}
			byTrace[pair.TraceID] = spans
		}
		spans[pair.SpanID] = struct{}{}
	}
	if len(byTrace) > maxTracesPerRead {
		return nil, errSearchWindowTooLarge("tag index")
	}
	return r.spansFromTraces(ctx, sortedTraceIDSet(byTrace), byTrace, prepared, query, order)
}

// spansFromTraces reads the candidate traces and returns the spans that pass the time window and
// the complete filter. wantedSpans, when non-nil, restricts candidates to the span IDs the tag
// intersection resolved to; otherwise every span of each candidate trace is one.
func (r *TraceReader) spansFromTraces(
	ctx context.Context,
	traceIDs []cassdbmodel.TraceID,
	wantedSpans map[cassdbmodel.TraceID]map[int64]struct{},
	prepared preparedFilter,
	query tracestore.SpanQueryParams,
	order []tracestore.SpanSortOrder,
) ([]matchedSpan, error) {
	var matched []matchedSpan
	for _, traceID := range traceIDs {
		dbSpans, err := r.reader.GetTrace(ctx, traceID)
		if err != nil {
			return nil, err
		}
		if len(dbSpans) == 0 {
			continue
		}
		td := FromDBModel(dbSpans)
		resourceSpans := td.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			// FromDBModel gives each db span its own ResourceSpans envelope holding one scope
			// with that one span.
			rs := resourceSpans.At(i)
			scopeSpans := rs.ScopeSpans().At(0)
			span := scopeSpans.Spans().At(0)
			if wantedSpans != nil {
				if _, ok := wantedSpans[traceID][spanIDAsInt64(span.SpanID())]; !ok {
					continue
				}
			}
			if !withinTimeRange(span, query.StartTimeMin, query.StartTimeMax) {
				continue
			}
			evalCtx := filterCtx{
				resource: rs.Resource(),
				scope:    scopeSpans.Scope(),
				span:     span,
				regexps:  prepared.regexps,
			}
			if prepared.call != nil && !evalPredicate(prepared.call, evalCtx) {
				continue
			}
			matched = append(matched, matchedSpan{
				key: makeSpanSortingKey(evalCtx, order),
				rs:  rs,
			})
		}
	}
	return matched, nil
}

// spanIDAsInt64 converts an OTLP span ID to the signed int64 the Cassandra schema stores.
func spanIDAsInt64(id pcommon.SpanID) int64 {
	//nolint:gosec // G115: bit-preserving conversion, matching spanIDToDbSpanId in to_dbmodel.go.
	return int64(binary.BigEndian.Uint64(id[:]))
}

// withinTimeRange applies the query's time window to one span. A zero bound is unbounded.
func withinTimeRange(span ptrace.Span, startTimeMin, startTimeMax time.Time) bool {
	ts := span.StartTimestamp()
	if !startTimeMin.IsZero() && ts < pcommon.NewTimestampFromTime(startTimeMin) {
		return false
	}
	if !startTimeMax.IsZero() && ts > pcommon.NewTimestampFromTime(startTimeMax) {
		return false
	}
	return true
}

func errSearchWindowTooLarge(index string) error {
	return fmt.Errorf("%w: the %s scan exceeds the per-query read limit; narrow the time range or the filter",
		tracestore.Unsupported("span search exceeds the Cassandra read cap"), index)
}

// sortedTraceIDs returns trace IDs in a deterministic order, so a stable sort has repeatable
// input across identical queries.
func sortedTraceIDs(ids []cassdbmodel.TraceID) []cassdbmodel.TraceID {
	sorted := append([]cassdbmodel.TraceID(nil), ids...)
	slices.SortFunc(sorted, compareTraceIDs)
	return sorted
}

func sortedTraceIDSet(byTrace map[cassdbmodel.TraceID]map[int64]struct{}) []cassdbmodel.TraceID {
	ids := make([]cassdbmodel.TraceID, 0, len(byTrace))
	for id := range byTrace {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, compareTraceIDs)
	return ids
}

func compareTraceIDs(a, b cassdbmodel.TraceID) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// dedupeTagDrivers drops repeated identical tag predicates, preserving their first order.
func dedupeTagDrivers(drivers []tagDriver) []tagDriver {
	seen := make(map[string]bool, len(drivers))
	out := drivers[:0]
	for _, driver := range drivers {
		key := driver.key + "\x00" + driver.value
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, driver)
	}
	return out
}

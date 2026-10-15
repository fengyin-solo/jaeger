// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/clickhousetest"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/sql"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/tracestore/dbmodel"
)

func intVal(v int64) *expression.IntValue { return &expression.IntValue{Value: v} }
func durVal(d time.Duration) *expression.DurationValue {
	return &expression.DurationValue{Value: d}
}

func spanOrderTerm(name string, direction tracestore.SortDirection) tracestore.SpanSortOrder {
	return tracestore.SpanSortOrder{
		Expression: fieldRef(expression.LevelSpan, name),
		Direction:  direction,
	}
}

func minimalSpanRow(i int, start time.Time) *dbmodel.SpanRow {
	return &dbmodel.SpanRow{
		ID:          fmt.Sprintf("%016x", i),
		TraceID:     fmt.Sprintf("%032x", i),
		Name:        fmt.Sprintf("span-%d", i),
		Kind:        "internal",
		StartTime:   start,
		StatusCode:  "Unset",
		Duration:    int64(i) * 1000,
		ServiceName: "svc",
	}
}

// pageSpanIDs lists span IDs from a page, across however many ResourceSpans it groups.
func pageSpanIDs(page ptrace.Traces) []string {
	var ids []string
	for _, rs := range page.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				ids = append(ids, span.SpanID().String())
			}
		}
	}
	return ids
}

// collectSpanPage consumes a FindSpans iterator, which yields exactly one page or one error.
func collectSpanPage(
	t *testing.T,
	seq func(func(tracestore.PageChunk[ptrace.Traces], error) bool),
) (ptrace.Traces, tracestore.PageToken, error) {
	t.Helper()
	var (
		page    ptrace.Traces
		token   tracestore.PageToken
		yielded int
	)
	for chunk, err := range seq {
		if err != nil {
			return ptrace.Traces{}, "", err
		}
		yielded++
		page = chunk.Results
		token = chunk.NextPageToken
	}
	require.Equal(t, 1, yielded, "FindSpans yields exactly one page chunk")
	return page, token, nil
}

func spanQuery(pageSize uint32) tracestore.SpanQueryParams {
	return tracestore.SpanQueryParams{
		StartTimeMin: now.Add(-time.Hour),
		StartTimeMax: now.Add(time.Hour),
		Pagination:   tracestore.Pagination{PageSize: pageSize},
	}
}

func TestFindSpans_ReturnsPageAndNextToken(t *testing.T) {
	rows := []*dbmodel.SpanRow{
		minimalSpanRow(1, now),
		minimalSpanRow(2, now.Add(time.Second)),
		minimalSpanRow(3, now.Add(2*time.Second)),
	}
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{Data: rows, ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	page, token, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(2)))
	require.NoError(t, err)
	require.NotEmpty(t, token, "a full page with a probe row carries a continuation token")
	assert.Len(t, pageSpanIDs(page), 2)
	assert.Equal(t, 2, page.ResourceSpans().Len(), "every matched span keeps its own resource")

	// The page bound is pageSize plus one probe row; the first page skips nothing.
	require.Len(t, conn.RecordedQueries, 1)
	verifyQuerySnapshot(t, conn.RecordedQueries[0])
}

func TestFindSpans_LastPageHasEmptyToken(t *testing.T) {
	rows := []*dbmodel.SpanRow{
		minimalSpanRow(1, now),
		minimalSpanRow(2, now.Add(time.Second)),
	}
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{Data: rows, ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	page, token, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(10)))
	require.NoError(t, err)
	assert.Empty(t, token, "the last page carries an empty token")
	assert.Len(t, pageSpanIDs(page), 2)
}

func TestFindSpans_EmptyResult(t *testing.T) {
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	page, token, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(10)))
	require.NoError(t, err)
	assert.Empty(t, token)
	assert.Equal(t, 0, page.ResourceSpans().Len())
}

func TestFindSpans_PreservesRowOrder(t *testing.T) {
	rows := []*dbmodel.SpanRow{
		minimalSpanRow(3, now.Add(2*time.Second)),
		minimalSpanRow(1, now),
		minimalSpanRow(2, now.Add(time.Second)),
	}
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{Data: rows, ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	page, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(10)))
	require.NoError(t, err)
	// The reader returns rows in the engine's order without re-sorting in Go.
	assert.Equal(t, []string{
		fmt.Sprintf("%016x", 3), fmt.Sprintf("%016x", 1), fmt.Sprintf("%016x", 2),
	}, pageSpanIDs(page))
}

func TestFindSpans_OversizedPageIsClamped(t *testing.T) {
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{
					Data:   []*dbmodel.SpanRow{minimalSpanRow(1, now)},
					ScanFn: scanSpanRowFn(),
				},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	query := spanQuery(tracestore.MaxPageSize + 50)
	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.NoError(t, err)
	// The clamped LIMIT is the maximum page size plus its probe row.
	assert.Contains(t, conn.RecordedQueries[0], "LIMIT ? OFFSET ?")
}

func TestFindSpans_ZeroPageSizeIsInvalid(t *testing.T) {
	reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
	query := spanQuery(0)
	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
}

func TestFindSpans_QueryErrors(t *testing.T) {
	tests := []struct {
		name        string
		response    *clickhousetest.QueryResponse
		expectedErr string
	}{
		{
			name:        "query fails",
			response:    &clickhousetest.QueryResponse{Err: assert.AnError},
			expectedErr: "failed to query spans",
		},
		{
			name: "scan fails",
			response: &clickhousetest.QueryResponse{Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{
				Data:    []*dbmodel.SpanRow{minimalSpanRow(1, now)},
				ScanErr: assert.AnError,
			}},
			expectedErr: "failed to scan span row",
		},
		{
			name: "rows fail",
			response: &clickhousetest.QueryResponse{Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{
				RowsErr: assert.AnError,
			}},
			expectedErr: "failed to read span rows",
		},
		{
			name: "close fails",
			response: &clickhousetest.QueryResponse{Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{
				Data:     []*dbmodel.SpanRow{minimalSpanRow(1, now)},
				ScanFn:   scanSpanRowFn(),
				CloseErr: assert.AnError,
			}},
			expectedErr: "failed to close rows",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &clickhousetest.Driver{
				QueryResponses: map[string]*clickhousetest.QueryResponse{
					sql.SelectSpansQuery: tt.response,
				},
			}
			reader := NewReader(conn, testReaderConfig)
			_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(10)))
			require.ErrorContains(t, err, tt.expectedErr)
		})
	}
}

func TestFindSpans_BuiltinFieldsAndAttributesAreBound(t *testing.T) {
	tests := []struct {
		name       string
		filter     *expression.Call
		metadata   []dbmodel.AttributeMetadata
		wantColumn string
		wantArgs   []any
	}{
		{
			name:       "span name equality",
			filter:     call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldName), str("checkout")),
			wantColumn: "s.name = ?",
			wantArgs:   []any{"checkout"},
		},
		{
			name:       "span kind equality",
			filter:     call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldKind), str("server")),
			wantColumn: "s.kind = ?",
			wantArgs:   []any{"server"},
		},
		{
			name:       "span status equality",
			filter:     call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldStatus), str("error")),
			wantColumn: "s.status_code = ?",
			wantArgs:   []any{"Error"},
		},
		{
			name:       "span duration ordered comparison",
			filter:     call(expression.OpGte, fieldRef(expression.LevelSpan, expression.SpanFieldDuration), durVal(5*time.Millisecond)),
			wantColumn: "s.duration >= ?",
			wantArgs:   []any{int64(5 * time.Millisecond)},
		},
		{
			name:       "resource service equality",
			filter:     call(expression.OpEq, fieldRef(expression.LevelResource, expression.ResourceFieldService), str("cart")),
			wantColumn: "s.service_name = ?",
			wantArgs:   []any{"cart"},
		},
		{
			name:       "span attribute equality",
			filter:     call(expression.OpEq, attrRef(expression.LevelSpan, "http.method"), str("GET")),
			wantColumn: "s.str_attributes",
			wantArgs:   []any{"http.method", "GET"},
		},
		{
			name:       "resource attribute equality",
			filter:     call(expression.OpEq, attrRef(expression.LevelResource, "region"), str("us-west")),
			wantColumn: "s.resource_str_attributes",
			wantArgs:   []any{"region", "us-west"},
		},
		{
			name:       "scope attribute equality",
			filter:     call(expression.OpEq, attrRef(expression.LevelScope, "attempt"), intVal(3)),
			wantColumn: "s.scope_int_attributes",
			wantArgs:   []any{"attempt", int64(3)},
		},
		{
			name:       "event attribute equality",
			filter:     call(expression.OpEq, attrRef(expression.LevelEvent, "exception.type"), str("TimeoutError")),
			wantColumn: "s.events",
			wantArgs:   []any{"exception.type", "TimeoutError"},
		},
		{
			name:       "link attribute equality",
			filter:     call(expression.OpEq, attrRef(expression.LevelLink, "link.type"), str("follows_from")),
			wantColumn: "s.links",
			wantArgs:   []any{"link.type", "follows_from"},
		},
		{
			name: "untyped attribute resolved through metadata",
			filter: call(expression.OpEq, attrRef(expression.LevelSpan, "http.status_code"),
				&expression.AnyValue{Value: "500"}),
			metadata: []dbmodel.AttributeMetadata{
				{AttributeKey: "http.status_code", Type: "int", Level: "span"},
			},
			wantColumn: "s.int_attributes",
			wantArgs:   []any{"http.status_code", int64(500)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &clickhousetest.Driver{
				QueryResponses: map[string]*clickhousetest.QueryResponse{
					sql.SelectSpansQuery: {
						Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{ScanFn: scanSpanRowFn()},
					},
				},
			}
			if tt.metadata != nil {
				conn.QueryResponses[sql.SelectAttributeMetadata] = &clickhousetest.QueryResponse{
					Rows: &clickhousetest.Rows[dbmodel.AttributeMetadata]{
						Data:   tt.metadata,
						ScanFn: scanAttributeMetadataFn(),
					},
				}
			}
			reader := NewReader(conn, testReaderConfig)
			query := spanQuery(10)
			query.Filter = tt.filter

			_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
			require.NoError(t, err)
			spansQuery := conn.RecordedQueries[len(conn.RecordedQueries)-1]
			assert.Contains(t, spansQuery, tt.wantColumn)
			if value, ok := tt.wantArgs[len(tt.wantArgs)-1].(string); ok {
				assert.NotContains(t, spansQuery, value,
					"constants are bound, never spliced into the SQL text")
			}
		})
	}
}

func TestFindSpans_CombinedFilter(t *testing.T) {
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)
	query := spanQuery(10)
	query.Filter = call(expression.OpAnd,
		call(expression.OpOr,
			call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldName), str("checkout")),
			call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldStatus), str("error")),
		),
		call(expression.OpNot,
			call(expression.OpLt, fieldRef(expression.LevelSpan, expression.SpanFieldDuration), durVal(time.Millisecond)),
		),
	)

	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.NoError(t, err)
	require.Len(t, conn.RecordedQueries, 1)
	verifyQuerySnapshot(t, conn.RecordedQueries[0])
}

func TestFindSpans_InvalidFilterReturnsErrFilterInvalid(t *testing.T) {
	tests := []struct {
		name   string
		filter *expression.Call
	}{
		{
			name:   "status word that is not in the closed set",
			filter: call(expression.OpEq, fieldRef(expression.LevelSpan, expression.SpanFieldStatus), str("bogus")),
		},
		{
			name:   "boolean combinator with no arguments",
			filter: call(expression.OpAnd),
		},
		{
			name: "equality with wrong arity",
			filter: call(expression.OpEq,
				fieldRef(expression.LevelSpan, expression.SpanFieldName)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
			query := spanQuery(10)
			query.Filter = tt.filter
			_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
			require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
		})
	}
}

func TestFindSpans_UnsupportedFilterReturnsErrUnsupported(t *testing.T) {
	tests := []struct {
		name   string
		filter *expression.Call
	}{
		{
			name: "regex operator",
			filter: call(expression.OpRegex,
				fieldRef(expression.LevelSpan, expression.SpanFieldName), str("check.*")),
		},
		{
			name: "ordered comparison on an attribute",
			filter: call(expression.OpGt,
				attrRef(expression.LevelSpan, "retry.count"), intVal(10)),
		},
		{
			name: "unmapped built-in field",
			filter: call(expression.OpEq,
				fieldRef(expression.LevelSpan, expression.SpanFieldTraceID), str("0123")),
		},
		{
			name: "ordered comparison on a word field",
			filter: call(expression.OpGt,
				fieldRef(expression.LevelSpan, expression.SpanFieldKind), str("server")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
			query := spanQuery(10)
			query.Filter = tt.filter
			_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
			require.ErrorIs(t, err, tracestore.ErrFilterUnsupported)
		})
	}
}

func TestFindSpans_MetadataLookupError(t *testing.T) {
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectAttributeMetadata: {Err: assert.AnError},
		},
	}
	reader := NewReader(conn, testReaderConfig)
	query := spanQuery(10)
	query.Filter = call(expression.OpEq, attrRef(expression.LevelSpan, "k"), &expression.AnyValue{Value: "v"})
	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.ErrorContains(t, err, "failed to get attribute metadata")
}

func TestFindSpans_InvalidOrder(t *testing.T) {
	reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
	query := spanQuery(10)
	// span.name is a built-in field the filter knows but not a sortable field.
	query.OrderBy = []tracestore.SpanSortOrder{spanOrderTerm(expression.SpanFieldName, tracestore.SortAscending)}
	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.ErrorIs(t, err, tracestore.ErrSpanOrderInvalid)
}

func TestFindSpans_ExplicitOrderAppearsInSQL(t *testing.T) {
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)
	query := spanQuery(10)
	query.OrderBy = []tracestore.SpanSortOrder{
		spanOrderTerm(expression.SpanFieldDuration, tracestore.SortDescending),
	}

	_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.NoError(t, err)
	recorded := conn.RecordedQueries[0]
	assert.Contains(t, recorded, "s.duration DESC,")
	// EffectiveSpanOrder settles the tie-breakers.
	assert.Contains(t, recorded, "s.start_time DESC,")
	assert.Contains(t, recorded, "s.trace_id ASC,")
	assert.Contains(t, recorded, "s.id ASC")
}

func TestFindSpans_BadPageTokens(t *testing.T) {
	settled, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	base := spanQuery(10)
	base.OrderBy = settled // FindSpans hashes the order it settles, so mint against that one.
	fingerprint, err := base.Fingerprint()
	require.NoError(t, err)

	tests := []struct {
		name  string
		token func(t *testing.T) tracestore.PageToken
	}{
		{
			name:  "not base64",
			token: func(*testing.T) tracestore.PageToken { return "!!!not-base64!!!" },
		},
		{
			name: "belongs to a different query",
			token: func(t *testing.T) tracestore.PageToken {
				other := spanQuery(10)
				other.StartTimeMin = now.Add(-2 * time.Hour)
				otherFP, err := other.Fingerprint()
				require.NoError(t, err)
				token, err := tracestore.NewPageToken(otherFP, []byte{1, 2, 3, 4})
				require.NoError(t, err)
				return token
			},
		},
		{
			name: "cursor longer than the allowed maximum",
			token: func(t *testing.T) tracestore.PageToken {
				token, err := tracestore.NewPageToken(fingerprint, bytes.Repeat([]byte{1}, maxSpanCursorBytes+1))
				require.NoError(t, err)
				return token
			},
		},
		{
			name: "well-formed token wrapping a malformed cursor",
			token: func(t *testing.T) tracestore.PageToken {
				token, err := tracestore.NewPageToken(fingerprint, []byte{9, 9, 9, 9})
				require.NoError(t, err)
				return token
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
			query := spanQuery(10)
			query.Pagination.PageToken = tt.token(t)
			_, _, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
			require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
		})
	}
}

// TestFindSpans_ResumeTokenBindsKeyset verifies the continuation query is the first query plus
// a keyset predicate over the effective order, with every cursor value bound and the duplicate
// count as OFFSET.
func TestFindSpans_ResumeTokenBindsKeyset(t *testing.T) {
	// Page one: three rows for a page of two, so a token is minted.
	pageOneRows := []*dbmodel.SpanRow{
		minimalSpanRow(3, now.Add(2*time.Second)),
		minimalSpanRow(2, now.Add(time.Second)),
		minimalSpanRow(1, now),
	}
	conn := &clickhousetest.Driver{
		QueryResponses: map[string]*clickhousetest.QueryResponse{
			sql.SelectSpansQuery: {
				Rows: &clickhousetest.Rows[*dbmodel.SpanRow]{Data: pageOneRows, ScanFn: scanSpanRowFn()},
			},
		},
	}
	reader := NewReader(conn, testReaderConfig)

	page, token, err := collectSpanPage(t, reader.FindSpans(context.Background(), spanQuery(2)))
	require.NoError(t, err)
	require.NotEmpty(t, token)
	assert.Equal(t, []string{fmt.Sprintf("%016x", 3), fmt.Sprintf("%016x", 2)}, pageSpanIDs(page))

	// Page two: the same mock serves the continuation query.
	conn.QueryResponses[sql.SelectSpansQuery].Reset()
	pageTwoRows := []*dbmodel.SpanRow{minimalSpanRow(1, now)}
	conn.QueryResponses[sql.SelectSpansQuery].Rows = &clickhousetest.Rows[*dbmodel.SpanRow]{Data: pageTwoRows, ScanFn: scanSpanRowFn()}

	query := spanQuery(2)
	query.Pagination.PageToken = token
	page, nextToken, err := collectSpanPage(t, reader.FindSpans(context.Background(), query))
	require.NoError(t, err)
	assert.Empty(t, nextToken)
	assert.Equal(t, []string{fmt.Sprintf("%016x", 1)}, pageSpanIDs(page))

	resume := conn.RecordedQueries[1]
	assert.Contains(t, resume, "s.start_time < ?") // default start-time order is descending
	assert.Contains(t, resume, "s.trace_id > ?")   // the trace ID tie-breaker is ascending
	assert.Contains(t, resume, "LIMIT ? OFFSET ?")
}

func TestBuildFindSpansQuery_DefaultOrder(t *testing.T) {
	reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	query := tracestore.SpanQueryParams{
		StartTimeMin: time.Unix(0, 0),
		StartTimeMax: time.Unix(100, 0),
		Pagination:   tracestore.Pagination{PageSize: 10},
	}
	text, args, err := reader.buildFindSpansQuery(context.Background(), query, columns, nil, 10)
	require.NoError(t, err)
	verifyQuerySnapshot(t, text)
	assert.Equal(t, []any{time.Unix(0, 0), time.Unix(100, 0), uint32(11), uint32(0)}, args)
}

func TestBuildFindSpansQuery_KeysetResume(t *testing.T) {
	reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	after := &spanCursor{
		values: []any{now.UTC(), fmt.Sprintf("%032x", 2), fmt.Sprintf("%016x", 2)},
		seen:   1,
	}
	text, args, err := reader.buildFindSpansQuery(
		context.Background(), tracestore.SpanQueryParams{}, columns, after, 5)
	require.NoError(t, err)
	verifyQuerySnapshot(t, text)

	// Strict-after branches bind 1+2+3 values, the equal branch binds 3, then LIMIT and OFFSET.
	require.Len(t, args, 6+3+2)
	assert.Equal(t, uint32(6), args[len(args)-2])
	assert.Equal(t, uint32(1), args[len(args)-1])
}

func TestBuildFindSpansQuery_FilterAndExplicitOrder(t *testing.T) {
	reader := NewReader(&clickhousetest.Driver{}, testReaderConfig)
	order, err := tracestore.EffectiveSpanOrder([]tracestore.SpanSortOrder{
		spanOrderTerm(expression.SpanFieldDuration, tracestore.SortAscending),
	})
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	query := tracestore.SpanQueryParams{
		StartTimeMin: time.Unix(0, 0),
		StartTimeMax: time.Unix(100, 0),
		Filter: call(expression.OpAnd,
			call(expression.OpEq, fieldRef(expression.LevelResource, expression.ResourceFieldService), str("cart")),
			call(expression.OpLte, fieldRef(expression.LevelSpan, expression.SpanFieldDuration), durVal(time.Second)),
		),
	}
	text, args, err := reader.buildFindSpansQuery(context.Background(), query, columns, nil, 20)
	require.NoError(t, err)
	verifyQuerySnapshot(t, text)
	assert.Equal(t,
		[]any{time.Unix(0, 0), time.Unix(100, 0), "cart", int64(time.Second), uint32(21), uint32(0)},
		args)
}

func TestSpanCursor_RoundTrip(t *testing.T) {
	order, err := tracestore.EffectiveSpanOrder([]tracestore.SpanSortOrder{
		spanOrderTerm(expression.SpanFieldDuration, tracestore.SortDescending),
	})
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	original := spanCursor{
		values: []any{int64(12345), now.UTC(), fmt.Sprintf("%032x", 7), fmt.Sprintf("%016x", 9)},
		seen:   3,
	}
	raw, err := original.encode(columns)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(raw), maxSpanCursorBytes)

	decoded, err := decodeSpanCursor(raw, columns)
	require.NoError(t, err)
	assert.Equal(t, original.seen, decoded.seen)
	for i := range columns {
		assert.True(t, sameSpanSortValue(original.values[i], decoded.values[i]), "value %d", i)
	}
}

func TestSpanCursor_RejectsForeignOrTruncated(t *testing.T) {
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	durationOrder, err := tracestore.EffectiveSpanOrder([]tracestore.SpanSortOrder{
		spanOrderTerm(expression.SpanFieldDuration, tracestore.SortDescending),
	})
	require.NoError(t, err)
	durationColumns := spanOrderColumns(durationOrder)

	cursor := spanCursor{
		values: []any{now.UTC(), fmt.Sprintf("%032x", 2), fmt.Sprintf("%016x", 2)},
		seen:   1,
	}
	raw, err := cursor.encode(columns)
	require.NoError(t, err)

	tests := []struct {
		name    string
		raw     []byte
		columns []spanOrderColumn
	}{
		{"empty", nil, columns},
		{"too long", bytes.Repeat([]byte{1}, maxSpanCursorBytes+1), columns},
		{"term count mismatch", raw, durationColumns},
		{"truncated", raw[:len(raw)-2], columns},
		{"seen zero", func() []byte {
			c := spanCursor{
				values: []any{now.UTC(), fmt.Sprintf("%032x", 2), fmt.Sprintf("%016x", 2)},
				seen:   0,
			}
			b, err := c.encode(columns)
			require.NoError(t, err)
			return b
		}(), columns},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeSpanCursor(tt.raw, tt.columns)
			require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
		})
	}
}

// TestSpanOrderColumnsCoverSettledFields pins the invariant spanOrderColumns relies on:
// EffectiveSpanOrder may only admit fields the columns map.
func TestSpanOrderColumnsCoverSettledFields(t *testing.T) {
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)
	require.Len(t, columns, len(order))
	for i, col := range columns {
		assert.NotEmpty(t, col.column, "settled order field %q must map to a column", order[i].Expression.(*expression.FieldRef).Name)
	}
}

// TestSpanCursor_EncodeRejectsMalformed exercises every defensive refusal encode makes for a
// cursor whose values do not agree with the order it is encoded for.
func TestSpanCursor_EncodeRejectsMalformed(t *testing.T) {
	startCol := []spanOrderColumn{{field: expression.SpanFieldStartTime, ascending: false}}
	durationCol := []spanOrderColumn{{field: expression.SpanFieldDuration, ascending: true}}
	traceCol := []spanOrderColumn{{field: expression.SpanFieldTraceID, ascending: true}}
	bogusCol := []spanOrderColumn{{field: "bogus", ascending: true}}

	tests := []struct {
		name    string
		columns []spanOrderColumn
		values  []any
	}{
		{"value count mismatch", startCol, nil},
		{
			name:    "more than 255 terms",
			columns: slices.Repeat(startCol, 256),
			values:  slices.Repeat([]any{now.UTC()}, 256),
		},
		{"unmapped field tag", bogusCol, []any{now.UTC()}},
		{"time value on the duration term", durationCol, []any{now.UTC()}},
		{"integer value on the start-time term", startCol, []any{int64(1)}},
		{"string value on the start-time term", startCol, []any{"x"}},
		{"integer value on an ID term", traceCol, []any{int64(1)}},
		{"unsupported value type", durationCol, []any{int32(1)}},
		{
			"overlong ID",
			[]spanOrderColumn{{field: expression.SpanFieldSpanID, ascending: true}},
			[]any{strings.Repeat("x", 0x10000)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := spanCursor{values: tt.values, seen: 1}.encode(tt.columns)
			require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
		})
	}
}

// TestDecodeSpanCursor_CorruptBytes flips or truncates the encoded bytes at each structural
// position, covering the decoder's bounds and tag/direction checks.
func TestDecodeSpanCursor_CorruptBytes(t *testing.T) {
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)
	cursor := spanCursor{
		values: []any{now.UTC(), fmt.Sprintf("%032x", 2), fmt.Sprintf("%016x", 2)},
		seen:   1,
	}
	raw, err := cursor.encode(columns)
	require.NoError(t, err)

	tagMismatch := slices.Clone(raw)
	tagMismatch[1] = 99
	dirMismatch := slices.Clone(raw)
	dirMismatch[2] = spanCursorDirAsc // default start-time order is descending
	zeroStringLen := slices.Clone(raw)
	zeroStringLen[13], zeroStringLen[14] = 0, 0
	overrunString := slices.Clone(raw)
	overrunString[13], overrunString[14] = 0xFF, 0xFF
	seenZero := slices.Clone(raw)
	seenZero[len(seenZero)-1] = 0

	tests := []struct {
		name    string
		raw     []byte
		columns []spanOrderColumn
	}{
		{"tag mismatch", tagMismatch, columns},
		{"direction mismatch", dirMismatch, columns},
		{"truncated at the term header", raw[:2], columns},
		{"truncated inside the timestamp", raw[:5], columns},
		{"truncated at an ID length prefix", raw[:13], columns},
		{"ID length of zero", zeroStringLen, columns},
		{"ID length overruns the cursor", overrunString, columns},
		{"seen count zero", seenZero, columns},
		{
			"term the order does not carry",
			[]byte{1, 0, 1},
			[]spanOrderColumn{{field: "bogus", ascending: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeSpanCursor(tt.raw, tt.columns)
			require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
		})
	}
}

func TestSpanSortValue_UnknownField(t *testing.T) {
	_, err := spanSortValue(minimalSpanRow(1, now), "bogus")
	require.ErrorIs(t, err, tracestore.ErrSpanOrderUnsupported)
}

func TestSameSpanSortValue_TypeMismatch(t *testing.T) {
	assert.False(t, sameSpanSortValue(now.UTC(), int64(1)))
	assert.False(t, sameSpanSortValue(int64(1), "1"))
	assert.False(t, sameSpanSortValue(nil, nil))
	assert.True(t, sameSpanSortValue("a", "a"))
}

func TestNewSpanCursor_UnknownField(t *testing.T) {
	_, err := newSpanCursor(
		[]spanOrderColumn{{field: "bogus", ascending: true}},
		[]*dbmodel.SpanRow{minimalSpanRow(1, now)}, nil)
	require.ErrorIs(t, err, tracestore.ErrSpanOrderUnsupported)

	_, err = rowMatchesSortKey(minimalSpanRow(1, now),
		[]spanOrderColumn{{field: "bogus", ascending: true}}, []any{nil})
	require.ErrorIs(t, err, tracestore.ErrSpanOrderUnsupported)
}

// TestNewSpanCursor_AccumulatesSeenAcrossPages covers the previous-cursor accumulation when
// duplicate keys span more than one page boundary.
func TestNewSpanCursor_AccumulatesSeenAcrossPages(t *testing.T) {
	order, err := tracestore.EffectiveSpanOrder(nil)
	require.NoError(t, err)
	columns := spanOrderColumns(order)

	first := minimalSpanRow(7, now)
	duplicate := minimalSpanRow(7, now) // identical key: same time, trace ID and span ID
	duplicate.Name = "different name, same key"

	cursor, err := newSpanCursor(columns, []*dbmodel.SpanRow{first}, nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), cursor.seen)

	cursor, err = newSpanCursor(columns, []*dbmodel.SpanRow{duplicate}, &cursor)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), cursor.seen, "the copy the previous page counted accumulates")
}

func TestMintSpanPageToken_InvalidOrder(t *testing.T) {
	_, err := mintSpanPageToken(
		[]byte("fingerprint"),
		[]spanOrderColumn{{field: "bogus", ascending: true}},
		[]*dbmodel.SpanRow{minimalSpanRow(1, now)}, nil)
	require.ErrorIs(t, err, tracestore.ErrSpanOrderUnsupported)
}

// emulatedCHConn is a driver.Conn test double that evaluates the ORDER BY, keyset predicate,
// OFFSET and LIMIT of a FindSpans query against an in-memory span set, so multi-page flows can
// be driven end to end without a ClickHouse server. Filters are not evaluated: the fixtures are
// built so every row matches.
type emulatedCHConn struct {
	driver.Conn

	mu      sync.Mutex
	all     []*dbmodel.SpanRow
	scanFn  func(dest any, src *dbmodel.SpanRow) error
	queries []emulatedQuery
}

type emulatedQuery struct {
	limit  uint32
	offset uint32
}

var emulatedColumnFields = map[string]string{
	"s.start_time": expression.SpanFieldStartTime,
	"s.duration":   expression.SpanFieldDuration,
	"s.trace_id":   expression.SpanFieldTraceID,
	"s.id":         expression.SpanFieldSpanID,
}

func (c *emulatedCHConn) Query(_ context.Context, queryText string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	columns := parseEmulatedOrder(queryText)
	limit, offset := args[len(args)-2].(uint32), args[len(args)-1].(uint32)
	c.queries = append(c.queries, emulatedQuery{limit: limit, offset: offset})

	// A continuation always carries seen >= 1 as its OFFSET; the equal-key branch binds one
	// value per column immediately before the LIMIT/OFFSET binds.
	var key []any
	if offset > 0 {
		key = args[len(args)-2-len(columns) : len(args)-2]
	}

	rows := sortRowsByColumns(slices.Clone(c.all), columns)
	if key != nil {
		rows = slices.DeleteFunc(rows, func(row *dbmodel.SpanRow) bool {
			return compareRowWithKey(row, nil, columns, key) < 0
		})
		if int(offset) <= len(rows) {
			rows = rows[offset:]
		} else {
			rows = nil
		}
	}
	if int(limit) < len(rows) {
		rows = rows[:limit]
	}
	return &emulatedRows{data: slices.Clone(rows), scanFn: c.scanFn}, nil
}

type emulatedRows struct {
	driver.Rows
	data   []*dbmodel.SpanRow
	index  int
	scanFn func(dest any, src *dbmodel.SpanRow) error
}

func (r *emulatedRows) Next() bool {
	if r.index >= len(r.data) {
		return false
	}
	r.index++
	return true
}

func (r *emulatedRows) Scan(dest ...any) error {
	return r.scanFn(dest, r.data[r.index-1])
}

func (*emulatedRows) Close() error { return nil }
func (*emulatedRows) Err() error   { return nil }

func parseEmulatedOrder(queryText string) []spanOrderColumn {
	_, orderPart, found := strings.Cut(queryText, "ORDER BY")
	if !found {
		panic("emulated query has no ORDER BY")
	}
	var columns []spanOrderColumn
	for _, line := range strings.Split(orderPart, "\n")[1:] {
		line = strings.TrimSpace(strings.TrimSuffix(line, ","))
		if line == "" || strings.HasPrefix(line, "LIMIT") {
			break
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			panic(fmt.Sprintf("unexpected ORDER BY line %q", line))
		}
		field, ok := emulatedColumnFields[parts[0]]
		if !ok {
			panic(fmt.Sprintf("unknown sort column %q", parts[0]))
		}
		columns = append(columns, spanOrderColumn{
			field:     field,
			column:    parts[0],
			ascending: parts[1] == "ASC",
		})
	}
	return columns
}

func compareEmulatedValue(field string, a, b any) int {
	switch av := a.(type) {
	case time.Time:
		return av.Compare(b.(time.Time))
	case int64:
		return cmp.Compare(av, b.(int64))
	case string:
		return cmp.Compare(av, b.(string))
	default:
		panic(fmt.Sprintf("unsupported sort value type %T for %s", a, field))
	}
}

// compareRowWithKey compares a row either against another row (key == nil) or against bound
// key values, term by term, honoring each term's direction.
func compareRowWithKey(row, other *dbmodel.SpanRow, columns []spanOrderColumn, key []any) int {
	for i, col := range columns {
		v, err := spanSortValue(row, col.field)
		if err != nil {
			panic(err)
		}
		var otherValue any
		if key != nil {
			otherValue = key[i]
		} else {
			otherValue, err = spanSortValue(other, col.field)
			if err != nil {
				panic(err)
			}
		}
		c := compareEmulatedValue(col.field, v, otherValue)
		if !col.ascending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func sortRowsByColumns(rows []*dbmodel.SpanRow, columns []spanOrderColumn) []*dbmodel.SpanRow {
	sorted := slices.Clone(rows)
	slices.SortStableFunc(sorted, func(a, b *dbmodel.SpanRow) int {
		return compareRowWithKey(a, b, columns, nil)
	})
	return sorted
}

func pagingFixture(n int) []*dbmodel.SpanRow {
	rows := make([]*dbmodel.SpanRow, n)
	for i := range n {
		rows[i] = minimalSpanRow(i+1, now.Add(time.Duration(i)*time.Second))
	}
	return rows
}

// collectAllSpanPages pages through the whole result without test-framework calls, so it is
// safe to run inside a goroutine: it returns the span IDs of each page and stops at the empty
// token.
func collectAllSpanPages(
	reader *Reader,
	query tracestore.SpanQueryParams,
) ([][]string, []tracestore.PageToken, error) {
	var (
		pages  [][]string
		tokens []tracestore.PageToken
	)
	for range 100 {
		var (
			pageIDs []string
			token   tracestore.PageToken
			yielded int
		)
		for chunk, err := range reader.FindSpans(context.Background(), query) {
			if err != nil {
				return nil, nil, err
			}
			yielded++
			pageIDs = pageSpanIDs(chunk.Results)
			token = chunk.NextPageToken
		}
		if yielded != 1 {
			return nil, nil, errors.New("FindSpans yielded more than one page chunk")
		}
		pages = append(pages, pageIDs)
		tokens = append(tokens, token)
		if token == "" {
			return pages, tokens, nil
		}
		query.Pagination.PageToken = token
	}
	return nil, nil, errors.New("pagination did not terminate with an empty token")
}

// walkSpanPages is the test-goroutine wrapper around collectAllSpanPages.
func walkSpanPages(
	t *testing.T,
	reader *Reader,
	query tracestore.SpanQueryParams,
) ([][]string, []tracestore.PageToken) {
	t.Helper()
	pages, tokens, err := collectAllSpanPages(reader, query)
	require.NoError(t, err)
	return pages, tokens
}

func TestFindSpans_PagesThroughWholeResult(t *testing.T) {
	conn := &emulatedCHConn{all: pagingFixture(5), scanFn: scanSpanRowFn()}
	reader := NewReader(conn, testReaderConfig)

	pages, tokens := walkSpanPages(t, reader, spanQuery(2))

	require.Len(t, pages, 3)
	// Default order is start time descending, so IDs come back newest-first.
	assert.Equal(t, [][]string{
		{fmt.Sprintf("%016x", 5), fmt.Sprintf("%016x", 4)},
		{fmt.Sprintf("%016x", 3), fmt.Sprintf("%016x", 2)},
		{fmt.Sprintf("%016x", 1)},
	}, pages)
	assert.NotEmpty(t, tokens[0])
	assert.NotEmpty(t, tokens[1])
	assert.Empty(t, tokens[2])

	var all []string
	for _, page := range pages {
		all = append(all, page...)
	}
	assert.Len(t, all, 5, "every span is returned exactly once")
}

// TestFindSpans_PaginationIsStable repeats the same requests against unchanged data and pins
// that both the page sequences and the cursors connecting them are identical.
func TestFindSpans_PaginationIsStable(t *testing.T) {
	conn := &emulatedCHConn{all: pagingFixture(6), scanFn: scanSpanRowFn()}
	reader := NewReader(conn, testReaderConfig)

	firstPages, firstTokens := walkSpanPages(t, reader, spanQuery(2))
	secondPages, secondTokens := walkSpanPages(t, reader, spanQuery(2))
	assert.Equal(t, firstPages, secondPages)
	assert.Equal(t, firstTokens, secondTokens)

	// Re-issuing one middle page with the token it gave back returns that same page.
	middle := spanQuery(2)
	middle.Pagination.PageToken = firstTokens[0]
	page, token, err := collectSpanPage(t, reader.FindSpans(context.Background(), middle))
	require.NoError(t, err)
	assert.Equal(t, firstPages[1], pageSpanIDs(page))
	assert.Equal(t, firstTokens[1], token)
}

// TestFindSpans_DuplicateRowsStraddlingBoundary pins that two stored rows tied on the whole
// sort key (byte-identical duplicates a retried write can leave in a MergeTree) are both
// returned across a page boundary through the cursor's seen count.
func TestFindSpans_DuplicateRowsStraddlingBoundary(t *testing.T) {
	fixture := pagingFixture(4)
	duplicate := *fixture[2] // same trace ID, span ID, time and duration as the third row
	fixture = append(fixture, &duplicate)
	conn := &emulatedCHConn{all: fixture, scanFn: scanSpanRowFn()}
	reader := NewReader(conn, testReaderConfig)

	// Sorted newest-first the duplicate pair sits in positions 2-3, straddling a page of two:
	// [4, 3 | 3, 2 | 1].
	pages, tokens := walkSpanPages(t, reader, spanQuery(2))
	require.Len(t, pages, 3)
	assert.Equal(t, [][]string{
		{fmt.Sprintf("%016x", 4), fmt.Sprintf("%016x", 3)},
		{fmt.Sprintf("%016x", 3), fmt.Sprintf("%016x", 2)},
		{fmt.Sprintf("%016x", 1)},
	}, pages)

	var all []string
	for _, page := range pages {
		all = append(all, page...)
	}
	assert.Len(t, all, 5, "the duplicate is returned, and nothing is skipped")
	assert.Equal(t, fmt.Sprintf("%016x", 3), all[1])
	assert.Equal(t, fmt.Sprintf("%016x", 3), all[2])
	assert.Empty(t, tokens[len(tokens)-1])

	// The continuation skips the duplicate copy the first page returned.
	var offsets []uint32
	for _, recorded := range conn.queries {
		offsets = append(offsets, recorded.offset)
		assert.LessOrEqual(t, recorded.limit, uint32(maxSpanPageSize)+1,
			"no query reads more than one row past the maximum page size")
	}
	assert.Equal(t, []uint32{0, 1, 1}, offsets)
}

func TestFindSpans_ExplicitOrderPaging(t *testing.T) {
	conn := &emulatedCHConn{all: pagingFixture(5), scanFn: scanSpanRowFn()}
	reader := NewReader(conn, testReaderConfig)

	query := spanQuery(2)
	query.OrderBy = []tracestore.SpanSortOrder{
		spanOrderTerm(expression.SpanFieldDuration, tracestore.SortAscending),
	}
	pages, _ := walkSpanPages(t, reader, query)

	var all []string
	for _, page := range pages {
		all = append(all, page...)
	}
	// Duration grows with fixture index; ascending duration returns IDs ascending.
	assert.Equal(t, []string{
		fmt.Sprintf("%016x", 1), fmt.Sprintf("%016x", 2), fmt.Sprintf("%016x", 3),
		fmt.Sprintf("%016x", 4), fmt.Sprintf("%016x", 5),
	}, all)
}

func TestFindSpans_ConcurrentPagination(t *testing.T) {
	conn := &emulatedCHConn{all: pagingFixture(12), scanFn: scanSpanRowFn()}
	reader := NewReader(conn, testReaderConfig)

	expected, expectedTokens, err := collectAllSpanPages(reader, spanQuery(3))
	require.NoError(t, err)

	const goroutines = 8
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for range goroutines {
		wg.Go(func() {
			pages, tokens, err := collectAllSpanPages(reader, spanQuery(3))
			if err != nil {
				errCh <- err
				return
			}
			if !slices.EqualFunc(pages, expected, slices.Equal) {
				errCh <- errors.New("page sequence differs under concurrent paging")
				return
			}
			if !slices.Equal(tokens, expectedTokens) {
				errCh <- errors.New("token sequence differs under concurrent paging")
			}
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		assert.NoError(t, err)
	}
}

func TestFindSpans_CapabilitiesDeclareSpanSearch(t *testing.T) {
	caps, err := NewReader(&clickhousetest.Driver{}, testReaderConfig).SearchCapabilities(context.Background())
	require.NoError(t, err)
	assert.True(t, caps.SpanSearch)
	assert.True(t, caps.Paginated)
	assert.True(t, caps.SpanSorting)
	require.NotNil(t, caps.Filter)
	assert.ElementsMatch(t, []expression.Level{
		expression.LevelSpan, expression.LevelResource, expression.LevelScope,
		expression.LevelEvent, expression.LevelLink,
	}, caps.Filter.Levels)
	assert.ElementsMatch(t, []expression.Operator{
		expression.OpAnd, expression.OpOr, expression.OpNot,
		expression.OpEq, expression.OpGt, expression.OpLt, expression.OpGte, expression.OpLte,
	}, caps.Filter.Operators)
}

// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	cspanstore "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore"
	cassdbmodel "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
	spanmocks "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/mocks"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

var spanBaseTime = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type eventSpec struct {
	name  string
	attrs map[string]any
}

type linkSpec struct {
	traceHex string
	spanHex  string
}

type spanSpec struct {
	traceHex      string
	spanHex       string
	service       string
	name          string
	kind          ptrace.SpanKind
	status        ptrace.StatusCode
	startOffset   time.Duration
	duration      time.Duration
	spanAttrs     map[string]any
	resourceAttrs map[string]any
	scopeName     string
	scopeAttrs    map[string]any
	events        []eventSpec
	links         []linkSpec
}

func putAttr(m pcommon.Map, key string, value any) {
	switch v := value.(type) {
	case string:
		m.PutStr(key, v)
	case int:
		m.PutInt(key, int64(v))
	case int64:
		m.PutInt(key, v)
	case float64:
		m.PutDouble(key, v)
	case bool:
		m.PutBool(key, v)
	default:
		m.PutStr(key, fmt.Sprint(v))
	}
}

func putAttrs(m pcommon.Map, attrs map[string]any) {
	for k, v := range attrs {
		putAttr(m, k, v)
	}
}

// buildDBTraces converts specs into stored dbmodel spans grouped by trace.
func buildDBTraces(t *testing.T, specs []spanSpec) map[cassdbmodel.TraceID][]cassdbmodel.Span {
	t.Helper()
	byTrace := map[string][]spanSpec{}
	var order []string
	for i := range specs {
		spec := &specs[i]
		if _, ok := byTrace[spec.traceHex]; !ok {
			order = append(order, spec.traceHex)
		}
		byTrace[spec.traceHex] = append(byTrace[spec.traceHex], *spec)
	}
	out := map[cassdbmodel.TraceID][]cassdbmodel.Span{}
	for _, traceHex := range order {
		traceSpecs := byTrace[traceHex]
		td := ptrace.NewTraces()
		traceKey, err := parseTraceID(traceHex)
		require.NoError(t, err)
		for i := range traceSpecs {
			spec := &traceSpecs[i]
			// Every spec gets its own resource and scope envelope, so two spans of one trace can
			// carry different resources; the db conversion keeps one process per span.
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", spec.service)
			putAttrs(rs.Resource().Attributes(), spec.resourceAttrs)
			ss := rs.ScopeSpans().AppendEmpty()
			if spec.scopeName != "" {
				ss.Scope().SetName(spec.scopeName)
			}
			putAttrs(ss.Scope().Attributes(), spec.scopeAttrs)
			span := ss.Spans().AppendEmpty()
			span.SetTraceID(pcommon.TraceID(traceKey))
			span.SetSpanID(spanIDFromHex(t, spec.spanHex))
			span.SetName(spec.name)
			span.SetKind(spec.kind)
			start := spanBaseTime.Add(spec.startOffset)
			span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(spec.duration)))
			if spec.status != ptrace.StatusCodeUnset {
				span.Status().SetCode(spec.status)
			}
			putAttrs(span.Attributes(), spec.spanAttrs)
			for _, ev := range spec.events {
				event := span.Events().AppendEmpty()
				event.SetName(ev.name)
				event.SetTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Millisecond)))
				putAttrs(event.Attributes(), ev.attrs)
			}
			for _, lk := range lkList(spec.links) {
				link := span.Links().AppendEmpty()
				linkedTrace, err := parseTraceID(lk.traceHex)
				require.NoError(t, err)
				link.SetTraceID(pcommon.TraceID(linkedTrace))
				link.SetSpanID(spanIDFromHex(t, lk.spanHex))
			}
		}
		dbSpans := ToDBModel(td)
		require.NotEmpty(t, dbSpans)
		out[traceKey] = dbSpans
	}
	return out
}

func lkList(links []linkSpec) []linkSpec { return links }

func spanIDFromHex(t *testing.T, hex string) pcommon.SpanID {
	t.Helper()
	var id pcommon.SpanID
	b, err := parseTraceID(hex) // 16 hex chars parses into the low 8 bytes
	require.NoError(t, err)
	copy(id[:], b[8:])
	return id
}

func pair(t *testing.T, traceHex, spanHex string) cspanstore.SpanIdentity {
	t.Helper()
	id, err := parseTraceID(traceHex)
	require.NoError(t, err)
	spanID := spanIDFromHex(t, spanHex)
	return cspanstore.SpanIdentity{TraceID: id, SpanID: int64(binary.BigEndian.Uint64(spanID[:]))}
}

// fakeFinder is a scripted span index reader.
type fakeFinder struct {
	mu sync.Mutex

	tagRows    map[string][]cspanstore.SpanIdentity
	tagReached map[string]bool
	tagQueries []cspanstore.TagIndexQuery

	serviceRows    map[string][]cassdbmodel.TraceID
	serviceReached bool
	serviceQuery   *cspanstore.ServiceIndexQuery

	durationRows    []cassdbmodel.TraceID
	durationReached bool
	durationQuery   *cspanstore.DurationIndexQuery
}

func tagKey(q cspanstore.TagIndexQuery) string {
	return q.ServiceName + "|" + q.TagKey + "|" + q.TagValue
}

func (f *fakeFinder) FindSpanIDsByTag(_ context.Context, q cspanstore.TagIndexQuery) ([]cspanstore.SpanIdentity, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tagQueries = append(f.tagQueries, q)
	return f.tagRows[tagKey(q)], f.tagReached[tagKey(q)], nil
}

func (f *fakeFinder) FindTraceIDsByServiceIndex(_ context.Context, q cspanstore.ServiceIndexQuery) ([]cassdbmodel.TraceID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serviceQuery = &q
	return f.serviceRows[q.ServiceName+"|"+q.OperationName], f.serviceReached, nil
}

func (f *fakeFinder) FindTraceIDsByDurationIndex(_ context.Context, q cspanstore.DurationIndexQuery) ([]cassdbmodel.TraceID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.durationQuery = &q
	return f.durationRows, f.durationReached, nil
}

func newTestReader(t *testing.T, traces map[cassdbmodel.TraceID][]cassdbmodel.Span, finder *fakeFinder) *TraceReader {
	t.Helper()
	core := &spanmocks.CoreSpanReader{}
	core.On("GetTrace", mock.Anything, mock.Anything).Return(
		func(_ context.Context, id cassdbmodel.TraceID) []cassdbmodel.Span { return traces[id] },
		func(_ context.Context, _ cassdbmodel.TraceID) error { return nil },
	)
	return NewTraceReaderWithSpanSearch(core, finder, nil)
}

func spanQuery(filter *expression.Call) tracestore.SpanQueryParams {
	return tracestore.SpanQueryParams{
		StartTimeMin: spanBaseTime.Add(-time.Hour),
		StartTimeMax: spanBaseTime.Add(time.Hour),
		Filter:       filter,
		Pagination:   tracestore.Pagination{PageSize: 100},
	}
}

// returnedSpan names one span in a FindSpans result.
type returnedSpan struct {
	service string
	name    string
}

func collectPage(t *testing.T, reader *TraceReader, query tracestore.SpanQueryParams) ([]returnedSpan, tracestore.PageToken, error) {
	t.Helper()
	var spans []returnedSpan
	var token tracestore.PageToken
	var pageErr error
	for chunk, err := range reader.FindSpans(context.Background(), query) {
		if err != nil {
			return nil, "", err
		}
		token = chunk.NextPageToken
		for i := 0; i < chunk.Results.ResourceSpans().Len(); i++ {
			rs := chunk.Results.ResourceSpans().At(i)
			svc, _ := rs.Resource().Attributes().Get("service.name")
			ss := rs.ScopeSpans().At(0).Spans()
			for j := 0; j < ss.Len(); j++ {
				spans = append(spans, returnedSpan{service: svc.Str(), name: ss.At(j).Name()})
			}
		}
	}
	return spans, token, pageErr
}

func collectAllPages(t *testing.T, reader *TraceReader, query tracestore.SpanQueryParams) []returnedSpan {
	t.Helper()
	var all []returnedSpan
	for {
		spans, token, err := collectPage(t, reader, query)
		require.NoError(t, err)
		all = append(all, spans...)
		if token == "" {
			return all
		}
		query.Pagination.PageToken = token
	}
}

func names(spans []returnedSpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.name
	}
	return out
}

const (
	traceA = "01010101010101010101010101010101"
	traceB = "02020202020202020202020202020202"
	span1  = "0000000000000001"
	span2  = "0000000000000002"
	span3  = "0000000000000003"
)

func TestFindSpans_Capabilities(t *testing.T) {
	t.Run("with span search", func(t *testing.T) {
		reader := newTestReader(t, nil, &fakeFinder{})
		caps, err := reader.SearchCapabilities(context.Background())
		require.NoError(t, err)
		assert.True(t, caps.SpanSearch)
		assert.True(t, caps.SpanSorting)
		assert.True(t, caps.Paginated)
		assert.False(t, caps.WithoutServiceName)
		require.NotNil(t, caps.Filter)
		assert.ElementsMatch(t, expression.Levels(), caps.Filter.Levels)
		for _, op := range expression.Operators() {
			assert.Contains(t, caps.Filter.Operators, op)
		}
	})

	t.Run("without the index reader keeps the old declaration", func(t *testing.T) {
		caps, err := (&TraceReader{}).SearchCapabilities(context.Background())
		require.NoError(t, err)
		assert.False(t, caps.SpanSearch)
		assert.False(t, caps.Paginated)
		assert.Nil(t, caps.Filter)
	})
}

func TestFindSpans_WithoutFinderIsUnsupported(t *testing.T) {
	reader := &TraceReader{}
	_, _, err := collectPage(t, reader, spanQuery(p.Resource().Service.Eq("shop")))
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestFindSpans_TagPlanSpanAttribute(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "gold-span",
			spanAttrs: map[string]any{"bag": "gold"}, startOffset: time.Minute,
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "silver-span",
			// The index also carries the tag for the process (resource), so this span is a
			// candidate even though it lacks the span attribute itself.
			resourceAttrs: map[string]any{"bag": "gold"},
		},
	})
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|bag|gold": {pair(t, traceA, span1), pair(t, traceA, span2)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, token, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("bag").Eq("gold"),
	)))
	require.NoError(t, err)
	assert.Empty(t, token)
	assert.Equal(t, []string{"gold-span"}, names(spans))
	require.Len(t, finder.tagQueries, 1)
	assert.Equal(t, "shop", finder.tagQueries[0].ServiceName)
	assert.Equal(t, "bag", finder.tagQueries[0].TagKey)
	assert.Equal(t, "gold", finder.tagQueries[0].TagValue)
}

func TestFindSpans_TagPlanResourceAttribute(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "span-tag",
			spanAttrs: map[string]any{"bag": "gold"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "resource-tag",
			resourceAttrs: map[string]any{"bag": "gold"},
		},
	})
	finder := &fakeFinder{
		tagRows:    map[string][]cspanstore.SpanIdentity{"shop|bag|gold": {pair(t, traceA, span1), pair(t, traceA, span2)}},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Resource().Attr("bag").Eq("gold"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"resource-tag"}, names(spans))
}

func TestFindSpans_TagPlanEventAttribute(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "with-event",
			events: []eventSpec{{name: "exception", attrs: map[string]any{"exception.type": "TimeoutError"}}},
		},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "without-event"},
	})
	finder := &fakeFinder{
		tagRows:    map[string][]cspanstore.SpanIdentity{"shop|exception.type|TimeoutError": {pair(t, traceA, span1), pair(t, traceA, span2)}},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Event().Attr("exception.type").Eq("TimeoutError"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"with-event"}, names(spans))
}

func TestFindSpans_ScopeAttributesAreNotStored(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "scoped",
			scopeName: "otelgrpc", scopeAttrs: map[string]any{"scope.tag": "scope-value"},
		},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{
		serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Scope().Attr("scope.tag").Eq("scope-value"),
	)))
	require.NoError(t, err)
	assert.Empty(t, spans)
}

func TestFindSpans_LinkField(t *testing.T) {
	linkedTrace := "03030303030303030303030303030303"
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "with-link",
			links: []linkSpec{{traceHex: linkedTrace, spanHex: span3}},
		},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "without-link"},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}}}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Some(p.Link(), p.Link().TraceID.Eq(linkedTrace)),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"with-link"}, names(spans))
}

func TestFindSpans_BuiltInKindAndStatus(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "server-error",
			kind: ptrace.SpanKindServer, status: ptrace.StatusCodeError,
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "client-ok",
			kind: ptrace.SpanKindClient, status: ptrace.StatusCodeOk,
		},
	})
	tid, _ := parseTraceID(traceA)

	t.Run("kind via tag index", func(t *testing.T) {
		finder := &fakeFinder{
			tagRows:    map[string][]cspanstore.SpanIdentity{"shop|span.kind|server": {pair(t, traceA, span1), pair(t, traceA, span2)}},
			tagReached: map[string]bool{},
		}
		reader := newTestReader(t, traces, finder)
		spans, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Kind.Eq("server"),
		)))
		require.NoError(t, err)
		assert.Equal(t, []string{"server-error"}, names(spans))
	})

	t.Run("status residual through the service index", func(t *testing.T) {
		finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}}}
		reader := newTestReader(t, traces, finder)
		spans, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Status.Eq("error"),
		)))
		require.NoError(t, err)
		assert.Equal(t, []string{"server-error"}, names(spans))
	})
}

func TestFindSpans_DurationPlan(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "slow", duration: 200 * time.Millisecond},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "fast", duration: 50 * time.Millisecond},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{durationRows: []cassdbmodel.TraceID{tid}}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Duration.Gte(100*time.Millisecond),
		p.Span().Duration.Lte(time.Second),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"slow"}, names(spans))
	require.NotNil(t, finder.durationQuery)
	assert.Equal(t, 100*time.Millisecond, finder.durationQuery.DurationMin)
	assert.Equal(t, time.Second, finder.durationQuery.DurationMax)
}

func TestFindSpans_DurationResidualOnTagPlan(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "gold-slow",
			spanAttrs: map[string]any{"bag": "gold"}, duration: 200 * time.Millisecond,
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "gold-fast",
			spanAttrs: map[string]any{"bag": "gold"}, duration: 50 * time.Millisecond,
		},
	})
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|bag|gold": {pair(t, traceA, span1), pair(t, traceA, span2)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("bag").Eq("gold"),
		p.Span().Duration.Gte(100*time.Millisecond),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"gold-slow"}, names(spans))
}

func TestFindSpans_ServiceOperationPlan(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "checkout"},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "browse"},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|checkout": {tid}}}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Name.Eq("checkout"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"checkout"}, names(spans))
	require.NotNil(t, finder.serviceQuery)
	assert.Equal(t, "checkout", finder.serviceQuery.OperationName)
}

func TestFindSpans_IdentityPlan(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "want"},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "skip"},
	})
	finder := &fakeFinder{}
	reader := newTestReader(t, traces, finder)
	// Identity needs neither service name nor a time range.
	query := tracestore.SpanQueryParams{
		Filter:     p.And(p.Span().TraceID.Eq(traceA), p.Span().Name.Eq("want")),
		Pagination: tracestore.Pagination{PageSize: 100},
	}
	spans, _, err := collectPage(t, reader, query)
	require.NoError(t, err)
	assert.Equal(t, []string{"want"}, names(spans))
	assert.Empty(t, finder.tagQueries)
	assert.Nil(t, finder.serviceQuery)
}

func TestFindSpans_SameSpanConjunction(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "both",
			spanAttrs: map[string]any{"a": "1", "b": "2"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "one",
			spanAttrs: map[string]any{"a": "1"},
		},
	})
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|a|1": {pair(t, traceA, span1), pair(t, traceA, span2)},
			"shop|b|2": {pair(t, traceA, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("a").Eq("1"),
		p.Span().Attr("b").Eq("2"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"both"}, names(spans))
}

func TestFindSpans_CorrelatedEventQuantifier(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "correlated", events: []eventSpec{
			{name: "exception", attrs: map[string]any{"exception.type": "TimeoutError"}},
		}},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "uncorrelated", events: []eventSpec{
			{name: "exception"},
			{attrs: map[string]any{"exception.type": "TimeoutError"}},
		}},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}}}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Some(p.Event(), p.And(
			p.Event().Name.Eq("exception"),
			p.Event().Attr("exception.type").Eq("TimeoutError"),
		)),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"correlated"}, names(spans))
}

func TestFindSpans_ResidualOperators(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "present",
			spanAttrs: map[string]any{"note": "Refund Policy Updated"},
		},
		{traceHex: traceA, spanHex: span2, service: "shop", name: "absent"},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}}}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("note").Exists(),
		p.Span().Attr("note").Matches("Refund"),
		p.Span().Attr("note").Ne("something else"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"present"}, names(spans))
}

func TestFindSpans_EmptyContradiction(t *testing.T) {
	finder := &fakeFinder{}
	reader := newTestReader(t, nil, finder)
	spans, token, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("a"),
		p.Resource().Service.Eq("b"),
	)))
	require.NoError(t, err)
	assert.Empty(t, spans)
	assert.Empty(t, token)
	assert.Empty(t, finder.tagQueries)
}

func TestFindSpans_UnsupportedQueries(t *testing.T) {
	cases := []struct {
		name       string
		query      tracestore.SpanQueryParams
		mutate     func(*fakeFinder)
		plainError bool
	}{
		{
			name:  "no filter",
			query: tracestore.SpanQueryParams{StartTimeMin: spanBaseTime.Add(-time.Hour), StartTimeMax: spanBaseTime.Add(time.Hour), Pagination: tracestore.Pagination{PageSize: 10}},
		},
		{
			name:  "tag without service",
			query: spanQuery(p.Span().Attr("k").Eq("v")),
		},
		{
			name:  "top level or",
			query: spanQuery(p.Or(p.Resource().Service.Eq("a"), p.Resource().Service.Eq("b"))),
		},
		{
			name:  "top level not",
			query: spanQuery(p.Not(p.Resource().Service.Eq("a"))),
		},
		{
			name:       "missing time range",
			query:      tracestore.SpanQueryParams{Filter: p.Resource().Service.Eq("shop"), Pagination: tracestore.Pagination{PageSize: 10}},
			plainError: true,
		},
		{
			name: "tag scan truncated",
			query: spanQuery(p.And(
				p.Resource().Service.Eq("shop"),
				p.Span().Attr("k").Eq("v"),
			)),
			mutate: func(f *fakeFinder) { f.tagReached["shop|k|v"] = true },
		},
		{
			name:   "service scan truncated",
			query:  spanQuery(p.Resource().Service.Eq("shop")),
			mutate: func(f *fakeFinder) { f.serviceReached = true },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tid, _ := parseTraceID(traceA)
			finder := &fakeFinder{
				tagRows:     map[string][]cspanstore.SpanIdentity{"shop|k|v": {pair(t, traceA, span1)}},
				tagReached:  map[string]bool{},
				serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}},
			}
			if tc.mutate != nil {
				tc.mutate(finder)
			}
			reader := newTestReader(t, nil, finder)
			_, _, err := collectPage(t, reader, tc.query)
			require.Error(t, err)
			if tc.plainError {
				return
			}
			require.ErrorIs(t, err, errors.ErrUnsupported, tc.name)
		})
	}
}

func TestFindSpans_TypeMismatch(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})
	_, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Duration.Gte(p.Int(5)),
	)))
	require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
}

func TestFindSpans_PaginationAndDefaultOrder(t *testing.T) {
	specs := []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "newest", startOffset: 5 * time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "middle", startOffset: 3 * time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceB, spanHex: span1, service: "shop", name: "older", startOffset: 2 * time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
	}
	traces := buildDBTraces(t, specs)
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|k|v": {pair(t, traceA, span1), pair(t, traceA, span2), pair(t, traceB, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	query := spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("k").Eq("v"),
	))

	walk := func() ([]string, []string) {
		q := query
		q.Pagination.PageSize = 2
		var got []string
		var tokens []string
		for {
			spans, token, err := collectPage(t, reader, q)
			require.NoError(t, err)
			got = append(got, names(spans)...)
			if token == "" {
				return got, tokens
			}
			tokens = append(tokens, string(token))
			q.Pagination.PageToken = token
		}
	}

	first, tokensFirst := walk()
	assert.Equal(t, []string{"newest", "middle", "older"}, first)
	require.Len(t, tokensFirst, 1)
	// Repeating the same query over unchanged data yields the same batch order and tokens.
	second, tokensSecond := walk()
	assert.Equal(t, first, second)
	assert.Equal(t, tokensFirst, tokensSecond)
}

func TestFindSpans_ExplicitOrder(t *testing.T) {
	specs := []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "fast", duration: 10 * time.Millisecond,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "slowest", duration: 30 * time.Millisecond,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceB, spanHex: span1, service: "shop", name: "medium", duration: 20 * time.Millisecond,
			spanAttrs: map[string]any{"k": "v"},
		},
	}
	traces := buildDBTraces(t, specs)
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|k|v": {pair(t, traceA, span1), pair(t, traceA, span2), pair(t, traceB, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	query := spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("k").Eq("v"),
	))
	query.OrderBy = []tracestore.SpanSortOrder{
		{
			Expression: &expression.FieldRef{Level: expression.LevelSpan, Name: expression.SpanFieldDuration},
			Direction:  tracestore.SortDescending,
		},
	}
	spans := collectAllPages(t, reader, query)
	assert.Equal(t, []string{"slowest", "medium", "fast"}, names(spans))
}

func TestFindSpans_TokenRejectedForAnotherQuery(t *testing.T) {
	specs := []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "a", startOffset: time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "b",
			spanAttrs: map[string]any{"k": "v"},
		},
	}
	traces := buildDBTraces(t, specs)
	finder := &fakeFinder{
		tagRows:    map[string][]cspanstore.SpanIdentity{"shop|k|v": {pair(t, traceA, span1), pair(t, traceA, span2)}},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	q1 := spanQuery(p.And(p.Resource().Service.Eq("shop"), p.Span().Attr("k").Eq("v")))
	q1.Pagination.PageSize = 1
	_, token, err := collectPage(t, reader, q1)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	q2 := spanQuery(p.And(p.Resource().Service.Eq("shop"), p.Span().Attr("k").Eq("other")))
	q2.Pagination.PageSize = 1
	q2.Pagination.PageToken = token
	_, _, err = collectPage(t, reader, q2)
	require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
}

func TestFindSpans_ConcurrentReads(t *testing.T) {
	specs := []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "a", startOffset: 3 * time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceA, spanHex: span2, service: "shop", name: "b", startOffset: 2 * time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceB, spanHex: span1, service: "shop", name: "c", startOffset: time.Minute,
			spanAttrs: map[string]any{"k": "v"},
		},
	}
	traces := buildDBTraces(t, specs)
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|k|v": {pair(t, traceA, span1), pair(t, traceA, span2), pair(t, traceB, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	query := spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("k").Eq("v"),
	))

	const goroutines = 16
	var wg sync.WaitGroup
	results := make([][]string, goroutines)
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Go(func() {
			spans, err := collectAllPagesErr(reader, query)
			results[i] = names(spans)
			errs[i] = err
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	for i := 1; i < goroutines; i++ {
		assert.Equal(t, results[0], results[i])
	}
	assert.Equal(t, []string{"a", "b", "c"}, results[0])
}

// collectAllPagesErr is collectAllPages without test assertions, for goroutines that must not
// call Fatal-family helpers outside the test's own goroutine.
func collectAllPagesErr(reader *TraceReader, query tracestore.SpanQueryParams) ([]returnedSpan, error) {
	var all []returnedSpan
	page := func() (tracestore.PageToken, error) {
		var token tracestore.PageToken
		for chunk, err := range reader.FindSpans(context.Background(), query) {
			if err != nil {
				return "", err
			}
			token = chunk.NextPageToken
			for i := 0; i < chunk.Results.ResourceSpans().Len(); i++ {
				rs := chunk.Results.ResourceSpans().At(i)
				svc, _ := rs.Resource().Attributes().Get("service.name")
				spans := rs.ScopeSpans().At(0).Spans()
				for j := 0; j < spans.Len(); j++ {
					all = append(all, returnedSpan{service: svc.Str(), name: spans.At(j).Name()})
				}
			}
		}
		return token, nil
	}
	for {
		token, err := page()
		if err != nil {
			return nil, err
		}
		if token == "" {
			return all, nil
		}
		query.Pagination.PageToken = token
	}
}

func TestFindSpans_InvalidOrder(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})
	query := spanQuery(p.Resource().Service.Eq("shop"))
	query.OrderBy = []tracestore.SpanSortOrder{{Expression: p.String("not a field")}}
	_, _, err := collectPage(t, reader, query)
	require.Error(t, err)
}

func TestFindSpans_MalformedTokens(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})

	baseQuery := spanQuery(p.Resource().Service.Eq("shop"))
	order, err := tracestore.EffectiveSpanOrder(baseQuery.OrderBy)
	require.NoError(t, err)
	baseQuery.OrderBy = order
	fingerprint, err := baseQuery.Fingerprint()
	require.NoError(t, err)

	wrap := func(cursor []byte) tracestore.PageToken {
		t.Helper()
		token, err := tracestore.NewPageToken(fingerprint, cursor)
		require.NoError(t, err)
		return token
	}

	for name, cursor := range map[string][]byte{
		"not a position":        []byte("junk"),
		"length past the bytes": append(binary.AppendUvarint(nil, 100), 1, 2, 3),
		"missing seen count":    append(binary.AppendUvarint(nil, 9), make([]byte, 9)...),
	} {
		t.Run(name, func(t *testing.T) {
			q := baseQuery
			q.Pagination.PageToken = wrap(cursor)
			_, _, err = collectPage(t, reader, q)
			require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
		})
	}

	t.Run("cursor over the size cap", func(t *testing.T) {
		q := baseQuery
		q.Pagination.PageToken = wrap(make([]byte, maxCursorBytes+1))
		_, _, err = collectPage(t, reader, q)
		require.ErrorIs(t, err, tracestore.ErrPaginationInvalid)
	})
}

func TestFindSpans_PageSizeIsClamped(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "a"},
	})
	tid, _ := parseTraceID(traceA)
	finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": {tid}}}
	reader := newTestReader(t, traces, finder)
	query := spanQuery(p.Resource().Service.Eq("shop"))
	query.Pagination.PageSize = tracestore.MaxPageSize + 1
	spans, _, err := collectPage(t, reader, query)
	require.NoError(t, err)
	assert.Len(t, spans, 1)
}

func TestFindSpans_UnresolvedConstantRejected(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})
	_, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Duration.Gt("banana"),
	)))
	require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
}

func TestFindSpans_FilterThatCannotBeFingerprinted(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})
	query := spanQuery(&expression.Call{
		Op:   expression.OpAnd,
		Args: []expression.Expression{(*expression.Call)(nil)},
	})
	_, _, err := collectPage(t, reader, query)
	require.Error(t, err)
	require.NotErrorIs(t, err, tracestore.ErrPaginationInvalid)
}

func TestFindSpans_DuplicateKeysAcrossPages(t *testing.T) {
	// Two stored spans tie on the whole default key (start time, trace ID and span ID), as a
	// retried write does; the cursor must return both across pages.
	specs := []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "first",
			spanAttrs: map[string]any{"k": "v"},
		},
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "second",
			spanAttrs: map[string]any{"k": "v"},
		},
	}
	traces := buildDBTraces(t, specs)
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|k|v": {pair(t, traceA, span1), pair(t, traceA, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	query := spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("k").Eq("v"),
	))
	query.Pagination.PageSize = 1
	var got []string
	for {
		spans, token, err := collectPage(t, reader, query)
		require.NoError(t, err)
		got = append(got, names(spans)...)
		if token == "" {
			break
		}
		query.Pagination.PageToken = token
	}
	assert.ElementsMatch(t, []string{"first", "second"}, got)
}

func TestFindSpans_DurationScanTruncated(t *testing.T) {
	finder := &fakeFinder{durationReached: true}
	reader := newTestReader(t, nil, finder)
	_, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Duration.Gte(time.Second),
	)))
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestFindSpans_DuplicateTagDriversIntersectOnce(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "a",
			spanAttrs: map[string]any{"k": "v"},
		},
	})
	finder := &fakeFinder{
		tagRows:    map[string][]cspanstore.SpanIdentity{"shop|k|v": {pair(t, traceA, span1)}},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("k").Eq("v"),
		p.Span().Attr("k").Eq("v"),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, names(spans))
	require.Len(t, finder.tagQueries, 1)
}

func TestPlanSpanSearch_MoreShapes(t *testing.T) {
	t.Run("duration equality", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Eq(2*time.Second),
		))
		assert.Equal(t, planDuration, plan.kind)
		assert.Equal(t, 2*time.Second, plan.durationMin)
		assert.Equal(t, 2*time.Second, plan.durationMax)
	})

	t.Run("repeated bounds keep the tightest", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Gte(time.Second),
			p.Span().Duration.Gte(3*time.Second),
			p.Span().Duration.Lte(5*time.Second),
			p.Span().Duration.Lte(10*time.Second),
		))
		assert.Equal(t, planDuration, plan.kind)
		assert.Equal(t, 3*time.Second, plan.durationMin)
		assert.Equal(t, 5*time.Second, plan.durationMax)
	})

	t.Run("trace id with invalid hex digits", func(t *testing.T) {
		plan := mustPlan(t, p.Span().TraceID.Eq("zzzzzzzzzzzzzzzz"))
		assert.Equal(t, planEmpty, plan.kind)
	})

	t.Run("non-string constants on string fields are type errors", func(t *testing.T) {
		_, err := planSpanSearch(p.And(
			p.Resource().Service.Eq(p.Double(1.5)),
		), time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)

		_, err = planSpanSearch(p.And(
			p.Resource().Service.Eq("s"),
			p.Span().Name.Eq(&expression.TimestampValue{Value: spanBaseTime}),
		), time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
	})

	t.Run("non-equality comparisons on pushed fields stay residual", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Name.Ne("x"),
			p.Span().TraceID.Ne(traceA),
			p.Span().Attr("k").In("a", "b"),
		))
		assert.Equal(t, planService, plan.kind)
		assert.Empty(t, plan.tags)
	})

	t.Run("legacy resource-prefixed unqualified key", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Attr("resource.region").Eq("eu"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.Equal(t, "region", plan.tags[0].key)
	})

	t.Run("and combining a non-predicate is malformed", func(t *testing.T) {
		_, err := planSpanSearch(&expression.Call{
			Op:   expression.OpAnd,
			Args: []expression.Expression{p.String("nope")},
		}, time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
	})
}

func TestCompareTraceIDs(t *testing.T) {
	id := cassdbmodel.TraceID{1, 2, 3}
	assert.Equal(t, 0, compareTraceIDs(id, id))
	assert.Equal(t, -1, compareTraceIDs(cassdbmodel.TraceID{1}, cassdbmodel.TraceID{2}))
	assert.Equal(t, 1, compareTraceIDs(cassdbmodel.TraceID{2}, cassdbmodel.TraceID{1}))
}

func TestFindSpans_MalformedFilterShape(t *testing.T) {
	reader := newTestReader(t, nil, &fakeFinder{})
	// An and combining a constant rather than predicates is rejected before storage is touched.
	query := spanQuery(&expression.Call{
		Op:   expression.OpAnd,
		Args: []expression.Expression{p.String("not a predicate")},
	})
	_, _, err := collectPage(t, reader, query)
	require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
}

func TestFindSpans_IndexErrors(t *testing.T) {
	t.Run("tag query error", func(t *testing.T) {
		finder := &errorFinder{tagErr: errors.New("tag down")}
		reader := NewTraceReaderWithSpanSearch(&spanmocks.CoreSpanReader{}, finder, nil)
		_, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr("k").Eq("v"),
		)))
		require.ErrorContains(t, err, "tag down")
	})
	t.Run("service query error", func(t *testing.T) {
		finder := &errorFinder{serviceErr: errors.New("service down")}
		reader := NewTraceReaderWithSpanSearch(&spanmocks.CoreSpanReader{}, finder, nil)
		_, _, err := collectPage(t, reader, spanQuery(p.Resource().Service.Eq("shop")))
		require.ErrorContains(t, err, "service down")
	})
	t.Run("duration query error", func(t *testing.T) {
		finder := &errorFinder{durationErr: errors.New("duration down")}
		reader := NewTraceReaderWithSpanSearch(&spanmocks.CoreSpanReader{}, finder, nil)
		_, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Gte(time.Second),
		)))
		require.ErrorContains(t, err, "duration down")
	})
	t.Run("trace read error", func(t *testing.T) {
		core := &spanmocks.CoreSpanReader{}
		core.On("GetTrace", mock.Anything, mock.Anything).Return(
			[]cassdbmodel.Span(nil), errors.New("trace down"))
		tid, _ := parseTraceID(traceA)
		finder := &fakeFinder{
			tagRows:    map[string][]cspanstore.SpanIdentity{"shop|k|v": {{TraceID: tid, SpanID: 1}}},
			tagReached: map[string]bool{},
		}
		reader := NewTraceReaderWithSpanSearch(core, finder, nil)
		_, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr("k").Eq("v"),
		)))
		require.ErrorContains(t, err, "trace down")
	})
}

type errorFinder struct {
	tagErr, serviceErr, durationErr error
}

func (f *errorFinder) FindSpanIDsByTag(context.Context, cspanstore.TagIndexQuery) ([]cspanstore.SpanIdentity, bool, error) {
	return nil, false, f.tagErr
}

func (f *errorFinder) FindTraceIDsByServiceIndex(context.Context, cspanstore.ServiceIndexQuery) ([]cassdbmodel.TraceID, bool, error) {
	return nil, false, f.serviceErr
}

func (f *errorFinder) FindTraceIDsByDurationIndex(context.Context, cspanstore.DurationIndexQuery) ([]cassdbmodel.TraceID, bool, error) {
	return nil, false, f.durationErr
}

func TestFindSpans_ReadCaps(t *testing.T) {
	t.Run("tag pairs over the trace cap", func(t *testing.T) {
		pairs := make([]cspanstore.SpanIdentity, maxTracesPerRead+1)
		for i := range pairs {
			pairs[i] = cspanstore.SpanIdentity{TraceID: cassdbmodel.TraceID{byte(i >> 8), byte(i)}, SpanID: 1}
		}
		finder := &fakeFinder{
			tagRows:    map[string][]cspanstore.SpanIdentity{"shop|k|v": pairs},
			tagReached: map[string]bool{},
		}
		reader := newTestReader(t, nil, finder)
		_, _, err := collectPage(t, reader, spanQuery(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr("k").Eq("v"),
		)))
		require.ErrorIs(t, err, errors.ErrUnsupported)
	})

	t.Run("service index union over the trace cap", func(t *testing.T) {
		ids := make([]cassdbmodel.TraceID, maxTracesPerRead+1)
		for i := range ids {
			ids[i] = cassdbmodel.TraceID{byte(i >> 8), byte(i)}
		}
		finder := &fakeFinder{serviceRows: map[string][]cassdbmodel.TraceID{"shop|": ids}}
		reader := newTestReader(t, nil, finder)
		_, _, err := collectPage(t, reader, spanQuery(p.Resource().Service.Eq("shop")))
		require.ErrorIs(t, err, errors.ErrUnsupported)
	})

	t.Run("identity filter naming too many traces", func(t *testing.T) {
		predicates := make([]expression.Expression, 0, maxIdentityTraces+1)
		for i := 0; i <= maxIdentityTraces; i++ {
			id := cassdbmodel.TraceID{byte(i >> 8), byte(i)}
			predicates = append(predicates, p.Span().TraceID.Eq(id.String()))
		}
		reader := newTestReader(t, nil, &fakeFinder{})
		_, _, err := collectPage(t, reader, tracestore.SpanQueryParams{
			Filter:     &expression.Call{Op: expression.OpAnd, Args: predicates},
			Pagination: tracestore.Pagination{PageSize: 10},
		})
		require.ErrorIs(t, err, errors.ErrUnsupported)
	})

	t.Run("too many tag predicates", func(t *testing.T) {
		predicates := []expression.Expression{p.Resource().Service.Eq("shop")}
		for i := 0; i <= maxTagPredicates; i++ {
			predicates = append(predicates, p.Span().Attr(fmt.Sprintf("k%d", i)).Eq("v"))
		}
		finder := &fakeFinder{tagRows: map[string][]cspanstore.SpanIdentity{}, tagReached: map[string]bool{}}
		reader := newTestReader(t, nil, finder)
		_, _, err := collectPage(t, reader, spanQuery(&expression.Call{Op: expression.OpAnd, Args: predicates}))
		require.ErrorIs(t, err, errors.ErrUnsupported)
	})
}

func TestFindSpans_IdentityTimeWindowExcludes(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{traceHex: traceA, spanHex: span1, service: "shop", name: "old"},
	})
	reader := newTestReader(t, traces, &fakeFinder{})
	query := tracestore.SpanQueryParams{
		StartTimeMin: spanBaseTime.Add(time.Hour),
		StartTimeMax: spanBaseTime.Add(2 * time.Hour),
		Filter:       p.Span().TraceID.Eq(traceA),
		Pagination:   tracestore.Pagination{PageSize: 10},
	}
	spans, _, err := collectPage(t, reader, query)
	require.NoError(t, err)
	assert.Empty(t, spans)
}

func TestFindSpans_EmptyTagIntersection(t *testing.T) {
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|a|1": {pair(t, traceA, span1)},
			"shop|b|2": {},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, nil, finder)
	spans, token, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("a").Eq("1"),
		p.Span().Attr("b").Eq("2"),
	)))
	require.NoError(t, err)
	assert.Empty(t, spans)
	assert.Empty(t, token)
}

func TestPlanSpanSearch_TagFilterDropsSources(t *testing.T) {
	finalize := func(c *expression.Call) *expression.Call {
		f, err := tracestore.ResolveFilterConstants(c)
		require.NoError(t, err)
		return f
	}
	t.Run("span tags dropped", func(t *testing.T) {
		filter := cassdbmodel.NewTagFilterDropAll(true, false, false)
		plan, err := planSpanSearch(finalize(p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Kind.Eq("server"),
		)), time.Unix(1000, 0), time.Unix(2000, 0), filter)
		require.NoError(t, err)
		assert.Equal(t, planService, plan.kind)
	})
	t.Run("process tags dropped", func(t *testing.T) {
		filter := cassdbmodel.NewTagFilterDropAll(false, true, false)
		plan, err := planSpanSearch(finalize(p.And(
			p.Resource().Service.Eq("shop"),
			p.Attr("k").Eq("v"),
		)), time.Unix(1000, 0), time.Unix(2000, 0), filter)
		require.NoError(t, err)
		assert.Equal(t, planService, plan.kind)
	})
}

func TestPlanSpanSearch_KindAndAttributeConstants(t *testing.T) {
	t.Run("unknown kind word is a contradiction", func(t *testing.T) {
		plan, err := planSpanSearch(call(expression.OpAnd,
			p.Resource().Service.Eq("shop"),
			call(expression.OpEq,
				&expression.FieldRef{Level: expression.LevelSpan, Name: expression.SpanFieldKind},
				p.String("weird"))),
			time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.NoError(t, err)
		assert.Equal(t, planEmpty, plan.kind)
	})
	t.Run("kind against an integer is a type error", func(t *testing.T) {
		_, err := planSpanSearch(
			call(expression.OpEq,
				&expression.FieldRef{Level: expression.LevelSpan, Name: expression.SpanFieldKind},
				p.Int(5)),
			time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
	})
	t.Run("attribute against a duration is a type error", func(t *testing.T) {
		_, err := planSpanSearch(call(expression.OpEq,
			&expression.AttributeRef{Level: expression.LevelSpan, Key: "k"},
			&expression.DurationValue{Value: time.Second}),
			time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
	})
	t.Run("duration against a list is a type error", func(t *testing.T) {
		_, err := planSpanSearch(call(expression.OpGte,
			&expression.FieldRef{Level: expression.LevelSpan, Name: expression.SpanFieldDuration},
			&expression.List{Values: []string{"1s"}}),
			time.Unix(1000, 0), time.Unix(2000, 0), nil)
		require.ErrorIs(t, err, tracestore.ErrFilterInvalid)
	})
	t.Run("oversized key stays residual", func(t *testing.T) {
		long := make([]byte, maxIndexedTagSize)
		for i := range long {
			long[i] = 'x'
		}
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr(string(long)).Eq("v"),
		))
		assert.Equal(t, planService, plan.kind)
	})
}

func TestFindSpans_NumericTagTypes(t *testing.T) {
	traces := buildDBTraces(t, []spanSpec{
		{
			traceHex: traceA, spanHex: span1, service: "shop", name: "n",
			spanAttrs: map[string]any{"retries": int64(3), "cold": false, "ratio": 1.5},
		},
	})
	finder := &fakeFinder{
		tagRows: map[string][]cspanstore.SpanIdentity{
			"shop|retries|3":  {pair(t, traceA, span1)},
			"shop|cold|false": {pair(t, traceA, span1)},
		},
		tagReached: map[string]bool{},
	}
	reader := newTestReader(t, traces, finder)
	spans, _, err := collectPage(t, reader, spanQuery(p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("retries").Eq(p.Int(3)),
		p.Span().Attr("cold").Eq(p.Bool(false)),
		p.Span().Attr("ratio").Lt(2.0),
	)))
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, names(spans))
}

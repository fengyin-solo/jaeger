// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"iter"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	"github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore"
	cassdbmodel "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

type TraceReader struct {
	// Cassandra does not compute trace summaries natively; fall back to
	// FindTraces + client-side aggregation.
	tracestore.UnsupportedTraceSummaries

	reader spanstore.CoreSpanReader
	// finder and tagFilter power FindSpans. A TraceReader built without them keeps the
	// span-search refusal the backend shipped with before FindSpans was implemented.
	finder    spanIndexReader
	tagFilter cassdbmodel.TagFilter
}

// SearchCapabilities reports the reader's search capabilities. A TraceReader without the span
// index reader keeps the least-capable declaration every index is keyed by service name; one
// with it additionally serves span search.
func (r *TraceReader) SearchCapabilities(context.Context) (tracestore.SearchCapabilities, error) {
	caps := tracestore.SearchCapabilities{
		// Every Cassandra index is keyed by service name, so a search cannot omit it.
		WithoutServiceName: false,
	}
	if r.finder == nil {
		return caps, nil
	}
	// FindSpans evaluates the complete filter on every candidate span after the indexes narrow
	// them, so every level and operator the evaluator covers is declared; a top-level `or`/`not`
	// the flat indexes cannot bound is refused in the lowering with ErrFilterUnsupported.
	filterCaps := tracestore.FilterCapabilities{
		Levels:    expression.Levels(),
		Operators: expression.Operators(),
	}
	caps.Filter = &filterCaps
	// FindSpans sorts the read-back window by every intrinsic field in both directions and
	// pages it with a keyset cursor.
	caps.SpanSearch = true
	caps.SpanSorting = true
	caps.Paginated = true
	return caps, nil
}

func NewTraceReader(reader spanstore.CoreSpanReader) *TraceReader {
	return &TraceReader{reader: reader}
}

// NewTraceReaderWithSpanSearch builds a reader whose FindSpans serves span searches through the
// Cassandra secondary indexes. tagFilter is the writer's tag indexing filter; when non-nil a tag
// the deployment does not index cannot be used to drive a query.
func NewTraceReaderWithSpanSearch(
	reader spanstore.CoreSpanReader,
	finder spanIndexReader,
	tagFilter cassdbmodel.TagFilter,
) *TraceReader {
	return &TraceReader{reader: reader, finder: finder, tagFilter: tagFilter}
}

func (r *TraceReader) GetServices(ctx context.Context) ([]string, error) {
	return r.reader.GetServices(ctx)
}

func (r *TraceReader) GetOperations(ctx context.Context, query tracestore.OperationQueryParams) ([]tracestore.Operation, error) {
	return r.reader.GetOperations(ctx, query)
}

func (r *TraceReader) GetTraces(ctx context.Context, traceIDs ...tracestore.GetTraceParams) iter.Seq2[[]ptrace.Traces, error] {
	return func(yield func([]ptrace.Traces, error) bool) {
		for _, id := range traceIDs {
			// pcommon.TraceID and cassdbmodel.TraceID are both [16]byte with same byte layout
			spans, err := r.reader.GetTrace(ctx, cassdbmodel.TraceID(id.TraceID))
			if err != nil {
				yield(nil, err)
				return
			}
			if len(spans) == 0 {
				continue
			}
			td := FromDBModel(spans)
			if !yield([]ptrace.Traces{td}, nil) {
				return
			}
		}
	}
}

func (r *TraceReader) FindTraces(ctx context.Context, query tracestore.TraceQueryParams) iter.Seq2[[]ptrace.Traces, error] {
	return func(yield func([]ptrace.Traces, error) bool) {
		for trace, err := range r.reader.FindTraces(ctx, &query) {
			if err != nil {
				yield(nil, err)
				return
			}
			td := FromDBModel(trace.Spans)
			if !yield([]ptrace.Traces{td}, nil) {
				return
			}
		}
	}
}

func (r *TraceReader) FindTraceIDs(ctx context.Context, query tracestore.TraceQueryParams) iter.Seq2[tracestore.PageChunk[[]tracestore.FoundTraceID], error] {
	return func(yield func(tracestore.PageChunk[[]tracestore.FoundTraceID], error) bool) {
		dbIDs, err := r.reader.FindTraceIDs(ctx, &query)
		if err != nil {
			yield(tracestore.PageChunk[[]tracestore.FoundTraceID]{}, err)
			return
		}
		if len(dbIDs) == 0 {
			return
		}
		otelIDs := make([]tracestore.FoundTraceID, 0, len(dbIDs))
		for _, id := range dbIDs {
			otelIDs = append(otelIDs, tracestore.FoundTraceID{
				TraceID: pcommon.TraceID(id),
			})
		}
		// TODO: Populate NextPageToken when Cassandra supports RFC 0014 pagination.
		yield(tracestore.PageChunk[[]tracestore.FoundTraceID]{Results: otelIDs}, nil)
	}
}

// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	cassdbmodel "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

func mustPlan(t *testing.T, filter *expression.Call) spanPlan {
	t.Helper()
	finalized, err := tracestore.ResolveFilterConstants(filter)
	require.NoError(t, err)
	plan, err := planSpanSearch(finalized, time.Unix(1000, 0), time.Unix(2000, 0), nil)
	require.NoError(t, err)
	return plan
}

func TestPlanSpanSearch_ServiceAndOperation(t *testing.T) {
	plan := mustPlan(t, p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Name.Eq("GET /items"),
	))
	assert.Equal(t, planService, plan.kind)
	assert.Equal(t, "shop", plan.service)
	assert.Equal(t, "GET /items", plan.operation)
}

func TestPlanSpanSearch_TagDrivers(t *testing.T) {
	t.Run("span tag", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr("bag").Eq("gold"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.Equal(t, "bag", plan.tags[0].key)
		assert.Equal(t, "gold", plan.tags[0].value)
		assert.True(t, plan.tags[0].sources[tagFromSpan])
	})

	t.Run("resource tag", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Resource().Attr("region").Eq("eu"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.True(t, plan.tags[0].sources[tagFromProcess])
	})

	t.Run("event tag uses the log source", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Event().Attr("exception.type").Eq("TimeoutError"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.True(t, plan.tags[0].sources[tagFromLog])
	})

	t.Run("unqualified tag needs span and process sources", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Attr("bag").Eq("gold"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.True(t, plan.tags[0].sources[tagFromSpan])
		assert.True(t, plan.tags[0].sources[tagFromProcess])
	})

	t.Run("numeric and bool tags carry their stringified value", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Attr("retries").Eq(p.Int(3)),
			p.Span().Attr("cold").Eq(p.Bool(true)),
			p.Span().Attr("ratio").Eq(p.Double(1.5)),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 3)
		assert.Equal(t, "3", plan.tags[0].value)
		assert.Equal(t, "true", plan.tags[1].value)
		assert.Equal(t, "1.5", plan.tags[2].value)
	})

	t.Run("span.kind is indexed", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Kind.Eq("server"),
		))
		require.Equal(t, planTag, plan.kind)
		require.Len(t, plan.tags, 1)
		assert.Equal(t, "span.kind", plan.tags[0].key)
		assert.Equal(t, "server", plan.tags[0].value)
	})
}

func TestPlanSpanSearch_VirtualKeysStayResidual(t *testing.T) {
	for key, call := range map[string]*expression.Call{
		"error":       p.Span().Attr("error").Eq(p.Bool(true)),
		"span.status": p.Span().Attr("span.status").Eq("error"),
	} {
		t.Run(key, func(t *testing.T) {
			plan := mustPlan(t, p.And(p.Resource().Service.Eq("shop"), call))
			assert.Equal(t, planService, plan.kind)
			assert.Empty(t, plan.tags)
		})
	}
}

func TestPlanSpanSearch_ScopeAndLinkTagsDoNotDrive(t *testing.T) {
	plan := mustPlan(t, p.And(
		p.Resource().Service.Eq("shop"),
		p.Scope().Attr("scope.tag").Eq("v"),
		p.Some(p.Link(), p.Link().Attr("link.tag").Eq("v")),
	))
	assert.Equal(t, planService, plan.kind)
	assert.Empty(t, plan.tags)
}

func TestPlanSpanSearch_OversizedTagStaysResidual(t *testing.T) {
	long := make([]byte, maxIndexedTagSize)
	for i := range long {
		long[i] = 'x'
	}
	plan := mustPlan(t, p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("bag").Eq(string(long)),
	))
	// The oversized value cannot drive tag_index, so the service index bounds the search and
	// the tag predicate stays residual.
	assert.Equal(t, planService, plan.kind)
	assert.Empty(t, plan.tags)
}

func TestPlanSpanSearch_JSONTagValueStaysResidual(t *testing.T) {
	plan := mustPlan(t, p.And(
		p.Resource().Service.Eq("shop"),
		p.Span().Attr("payload").Eq(`{"a":1}`),
	))
	assert.Equal(t, planService, plan.kind)
	assert.Empty(t, plan.tags)
}

func TestPlanSpanSearch_DroppedTagStaysResidual(t *testing.T) {
	filter := cassdbmodel.NewTagFilterDropAll(false, false, true) // log fields dropped
	finalized, err := tracestore.ResolveFilterConstants(p.And(
		p.Resource().Service.Eq("shop"),
		p.Event().Attr("exception.type").Eq("TimeoutError"),
	))
	require.NoError(t, err)
	plan, err := planSpanSearch(finalized, time.Unix(1000, 0), time.Unix(2000, 0), filter)
	require.NoError(t, err)
	assert.Equal(t, planService, plan.kind)
	assert.Empty(t, plan.tags)
}

func TestPlanSpanSearch_Duration(t *testing.T) {
	t.Run("bounds drive the duration index", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Gte(2*time.Second),
			p.Span().Duration.Lte(5*time.Second),
		))
		assert.Equal(t, planDuration, plan.kind)
		assert.Equal(t, 2*time.Second, plan.durationMin)
		assert.Equal(t, 5*time.Second, plan.durationMax)
	})

	t.Run("operation rides the duration partition", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Name.Eq("op"),
			p.Span().Duration.Gte(2*time.Second),
		))
		assert.Equal(t, planDuration, plan.kind)
		assert.Equal(t, "op", plan.operation)
	})

	t.Run("gt stays residual", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Gt(2*time.Second),
		))
		assert.Equal(t, planService, plan.kind)
	})

	t.Run("contradictory bounds are empty", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("shop"),
			p.Span().Duration.Gte(5*time.Second),
			p.Span().Duration.Lte(2*time.Second),
		))
		assert.Equal(t, planEmpty, plan.kind)
	})
}

func TestPlanSpanSearch_Identity(t *testing.T) {
	traceID := "0102030405060708090a0b0c0d0e0f10"
	plan := mustPlan(t, p.Span().TraceID.Eq(traceID))
	require.Equal(t, planIdentity, plan.kind)
	require.Len(t, plan.traceIDs, 1)
	assert.Equal(t, traceID, plan.traceIDs[0].String())

	t.Run("a malformed trace ID is a contradiction", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Span().TraceID.Eq("not-hex"),
			p.Resource().Service.Eq("shop"),
		))
		assert.Equal(t, planEmpty, plan.kind)
	})
}

func TestPlanSpanSearch_Contradictions(t *testing.T) {
	t.Run("two service names", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("a"),
			p.Resource().Service.Eq("b"),
		))
		assert.Equal(t, planEmpty, plan.kind)
	})
	t.Run("two operation names", func(t *testing.T) {
		plan := mustPlan(t, p.And(
			p.Resource().Service.Eq("a"),
			p.Span().Name.Eq("x"),
			p.Span().Name.Eq("y"),
		))
		assert.Equal(t, planEmpty, plan.kind)
	})
}

func TestPlanSpanSearch_Unsupported(t *testing.T) {
	cases := []struct {
		name   string
		filter *expression.Call
	}{
		{"no filter", nil},
		{"tag without service", p.And(p.Span().Attr("k").Eq("v"))},
		{"duration without service", p.Span().Duration.Gte(time.Second)},
		{"top-level or", p.Or(
			p.Resource().Service.Eq("a"),
			p.Resource().Service.Eq("b"),
		)},
		{"top-level not", p.Not(p.Resource().Service.Eq("a"))},
		{"scope attribute without a driver", p.Scope().Attr("k").Eq("v")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var call *expression.Call
			if tc.filter != nil {
				finalized, err := tracestore.ResolveFilterConstants(tc.filter)
				require.NoError(t, err)
				call = finalized
			}
			_, err := planSpanSearch(call, time.Unix(1000, 0), time.Unix(2000, 0), nil)
			require.ErrorIs(t, err, tracestore.ErrFilterUnsupported)
		})
	}
}

func TestPlanSpanSearch_MissingTimeRange(t *testing.T) {
	finalized, err := tracestore.ResolveFilterConstants(p.Resource().Service.Eq("shop"))
	require.NoError(t, err)
	_, err = planSpanSearch(finalized, time.Time{}, time.Time{}, nil)
	require.ErrorIs(t, err, errStartAndEndTimeNotSet)

	_, err = planSpanSearch(finalized, time.Unix(2000, 0), time.Unix(1000, 0), nil)
	require.ErrorIs(t, err, errStartTimeMinGreaterThanMax)
}

func TestPlanSpanSearch_TypeMismatches(t *testing.T) {
	cases := map[string]*expression.Call{
		"service vs int": p.And(p.Resource().Service.Eq(p.Int(5))),
		"name vs bool":   p.And(p.Resource().Service.Eq("s"), p.Span().Name.Eq(p.Bool(true))),
		"traceID vs int": p.Span().TraceID.Eq(p.Int(5)),
		"duration vs string": p.And(
			p.Resource().Service.Eq("s"),
			p.Span().Duration.Gte("banana"),
		),
	}
	for name, filter := range cases {
		t.Run(name, func(t *testing.T) {
			// The raw tree reaches the planner without query-service finalization, the path a
			// remote-storage client can take.
			_, err := planSpanSearch(filter, time.Unix(1000, 0), time.Unix(2000, 0), nil)
			require.ErrorIs(t, err, tracestore.ErrFilterInvalid, name)
		})
	}
}

func TestPlanSpanSearch_FlattenedAnd(t *testing.T) {
	plan := mustPlan(t, p.And(
		p.Resource().Service.Eq("shop"),
		p.And(
			p.Span().Name.Eq("op"),
			p.Span().Attr("k").Eq("v"),
		),
	))
	assert.Equal(t, planTag, plan.kind)
	assert.Equal(t, "op", plan.operation)
}

func TestPlanSpanSearch_ErrorSentinelIsUnsupported(t *testing.T) {
	_, err := planSpanSearch(nil, time.Unix(1000, 0), time.Unix(2000, 0), nil)
	assert.ErrorIs(t, err, tracestore.ErrFilterUnsupported)
}

// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	cassdbmodel "github.com/jaegertracing/jaeger/internal/storage/v1/cassandra/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

// A span search is planned as one bounded index scan whose rows are a superset of the spans the
// full filter matches, followed by exact evaluation of that filter on every candidate span
// (RFC 0016 §9). The plan records which scan bounds the request; nothing it cannot push is
// dropped — the whole filter stays and is re-applied to the spans read back.

// planKind names the index a scan reads.
type planKind int

const (
	// planEmpty is a contradiction (two different service names, ...): the answer is empty
	// without touching storage.
	planEmpty planKind = iota
	// planIdentity point-reads the traces table by trace ID, the cheapest path and the only
	// one that needs neither service name nor time range.
	planIdentity
	// planTag intersects tag_index reads on (trace ID, span ID) pairs, at span granularity.
	planTag
	// planDuration scans duration_index, which stops at trace granularity.
	planDuration
	// planService scans service_name_index or service_operation_index, also trace granularity.
	planService
)

// tagDriver is one tag equality pushed to tag_index.
type tagDriver struct {
	key   string
	value string
	// sources records where the predicate looks for the tag: span tags, process tags or log
	// fields. The tag index mixes all three, but a miss is only impossible when every source
	// the predicate names retains the tag, so the planner checks the configured TagFilter for
	// exactly these sources.
	sources map[tagSource]bool
}

type tagSource int

const (
	tagFromSpan tagSource = iota
	tagFromProcess
	tagFromLog
)

// spanPlan is the lowered query.
type spanPlan struct {
	kind planKind

	service   string
	operation string

	durationMin time.Duration
	durationMax time.Duration

	tags []tagDriver

	traceIDs []cassdbmodel.TraceID
}

// virtualTagKeys are attribute keys the evaluator derives rather than reads out of the stored
// tag maps. The tag index has no row under them for every span that matches — error=false spans
// carry no error tag — so an index read on one can miss matches. Predicates on them stay
// residual.
var virtualTagKeys = map[string]bool{
	tagError:               true, // derived from the span status
	"span.status":          true, // derived from error/otel.status_code tags
	"opentracing.ref.type": true, // synthesized on the way out, on links
}

// planSpanSearch lowers a finalized filter into one index plan. It returns tracestore
// sentinels: ErrFilterInvalid for a constant that does not fit the field it compares, and
// ErrFilterUnsupported for a well-formed query no Cassandra index can bound safely.
func planSpanSearch(
	filter *expression.Call,
	startTimeMin, startTimeMax time.Time,
	tagFilter cassdbmodel.TagFilter,
) (spanPlan, error) {
	conjuncts, err := flattenConjunction(filter)
	if err != nil {
		return spanPlan{}, err
	}

	plan := spanPlan{}
	var serviceSet, operationSet, durationMinSet, durationMaxSet bool
	traceIDSet := map[cassdbmodel.TraceID]bool{}

	for _, conjunct := range conjuncts {
		switch {
		case isFieldRef(conjunct.Args[0], expression.LevelResource, expression.ResourceFieldService):
			text, err := stringConstant("resource.service", conjunct.Args[1])
			if err != nil {
				return spanPlan{}, err
			}
			if conjunct.Op != expression.OpEq {
				// Only equality names a partition; the predicate stays residual.
				continue
			}
			if serviceSet && plan.service != text {
				return spanPlan{kind: planEmpty}, nil
			}
			plan.service = text
			serviceSet = true
		case isFieldRef(conjunct.Args[0], expression.LevelSpan, expression.SpanFieldName):
			if conjunct.Op != expression.OpEq {
				continue
			}
			text, err := stringConstant("span.name", conjunct.Args[1])
			if err != nil {
				return spanPlan{}, err
			}
			if operationSet && plan.operation != text {
				return spanPlan{kind: planEmpty}, nil
			}
			plan.operation = text
			operationSet = true
		case isFieldRef(conjunct.Args[0], expression.LevelSpan, expression.SpanFieldKind):
			if conjunct.Op != expression.OpEq {
				continue
			}
			driver, valid, err := spanKindDriver(conjunct.Args[1], tagFilter)
			if err != nil {
				return spanPlan{}, err
			}
			if !valid {
				// A word outside the span.kind vocabulary can match no stored span.
				return spanPlan{kind: planEmpty}, nil
			}
			if driver != nil {
				plan.tags = append(plan.tags, *driver)
			}
		case isFieldRef(conjunct.Args[0], expression.LevelSpan, expression.SpanFieldTraceID):
			if conjunct.Op != expression.OpEq {
				continue
			}
			text, err := stringConstant("span.traceID", conjunct.Args[1])
			if err != nil {
				return spanPlan{}, err
			}
			id, parseErr := parseTraceID(text)
			if parseErr != nil {
				// A string that is not a trace ID can never equal a stored one, so the conjunction
				// is a contradiction rather than a type error.
				//nolint:nilerr // the malformed value is an impossible match, not a request error
				return spanPlan{kind: planEmpty}, nil
			}
			traceIDSet[id] = true
		case isFieldRef(conjunct.Args[0], expression.LevelSpan, expression.SpanFieldDuration):
			switch conjunct.Op {
			case expression.OpGte, expression.OpLte, expression.OpEq:
				// only these range comparisons key the duration index clustering
			default:
				// gt/lt/ne on duration cannot name the inclusive index bounds and stay residual
				continue
			}
			d, err := durationConstant(conjunct.Args[1])
			if err != nil {
				return spanPlan{}, err
			}
			switch conjunct.Op {
			case expression.OpGte:
				if durationMinSet && d < plan.durationMin {
					d = plan.durationMin
				}
				plan.durationMin = d
				durationMinSet = true
			case expression.OpLte:
				if durationMaxSet && d > plan.durationMax {
					d = plan.durationMax
				}
				plan.durationMax = d
				durationMaxSet = true
			case expression.OpEq:
				plan.durationMin, plan.durationMax = d, d
				durationMinSet, durationMaxSet = true, true
			default:
				// unreachable: the outer switch admits only gte, lte and eq
			}
		default:
			if driver, ok, err := tagDriverFromConjunct(conjunct, tagFilter); err != nil {
				return spanPlan{}, err
			} else if ok {
				plan.tags = append(plan.tags, driver)
			}
		}
	}

	if durationMinSet && durationMaxSet && plan.durationMin > plan.durationMax {
		return spanPlan{kind: planEmpty}, nil
	}

	switch {
	case len(traceIDSet) > 0:
		plan.kind = planIdentity
		for id := range traceIDSet {
			plan.traceIDs = append(plan.traceIDs, id)
		}
	case len(plan.tags) > 0:
		// tag_index is partitioned by the service name.
		if plan.service == "" {
			return spanPlan{}, fmt.Errorf(
				"%w: a tag search on Cassandra names a service (resource.service = ...) to bound the tag index partition",
				tracestore.ErrFilterUnsupported)
		}
		plan.kind = planTag
	case durationMinSet || durationMaxSet:
		if plan.service == "" {
			return spanPlan{}, fmt.Errorf(
				"%w: a duration search on Cassandra names a service (resource.service = ...) to bound the duration index partition",
				tracestore.ErrFilterUnsupported)
		}
		plan.kind = planDuration
	case serviceSet:
		plan.kind = planService
	default:
		// No filter at all, or one the secondary indexes cannot bound: the reader would have to
		// scan the keyspace, which is the query it refuses rather than approximates.
		return spanPlan{}, fmt.Errorf(
			"%w: Cassandra can serve a span search by service, operation, tag, duration or trace identity; "+
				"this filter names none the indexes can bound",
			tracestore.ErrFilterUnsupported)
	}

	if plan.kind != planIdentity {
		if startTimeMin.IsZero() || startTimeMax.IsZero() {
			return spanPlan{}, errStartAndEndTimeNotSet
		}
		if startTimeMax.Before(startTimeMin) {
			return spanPlan{}, errStartTimeMinGreaterThanMax
		}
	}

	return plan, nil
}

// flattenConjunction returns the leaves of a flat conjunction. Nested `and` is the same query
// flattened. `or` and `not` at the top cannot drive a flat inverted index — neither a single
// partition nor a sound intersection exists — so they are refused here even though the
// evaluator could answer them on fetched spans. Nested inside another leaf (such as `some`),
// they ride along as residual predicates.
func flattenConjunction(filter *expression.Call) ([]*expression.Call, error) {
	if filter == nil {
		return nil, nil
	}
	var conjuncts []*expression.Call
	var walk func(call *expression.Call) error
	walk = func(call *expression.Call) error {
		switch call.Op {
		case expression.OpAnd:
			for _, arg := range call.Args {
				nested, ok := arg.(*expression.Call)
				if !ok {
					return fmt.Errorf("%w: %q combines predicates only", tracestore.ErrFilterInvalid, call.Op)
				}
				if err := walk(nested); err != nil {
					return err
				}
			}
		case expression.OpOr, expression.OpNot:
			return fmt.Errorf(
				"%w: Cassandra's indexes intersect conjunctive predicates only; %q cannot be pushed",
				tracestore.ErrFilterUnsupported, call.Op)
		default:
			conjuncts = append(conjuncts, call)
		}
		return nil
	}
	if err := walk(filter); err != nil {
		return nil, err
	}
	return conjuncts, nil
}

// isFieldRef reports whether expr is a FieldRef of the given level and name.
func isFieldRef(expr expression.Expression, level expression.Level, name string) bool {
	ref, ok := expr.(*expression.FieldRef)
	return ok && ref.Level == level && ref.Name == name
}

// stringConstant reads a text constant a string-valued field is compared against. A filter
// reaching a reader through the query service is finalized, so an untyped constant has been
// resolved to a string; a remote-storage client can send anything, and a constant of another
// kind is a type error here rather than a query that matches nothing.
func stringConstant(field string, value expression.Expression) (string, error) {
	switch v := value.(type) {
	case *expression.StringValue:
		return v.Value, nil
	case *expression.AnyValue:
		return v.Value, nil
	default:
		return "", fmt.Errorf("%w: %q is compared against a string, not %s",
			tracestore.ErrFilterInvalid, field, expressionTermName(value))
	}
}

// durationConstant reads a duration constant.
func durationConstant(value expression.Expression) (time.Duration, error) {
	if v, ok := value.(*expression.DurationValue); ok {
		return v.Value, nil
	}
	return 0, fmt.Errorf("%w: span.duration is compared against a duration such as \"2s\", not %s",
		tracestore.ErrFilterInvalid, expressionTermName(value))
}

// expressionTermName is a small kind name for the type errors above.
func expressionTermName(e expression.Expression) string {
	switch e.(type) {
	case *expression.IntValue:
		return "an integer constant"
	case *expression.DoubleValue:
		return "a floating-point constant"
	case *expression.BoolValue:
		return "a boolean constant"
	case *expression.DurationValue:
		return "a duration constant"
	case *expression.TimestampValue:
		return "a timestamp constant"
	case *expression.List:
		return "a list"
	default:
		return "that constant"
	}
}

// parseTraceID parses 16 to 32 hex digits into a 16-byte trace ID.
func parseTraceID(s string) (cassdbmodel.TraceID, error) {
	if s == "" || len(s) > 32 || len(s)%2 != 0 {
		return cassdbmodel.TraceID{}, errors.New("bad length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return cassdbmodel.TraceID{}, err
	}
	var id cassdbmodel.TraceID
	copy(id[16-len(b):], b)
	return id, nil
}

// spanKindDriver lowers an equality on span.kind to a tag_index read on the span.kind tag the
// writer stores for every explicitly-typed span. valid is false for a word outside the closed
// vocabulary (the predicate matches no stored span); a nil driver with valid true means the
// predicate rides along residually because the deployment does not index the tag.
func spanKindDriver(value expression.Expression, tagFilter cassdbmodel.TagFilter) (driver *tagDriver, valid bool, err error) {
	text, err := stringConstant("span.kind", value)
	if err != nil {
		return nil, false, err
	}
	if !slices.Contains(expression.SpanKinds(), text) {
		return nil, false, nil
	}
	tag, err := indexableTag(model.SpanKindKey, &expression.StringValue{Value: text})
	if err != nil {
		return nil, true, err
	}
	sources := map[tagSource]bool{tagFromSpan: true}
	if !tagRetained(tagFilter, tag, sources) {
		return nil, true, nil
	}
	return &tagDriver{key: tag.Key, value: tag.AsString(), sources: sources}, true, nil
}

// tagDriverFromConjunct turns one conjunct into a tag_index driver when it is an equality on
// an attribute the tag index records (span, process/resource, or event/log fields) and the
// compared value is indexed in full. The second result is false when the predicate stays
// residual (another operator, the scope or link level, an unindexed virtual key, a value the
// index would truncate, or a key the deployment's TagFilter drops).
func tagDriverFromConjunct(conjunct *expression.Call, tagFilter cassdbmodel.TagFilter) (tagDriver, bool, error) {
	if conjunct.Op != expression.OpEq || len(conjunct.Args) != 2 {
		return tagDriver{}, false, nil
	}
	ref, ok := conjunct.Args[0].(*expression.AttributeRef)
	if !ok {
		return tagDriver{}, false, nil
	}
	if ref.Level == expression.LevelScope || ref.Level == expression.LevelLink {
		return tagDriver{}, false, nil
	}
	key := ref.Key
	resourceOnly := false
	if ref.Level == "" {
		// The evaluator reads an unqualified "resource.foo" as the resource attribute foo.
		if resourceKey, ok := strings.CutPrefix(key, "resource."); ok {
			key = resourceKey
			resourceOnly = true
		}
	}
	if virtualTagKeys[key] {
		return tagDriver{}, false, nil
	}

	tag, err := indexableTag(key, conjunct.Args[1])
	if err != nil {
		if errors.Is(err, errTagNotIndexed) {
			// The writer would not index this value, so pushing it could miss matches; it stays
			// residual and another driver has to bound the search.
			return tagDriver{}, false, nil
		}
		return tagDriver{}, false, err
	}

	// Which stored tag sources the predicate can be satisfied from decides which TagFilter
	// methods have to retain the tag for an index read to be complete.
	var sources map[tagSource]bool
	switch {
	case ref.Level == expression.LevelSpan:
		sources = map[tagSource]bool{tagFromSpan: true}
	case ref.Level == expression.LevelResource || resourceOnly:
		sources = map[tagSource]bool{tagFromProcess: true}
	case ref.Level == expression.LevelEvent:
		sources = map[tagSource]bool{tagFromLog: true}
	default:
		sources = map[tagSource]bool{tagFromSpan: true, tagFromProcess: true}
	}
	if !tagRetained(tagFilter, tag, sources) {
		return tagDriver{}, false, nil
	}
	return tagDriver{key: tag.Key, value: tag.AsString(), sources: sources}, true, nil
}

// indexableTag builds the stored KeyValue a constant equals and rejects one the writer never
// indexes: text the size rule drops, a JSON value, or a constant whose kind has no scalar tag
// form. The check is against the *constant*, because a stored value that matches it is the same
// text and is dropped under the same rule — so accepting only indexable constants can never
// miss a match.
func indexableTag(key string, value expression.Expression) (cassdbmodel.KeyValue, error) {
	tag := cassdbmodel.KeyValue{Key: key}
	switch v := value.(type) {
	case *expression.StringValue:
		tag.ValueType = cassdbmodel.StringType
		tag.ValueString = v.Value
	case *expression.AnyValue:
		tag.ValueType = cassdbmodel.StringType
		tag.ValueString = v.Value
	case *expression.IntValue:
		tag.ValueType = cassdbmodel.Int64Type
		tag.ValueInt64 = v.Value
	case *expression.DoubleValue:
		tag.ValueType = cassdbmodel.Float64Type
		tag.ValueFloat64 = v.Value
	case *expression.BoolValue:
		tag.ValueType = cassdbmodel.BoolType
		tag.ValueBool = v.Value
	case *expression.DurationValue, *expression.TimestampValue:
		// A duration or a timestamp has no wire form as an attribute value, and ValidateFilter
		// refuses the comparison upstream.
		return cassdbmodel.KeyValue{}, fmt.Errorf("%w: an attribute is not compared against %s",
			tracestore.ErrFilterInvalid, expressionTermName(value))
	default:
		return cassdbmodel.KeyValue{}, fmt.Errorf("%w: an attribute is not compared against %s",
			tracestore.ErrFilterInvalid, expressionTermName(value))
	}
	text := tag.AsString()
	if len(key) >= maxIndexedTagSize || len(text) >= maxIndexedTagSize ||
		!utf8.ValidString(key) || !utf8.ValidString(text) || isJSONValue(text) {
		return cassdbmodel.KeyValue{}, errTagNotIndexed
	}
	return tag, nil
}

// maxIndexedTagSize mirrors the writer's shouldIndexTag rule: a longer key or value is never
// written to tag_index.
const maxIndexedTagSize = 256

// errTagNotIndexed is the sentinel for a constant the writer never indexes. The caller converts
// it into an unsupported-query refusal when no other index can bound the search.
var errTagNotIndexed = errors.New("tag value is not indexed")

// isJSONValue mirrors the writer's rule that refuses to index a JSON-looking string.
func isJSONValue(s string) bool {
	return strings.HasPrefix(s, "{")
}

// tagRetained asks the deployment's TagFilter whether the tag survives indexing in every source
// the predicate names. A nil filter is the default that retains everything.
func tagRetained(filter cassdbmodel.TagFilter, tag cassdbmodel.KeyValue, sources map[tagSource]bool) bool {
	if filter == nil {
		return true
	}
	span := &cassdbmodel.Span{}
	tags := []cassdbmodel.KeyValue{tag}
	if sources[tagFromSpan] && len(filter.FilterTags(span, tags)) == 0 {
		return false
	}
	if sources[tagFromProcess] && len(filter.FilterProcessTags(span, tags)) == 0 {
		return false
	}
	if sources[tagFromLog] && len(filter.FilterLogFields(span, tags)) == 0 {
		return false
	}
	return true
}

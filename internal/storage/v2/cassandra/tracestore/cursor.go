// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
)

// Keyset pagination over the in-process sorted result, mirroring the memory backend's cursor:
// the cursor is the last returned span's sort key plus how many spans with that key have been
// returned, so duplicate spans that tie on every public key are each returned across pages.

// spanSortingKey holds one encoded value per ordering term, in query precedence order. Each
// value's byte order matches its ascending sort order.
type spanSortingKey [][]byte

// makeSpanSortingKey resolves each ordering term against the span the filter evaluator does,
// so every intrinsic sort field orders on the value the evaluator would read.
func makeSpanSortingKey(ctx filterCtx, order []tracestore.SpanSortOrder) spanSortingKey {
	key := make(spanSortingKey, len(order))
	for i, expr := range order {
		key[i] = encodeSortValue(resolveOperand(expr.Expression, ctx))
	}
	return key
}

// encodeSortValue encodes the value so bytes.Compare orders encodings as the values are
// ordered. The effective order names four intrinsic fields, and each resolves to either an
// integer (startTime, duration) or a string (traceID, spanID); the kind byte keeps the two
// encodings apart, and integers get their sign bit flipped so unsigned byte order is signed
// value order.
func encodeSortValue(values []evalValue) []byte {
	v := values[0]
	tag := []byte{byte(v.kind)}
	if v.kind == kindInt {
		return binary.BigEndian.AppendUint64(tag, uint64(v.numInt)^(1<<63)) //nolint:gosec // G115: sign-bit cast preserves signed order.
	}
	return append(tag, v.str...)
}

func compareSpanSortingKeys(order []tracestore.SpanSortOrder) func(spanSortingKey, spanSortingKey) int {
	return func(a, b spanSortingKey) int {
		for i, term := range order {
			result := bytes.Compare(a[i], b[i])
			if term.Direction == tracestore.SortDescending {
				result = -result
			}
			if result != 0 {
				return result
			}
		}
		return 0
	}
}

func (k spanSortingKey) encode() []byte {
	var buf []byte
	for _, value := range k {
		buf = binary.AppendUvarint(buf, uint64(len(value)))
		buf = append(buf, value...)
	}
	return buf
}

// spanCursor is where a page ended: the last span's key and how many spans with that key the
// pages so far have returned.
type spanCursor struct {
	key  spanSortingKey
	seen uint32
}

func (c spanCursor) encode() []byte {
	return binary.BigEndian.AppendUint32(c.key.encode(), c.seen)
}

func decodeSpanCursor(raw []byte, terms int) (spanCursor, error) {
	var key spanSortingKey
	for range terms {
		size, n := binary.Uvarint(raw)
		if n <= 0 {
			return spanCursor{}, errInvalidSpanPosition
		}
		raw = raw[n:]
		if size > uint64(len(raw)) {
			return spanCursor{}, errInvalidSpanPosition
		}
		key = append(key, raw[:size])
		raw = raw[size:]
	}
	if len(raw) != 4 {
		return spanCursor{}, errInvalidSpanPosition
	}
	return spanCursor{key: key, seen: binary.BigEndian.Uint32(raw)}, nil
}

var errInvalidSpanPosition = fmt.Errorf("%w: page token does not carry a span position",
	tracestore.ErrPaginationInvalid)

// pageSpans returns the spans that follow the cursor (all spans when after is nil), at most
// size of them, plus the next cursor when more spans remain. It is the memory backend's paging
// cut over the Cassandra result window.
func pageSpans(
	sorted []matchedSpan,
	compare func(spanSortingKey, spanSortingKey) int,
	after *spanCursor,
	size uint32,
) ([]matchedSpan, *spanCursor) {
	start := 0
	if after != nil {
		start = sort.Search(len(sorted), func(i int) bool { return compare(sorted[i].key, after.key) >= 0 })
		for skipped := uint32(0); skipped < after.seen && start < len(sorted) &&
			compare(sorted[start].key, after.key) == 0; skipped++ {
			start++
		}
	}
	rest := sorted[start:]
	if size == 0 || uint64(len(rest)) <= uint64(size) {
		return rest, nil
	}
	next := spanCursor{key: rest[size-1].key}
	for i := size; i > 0 && compare(rest[i-1].key, next.key) == 0; i-- {
		next.seen++
	}
	if after != nil && compare(next.key, after.key) == 0 {
		next.seen += after.seen
	}
	return rest[:size], &next
}

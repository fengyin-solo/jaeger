// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	expression "github.com/jaegertracing/jaeger-idl/query/expression/v1"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/sql"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/tracestore/dbmodel"
)

const (
	// maxSpanPageSize is the largest page FindSpans returns. The query service clamps a span
	// page size to the same MaxPageSize, and a caller reaching this reader directly gets the
	// same bound rather than an unconstrained scan of the spans table.
	maxSpanPageSize = tracestore.MaxPageSize
	// maxSpanCursorBytes bounds the cursor a continuation token may carry. A valid cursor is a
	// handful of fixed-width sort values (see spanCursor.encode), so one larger than this can
	// only have been minted by something else and is refused.
	maxSpanCursorBytes = 256
)

// spanSortColumns maps the built-in span fields an ordering term may name to the spans table
// column that carries them. The two IDs are lower-case fixed-width hex strings, whose byte
// order is their numeric order, so they sort directly as text.
var spanSortColumns = map[string]string{
	expression.SpanFieldStartTime: "s.start_time",
	expression.SpanFieldDuration:  "s.duration",
	expression.SpanFieldTraceID:   "s.trace_id",
	expression.SpanFieldSpanID:    "s.id",
}

// spanOrderColumn is one settled ordering term lowered to a column and its direction.
type spanOrderColumn struct {
	field     string
	column    string
	ascending bool
}

// spanOrderColumns lowers an order EffectiveSpanOrder has settled. EffectiveSpanOrder admits
// only the four built-in fields spanSortColumns maps and rebuilds every term as a FieldRef, so
// every term maps here; an unmapped term would mean the settlement changed, which
// TestSpanOrderColumnsCoverSettledFields pins.
func spanOrderColumns(order []tracestore.SpanSortOrder) []spanOrderColumn {
	columns := make([]spanOrderColumn, 0, len(order))
	for _, term := range order {
		ref := term.Expression.(*expression.FieldRef)
		columns = append(columns, spanOrderColumn{
			field:     ref.Name,
			column:    spanSortColumns[ref.Name],
			ascending: term.Direction != tracestore.SortDescending,
		})
	}
	return columns
}

// spanCursor is where a span page ended in the effective ordering: the last row's sort values
// and how many rows with that whole key the pages so far have returned. The tie-breakers
// EffectiveSpanOrder appends (trace ID, span ID) make the key unique per span; two rows that
// tie on every term are byte-identical duplicates, and seen is what lets the next page resume
// at the next copy instead of skip it or return the previous one again.
type spanCursor struct {
	values []any
	seen   uint32
}

// Cursor field tags and direction markers. They exist so a decoded cursor is checked against
// the order it is resumed in: a token minted behind one ordering is a position in that order.
const (
	spanCursorTagStart byte = 1
	spanCursorTagDur   byte = 2
	spanCursorTagTrace byte = 3
	spanCursorTagSpan  byte = 4

	spanCursorDirAsc  byte = 1
	spanCursorDirDesc byte = 2
)

var spanCursorFieldTags = map[string]byte{
	expression.SpanFieldStartTime: spanCursorTagStart,
	expression.SpanFieldDuration:  spanCursorTagDur,
	expression.SpanFieldTraceID:   spanCursorTagTrace,
	expression.SpanFieldSpanID:    spanCursorTagSpan,
}

func spanCursorInvalid() error {
	return fmt.Errorf("%w: page token does not carry a span position", tracestore.ErrPaginationInvalid)
}

// encode writes the cursor as one term count followed by, per ordering term, a field tag, a
// direction marker and the term's value (8 bytes for time and duration, a 2-byte length and
// bytes for the IDs), and finally the seen count. Its shape alone says which order it belongs
// to, which decode checks.
func (c spanCursor) encode(columns []spanOrderColumn) ([]byte, error) {
	if len(c.values) != len(columns) {
		return nil, spanCursorInvalid()
	}
	if len(columns) > 0xFF {
		return nil, spanCursorInvalid()
	}
	buf := make([]byte, 0, maxSpanCursorBytes)
	buf = append(buf, byte(len(columns))) //nolint:gosec // G115: bounded by the check above.
	for i, col := range columns {
		tag, ok := spanCursorFieldTags[col.field]
		if !ok {
			return nil, spanCursorInvalid()
		}
		buf = append(buf, tag)
		if col.ascending {
			buf = append(buf, spanCursorDirAsc)
		} else {
			buf = append(buf, spanCursorDirDesc)
		}
		switch value := c.values[i].(type) {
		case time.Time:
			if col.field != expression.SpanFieldStartTime {
				return nil, spanCursorInvalid()
			}
			buf = binary.BigEndian.AppendUint64(buf, uint64(value.UnixNano()))
		case int64:
			if col.field != expression.SpanFieldDuration {
				return nil, spanCursorInvalid()
			}
			// A bit-preserving cast: the value is decoded with the same cast back to int64.
			buf = binary.BigEndian.AppendUint64(buf, uint64(value)) //nolint:gosec // G115: round-trip encoding is bit-preserving.
		case string:
			if col.field != expression.SpanFieldTraceID && col.field != expression.SpanFieldSpanID {
				return nil, spanCursorInvalid()
			}
			if len(value) > 0xFFFF {
				return nil, spanCursorInvalid()
			}
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(value))) //nolint:gosec // G115: bounded by the check above.
			buf = append(buf, value...)
		default:
			return nil, spanCursorInvalid()
		}
	}
	buf = binary.BigEndian.AppendUint32(buf, c.seen)
	return buf, nil
}

// decodeSpanCursor reads a cursor encode wrote, checking it against the order it resumes.
func decodeSpanCursor(raw []byte, columns []spanOrderColumn) (spanCursor, error) {
	if len(raw) == 0 || len(raw) > maxSpanCursorBytes {
		return spanCursor{}, spanCursorInvalid()
	}
	if int(raw[0]) != len(columns) {
		return spanCursor{}, spanCursorInvalid()
	}
	pos := 1
	values := make([]any, len(columns))
	for i, col := range columns {
		if pos+2 > len(raw) {
			return spanCursor{}, spanCursorInvalid()
		}
		tag, dir := raw[pos], raw[pos+1]
		pos += 2
		if tag != spanCursorFieldTags[col.field] {
			return spanCursor{}, spanCursorInvalid()
		}
		wantDir := spanCursorDirAsc
		if !col.ascending {
			wantDir = spanCursorDirDesc
		}
		if dir != wantDir {
			return spanCursor{}, spanCursorInvalid()
		}
		switch col.field {
		case expression.SpanFieldStartTime, expression.SpanFieldDuration:
			if pos+8 > len(raw) {
				return spanCursor{}, spanCursorInvalid()
			}
			u := binary.BigEndian.Uint64(raw[pos : pos+8])
			pos += 8
			if col.field == expression.SpanFieldStartTime {
				values[i] = time.Unix(0, int64(u)).UTC() //nolint:gosec // G115: round-trip encoding is bit-preserving.
			} else {
				values[i] = int64(u) //nolint:gosec // G115: round-trip encoding is bit-preserving.
			}
		case expression.SpanFieldTraceID, expression.SpanFieldSpanID:
			if pos+2 > len(raw) {
				return spanCursor{}, spanCursorInvalid()
			}
			n := int(binary.BigEndian.Uint16(raw[pos : pos+2]))
			pos += 2
			if n == 0 || pos+n > len(raw) {
				return spanCursor{}, spanCursorInvalid()
			}
			values[i] = string(raw[pos : pos+n])
			pos += n
		default:
			return spanCursor{}, spanCursorInvalid()
		}
	}
	if len(raw)-pos != 4 {
		return spanCursor{}, spanCursorInvalid()
	}
	seen := binary.BigEndian.Uint32(raw[pos:])
	if seen == 0 {
		return spanCursor{}, spanCursorInvalid()
	}
	return spanCursor{values: values, seen: seen}, nil
}

// spanSortValue reads the value an ordering term sorts by from a span row.
func spanSortValue(row *dbmodel.SpanRow, field string) (any, error) {
	switch field {
	case expression.SpanFieldStartTime:
		return row.StartTime.UTC(), nil
	case expression.SpanFieldDuration:
		return row.Duration, nil
	case expression.SpanFieldTraceID:
		return row.TraceID, nil
	case expression.SpanFieldSpanID:
		return row.ID, nil
	default:
		return nil, fmt.Errorf("%w: it does not order spans by %q", tracestore.ErrSpanOrderUnsupported, field)
	}
}

// sameSpanSortValue reports whether two bound sort values are equal.
func sameSpanSortValue(a, b any) bool {
	switch av := a.(type) {
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	default:
		return false
	}
}

// rowMatchesSortKey reports whether row carries every value in key, term by term.
func rowMatchesSortKey(row *dbmodel.SpanRow, columns []spanOrderColumn, key []any) (bool, error) {
	for i, col := range columns {
		value, err := spanSortValue(row, col.field)
		if err != nil {
			return false, err
		}
		if !sameSpanSortValue(value, key[i]) {
			return false, nil
		}
	}
	return true, nil
}

// newSpanCursor builds the cursor for a page whose last returned row is rows' last element.
// seen counts the page's trailing rows tied on the whole sort key, plus the copies an earlier
// page already counted when the key carried across the boundary, so duplicates spanning more
// than one boundary keep resuming at the right copy.
func newSpanCursor(columns []spanOrderColumn, rows []*dbmodel.SpanRow, previous *spanCursor) (spanCursor, error) {
	last := rows[len(rows)-1]
	key := make([]any, len(columns))
	for i, col := range columns {
		value, err := spanSortValue(last, col.field)
		if err != nil {
			return spanCursor{}, err
		}
		key[i] = value
	}
	cursor := spanCursor{values: key}
	for i := len(rows) - 1; i >= 0; i-- {
		equal, err := rowMatchesSortKey(rows[i], columns, key)
		if err != nil {
			return spanCursor{}, err
		}
		if !equal {
			break
		}
		cursor.seen++
	}
	if previous != nil && len(previous.values) == len(key) {
		same := true
		for i := range key {
			if !sameSpanSortValue(previous.values[i], key[i]) {
				same = false
				break
			}
		}
		if same {
			cursor.seen += previous.seen
		}
	}
	return cursor, nil
}

// FindSpans returns one page of the spans matching query, in the effective order, with the
// token that resumes the search after it (RFC 0016 §6). Unlike FindTraces, the page holds
// exactly the matching span rows — each with its own resource and scope — from however many
// traces they belong to.
//
// The page is cut with a keyset predicate over the effective order: every value the predicate,
// the time window and the page bound compare against is a bind parameter, never SQL text. One
// extra row is fetched to tell a full page from the last one, and the next page resumes after
// the last returned key, skipping only duplicates of it the earlier pages already returned.
func (r *Reader) FindSpans(
	ctx context.Context,
	query tracestore.SpanQueryParams,
) iter.Seq2[tracestore.PageChunk[ptrace.Traces], error] {
	return func(yield func(tracestore.PageChunk[ptrace.Traces], error) bool) {
		// The query service settles the order before calling a reader, but a reader reached
		// directly must still refuse terms it cannot execute, and the settled order is the one
		// the fingerprint and the cursor agree on.
		order, err := tracestore.EffectiveSpanOrder(query.OrderBy)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}
		query.OrderBy = order
		columns := spanOrderColumns(order)
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
			cursor, err := decodeSpanCursor(raw, columns)
			if err != nil {
				yield(tracestore.PageChunk[ptrace.Traces]{}, err)
				return
			}
			after = &cursor
		}

		// The query service fills a default in and clamps an oversized page; a direct caller
		// gets the same treatment, with zero refused since a page with no bound would describe
		// an unbounded scan rather than a page.
		pageSize := query.Pagination.PageSize
		if pageSize == 0 {
			yield(tracestore.PageChunk[ptrace.Traces]{},
				fmt.Errorf("%w: page size must be greater than 0", tracestore.ErrPaginationInvalid))
			return
		}
		if pageSize > maxSpanPageSize {
			pageSize = maxSpanPageSize
		}

		sqlQuery, args, err := r.buildFindSpansQuery(ctx, query, columns, after, pageSize)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, fmt.Errorf("failed to build query: %w", err))
			return
		}
		rows, err := r.conn.Query(ctx, sqlQuery, args...)
		if err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, fmt.Errorf("failed to query spans: %w", err))
			return
		}

		var (
			spanRows []*dbmodel.SpanRow
			errs     []error
		)
		for rows.Next() {
			row, scanErr := dbmodel.ScanRow(rows)
			if scanErr != nil {
				errs = append(errs, fmt.Errorf("failed to scan span row: %w", scanErr))
				break
			}
			spanRows = append(spanRows, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			errs = append(errs, fmt.Errorf("failed to read span rows: %w", rowsErr))
		}
		if closeErr := rows.Close(); closeErr != nil {
			errs = append(errs, fmt.Errorf("failed to close rows: %w", closeErr))
		}
		if err := errors.Join(errs...); err != nil {
			yield(tracestore.PageChunk[ptrace.Traces]{}, err)
			return
		}

		// One row past the page distinguishes a full page from the last one.
		fetch := int(pageSize) + 1
		more := len(spanRows) >= fetch
		if more {
			spanRows = spanRows[:int(pageSize)]
		}

		// Every matched row keeps its own resource and scope, so two rows from different
		// services may sit next to each other in the same Traces (RFC 0016 §6.4).
		page := ptrace.NewTraces()
		for _, row := range spanRows {
			dbmodel.FromRow(row).ResourceSpans().MoveAndAppendTo(page.ResourceSpans())
		}
		chunk := tracestore.PageChunk[ptrace.Traces]{Results: page}
		if more {
			token, err := mintSpanPageToken(fingerprint, columns, spanRows, after)
			if err != nil {
				yield(tracestore.PageChunk[ptrace.Traces]{}, err)
				return
			}
			chunk.NextPageToken = token
		}
		yield(chunk, nil)
	}
}

// mintSpanPageToken builds the continuation token for a page: the cursor over the last returned
// row's sort key, wrapped with the query's fingerprint.
func mintSpanPageToken(
	fingerprint []byte,
	columns []spanOrderColumn,
	rows []*dbmodel.SpanRow,
	after *spanCursor,
) (tracestore.PageToken, error) {
	cursor, err := newSpanCursor(columns, rows, after)
	if err != nil {
		return "", err
	}
	raw, err := cursor.encode(columns)
	if err != nil {
		return "", err
	}
	return tracestore.NewPageToken(fingerprint, raw)
}

// buildFindSpansQuery lowers one page request onto the spans table.
func (r *Reader) buildFindSpansQuery(
	ctx context.Context,
	query tracestore.SpanQueryParams,
	columns []spanOrderColumn,
	after *spanCursor,
	pageSize uint32,
) (string, []any, error) {
	var q strings.Builder
	q.WriteString(strings.TrimRight(sql.SelectSpansQuery, "\n"))
	q.WriteString("\nWHERE 1=1")
	args := make([]any, 0)

	if !query.StartTimeMin.IsZero() {
		appendAnd(&q, "s.start_time >= ?")
		args = append(args, query.StartTimeMin)
	}
	if !query.StartTimeMax.IsZero() {
		appendAnd(&q, "s.start_time <= ?")
		args = append(args, query.StartTimeMax)
	}

	if query.Filter != nil {
		// The same lowering FindTraceIDs uses, with one metadata lookup for the whole tree;
		// every refusal it returns is made here too, because a remote-storage client reaches
		// this reader without the query service's capability checks.
		metadata, err := r.lookupUntypedMetadata(ctx, query.Filter)
		if err != nil {
			return "", nil, err
		}
		appendAnd(&q, "(")
		args, err = buildFilterCondition(&q, 2, args, metadata, query.Filter)
		if err != nil {
			return "", nil, err
		}
		appendNewlineAndIndent(&q, 1)
		q.WriteString(")")
	}

	if after != nil {
		args = appendSpanKeysetPredicate(&q, 1, args, columns, after)
	}

	q.WriteString("\nORDER BY")
	for i, col := range columns {
		q.WriteString("\n\t")
		q.WriteString(col.column)
		if col.ascending {
			q.WriteString(" ASC")
		} else {
			q.WriteString(" DESC")
		}
		if i < len(columns)-1 {
			q.WriteString(",")
		}
	}

	// The LIMIT is the page plus its probe row; the OFFSET skips the duplicate copies of the
	// cursor key the previous pages returned (zero on the first page). Both are bound.
	q.WriteString("\nLIMIT ? OFFSET ?")
	var offset uint32
	if after != nil {
		offset = after.seen
	}
	args = append(args, pageSize+1, offset)

	return q.String(), args, nil
}

// appendSpanKeysetPredicate appends the keyset resume condition: a row follows the cursor when
// it is strictly after the cursor's key in the effective ordering (at the first differing term
// it points past the cursor's value, every earlier term equal), or ties on every term and is
// one of the duplicates the OFFSET then skips. A descending term flips only that term's
// comparison. Every value is a bind parameter.
func appendSpanKeysetPredicate(
	q *strings.Builder,
	indent int,
	args []any,
	columns []spanOrderColumn,
	cursor *spanCursor,
) []any {
	appendNewlineAndIndent(q, indent)
	q.WriteString("AND (")
	for i := range columns {
		appendNewlineAndIndent(q, indent+1)
		if i > 0 {
			q.WriteString("OR ")
		}
		q.WriteString("(")
		for j := 0; j < i; j++ {
			appendNewlineAndIndent(q, indent+2)
			if j > 0 {
				q.WriteString("AND ")
			}
			q.WriteString(columns[j].column + " = ?")
			args = append(args, cursor.values[j])
		}
		appendNewlineAndIndent(q, indent+2)
		if i > 0 {
			q.WriteString("AND ")
		}
		op := ">"
		if !columns[i].ascending {
			op = "<"
		}
		q.WriteString(columns[i].column + " " + op + " ?")
		args = append(args, cursor.values[i])
		appendNewlineAndIndent(q, indent+1)
		q.WriteString(")")
	}

	appendNewlineAndIndent(q, indent+1)
	q.WriteString("OR (")
	for i := range columns {
		appendNewlineAndIndent(q, indent+2)
		if i > 0 {
			q.WriteString("AND ")
		}
		q.WriteString(columns[i].column + " = ?")
		args = append(args, cursor.values[i])
	}
	appendNewlineAndIndent(q, indent+1)
	q.WriteString(")")

	appendNewlineAndIndent(q, indent)
	q.WriteString(")")
	return args
}

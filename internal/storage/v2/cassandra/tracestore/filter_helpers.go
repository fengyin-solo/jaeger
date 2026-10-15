// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/telemetry/otelsemconv"
)

// fromOTELSpanKind maps an OTLP span kind to the lower-case word the filter vocabulary uses.
// Unspecified maps to empty, which resolveSpanField replaces with the vocabulary's "unspecified".
func fromOTELSpanKind(kind ptrace.SpanKind) string {
	if kind == ptrace.SpanKindUnspecified {
		return ""
	}
	return strings.ToLower(kind.String())
}

// getServiceNameFromResource reads the service.name resource attribute.
func getServiceNameFromResource(resource pcommon.Resource) string {
	val, ok := resource.Attributes().Get(otelsemconv.ServiceNameKey)
	if !ok {
		return ""
	}
	return val.Str()
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package policyprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"
)

func TestTraceFilterPolicy(t *testing.T) {
	traceID := pcommon.TraceID([16]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99})
	spanID := pcommon.SpanID([8]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
	parentSpanID := pcommon.SpanID([8]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80})

	tests := []struct {
		name         string
		rule         map[string]any
		setupSpan    func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource)
		expectedDrop bool
	}{
		{
			name: "operation name regex match - drop",
			rule: map[string]any{
				"id":     "drop-healthcheck-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
						"regex":  ".*/healthz.*",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetName("GET /healthz")
			},
			expectedDrop: true,
		},
		{
			name: "trace ID exact match - drop",
			rule: map[string]any{
				"id":     "drop-trace-id",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_TRACE_ID"},
						"exact":  "aabbccddeeff00112233445566778899",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetTraceID(traceID)
			},
			expectedDrop: true,
		},
		{
			name: "span ID exact match - drop",
			rule: map[string]any{
				"id":     "drop-span-id",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_SPAN_ID"},
						"exact":  "0102030405060708",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetSpanID(spanID)
			},
			expectedDrop: true,
		},
		{
			name: "parent span ID exact match - drop",
			rule: map[string]any{
				"id":     "drop-child-of-parent",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_PARENT_SPAN_ID"},
						"exact":  "1020304050607080",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetParentSpanID(parentSpanID)
			},
			expectedDrop: true,
		},
		{
			name: "root span empty parent span ID - exists is false, does not drop",
			rule: map[string]any{
				"id":     "drop-if-parent-exists",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_PARENT_SPAN_ID"},
						"exists": map[string]any{},
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetParentSpanID(pcommon.NewSpanIDEmpty()) // Root span
			},
			expectedDrop: false,
		},
		{
			name: "root span empty parent span ID with negate exists - drops root spans",
			rule: map[string]any{
				"id":     "drop-root-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_PARENT_SPAN_ID"},
						"exists": map[string]any{},
						"negate": true,
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetParentSpanID(pcommon.NewSpanIDEmpty()) // Root span
			},
			expectedDrop: true,
		},
		{
			name: "status message exact match - drop",
			rule: map[string]any{
				"id":     "drop-status-msg",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_STATUS_MESSAGE"},
						"exact":  "deadline exceeded",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.Status().SetMessage("deadline exceeded")
			},
			expectedDrop: true,
		},
		{
			name: "span kind INTERNAL - drop",
			rule: map[string]any{
				"id":     "drop-internal-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
						"exact":  "INTERNAL",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetKind(ptrace.SpanKindInternal)
			},
			expectedDrop: true,
		},
		{
			name: "span kind SERVER - drop",
			rule: map[string]any{
				"id":     "drop-server-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
						"exact":  "SERVER",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetKind(ptrace.SpanKindServer)
			},
			expectedDrop: true,
		},
		{
			name: "span kind CLIENT - drop",
			rule: map[string]any{
				"id":     "drop-client-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
						"exact":  "CLIENT",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetKind(ptrace.SpanKindClient)
			},
			expectedDrop: true,
		},
		{
			name: "span kind PRODUCER - drop",
			rule: map[string]any{
				"id":     "drop-producer-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
						"exact":  "PRODUCER",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetKind(ptrace.SpanKindProducer)
			},
			expectedDrop: true,
		},
		{
			name: "span kind CONSUMER - drop",
			rule: map[string]any{
				"id":     "drop-consumer-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
						"exact":  "CONSUMER",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.SetKind(ptrace.SpanKindConsumer)
			},
			expectedDrop: true,
		},
		{
			name: "status code ERROR - drop",
			rule: map[string]any{
				"id":     "drop-error-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_STATUS_CODE"},
						"exact":  "ERROR",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.Status().SetCode(ptrace.StatusCodeError)
			},
			expectedDrop: true,
		},
		{
			name: "status code OK - drop",
			rule: map[string]any{
				"id":     "drop-ok-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_STATUS_CODE"},
						"exact":  "OK",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.Status().SetCode(ptrace.StatusCodeOk)
			},
			expectedDrop: true,
		},
		{
			name: "status code UNSET - drop",
			rule: map[string]any{
				"id":     "drop-unset-spans",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_STATUS_CODE"},
						"exact":  "UNSET",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.Status().SetCode(ptrace.StatusCodeUnset)
			},
			expectedDrop: true,
		},
		{
			name: "span attribute match - drop",
			rule: map[string]any{
				"id":     "drop-db-query",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"span_attribute": "db.system"},
						"exact":  "redis",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				span.Attributes().PutStr("db.system", "redis")
			},
			expectedDrop: true,
		},
		{
			name: "scope field NAME match - drop",
			rule: map[string]any{
				"id":     "drop-scope-tracer",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_NAME"},
						"exact":  "tracer-a",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				scope.SetName("tracer-a")
			},
			expectedDrop: true,
		},
		{
			name: "resource attribute match - drop",
			rule: map[string]any{
				"id":     "drop-res-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"resource_attribute": "service.name"},
						"exact":  "auth-service",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				res.Attributes().PutStr("service.name", "auth-service")
			},
			expectedDrop: true,
		},
		{
			name: "scope attribute match - drop",
			rule: map[string]any{
				"id":     "drop-scope-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_attribute": "instrumentation.library"},
						"exact":  "custom-tracer",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				scope.Attributes().PutStr("instrumentation.library", "custom-tracer")
			},
			expectedDrop: true,
		},
		{
			name: "scope field VERSION match - drop",
			rule: map[string]any{
				"id":     "drop-scope-ver",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_VERSION"},
						"exact":  "v1.2.3",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				scope.SetVersion("v1.2.3")
			},
			expectedDrop: true,
		},
		{
			name: "scope field SCHEMA_URL match - drop",
			rule: map[string]any{
				"id":     "drop-scope-schema",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_SCHEMA_URL"},
						"exact":  "https://opentelemetry.io/schemas/1.24.0",
					},
				},
			},
			setupSpan: func(span ptrace.Span, scope pcommon.InstrumentationScope, ss ptrace.ScopeSpans, res pcommon.Resource) {
				ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.24.0")
			},
			expectedDrop: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Policies: []PolicyConfig{
					{
						ID:      tc.rule["id"].(string),
						TypeURL: TypeURLTraceFilterPolicy,
						Rule:    tc.rule,
					},
				},
			}
			err := cfg.Validate()
			require.NoError(t, err)

			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			ss := rs.ScopeSpans().AppendEmpty()
			span := ss.Spans().AppendEmpty()

			tc.setupSpan(span, ss.Scope(), ss, rs.Resource())

			dropped := evaluateSpan(context.Background(), span, ss.Scope(), ss, rs.Resource(), cfg.Compiled.TracePolicies, nil)
			assert.Equal(t, tc.expectedDrop, dropped)
		})
	}
}

func TestTracePruning(t *testing.T) {
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-health",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-health",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
							"exact":  "/health",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	td := ptrace.NewTraces()
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("res", "1")

	// Scope 1 has only /health (should be pruned)
	ss1 := rs1.ScopeSpans().AppendEmpty()
	ss1.Scope().SetName("scope-1")
	span1 := ss1.Spans().AppendEmpty()
	span1.SetName("/health")

	// Scope 2 has /health and /order (Scope 2 retained with /order)
	ss2 := rs1.ScopeSpans().AppendEmpty()
	ss2.Scope().SetName("scope-2")
	span2_1 := ss2.Spans().AppendEmpty()
	span2_1.SetName("/health")
	span2_2 := ss2.Spans().AppendEmpty()
	span2_2.SetName("/order")

	// Resource 2 has only /health (Resource 2 pruned entirely)
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("res", "2")
	ss3 := rs2.ScopeSpans().AppendEmpty()
	span3 := ss3.Spans().AppendEmpty()
	span3.SetName("/health")

	pruneTraces(context.Background(), td, cfg.Compiled.TracePolicies, nil)

	// Resource 2 pruned
	assert.Equal(t, 1, td.ResourceSpans().Len())
	remainingRS := td.ResourceSpans().At(0)
	assert.Equal(t, "1", remainingRS.Resource().Attributes().AsRaw()["res"])

	// Scope 1 pruned
	assert.Equal(t, 1, remainingRS.ScopeSpans().Len())
	remainingSS := remainingRS.ScopeSpans().At(0)
	assert.Equal(t, "scope-2", remainingSS.Scope().Name())

	// Only /order remains
	assert.Equal(t, 1, remainingSS.Spans().Len())
	assert.Equal(t, "/order", remainingSS.Spans().At(0).Name())
}

func TestTraceFilterPolicy_PrecedenceKeepOverridesDrop(t *testing.T) {
	// Policy 1: Drop all client spans
	// Policy 2: Keep spans named "keep-this"
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-client",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-client",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_KIND"},
							"exact":  "CLIENT",
						},
					},
				},
			},
			{
				ID:      "keep-important",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "keep-important",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
							"exact":  "keep-this",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	var recordedOpt metric.MeasurementOption
	evalRecorder := func(ctx context.Context, opt metric.MeasurementOption) {
		recordedOpt = opt
	}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetKind(ptrace.SpanKindClient)
	span.SetName("keep-this")

	dropped := evaluateSpan(context.Background(), span, ss.Scope(), ss, rs.Resource(), cfg.Compiled.TracePolicies, evalRecorder)
	assert.False(t, dropped, "ACTION_KEEP should override ACTION_DROP")
	assert.Equal(t, cfg.Compiled.TracePolicies[1].KeepOption, recordedOpt)
}

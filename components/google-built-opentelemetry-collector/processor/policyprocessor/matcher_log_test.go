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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric"
)

func TestLogFilterPolicy(t *testing.T) {
	traceID := pcommon.TraceID([16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10})
	spanID := pcommon.SpanID([8]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0})

	tests := []struct {
		name         string
		rule         map[string]any
		setupLog     func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource)
		expectedDrop bool
	}{
		{
			name: "body primitive string exact match - drop",
			rule: map[string]any{
				"id":     "drop-hello",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
						"exact":  "hello world",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.Body().SetStr("hello world")
			},
			expectedDrop: true,
		},
		{
			name: "body complex map serialized to JSON exact match - drop",
			rule: map[string]any{
				"id":     "drop-json-body",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
						"exact":  `{"msg":"access_denied","user":"alice"}`,
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				m := lr.Body().SetEmptyMap()
				m.PutStr("msg", "access_denied")
				m.PutStr("user", "alice")
			},
			expectedDrop: true,
		},
		{
			name: "body complex array serialized to JSON regex match - drop",
			rule: map[string]any{
				"id":     "drop-json-array",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
						"regex":  `\[.*"error".*\]`,
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				s := lr.Body().SetEmptySlice()
				s.AppendEmpty().SetStr("warning")
				s.AppendEmpty().SetStr("error")
			},
			expectedDrop: true,
		},
		{
			name: "severity text RE2 regex match - drop",
			rule: map[string]any{
				"id":     "drop-debug-logs",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"},
						"regex":  "(?i)^debug.*",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetSeverityText("DEBUG2")
			},
			expectedDrop: true,
		},
		{
			name: "severity number positive match (severity 17 = ERROR) - drop",
			rule: map[string]any{
				"id":     "drop-severity-17",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_NUMBER"},
						"exact":  "17",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetSeverityNumber(plog.SeverityNumberError)
			},
			expectedDrop: true,
		},
		{
			name: "severity number 0 unspecified evaluates as non-existent - exists matches false",
			rule: map[string]any{
				"id":     "drop-if-severity-number-exists",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_NUMBER"},
						"exists": map[string]any{},
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetSeverityNumber(plog.SeverityNumberUnspecified) // 0
			},
			expectedDrop: false, // 0 is non-existent, so exists evaluates to false
		},
		{
			name: "severity number 0 unspecified with negate exists evaluates to true",
			rule: map[string]any{
				"id":     "drop-if-no-severity-number",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_NUMBER"},
						"exists": map[string]any{},
						"negate": true,
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetSeverityNumber(plog.SeverityNumberUnspecified)
			},
			expectedDrop: true,
		},
		{
			name: "trace ID hex matching - drop",
			rule: map[string]any{
				"id":     "drop-trace-id",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_TRACE_ID"},
						"exact":  "0123456789abcdeffedcba9876543210",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetTraceID(traceID)
			},
			expectedDrop: true,
		},
		{
			name: "span ID hex matching - drop",
			rule: map[string]any{
				"id":     "drop-span-id",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SPAN_ID"},
						"exact":  "123456789abcdef0",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.SetSpanID(spanID)
			},
			expectedDrop: true,
		},
		{
			name: "log attribute matching - drop",
			rule: map[string]any{
				"id":     "drop-healthcheck-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"log_attribute": "http.route"},
						"exact":  "/healthz",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				lr.Attributes().PutStr("http.route", "/healthz")
			},
			expectedDrop: true,
		},
		{
			name: "resource attribute matching - drop",
			rule: map[string]any{
				"id":     "drop-canary-service",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"resource_attribute": "service.name"},
						"exact":  "canary-service",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				res.Attributes().PutStr("service.name", "canary-service")
			},
			expectedDrop: true,
		},
		{
			name: "scope attribute matching - drop",
			rule: map[string]any{
				"id":     "drop-scope-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_attribute": "lib.env"},
						"exact":  "test",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				scope.Attributes().PutStr("lib.env", "test")
			},
			expectedDrop: true,
		},
		{
			name: "scope field NAME matching - drop",
			rule: map[string]any{
				"id":     "drop-scope-name",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_NAME"},
						"exact":  "noisy-logger",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				scope.SetName("noisy-logger")
			},
			expectedDrop: true,
		},
		{
			name: "scope field VERSION matching - drop",
			rule: map[string]any{
				"id":     "drop-scope-version",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_VERSION"},
						"exact":  "v0.0.1-alpha",
					},
				},
			},
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				scope.SetVersion("v0.0.1-alpha")
			},
			expectedDrop: true,
		},
		{
			name: "scope field SCHEMA_URL matching - drop",
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
			setupLog: func(lr plog.LogRecord, scope pcommon.InstrumentationScope, sl plog.ScopeLogs, res pcommon.Resource) {
				sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.24.0")
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
						TypeURL: TypeURLLogFilterPolicy,
						Rule:    tc.rule,
					},
				},
			}
			err := cfg.Validate()
			require.NoError(t, err)

			ld := plog.NewLogs()
			rl := ld.ResourceLogs().AppendEmpty()
			sl := rl.ScopeLogs().AppendEmpty()
			lr := sl.LogRecords().AppendEmpty()

			tc.setupLog(lr, sl.Scope(), sl, rl.Resource())

			dropped := evaluateLogRecord(context.Background(), lr, sl.Scope(), sl, rl.Resource(), cfg.Compiled.LogPolicies, nil)
			assert.Equal(t, tc.expectedDrop, dropped)
		})
	}
}

func TestLogFilterPolicy_PrecedenceKeepOverridesDrop(t *testing.T) {
	// Policy 1: Drop all INFO logs
	// Policy 2: Keep logs whose body contains "CRITICAL"
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-info",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-info",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"},
							"exact":  "INFO",
						},
					},
				},
			},
			{
				ID:      "keep-critical",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "keep-critical",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
							"regex":  ".*CRITICAL.*",
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

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("This is a CRITICAL incident!")

	dropped := evaluateLogRecord(context.Background(), lr, sl.Scope(), sl, rl.Resource(), cfg.Compiled.LogPolicies, evalRecorder)
	assert.False(t, dropped, "ACTION_KEEP should override ACTION_DROP")
	assert.Equal(t, cfg.Compiled.LogPolicies[1].KeepOption, recordedOpt)
}

func TestLogPruning(t *testing.T) {
	// Drop all logs with severity text "DEBUG"
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-debug",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-debug",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"},
							"exact":  "DEBUG",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	ld := plog.NewLogs()
	rl1 := ld.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "svc-1")

	// Scope 1: Has only DEBUG logs (should be pruned)
	sl1 := rl1.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-1")
	lr1 := sl1.LogRecords().AppendEmpty()
	lr1.SetSeverityText("DEBUG")

	// Scope 2: Has one DEBUG log and one INFO log (should retain Scope 2 with only INFO log)
	sl2 := rl1.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("scope-2")
	lr2_1 := sl2.LogRecords().AppendEmpty()
	lr2_1.SetSeverityText("DEBUG")
	lr2_2 := sl2.LogRecords().AppendEmpty()
	lr2_2.SetSeverityText("INFO")

	// Resource 2: Has only DEBUG logs (entire ResourceLogs should be pruned)
	rl2 := ld.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "svc-2")
	sl3 := rl2.ScopeLogs().AppendEmpty()
	sl3.Scope().SetName("scope-3")
	lr3 := sl3.LogRecords().AppendEmpty()
	lr3.SetSeverityText("DEBUG")

	pruneLogs(context.Background(), ld, cfg.Compiled.LogPolicies, nil)

	// Resource 2 should have been pruned entirely
	assert.Equal(t, 1, ld.ResourceLogs().Len())
	remainingRL := ld.ResourceLogs().At(0)
	assert.Equal(t, "svc-1", remainingRL.Resource().Attributes().AsRaw()["service.name"])

	// Scope 1 in Resource 1 should have been pruned entirely
	assert.Equal(t, 1, remainingRL.ScopeLogs().Len())
	remainingSL := remainingRL.ScopeLogs().At(0)
	assert.Equal(t, "scope-2", remainingSL.Scope().Name())

	// Only INFO log should remain in Scope 2
	assert.Equal(t, 1, remainingSL.LogRecords().Len())
	assert.Equal(t, "INFO", remainingSL.LogRecords().At(0).SeverityText())
}

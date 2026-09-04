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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func BenchmarkTraceIDMatching(b *testing.B) {
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "trace-id-match",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "trace-id-match",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_TRACE_ID"},
							"exact":  "0123456789abcdeffedcba9876543210",
						},
					},
				},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		b.Fatal(err)
	}

	span := ptrace.NewSpan()
	span.SetTraceID(pcommon.TraceID([16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}))
	scope := pcommon.NewInstrumentationScope()
	ss := ptrace.NewScopeSpans()
	res := pcommon.NewResource()
	policies := cfg.Compiled.TracePolicies
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = evaluateSpan(ctx, span, scope, ss, res, policies, nil)
	}
}

func BenchmarkSpanIDMatching(b *testing.B) {
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "span-id-match",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "span-id-match",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_SPAN_ID"},
							"exact":  "0123456789abcdef",
						},
					},
				},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		b.Fatal(err)
	}

	span := ptrace.NewSpan()
	span.SetSpanID(pcommon.SpanID([8]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}))
	scope := pcommon.NewInstrumentationScope()
	ss := ptrace.NewScopeSpans()
	res := pcommon.NewResource()
	policies := cfg.Compiled.TracePolicies
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = evaluateSpan(ctx, span, scope, ss, res, policies, nil)
	}
}

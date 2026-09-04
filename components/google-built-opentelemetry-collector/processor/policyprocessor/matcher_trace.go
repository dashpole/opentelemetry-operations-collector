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
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
)

// CompiledTraceMatcher evaluates a single matcher condition against a span and its metadata.
type CompiledTraceMatcher struct {
	Target    *policyv1alpha1.TraceFieldSelector
	Predicate PredicateEvaluator
}

// Eval evaluates the trace matcher using stack-allocated buffers for zero-heap-allocation ID matching.
func (m *CompiledTraceMatcher) Eval(
	span ptrace.Span,
	scope pcommon.InstrumentationScope,
	scopeSpans ptrace.ScopeSpans,
	resource pcommon.Resource,
) bool {
	switch t := m.Target.Target.(type) {
	case *policyv1alpha1.TraceFieldSelector_RecordField:
		switch t.RecordField {
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_NAME:
			name := span.Name()
			return m.Predicate.Eval(name, name != "")
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_TRACE_ID:
			traceID := span.TraceID()
			if traceID.IsEmpty() {
				return m.Predicate.EvalExactBytes(nil, false)
			}
			if m.Predicate.isExact {
				var buf [32]byte
				hex.Encode(buf[:], traceID[:])
				return m.Predicate.EvalExactBytes(buf[:], true)
			}
			if m.Predicate.isExists {
				return m.Predicate.EvalExactBytes(nil, true)
			}
			return m.Predicate.Eval(traceID.String(), true)
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_SPAN_ID:
			spanID := span.SpanID()
			if spanID.IsEmpty() {
				return m.Predicate.EvalExactBytes(nil, false)
			}
			if m.Predicate.isExact {
				var buf [16]byte
				hex.Encode(buf[:], spanID[:])
				return m.Predicate.EvalExactBytes(buf[:], true)
			}
			if m.Predicate.isExists {
				return m.Predicate.EvalExactBytes(nil, true)
			}
			return m.Predicate.Eval(spanID.String(), true)
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_PARENT_SPAN_ID:
			parentSpanID := span.ParentSpanID()
			if parentSpanID.IsEmpty() {
				// Root span: parent span ID is empty / does not exist
				return m.Predicate.EvalExactBytes(nil, false)
			}
			if m.Predicate.isExact {
				var buf [16]byte
				hex.Encode(buf[:], parentSpanID[:])
				return m.Predicate.EvalExactBytes(buf[:], true)
			}
			if m.Predicate.isExists {
				return m.Predicate.EvalExactBytes(nil, true)
			}
			return m.Predicate.Eval(parentSpanID.String(), true)
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_STATUS_MESSAGE:
			msg := span.Status().Message()
			return m.Predicate.Eval(msg, msg != "")
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_KIND:
			k := span.Kind()
			kStr := spanKindString(k)
			return m.Predicate.Eval(kStr, k != ptrace.SpanKindUnspecified)
		case policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_STATUS_CODE:
			codeStr := statusCodeString(span.Status().Code())
			return m.Predicate.Eval(codeStr, true)
		default:
			return false
		}
	case *policyv1alpha1.TraceFieldSelector_SpanAttribute:
		val, ok := span.Attributes().Get(t.SpanAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.TraceFieldSelector_ResourceAttribute:
		val, ok := resource.Attributes().Get(t.ResourceAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.TraceFieldSelector_ScopeAttribute:
		val, ok := scope.Attributes().Get(t.ScopeAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.TraceFieldSelector_ScopeField:
		switch t.ScopeField {
		case policyv1alpha1.ScopeField_SCOPE_FIELD_NAME:
			name := scope.Name()
			return m.Predicate.Eval(name, name != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_VERSION:
			ver := scope.Version()
			return m.Predicate.Eval(ver, ver != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_SCHEMA_URL:
			schemaURL := scopeSpans.SchemaUrl()
			return m.Predicate.Eval(schemaURL, schemaURL != "")
		default:
			return false
		}
	default:
		return false
	}
}

func spanKindString(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindInternal:
		return "INTERNAL"
	case ptrace.SpanKindServer:
		return "SERVER"
	case ptrace.SpanKindClient:
		return "CLIENT"
	case ptrace.SpanKindProducer:
		return "PRODUCER"
	case ptrace.SpanKindConsumer:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

func statusCodeString(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeOk:
		return "OK"
	case ptrace.StatusCodeError:
		return "ERROR"
	case ptrace.StatusCodeUnset:
		return "UNSET"
	default:
		return "UNSET"
	}
}

// evaluateSpan evaluates all compiled trace policies against a single span.
// Returns true if the span should be dropped, false if kept.
// Follows precedence: ACTION_KEEP overrides ACTION_DROP with immediate break.
func evaluateSpan(
	ctx context.Context,
	span ptrace.Span,
	scope pcommon.InstrumentationScope,
	scopeSpans ptrace.ScopeSpans,
	resource pcommon.Resource,
	policies []CompiledTracePolicy,
	recordEval evaluationRecorderFunc,
) bool {
	if len(policies) == 0 {
		return false // default allow
	}

	var keepPolicy *CompiledTracePolicy
	var dropPolicy *CompiledTracePolicy

	for i := range policies {
		p := &policies[i]
		matchesAll := true
		for _, m := range p.Matchers {
			if !m.Eval(span, scope, scopeSpans, resource) {
				matchesAll = false
				break
			}
		}

		if matchesAll {
			if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_KEEP {
				keepPolicy = p
				break // ACTION_KEEP overrides ACTION_DROP, break immediately!
			} else if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_DROP {
				if dropPolicy == nil {
					dropPolicy = p
				}
			}
		}
	}

	if keepPolicy != nil {
		if recordEval != nil {
			recordEval(ctx, keepPolicy.KeepOption)
		}
		return false
	}
	if dropPolicy != nil {
		if recordEval != nil {
			recordEval(ctx, dropPolicy.DropOption)
		}
		return true
	}

	return false // default allow
}

// pruneTraces recursively filters spans and removes empty scopes and resources.
func pruneTraces(
	ctx context.Context,
	td ptrace.Traces,
	policies []CompiledTracePolicy,
	recordEval evaluationRecorderFunc,
) {
	if len(policies) == 0 {
		return
	}

	rss := td.ResourceSpans()
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		res := rs.Resource()
		sss := rs.ScopeSpans()
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			scope := ss.Scope()
			spans := ss.Spans()
			spans.RemoveIf(func(span ptrace.Span) bool {
				return evaluateSpan(ctx, span, scope, ss, res, policies, recordEval)
			})
			return spans.Len() == 0
		})
		return sss.Len() == 0
	})
}

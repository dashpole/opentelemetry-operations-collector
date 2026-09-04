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
	"encoding/json"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
)

// CompiledLogMatcher evaluates a single matcher condition against a log record and its metadata.
type CompiledLogMatcher struct {
	Target    *policyv1alpha1.LogFieldSelector
	Predicate PredicateEvaluator
}

// Eval evaluates the log matcher.
func (m *CompiledLogMatcher) Eval(
	lr plog.LogRecord,
	scope pcommon.InstrumentationScope,
	scopeLogs plog.ScopeLogs,
	resource pcommon.Resource,
) bool {
	switch t := m.Target.Target.(type) {
	case *policyv1alpha1.LogFieldSelector_RecordField:
		switch t.RecordField {
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY:
			val, exists := stringifyValue(lr.Body())
			return m.Predicate.Eval(val, exists)
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT:
			sevText := lr.SeverityText()
			return m.Predicate.Eval(sevText, sevText != "")
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_NUMBER:
			sevNum := lr.SeverityNumber()
			if sevNum == plog.SeverityNumberUnspecified {
				return m.Predicate.Eval("", false)
			}
			return m.Predicate.Eval(strconv.Itoa(int(sevNum)), true)
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_TRACE_ID:
			traceID := lr.TraceID()
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
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SPAN_ID:
			spanID := lr.SpanID()
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
		default:
			return false
		}
	case *policyv1alpha1.LogFieldSelector_LogAttribute:
		val, ok := lr.Attributes().Get(t.LogAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.LogFieldSelector_ResourceAttribute:
		val, ok := resource.Attributes().Get(t.ResourceAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.LogFieldSelector_ScopeAttribute:
		val, ok := scope.Attributes().Get(t.ScopeAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.LogFieldSelector_ScopeField:
		switch t.ScopeField {
		case policyv1alpha1.ScopeField_SCOPE_FIELD_NAME:
			name := scope.Name()
			return m.Predicate.Eval(name, name != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_VERSION:
			ver := scope.Version()
			return m.Predicate.Eval(ver, ver != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_SCHEMA_URL:
			schemaURL := scopeLogs.SchemaUrl()
			return m.Predicate.Eval(schemaURL, schemaURL != "")
		default:
			return false
		}
	default:
		return false
	}
}

// stringifyValue converts a pcommon.Value to its string representation.
// Primitive values are converted directly; complex maps/slices are canonical JSON.
func stringifyValue(val pcommon.Value) (string, bool) {
	switch val.Type() {
	case pcommon.ValueTypeEmpty:
		return "", false
	case pcommon.ValueTypeStr:
		return val.Str(), true
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(val.Int(), 10), true
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(val.Double(), 'f', -1, 64), true
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(val.Bool()), true
	case pcommon.ValueTypeBytes:
		return string(val.Bytes().AsRaw()), true
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		b, err := json.Marshal(val.AsRaw())
		if err != nil {
			return val.AsString(), true
		}
		return string(b), true
	default:
		return val.AsString(), true
	}
}

// evaluateLogRecord evaluates all compiled log policies against a single log record.
// Returns true if the record should be dropped, false if kept.
// Follows precedence: ACTION_KEEP overrides ACTION_DROP with immediate break.
func evaluateLogRecord(
	ctx context.Context,
	lr plog.LogRecord,
	scope pcommon.InstrumentationScope,
	scopeLogs plog.ScopeLogs,
	resource pcommon.Resource,
	policies []CompiledLogPolicy,
	recordEval evaluationRecorderFunc,
) bool {
	if len(policies) == 0 {
		return false // default allow
	}

	var keepPolicy *CompiledLogPolicy
	var dropPolicy *CompiledLogPolicy

	for i := range policies {
		p := &policies[i]
		matchesAll := true
		for _, m := range p.Matchers {
			if !m.Eval(lr, scope, scopeLogs, resource) {
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

// pruneLogs recursively filters logs and removes empty scopes and resources.
func pruneLogs(
	ctx context.Context,
	ld plog.Logs,
	policies []CompiledLogPolicy,
	recordEval evaluationRecorderFunc,
) {
	if len(policies) == 0 {
		return
	}

	rls := ld.ResourceLogs()
	rls.RemoveIf(func(rl plog.ResourceLogs) bool {
		res := rl.Resource()
		sls := rl.ScopeLogs()
		sls.RemoveIf(func(sl plog.ScopeLogs) bool {
			scope := sl.Scope()
			lrs := sl.LogRecords()
			lrs.RemoveIf(func(lr plog.LogRecord) bool {
				return evaluateLogRecord(ctx, lr, scope, sl, res, policies, recordEval)
			})
			return lrs.Len() == 0
		})
		return sls.Len() == 0
	})
}

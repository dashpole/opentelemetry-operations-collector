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
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
)

// CompiledMetricMatcher evaluates a single matcher condition against a metric descriptor,
// metadata, or data point attribute.
type CompiledMetricMatcher struct {
	Target    *policyv1alpha1.MetricFieldSelector
	Predicate PredicateEvaluator
}

// EvalInstrument evaluates an instrument-level matcher without requiring data point attributes.
func (m *CompiledMetricMatcher) EvalInstrument(
	metric pmetric.Metric,
	scope pcommon.InstrumentationScope,
	scopeMetrics pmetric.ScopeMetrics,
	resource pcommon.Resource,
) bool {
	switch t := m.Target.Target.(type) {
	case *policyv1alpha1.MetricFieldSelector_DescriptorField:
		switch t.DescriptorField {
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME:
			name := metric.Name()
			return m.Predicate.Eval(name, name != "")
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_DESCRIPTION:
			desc := metric.Description()
			return m.Predicate.Eval(desc, desc != "")
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_UNIT:
			unit := metric.Unit()
			return m.Predicate.Eval(unit, unit != "")
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_TYPE:
			tStr := metricTypeString(metric.Type())
			return m.Predicate.Eval(tStr, tStr != "")
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_AGGREGATION_TEMPORALITY:
			tempStr, exists := metricTemporalityString(metric)
			return m.Predicate.Eval(tempStr, exists)
		case policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_IS_MONOTONIC:
			monoStr, exists := metricIsMonotonicString(metric)
			return m.Predicate.Eval(monoStr, exists)
		default:
			return false
		}
	case *policyv1alpha1.MetricFieldSelector_ResourceAttribute:
		val, ok := resource.Attributes().Get(t.ResourceAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.MetricFieldSelector_ScopeAttribute:
		val, ok := scope.Attributes().Get(t.ScopeAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	case *policyv1alpha1.MetricFieldSelector_ScopeField:
		switch t.ScopeField {
		case policyv1alpha1.ScopeField_SCOPE_FIELD_NAME:
			name := scope.Name()
			return m.Predicate.Eval(name, name != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_VERSION:
			ver := scope.Version()
			return m.Predicate.Eval(ver, ver != "")
		case policyv1alpha1.ScopeField_SCOPE_FIELD_SCHEMA_URL:
			schemaURL := scopeMetrics.SchemaUrl()
			return m.Predicate.Eval(schemaURL, schemaURL != "")
		default:
			return false
		}
	default:
		return false
	}
}

// EvalDataPoint evaluates a matcher against a data point attribute or parent descriptor/scope/resource fields.
func (m *CompiledMetricMatcher) EvalDataPoint(
	metric pmetric.Metric,
	dpAttributes pcommon.Map,
	scope pcommon.InstrumentationScope,
	scopeMetrics pmetric.ScopeMetrics,
	resource pcommon.Resource,
) bool {
	if t, ok := m.Target.Target.(*policyv1alpha1.MetricFieldSelector_DatapointAttribute); ok {
		val, ok := dpAttributes.Get(t.DatapointAttribute)
		if !ok {
			return m.Predicate.Eval("", false)
		}
		strVal, exists := stringifyValue(val)
		return m.Predicate.Eval(strVal, exists)
	}
	return m.EvalInstrument(metric, scope, scopeMetrics, resource)
}

func metricTypeString(t pmetric.MetricType) string {
	switch t {
	case pmetric.MetricTypeGauge:
		return "GAUGE"
	case pmetric.MetricTypeSum:
		return "SUM"
	case pmetric.MetricTypeHistogram:
		return "HISTOGRAM"
	case pmetric.MetricTypeExponentialHistogram:
		return "EXPONENTIAL_HISTOGRAM"
	case pmetric.MetricTypeSummary:
		return "SUMMARY"
	default:
		return ""
	}
}

func metricTemporalityString(m pmetric.Metric) (string, bool) {
	var temp pmetric.AggregationTemporality
	switch m.Type() {
	case pmetric.MetricTypeSum:
		temp = m.Sum().AggregationTemporality()
	case pmetric.MetricTypeHistogram:
		temp = m.Histogram().AggregationTemporality()
	case pmetric.MetricTypeExponentialHistogram:
		temp = m.ExponentialHistogram().AggregationTemporality()
	default:
		return "", false
	}

	switch temp {
	case pmetric.AggregationTemporalityDelta:
		return "DELTA", true
	case pmetric.AggregationTemporalityCumulative:
		return "CUMULATIVE", true
	default:
		return "", false
	}
}

func metricIsMonotonicString(m pmetric.Metric) (string, bool) {
	if m.Type() == pmetric.MetricTypeSum {
		return strconv.FormatBool(m.Sum().IsMonotonic()), true
	}
	return "", false
}

// pruneMetrics evaluates pre-partitioned metric policies using inheritance and recursive pruning.
func pruneMetrics(
	ctx context.Context,
	md pmetric.Metrics,
	instrumentPolicies []CompiledMetricPolicy,
	dataPointPolicies []CompiledMetricPolicy,
	recordEval evaluationRecorderFunc,
) {
	if len(instrumentPolicies) == 0 && len(dataPointPolicies) == 0 {
		return
	}

	rms := md.ResourceMetrics()
	rms.RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		res := rm.Resource()
		sms := rm.ScopeMetrics()
		sms.RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			scope := sm.Scope()
			ms := sm.Metrics()
			ms.RemoveIf(func(m pmetric.Metric) bool {
				// Step 1: Single-pass evaluate Instrument-Level Matchers
				var keepByDefaultPolicy *CompiledMetricPolicy
				var dropByDefaultPolicy *CompiledMetricPolicy

				for i := range instrumentPolicies {
					p := &instrumentPolicies[i]
					matchesAll := true
					for _, matcher := range p.Matchers {
						if !matcher.EvalInstrument(m, scope, sm, res) {
							matchesAll = false
							break
						}
					}
					if matchesAll {
						if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_KEEP {
							keepByDefaultPolicy = p
							dropByDefaultPolicy = nil // ACTION_KEEP overrides ACTION_DROP
							break                     // Immediate break on ACTION_KEEP
						} else if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_DROP {
							if dropByDefaultPolicy == nil {
								dropByDefaultPolicy = p
							}
						}
					}
				}

				// Step 2: Single-pass evaluate Data Points across all 5 OTel data point types
				evalDP := func(dpAttr pcommon.Map) bool {
					var dpKeepPolicy *CompiledMetricPolicy
					var dpDropPolicy *CompiledMetricPolicy

					for i := range dataPointPolicies {
						p := &dataPointPolicies[i]
						matchesAll := true
						for _, matcher := range p.Matchers {
							if !matcher.EvalDataPoint(m, dpAttr, scope, sm, res) {
								matchesAll = false
								break
							}
						}
						if matchesAll {
							if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_KEEP {
								dpKeepPolicy = p
								break // Immediate break on ACTION_KEEP
							} else if p.Policy.GetAction() == policyv1alpha1.Action_ACTION_DROP {
								if dpDropPolicy == nil {
									dpDropPolicy = p
								}
							}
						}
					}

					if dpKeepPolicy != nil {
						if recordEval != nil {
							recordEval(ctx, dpKeepPolicy.KeepOption)
						}
						return false // retain data point
					}
					if dpDropPolicy != nil {
						if recordEval != nil {
							recordEval(ctx, dpDropPolicy.DropOption)
						}
						return true // drop data point
					}
					if dropByDefaultPolicy != nil {
						if recordEval != nil {
							recordEval(ctx, dropByDefaultPolicy.DropOption)
						}
						return true // drop data point by instrument drop
					}
					if keepByDefaultPolicy != nil {
						if recordEval != nil {
							recordEval(ctx, keepByDefaultPolicy.KeepOption)
						}
						return false // retain data point by instrument keep
					}

					return false // default allow
				}

				var remainingPoints int
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					dps.RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						return evalDP(dp.Attributes())
					})
					remainingPoints = dps.Len()
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					dps.RemoveIf(func(dp pmetric.NumberDataPoint) bool {
						return evalDP(dp.Attributes())
					})
					remainingPoints = dps.Len()
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					dps.RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
						return evalDP(dp.Attributes())
					})
					remainingPoints = dps.Len()
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					dps.RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
						return evalDP(dp.Attributes())
					})
					remainingPoints = dps.Len()
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					dps.RemoveIf(func(dp pmetric.SummaryDataPoint) bool {
						return evalDP(dp.Attributes())
					})
					remainingPoints = dps.Len()
				default:
					if dropByDefaultPolicy != nil {
						if recordEval != nil {
							recordEval(ctx, dropByDefaultPolicy.DropOption)
						}
						return true
					}
					return false
				}

				// Step 3: Prune metric instrument if 0 data points remain
				return remainingPoints == 0
			})
			return ms.Len() == 0
		})
		return sms.Len() == 0
	})
}

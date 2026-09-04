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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric"
)

func TestMetricFilterPolicy_DescriptorFields(t *testing.T) {
	types := []struct {
		typeName   string
		setup      func(m pmetric.Metric)
		metricType string
	}{
		{
			typeName: "Gauge",
			setup: func(m pmetric.Metric) {
				m.SetName("system.cpu.utilization")
				m.SetDescription("CPU load average")
				m.SetUnit("1")
				dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetDoubleValue(0.85)
				dp.Attributes().PutStr("cpu", "0")
			},
			metricType: "GAUGE",
		},
		{
			typeName: "Sum",
			setup: func(m pmetric.Metric) {
				m.SetName("system.cpu.utilization")
				m.SetDescription("CPU load average")
				m.SetUnit("1")
				s := m.SetEmptySum()
				s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				s.SetIsMonotonic(true)
				dp := s.DataPoints().AppendEmpty()
				dp.SetIntValue(100)
			},
			metricType: "SUM",
		},
		{
			typeName: "Histogram",
			setup: func(m pmetric.Metric) {
				m.SetName("system.cpu.utilization")
				m.SetDescription("CPU load average")
				m.SetUnit("1")
				h := m.SetEmptyHistogram()
				h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := h.DataPoints().AppendEmpty()
				dp.SetCount(10)
			},
			metricType: "HISTOGRAM",
		},
		{
			typeName: "ExponentialHistogram",
			setup: func(m pmetric.Metric) {
				m.SetName("system.cpu.utilization")
				m.SetDescription("CPU load average")
				m.SetUnit("1")
				eh := m.SetEmptyExponentialHistogram()
				eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := eh.DataPoints().AppendEmpty()
				dp.SetCount(5)
			},
			metricType: "EXPONENTIAL_HISTOGRAM",
		},
		{
			typeName: "Summary",
			setup: func(m pmetric.Metric) {
				m.SetName("system.cpu.utilization")
				m.SetDescription("CPU load average")
				m.SetUnit("1")
				s := m.SetEmptySummary()
				dp := s.DataPoints().AppendEmpty()
				dp.SetCount(20)
			},
			metricType: "SUMMARY",
		},
	}

	for _, tt := range types {
		t.Run(tt.typeName+"_Name_Exact", func(t *testing.T) {
			cfg := &Config{
				Policies: []PolicyConfig{
					{
						ID:      "drop-by-name",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "drop-by-name",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_NAME"},
									"exact":  "system.cpu.utilization",
								},
							},
						},
					},
				},
			}
			require.NoError(t, cfg.Validate())

			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			tt.setup(m)

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
			assert.Equal(t, 0, md.ResourceMetrics().Len(), "metric should be dropped by name")
		})

		t.Run(tt.typeName+"_Description_Regex", func(t *testing.T) {
			cfg := &Config{
				Policies: []PolicyConfig{
					{
						ID:      "drop-by-desc",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "drop-by-desc",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_DESCRIPTION"},
									"regex":  ".*load average.*",
								},
							},
						},
					},
				},
			}
			require.NoError(t, cfg.Validate())

			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			tt.setup(m)

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
			assert.Equal(t, 0, md.ResourceMetrics().Len(), "metric should be dropped by description")
		})

		t.Run(tt.typeName+"_Unit_Exact", func(t *testing.T) {
			cfg := &Config{
				Policies: []PolicyConfig{
					{
						ID:      "drop-by-unit",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "drop-by-unit",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_UNIT"},
									"exact":  "1",
								},
							},
						},
					},
				},
			}
			require.NoError(t, cfg.Validate())

			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			tt.setup(m)

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
			assert.Equal(t, 0, md.ResourceMetrics().Len(), "metric should be dropped by unit")
		})

		t.Run(tt.typeName+"_Type_Exact", func(t *testing.T) {
			cfg := &Config{
				Policies: []PolicyConfig{
					{
						ID:      "drop-by-type",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "drop-by-type",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_TYPE"},
									"exact":  tt.metricType,
								},
							},
						},
					},
				},
			}
			require.NoError(t, cfg.Validate())

			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			tt.setup(m)

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
			assert.Equal(t, 0, md.ResourceMetrics().Len(), "metric should be dropped by type")
		})
	}

	// Specific tests for AggregationTemporality (Sum, Histogram, ExponentialHistogram)
	t.Run("AggregationTemporality_Cumulative_Sum", func(t *testing.T) {
		cfg := &Config{
			Policies: []PolicyConfig{
				{
					ID:      "drop-cumulative",
					TypeURL: TypeURLMetricFilterPolicy,
					Rule: map[string]any{
						"id":     "drop-cumulative",
						"action": "ACTION_DROP",
						"matches": []any{
							map[string]any{
								"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_AGGREGATION_TEMPORALITY"},
								"exact":  "CUMULATIVE",
							},
						},
					},
				},
			},
		}
		require.NoError(t, cfg.Validate())

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		s := m.SetEmptySum()
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		s.DataPoints().AppendEmpty()

		pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
		assert.Equal(t, 0, md.ResourceMetrics().Len())
	})

	t.Run("AggregationTemporality_Delta_Histogram", func(t *testing.T) {
		cfg := &Config{
			Policies: []PolicyConfig{
				{
					ID:      "drop-delta",
					TypeURL: TypeURLMetricFilterPolicy,
					Rule: map[string]any{
						"id":     "drop-delta",
						"action": "ACTION_DROP",
						"matches": []any{
							map[string]any{
								"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_AGGREGATION_TEMPORALITY"},
								"exact":  "DELTA",
							},
						},
					},
				},
			},
		}
		require.NoError(t, cfg.Validate())

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		h.DataPoints().AppendEmpty()

		pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
		assert.Equal(t, 0, md.ResourceMetrics().Len())
	})

	// Specific tests for IsMonotonic (Sum)
	t.Run("IsMonotonic_True_Sum", func(t *testing.T) {
		cfg := &Config{
			Policies: []PolicyConfig{
				{
					ID:      "drop-monotonic",
					TypeURL: TypeURLMetricFilterPolicy,
					Rule: map[string]any{
						"id":     "drop-monotonic",
						"action": "ACTION_DROP",
						"matches": []any{
							map[string]any{
								"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_IS_MONOTONIC"},
								"exact":  "true",
							},
						},
					},
				},
			},
		}
		require.NoError(t, cfg.Validate())

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.DataPoints().AppendEmpty()

		pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
		assert.Equal(t, 0, md.ResourceMetrics().Len())
	})

	t.Run("IsMonotonic_False_NonSum_Does_Not_Exist", func(t *testing.T) {
		cfg := &Config{
			Policies: []PolicyConfig{
				{
					ID:      "drop-if-monotonic-exists",
					TypeURL: TypeURLMetricFilterPolicy,
					Rule: map[string]any{
						"id":     "drop-if-monotonic-exists",
						"action": "ACTION_DROP",
						"matches": []any{
							map[string]any{
								"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_IS_MONOTONIC"},
								"exists": map[string]any{},
							},
						},
					},
				},
			},
		}
		require.NoError(t, cfg.Validate())

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetEmptyGauge().DataPoints().AppendEmpty()

		pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
		assert.Equal(t, 1, md.ResourceMetrics().Len(), "Gauge has no is_monotonic, should not match exists")
	})
}

func TestMetricFilterPolicy_Inheritance(t *testing.T) {
	// Policy 1: Instrument-level DROP on "http.server.duration"
	// Policy 2: DataPoint-level KEEP for "http.route == /checkout"
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-http-instrument",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-http-instrument",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_NAME"},
							"exact":  "http.server.duration",
						},
					},
				},
			},
			{
				ID:      "keep-checkout-route",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "keep-checkout-route",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"datapoint_attribute": "http.route"},
							"exact":  "/checkout",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	var records []metric.MeasurementOption
	recordEval := func(ctx context.Context, opt metric.MeasurementOption) {
		records = append(records, opt)
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.server.duration")
	g := m.SetEmptyGauge()

	// Data Point 1: /metrics (should be dropped via dropByDefault from Policy 1)
	dp1 := g.DataPoints().AppendEmpty()
	dp1.Attributes().PutStr("http.route", "/metrics")
	dp1.SetDoubleValue(10.0)

	// Data Point 2: /checkout (should be kept via Policy 2)
	dp2 := g.DataPoints().AppendEmpty()
	dp2.Attributes().PutStr("http.route", "/checkout")
	dp2.SetDoubleValue(50.0)

	pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, recordEval)

	// Metric stream must be retained because 1 data point remained
	assert.Equal(t, 1, md.ResourceMetrics().Len())
	remainingM := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, "http.server.duration", remainingM.Name())
	assert.Equal(t, 1, remainingM.Gauge().DataPoints().Len())
	remainingDP := remainingM.Gauge().DataPoints().At(0)
	assert.Equal(t, "/checkout", remainingDP.Attributes().AsRaw()["http.route"])
	assert.Equal(t, 50.0, remainingDP.DoubleValue())

	// Verify evaluations attribution
	require.Len(t, records, 2)
	// dp1: dropped by drop-http-instrument
	assert.Equal(t, cfg.Compiled.MetricInstrumentPolicies[0].DropOption, records[0])
	// dp2: kept by keep-checkout-route
	assert.Equal(t, cfg.Compiled.MetricDataPointPolicies[0].KeepOption, records[1])
}

func TestMetricPruning(t *testing.T) {
	// Drop all metrics with name "drop.me"
	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-metric",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-metric",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_NAME"},
							"exact":  "drop.me",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	md := pmetric.NewMetrics()

	// Resource 1:
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("res", "1")

	// Scope 1 in Resource 1: Metric 1 (drop.me) and Metric 2 (keep.me)
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	sm1.Scope().SetName("scope-1")
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("drop.me")
	m1.SetEmptyGauge().DataPoints().AppendEmpty()

	m2 := sm1.Metrics().AppendEmpty()
	m2.SetName("keep.me")
	m2.SetEmptyGauge().DataPoints().AppendEmpty()

	// Scope 2 in Resource 1: Metric 3 (drop.me only -> entire Scope 2 pruned)
	sm2 := rm1.ScopeMetrics().AppendEmpty()
	sm2.Scope().SetName("scope-2")
	m3 := sm2.Metrics().AppendEmpty()
	m3.SetName("drop.me")
	m3.SetEmptyGauge().DataPoints().AppendEmpty()

	// Resource 2: Metric 4 (drop.me only -> entire Resource 2 pruned)
	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("res", "2")
	sm3 := rm2.ScopeMetrics().AppendEmpty()
	sm3.Scope().SetName("scope-3")
	m4 := sm3.Metrics().AppendEmpty()
	m4.SetName("drop.me")
	m4.SetEmptyGauge().DataPoints().AppendEmpty()

	pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)

	// Resource 2 must be completely pruned
	assert.Equal(t, 1, md.ResourceMetrics().Len())
	remainingRM := md.ResourceMetrics().At(0)
	assert.Equal(t, "1", remainingRM.Resource().Attributes().AsRaw()["res"])

	// Scope 2 must be pruned
	assert.Equal(t, 1, remainingRM.ScopeMetrics().Len())
	remainingSM := remainingRM.ScopeMetrics().At(0)
	assert.Equal(t, "scope-1", remainingSM.Scope().Name())

	// Metric 1 must be pruned, Metric 2 retained
	assert.Equal(t, 1, remainingSM.Metrics().Len())
	assert.Equal(t, "keep.me", remainingSM.Metrics().At(0).Name())
}

func TestMetricFilterPolicy_TargetSelectors(t *testing.T) {
	tests := []struct {
		name         string
		rule         map[string]any
		setup        func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource)
		expectedDrop bool
	}{
		{
			name: "resource_attribute exact match - drop",
			rule: map[string]any{
				"id":     "drop-res-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"resource_attribute": "service.name"},
						"exact":  "billing-service",
					},
				},
			},
			setup: func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource) {
				res.Attributes().PutStr("service.name", "billing-service")
				m.SetName("custom.metric")
				m.SetEmptyGauge().DataPoints().AppendEmpty()
			},
			expectedDrop: true,
		},
		{
			name: "scope_attribute exact match - drop",
			rule: map[string]any{
				"id":     "drop-scope-attr",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_attribute": "otel.library.name"},
						"exact":  "runtime",
					},
				},
			},
			setup: func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource) {
				scope.Attributes().PutStr("otel.library.name", "runtime")
				m.SetName("custom.metric")
				m.SetEmptyGauge().DataPoints().AppendEmpty()
			},
			expectedDrop: true,
		},
		{
			name: "scope_field NAME exact match - drop",
			rule: map[string]any{
				"id":     "drop-scope-name",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_NAME"},
						"exact":  "otel.scope.cpu",
					},
				},
			},
			setup: func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource) {
				scope.SetName("otel.scope.cpu")
				m.SetName("custom.metric")
				m.SetEmptyGauge().DataPoints().AppendEmpty()
			},
			expectedDrop: true,
		},
		{
			name: "scope_field VERSION exact match - drop",
			rule: map[string]any{
				"id":     "drop-scope-ver",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"scope_field": "SCOPE_FIELD_VERSION"},
						"exact":  "v2.1.0",
					},
				},
			},
			setup: func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource) {
				scope.SetVersion("v2.1.0")
				m.SetName("custom.metric")
				m.SetEmptyGauge().DataPoints().AppendEmpty()
			},
			expectedDrop: true,
		},
		{
			name: "scope_field SCHEMA_URL exact match - drop",
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
			setup: func(m pmetric.Metric, scope pcommon.InstrumentationScope, sm pmetric.ScopeMetrics, res pcommon.Resource) {
				sm.SetSchemaUrl("https://opentelemetry.io/schemas/1.24.0")
				m.SetName("custom.metric")
				m.SetEmptyGauge().DataPoints().AppendEmpty()
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
						TypeURL: TypeURLMetricFilterPolicy,
						Rule:    tc.rule,
					},
				},
			}
			require.NoError(t, cfg.Validate())

			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()

			tc.setup(m, sm.Scope(), sm, rm.Resource())

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)
			if tc.expectedDrop {
				assert.Equal(t, 0, md.ResourceMetrics().Len())
			} else {
				assert.Equal(t, 1, md.ResourceMetrics().Len())
			}
		})
	}
}

func TestMetricFilterPolicy_DataPointAttributes_AllTypes(t *testing.T) {
	types := []struct {
		typeName     string
		setupPoints  func(m pmetric.Metric)
		verifyPoints func(t *testing.T, m pmetric.Metric)
	}{
		{
			typeName: "Gauge",
			setupPoints: func(m pmetric.Metric) {
				m.SetName("gauge.test")
				g := m.SetEmptyGauge()
				dp1 := g.DataPoints().AppendEmpty()
				dp1.Attributes().PutStr("env", "dev")
				dp1.SetDoubleValue(10.0)
				dp2 := g.DataPoints().AppendEmpty()
				dp2.Attributes().PutStr("env", "prod")
				dp2.SetDoubleValue(20.0)
			},
			verifyPoints: func(t *testing.T, m pmetric.Metric) {
				assert.Equal(t, 1, m.Gauge().DataPoints().Len())
				dp := m.Gauge().DataPoints().At(0)
				assert.Equal(t, "prod", dp.Attributes().AsRaw()["env"])
				assert.Equal(t, 20.0, dp.DoubleValue())
			},
		},
		{
			typeName: "Sum",
			setupPoints: func(m pmetric.Metric) {
				m.SetName("sum.test")
				s := m.SetEmptySum()
				dp1 := s.DataPoints().AppendEmpty()
				dp1.Attributes().PutStr("env", "dev")
				dp1.SetIntValue(100)
				dp2 := s.DataPoints().AppendEmpty()
				dp2.Attributes().PutStr("env", "prod")
				dp2.SetIntValue(200)
			},
			verifyPoints: func(t *testing.T, m pmetric.Metric) {
				assert.Equal(t, 1, m.Sum().DataPoints().Len())
				dp := m.Sum().DataPoints().At(0)
				assert.Equal(t, "prod", dp.Attributes().AsRaw()["env"])
				assert.Equal(t, int64(200), dp.IntValue())
			},
		},
		{
			typeName: "Histogram",
			setupPoints: func(m pmetric.Metric) {
				m.SetName("histogram.test")
				h := m.SetEmptyHistogram()
				dp1 := h.DataPoints().AppendEmpty()
				dp1.Attributes().PutStr("env", "dev")
				dp1.SetCount(10)
				dp2 := h.DataPoints().AppendEmpty()
				dp2.Attributes().PutStr("env", "prod")
				dp2.SetCount(20)
			},
			verifyPoints: func(t *testing.T, m pmetric.Metric) {
				assert.Equal(t, 1, m.Histogram().DataPoints().Len())
				dp := m.Histogram().DataPoints().At(0)
				assert.Equal(t, "prod", dp.Attributes().AsRaw()["env"])
				assert.Equal(t, uint64(20), dp.Count())
			},
		},
		{
			typeName: "ExponentialHistogram",
			setupPoints: func(m pmetric.Metric) {
				m.SetName("exp_histogram.test")
				eh := m.SetEmptyExponentialHistogram()
				dp1 := eh.DataPoints().AppendEmpty()
				dp1.Attributes().PutStr("env", "dev")
				dp1.SetCount(1)
				dp2 := eh.DataPoints().AppendEmpty()
				dp2.Attributes().PutStr("env", "prod")
				dp2.SetCount(2)
			},
			verifyPoints: func(t *testing.T, m pmetric.Metric) {
				assert.Equal(t, 1, m.ExponentialHistogram().DataPoints().Len())
				dp := m.ExponentialHistogram().DataPoints().At(0)
				assert.Equal(t, "prod", dp.Attributes().AsRaw()["env"])
				assert.Equal(t, uint64(2), dp.Count())
			},
		},
		{
			typeName: "Summary",
			setupPoints: func(m pmetric.Metric) {
				m.SetName("summary.test")
				s := m.SetEmptySummary()
				dp1 := s.DataPoints().AppendEmpty()
				dp1.Attributes().PutStr("env", "dev")
				dp1.SetCount(5)
				dp2 := s.DataPoints().AppendEmpty()
				dp2.Attributes().PutStr("env", "prod")
				dp2.SetCount(15)
			},
			verifyPoints: func(t *testing.T, m pmetric.Metric) {
				assert.Equal(t, 1, m.Summary().DataPoints().Len())
				dp := m.Summary().DataPoints().At(0)
				assert.Equal(t, "prod", dp.Attributes().AsRaw()["env"])
				assert.Equal(t, uint64(15), dp.Count())
			},
		},
	}

	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "drop-dev-datapoint",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "drop-dev-datapoint",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"datapoint_attribute": "env"},
							"exact":  "dev",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	for _, tt := range types {
		t.Run(tt.typeName, func(t *testing.T) {
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			tt.setupPoints(m)

			pruneMetrics(context.Background(), md, cfg.Compiled.MetricInstrumentPolicies, cfg.Compiled.MetricDataPointPolicies, nil)

			// The metric stream should be preserved because the 'prod' data point remains
			assert.Equal(t, 1, md.ResourceMetrics().Len(), "metric should not be pruned when prod point remains")
			remainingM := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			tt.verifyPoints(t, remainingM)
		})
	}
}

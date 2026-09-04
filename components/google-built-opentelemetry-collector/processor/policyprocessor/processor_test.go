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
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

func TestPolicyEvaluationCounter(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	cfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "log-drop-debug",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "log-drop-debug",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"},
							"exact":  "DEBUG",
						},
					},
				},
			},
			{
				ID:      "log-keep-error",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "log-keep-error",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"},
							"exact":  "ERROR",
						},
					},
				},
			},
			{
				ID:      "metric-instrument-drop-cpu",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "metric-instrument-drop-cpu",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_NAME"},
							"exact":  "system.cpu.load",
						},
					},
				},
			},
			{
				ID:      "metric-datapoint-keep-core0",
				TypeURL: TypeURLMetricFilterPolicy,
				Rule: map[string]any{
					"id":     "metric-datapoint-keep-core0",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"datapoint_attribute": "core"},
							"exact":  "0",
						},
					},
				},
			},
			{
				ID:      "trace-drop-ping",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "trace-drop-ping",
					"action": "ACTION_DROP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
							"exact":  "PING",
						},
					},
				},
			},
			{
				ID:      "trace-keep-login",
				TypeURL: TypeURLTraceFilterPolicy,
				Rule: map[string]any{
					"id":     "trace-keep-login",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
							"exact":  "LOGIN",
						},
					},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	set := processortest.NewNopSettings(processortest.NopType)
	set.TelemetrySettings.MeterProvider = mp
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	// 1. Process Logs: 1 DEBUG (dropped by log-drop-debug), 1 ERROR (kept by log-keep-error), 1 INFO (default allow)
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lr1 := lrs.AppendEmpty()
	lr1.SetSeverityText("DEBUG")
	lr2 := lrs.AppendEmpty()
	lr2.SetSeverityText("ERROR")
	lr3 := lrs.AppendEmpty()
	lr3.SetSeverityText("INFO")

	_, err = proc.processLogs(ctx, ld)
	require.NoError(t, err)

	// 2. Process Metrics: system.cpu.load with core=0 (kept by metric-datapoint-keep-core0), core=1 (dropped by metric-instrument-drop-cpu)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("system.cpu.load")
	g := m.SetEmptyGauge()
	dpCore0 := g.DataPoints().AppendEmpty()
	dpCore0.Attributes().PutStr("core", "0")
	dpCore0.SetDoubleValue(0.1)
	dpCore1 := g.DataPoints().AppendEmpty()
	dpCore1.Attributes().PutStr("core", "1")
	dpCore1.SetDoubleValue(0.9)

	_, err = proc.processMetrics(ctx, md)
	require.NoError(t, err)

	// 3. Process Traces: 1 PING (dropped by trace-drop-ping), 1 LOGIN (kept by trace-keep-login)
	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	s1 := spans.AppendEmpty()
	s1.SetName("PING")
	s2 := spans.AppendEmpty()
	s2.SetName("LOGIN")

	_, err = proc.processTraces(ctx, td)
	require.NoError(t, err)

	// Collect metrics from manual reader
	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	counts := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "telemetry_policy_evaluations_total" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						var signal, policyID, action string
						for _, attr := range dp.Attributes.ToSlice() {
							switch attr.Key {
							case "signal":
								signal = attr.Value.AsString()
							case "policy_id":
								policyID = attr.Value.AsString()
							case "action":
								action = attr.Value.AsString()
							}
						}
						key := signal + ":" + policyID + ":" + action
						counts[key] += dp.Value
					}
				}
			}
		}
	}

	// Verify all evaluations
	assert.Equal(t, int64(1), counts["logs:log-drop-debug:drop"], "1 debug log dropped")
	assert.Equal(t, int64(1), counts["logs:log-keep-error:keep"], "1 error log kept")
	assert.Equal(t, int64(1), counts["metrics:metric-datapoint-keep-core0:keep"], "1 core=0 data point kept")
	assert.Equal(t, int64(1), counts["metrics:metric-instrument-drop-cpu:drop"], "1 core=1 data point dropped attributed to instrument policy")
	assert.Equal(t, int64(1), counts["traces:trace-drop-ping:drop"], "1 ping span dropped")
	assert.Equal(t, int64(1), counts["traces:trace-keep-login:keep"], "1 login span kept")
}

func TestPolicyProcessor_NopCapabilities(t *testing.T) {
	set := processortest.NewNopSettings(processortest.NopType)
	cfg := &Config{}
	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NotNil(t, host)
	require.NotNil(t, proc)
}

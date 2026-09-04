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

package googlecontrolplaneextension

import (
	"context"
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

func TestGoogleControlPlaneExtension(t *testing.T) {
	ctx := context.Background()

	// 1. Test Factory
	factory := NewFactory()
	assert.Equal(t, component.MustNewType("googlecontrolplaneextension"), factory.Type())
	cfg := factory.CreateDefaultConfig()
	assert.Equal(t, &Config{}, cfg)

	// 2. Set up metric manual reader
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		_ = mp.Shutdown(ctx)
	}()

	set := extensiontest.NewNopSettings(typeStr)
	set.TelemetrySettings.MeterProvider = mp
	set.Logger = zap.NewNop()

	// Use custom status registry to ensure test isolation
	statusReg := googlecontrolplane.NewStatusRegistry()

	extAny, err := factory.Create(ctx, set, cfg)
	require.NoError(t, err)
	require.NotNil(t, extAny)

	ext, ok := extAny.(*googleControlPlaneExtension)
	require.True(t, ok)
	ext.WithStatusRegistry(statusReg)

	// Start extension
	err = ext.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)

	// Step 1: StatusRegistry is at initial state (0, empty)
	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	rev, statuses := extractMetrics(&rm)
	assert.Equal(t, int64(0), rev)
	assert.Empty(t, statuses)

	// Step 2: Update StatusRegistry to Revision 1 with two policies
	statusReg.SetRevisionAndStatuses(1, map[string]googlecontrolplane.PolicyStatusEntry{
		"policy-1": {
			TypeURL: "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Status:  "accepted",
		},
		"policy-2": {
			TypeURL: "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
			Status:  "rejected",
		},
	})

	rm = metricdata.ResourceMetrics{}
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	rev, statuses = extractMetrics(&rm)
	assert.Equal(t, int64(1), rev)
	assert.Len(t, statuses, 2)
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy:accepted", statuses["policy-1"])
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy:rejected", statuses["policy-2"])

	// Step 3: Update to Revision 2, pruning policy-2 and adding policy-3
	statusReg.SetRevisionAndStatuses(2, map[string]googlecontrolplane.PolicyStatusEntry{
		"policy-1": {
			TypeURL: "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Status:  "accepted",
		},
		"policy-3": {
			TypeURL: "type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy",
			Status:  "unsupported",
		},
	})

	rm = metricdata.ResourceMetrics{}
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	rev, statuses = extractMetrics(&rm)
	assert.Equal(t, int64(2), rev)
	assert.Len(t, statuses, 2)
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy:accepted", statuses["policy-1"])
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy:unsupported", statuses["policy-3"])
	_, policy2Exists := statuses["policy-2"]
	assert.False(t, policy2Exists, "policy-2 should have been pruned from snapshot")

	// Step 4: Shutdown
	err = ext.Shutdown(ctx)
	require.NoError(t, err)
}

func TestGoogleControlPlaneExtension_DefaultStatusRegistry(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		_ = mp.Shutdown(ctx)
	}()

	set := extensiontest.NewNopSettings(extensiontest.NopType)
	set.TelemetrySettings.MeterProvider = mp

	ext := newExtension(set, &Config{})
	err := ext.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)

	googlecontrolplane.DefaultStatusRegistry.SetRevisionAndStatuses(42, map[string]googlecontrolplane.PolicyStatusEntry{
		"default-p1": {
			TypeURL: "type.googleapis.com/test",
			Status:  "accepted",
		},
	})
	defer googlecontrolplane.DefaultStatusRegistry.SetRevisionAndStatuses(0, nil)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	rev, statuses := extractMetrics(&rm)
	assert.Equal(t, int64(42), rev)
	assert.Equal(t, "type.googleapis.com/test:accepted", statuses["default-p1"])

	err = ext.Shutdown(ctx)
	require.NoError(t, err)
}

func extractMetrics(rm *metricdata.ResourceMetrics) (int64, map[string]string) {
	var rev int64
	statuses := make(map[string]string)

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "telemetry_policy_set_revision":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						rev = dp.Value
					}
				}
			case "telemetry_policy_status":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						var policyID, policyType, status string
						for _, attr := range dp.Attributes.ToSlice() {
							switch attr.Key {
							case "policy_id":
								policyID = attr.Value.AsString()
							case "policy_type":
								policyType = attr.Value.AsString()
							case "status":
								status = attr.Value.AsString()
							}
						}
						statuses[policyID] = policyType + ":" + status
					}
				}
			}
		}
	}
	return rev, statuses
}

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

package googlecontrolplaneprovider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/event"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func createProvider() confmap.Provider {
	return NewFactory().Create(confmaptest.NewNopProviderSettings())
}

func createProviderWithLogger(logger *zap.Logger) confmap.Provider {
	settings := confmaptest.NewNopProviderSettings()
	settings.Logger = logger
	return NewFactory().Create(settings)
}

func TestValidateProviderScheme(t *testing.T) {
	assert.NoError(t, confmaptest.ValidateProviderScheme(createProvider()))
}

func TestScheme(t *testing.T) {
	p := createProvider()
	assert.Equal(t, "googlecontrolplane", p.Scheme())
}

func TestUnsupportedScheme(t *testing.T) {
	p := createProvider()
	_, err := p.Retrieve(context.Background(), "file:/path/to/conf", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrURINotSupported)
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestEmptyURI(t *testing.T) {
	p := createProvider()
	_, err := p.Retrieve(context.Background(), "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrURINotSupported)
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestEmptyTarget(t *testing.T) {
	p := createProvider()
	_, err := p.Retrieve(context.Background(), "googlecontrolplane:", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyURI)
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()
	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.NotNil(t, conf)

	// Verify both destination policy and selfmetrics policy configurations are merged
	assert.True(t, conf.IsSet("extensions::googleclientauth/default_gcp_destination"))
	assert.True(t, conf.IsSet("exporters::otlp_grpc/default_gcp_destination"))
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))
	assert.True(t, conf.IsSet("service::telemetry::resource::attributes"))
	assert.True(t, conf.IsSet("service::pipelines::logs/default_self_metrics"))
	assert.True(t, conf.IsSet("service::pipelines::metrics/default_self_metrics"))

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestShutdown(t *testing.T) {
	p := createProvider()
	assert.NoError(t, p.Shutdown(context.Background()))
}

type mockDestinationPolicy struct {
	name string
}

func (m *mockDestinationPolicy) PolicyName() string { return m.name }
func (m *mockDestinationPolicy) PolicyType() string { return "mock_destination" }
func (m *mockDestinationPolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassDestination
}
func (m *mockDestinationPolicy) Validate() error { return nil }
func (m *mockDestinationPolicy) Evaluate(context.Context) (*confmap.Conf, error) {
	return confmap.NewFromStringMap(map[string]any{
		"exporters": map[string]any{
			"otlp/" + m.name: map[string]any{
				"endpoint": "example.com:4317",
			},
		},
	}), nil
}
func (m *mockDestinationPolicy) ExporterIDs() []component.ID {
	otlpType, _ := component.NewType("otlp")
	return []component.ID{component.NewIDWithName(otlpType, m.name)}
}
func (m *mockDestinationPolicy) PreProcessMetricIDs() []component.ID { return nil }
func (m *mockDestinationPolicy) PreProcessLogIDs() []component.ID    { return nil }
func (m *mockDestinationPolicy) PreProcessTraceIDs() []component.ID  { return nil }
func (m *mockDestinationPolicy) ExtensionIDs() []component.ID        { return nil }

var _ googlepolicy.DestinationPolicy = (*mockDestinationPolicy)(nil)

type mockSourcePolicy struct {
	name    string
	evalErr error
}

func (m *mockSourcePolicy) PolicyName() string { return m.name }
func (m *mockSourcePolicy) PolicyType() string { return "mock_source" }
func (m *mockSourcePolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassSource
}
func (m *mockSourcePolicy) Validate() error { return nil }
func (m *mockSourcePolicy) Evaluate(context.Context) (*confmap.Conf, error) {
	if m.evalErr != nil {
		return nil, m.evalErr
	}
	return confmap.NewFromStringMap(map[string]any{
		"receivers": map[string]any{
			"otlp/" + m.name: map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{"endpoint": "localhost:4317"},
				},
			},
		},
	}), nil
}
func (m *mockSourcePolicy) LogsPipelines(preExportProcessors []component.ID, exporters []component.ID, extensions []component.ID) (*confmap.Conf, error) {
	procStrs := make([]any, 0, len(preExportProcessors))
	for _, id := range preExportProcessors {
		procStrs = append(procStrs, id.String())
	}
	return confmap.NewFromStringMap(map[string]any{
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs/" + m.name: map[string]any{
					"receivers":  []any{"otlp/" + m.name},
					"processors": procStrs,
					"exporters":  []any{"otlp_grpc/default_gcp_destination"},
				},
			},
		},
	}), nil
}
func (m *mockSourcePolicy) MetricsPipelines(preExportProcessors []component.ID, exporters []component.ID, extensions []component.ID) (*confmap.Conf, error) {
	return nil, nil
}
func (m *mockSourcePolicy) TracesPipelines(preExportProcessors []component.ID, exporters []component.ID, extensions []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

var _ googlepolicy.SourcePolicy = (*mockSourcePolicy)(nil)

func TestRetrieve_NoFleetID(t *testing.T) {
	t.Setenv("FLEET_ID", "")
	p := createProvider()
	_, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, selfmetrics.ErrNoFleetID)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// withFakeGCEMetadata points the GCE metadata library at a local server that
// answers 404 to every lookup, so xDS project/locality resolution sees no
// metadata, as it would off GCE.
//
// Without it, tests run on a GCE host (e.g. a cloudtop) reach the real metadata
// server through the library's shared HTTP client, which keeps the connection
// alive for a minute afterwards and trips goleak. The library offers no way to
// close that connection, but closing the server on cleanup drops it from the
// server side, so the client's connection goroutines exit.
func withFakeGCEMetadata(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	t.Setenv("GCE_METADATA_HOST", srv.Listener.Addr().String())
}

// TestRetrieve_XDSServerUnreachable asserts that a control plane that is down
// does not stop the collector from starting: Start's initial-sync wait expires,
// Retrieve still succeeds on the built-in policies, and the xDS connection keeps
// retrying in the background.
//
// This is the degraded path that matters most in production -- the control plane
// being unavailable must never be the reason a collector fails to come up.
func TestRetrieve_XDSServerUnreachable(t *testing.T) {
	withFakeGCEMetadata(t)
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()

	// Port 1 on loopback: nothing is listening, so the stream can never be
	// established and the manager is left retrying with backoff.
	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:xds://127.0.0.1:1?project=my-project&insecure=true", nil)
	require.NoError(t, err, "an unreachable xDS control plane must not block collector startup")
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)

	// The built-in destination and self metrics policies are still applied...
	assert.True(t, conf.IsSet("exporters::otlp_grpc/default_gcp_destination"))
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))

	// ...so the collector always has at least one pipeline to stand up with,
	// even though no policies were ever received from the control plane.
	assert.True(t, conf.IsSet("service::pipelines::metrics/default_self_metrics"))
	assert.True(t, conf.IsSet("service::pipelines::logs/default_self_metrics"))

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_XDSNumericProjectDoesNotClobberProjectID(t *testing.T) {
	withFakeGCEMetadata(t)
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:xds://127.0.0.1:1?project=123456789012&insecure=true", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)

	rawAttrs := conf.Get("service::telemetry::resource::attributes")
	attrs, ok := rawAttrs.([]any)
	require.True(t, ok)
	for _, entry := range attrs {
		if m, ok := entry.(map[string]any); ok {
			assert.NotEqual(t, "gcp.project_id", m["name"], "numeric project number in ?project= must not be stamped as gcp.project_id")
		}
	}

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_MultipleDestinationPolicies(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	p := createProviderWithLogger(logger)

	ps := &googlepolicy.PolicySet{
		PolicySetID: "projects/my-project/locations/us-central1/policySets/my-policy-set",
		RevisionID:  "rev-mult-dest",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"dest1": {PolicyObj: &mockDestinationPolicy{name: "dest1"}},
			"dest2": {PolicyObj: &mockDestinationPolicy{name: "dest2"}},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	// Invalid policy set should be rolled back to nil (built-in fallback)
	assert.Nil(t, googlepolicy.ActivePolicySet())

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("exporters::otlp_grpc/default_gcp_destination"))
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))

	entries := recorded.All()
	require.Len(t, entries, 1)
	entry := entries[0]
	assert.Equal(t, zapcore.ErrorLevel, entry.Level)
	assert.Contains(t, entry.Message, "more than one destination policy found")
	assert.Equal(t, map[string]any{
		"event.name":                 event.PolicySetInvalidEventName,
		"gcp.policy.set.id":          "projects/my-project/locations/us-central1/policySets/my-policy-set",
		"gcp.policy.set.revision.id": "rev-mult-dest",
		"context":                    "context.Background.WithValue(FLEET_ID, 1234)",
	}, entry.ContextMap())

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_ActivePolicySetWithCustomDestination(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()

	ps := &googlepolicy.PolicySet{
		RevisionID: "rev-custom-dest",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"custom_dest": {PolicyObj: &mockDestinationPolicy{name: "custom_dest"}},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("exporters::otlp/custom_dest"))
	assert.False(t, conf.IsSet("exporters::otlp_grpc/default_gcp_destination"))
	// Built-in self metrics should still be added
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_ActivePolicySetWithCustomSelfMetrics(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()

	customSelf := &selfmetrics.SelfMetricsPolicy{
		Name: "custom_self_metrics",
		Port: 9999,
	}
	ps := &googlepolicy.PolicySet{
		RevisionID: "rev-custom-self",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"custom_self_metrics": {PolicyObj: customSelf},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("receivers::otlp/custom_self_metrics"))
	assert.False(t, conf.IsSet("receivers::otlp/default_self_metrics"))
	assert.NotContains(t, conf.Get("service::pipelines::logs/custom_self_metrics::processors"), "googlepolicy")
	assert.NotContains(t, conf.Get("service::pipelines::metrics/custom_self_metrics::processors"), "googlepolicy")

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_ActivePolicySetWithOtherSource(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	p := createProvider()

	ps := &googlepolicy.PolicySet{
		RevisionID: "rev-other-source",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"other_source": {PolicyObj: &mockSourcePolicy{name: "other_source"}},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("receivers::otlp/other_source"))
	// Customer source pipelines MUST include googlepolicy processor
	assert.Contains(t, conf.Get("service::pipelines::logs/other_source::processors"), "googlepolicy")
	// Built-in self metrics should also be added, but MUST bypass googlepolicy processor
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))
	assert.NotContains(t, conf.Get("service::pipelines::logs/default_self_metrics::processors"), "googlepolicy")
	assert.NotContains(t, conf.Get("service::pipelines::metrics/default_self_metrics::processors"), "googlepolicy")

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestFilterPolicyDriversRegistered(t *testing.T) {
	// Verify that importing googlecontrolplaneprovider registers log_filter,
	// metric_filter, and trace_filter policy drivers in googlepolicy.
	for _, tc := range []struct {
		policyType string
		raw        map[string]any
	}{
		{
			policyType: "log_filter",
			raw: map[string]any{
				"type":   "log_filter",
				"id":     "test-log",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
						"exists": map[string]any{},
					},
				},
			},
		},
		{
			policyType: "metric_filter",
			raw: map[string]any{
				"type":   "metric_filter",
				"id":     "test-metric",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_NAME"},
						"exists": map[string]any{},
					},
				},
			},
		},
		{
			policyType: "trace_filter",
			raw: map[string]any{
				"type":   "trace_filter",
				"id":     "test-trace",
				"action": "ACTION_DROP",
				"matches": []any{
					map[string]any{
						"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_NAME"},
						"exists": map[string]any{},
					},
				},
			},
		},
	} {
		t.Run(tc.policyType, func(t *testing.T) {
			pol, err := googlepolicy.LoadPolicy(tc.policyType, tc.raw)
			require.NoError(t, err, "driver for %s must be registered", tc.policyType)
			assert.NotNil(t, pol)
		})
	}
}

// TestMergeAppendUnionsServiceExtensions pins the behaviour that makes it safe
// for more than one policy (or confmap provider) to declare an extension.
//
// Creating the provider enables the confmap.enableMergeAppendOption feature
// gate so confmap.Merge appends and deduplicates service::extensions instead of
// replacing the slice with only the last policy's extension.
func TestMergeAppendUnionsServiceExtensions(t *testing.T) {
	_ = createProvider()
	conf := confmap.New()

	destination := confmap.NewFromStringMap(map[string]any{
		"service": map[string]any{
			"extensions": []any{"googleclientauth/default_gcp_destination"},
		},
	})
	selfMetrics := confmap.NewFromStringMap(map[string]any{
		"service": map[string]any{
			"extensions": []any{"googlecontrolplane/default_self_metrics"},
		},
	})

	require.NoError(t, conf.Merge(cleanConf(destination)))
	require.NoError(t, conf.Merge(cleanConf(selfMetrics)))

	assert.Equal(t, []any{
		"googleclientauth/default_gcp_destination",
		"googlecontrolplane/default_self_metrics",
	}, conf.Get("service::extensions"))
}

func TestMergeAppendDeduplicatesServiceExtensions(t *testing.T) {
	_ = createProvider()
	conf := confmap.New()
	declare := func() *confmap.Conf {
		return confmap.NewFromStringMap(map[string]any{
			"service": map[string]any{
				"extensions": []any{"googleclientauth/default_gcp_destination"},
			},
		})
	}

	require.NoError(t, conf.Merge(cleanConf(declare())))
	require.NoError(t, conf.Merge(cleanConf(declare())))

	assert.Equal(t, []any{"googleclientauth/default_gcp_destination"}, conf.Get("service::extensions"))
}

// TestMergeAppendWithoutServiceExtensions guards the common case: a policy that
// declares no extensions must not introduce an empty service::extensions key.
func TestMergeAppendWithoutServiceExtensions(t *testing.T) {
	_ = createProvider()
	conf := confmap.New()
	require.NoError(t, conf.Merge(cleanConf(confmap.NewFromStringMap(map[string]any{
		"receivers": map[string]any{"otlp/x": map[string]any{}},
	}))))

	assert.False(t, conf.IsSet("service::extensions"))
}

func TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	p := createProviderWithLogger(logger)

	failingSource := &mockSourcePolicy{
		name:    "failing_source",
		evalErr: errors.New("boom"),
	}
	validSource := &mockSourcePolicy{
		name: "valid_source",
	}
	ps := &googlepolicy.PolicySet{
		PolicySetID: "projects/my-project/locations/us-central1/policySets/my-policy-set",
		RevisionID:  "886313e1-3b8a-5372-9b90-0c9aee199e5d",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"failing_source": {PolicyObj: failingSource},
			"valid_source":   {PolicyObj: validSource},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	// Active policy set remains active (not rolled back) because individual policy failures are non-fatal
	require.NotNil(t, googlepolicy.ActivePolicySet())
	assert.Equal(t, "886313e1-3b8a-5372-9b90-0c9aee199e5d", googlepolicy.ActivePolicySet().RevisionID)

	conf, err := ret.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("receivers::otlp/valid_source"))
	assert.False(t, conf.IsSet("receivers::otlp/failing_source"))
	assert.True(t, conf.IsSet("exporters::otlp_grpc/default_gcp_destination"))
	assert.True(t, conf.IsSet("receivers::otlp/default_self_metrics"))

	entries := recorded.All()
	require.Len(t, entries, 1)
	entry := entries[0]
	assert.Equal(t, zapcore.ErrorLevel, entry.Level)
	assert.Contains(t, entry.Message, "failed to evaluate source policy \"failing_source\": boom")
	assert.Equal(t, map[string]any{
		"event.name":                 event.PolicyEvaluateErrorEventName,
		"gcp.policy.id":              "failing_source",
		"gcp.policy.set.id":          "projects/my-project/locations/us-central1/policySets/my-policy-set",
		"gcp.policy.set.revision.id": "886313e1-3b8a-5372-9b90-0c9aee199e5d",
		"error.type":                 "policy_evaluation_failed",
		"gcp.policy.class":           "source",
		"context":                    "context.Background.WithValue(FLEET_ID, 1234)",
	}, entry.ContextMap())

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet(t *testing.T) {
	t.Setenv("FLEET_ID", "")
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	p := createProviderWithLogger(logger)

	ps := &googlepolicy.PolicySet{
		PolicySetID: "projects/my-project/locations/us-central1/policySets/my-policy-set",
		RevisionID:  "rev-should-stay-active",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			"valid_source": {PolicyObj: &mockSourcePolicy{name: "valid_source"}},
		},
	}
	googlepolicy.SetActivePolicySet(ps)
	t.Cleanup(func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	_, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", nil)
	require.ErrorIs(t, err, selfmetrics.ErrNoFleetID)

	// Built-in policy failures must NOT roll back the active policy set or attribute the error to activePolicySet.RevisionID.
	require.NotNil(t, googlepolicy.ActivePolicySet())
	assert.Equal(t, "rev-should-stay-active", googlepolicy.ActivePolicySet().RevisionID)

	entries := recorded.All()
	require.Len(t, entries, 1)
	assert.Equal(t, map[string]any{
		"event.name":                 event.PolicyEvaluateErrorEventName,
		"gcp.policy.id":              "default_self_metrics",
		"gcp.policy.set.id":          "",
		"gcp.policy.set.revision.id": "",
		"error.type":                 "policy_evaluation_failed",
		"gcp.policy.class":           "source",
		"context":                    "context.Background",
	}, entries[0].ContextMap())

	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestPartialReloadFeatureGatesEnabled(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(partialReloadFeatureGateID, false))
	require.NoError(t, featuregate.GlobalRegistry().Set(partialReloadReceiversGateID, false))
	assert.False(t, service.ReceiverPartialReloadEnabled())

	p := createProvider()
	assert.True(t, service.ReceiverPartialReloadEnabled(), "creating googlecontrolplaneprovider must enable service.partialReload and service.partialReloadReceivers")
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestWatcher_DiffGatedVsBaseline(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")
	cleanupPolicySets := func() {
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	}
	t.Cleanup(cleanupPolicySets)

	makeLogFilterPolicy := func(id, bodySubstring string) googlepolicy.Policy {
		pol, err := googlepolicy.LoadPolicy("log_filter", map[string]any{
			"type":   "log_filter",
			"id":     id,
			"action": "ACTION_DROP",
			"matches": []any{
				map[string]any{
					"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
					"regex":  bodySubstring,
				},
			},
		})
		require.NoError(t, err)
		return pol
	}

	t.Run("baseline_unconditional_watcher_fires_on_log_filter_only", func(t *testing.T) {
		cleanupPolicySets()
		p := createProvider()
		p.(*provider).diffGateReloads = false
		t.Cleanup(func() {
			assert.NoError(t, p.Shutdown(context.Background()))
		})

		var watcherCalls atomic.Int32
		watcherCh := make(chan *confmap.ChangeEvent, 4)
		watcher := func(ev *confmap.ChangeEvent) {
			watcherCalls.Add(1)
			watcherCh <- ev
		}

		ret, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", watcher)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, ret.Close(context.Background()))
		})

		// Rolling out a LogFilterPolicy-only revision with diffGateReloads=false
		// unconditionally fires WatcherFunc even though the synthesized confmap is identical.
		filter1 := makeLogFilterPolicy("filter-1", "secret")
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			RevisionID: "rev-baseline-filter-1",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				filter1.PolicyName(): {PolicyObj: filter1},
			},
		})

		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
			assert.NoError(t, ev.Error)
		case <-time.After(2 * time.Second):
			t.Fatal("expected unconditional baseline watcher to fire on LogFilterPolicy update")
		}
		assert.Equal(t, int32(1), watcherCalls.Load())
	})

	t.Run("prototype_diff_gated_watcher_suppresses_filter_only_and_fires_on_source", func(t *testing.T) {
		cleanupPolicySets()
		core, recorded := observer.New(zapcore.DebugLevel)
		p := createProviderWithLogger(zap.New(core))
		t.Cleanup(func() {
			assert.NoError(t, p.Shutdown(context.Background()))
		})

		var watcherCalls atomic.Int32
		watcherCh := make(chan *confmap.ChangeEvent, 4)
		watcher := func(ev *confmap.ChangeEvent) {
			watcherCalls.Add(1)
			watcherCh <- ev
		}

		ret1, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", watcher)
		require.NoError(t, err)

		// 1. Roll out LogFilterPolicy revision 1 -> synthesized confmap is identical -> 0 watcher calls.
		filter1 := makeLogFilterPolicy("filter-1", "drop-1")
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			RevisionID: "rev-proto-filter-1",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				filter1.PolicyName(): {PolicyObj: filter1},
			},
		})
		require.Eventually(t, func() bool {
			return len(recorded.FilterMessage("Synthesized confmap unchanged after policy set update; skipping collector reload").All()) >= 1
		}, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(0), watcherCalls.Load(), "diff-gated watcher must not fire on LogFilterPolicy update")

		// 2. Roll out LogFilterPolicy revision 2 -> still identical confmap -> 0 watcher calls.
		filter2 := makeLogFilterPolicy("filter-2", "drop-2")
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			RevisionID: "rev-proto-filter-2",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				filter1.PolicyName(): {PolicyObj: filter1},
				filter2.PolicyName(): {PolicyObj: filter2},
			},
		})
		require.Eventually(t, func() bool {
			return len(recorded.FilterMessage("Synthesized confmap unchanged after policy set update; skipping collector reload").All()) >= 2
		}, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(0), watcherCalls.Load())

		// 3. Roll out an invalid PolicySet (2 destination policies) -> rolls back to rev-proto-filter-2 -> identical confmap -> 0 watcher calls.
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			RevisionID: "rev-proto-invalid-dest",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				"dest1": {PolicyObj: &mockDestinationPolicy{name: "dest1"}},
				"dest2": {PolicyObj: &mockDestinationPolicy{name: "dest2"}},
			},
		})
		require.Eventually(t, func() bool {
			return len(recorded.FilterMessage("Synthesized confmap unchanged after policy set update; skipping collector reload").All()) >= 3
		}, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(0), watcherCalls.Load(), "rollback to previous PolicySet with identical confmap must not fire watcher")
		assert.Equal(t, "rev-proto-filter-2", googlepolicy.ActivePolicySetRevisionID())

		// 4. Roll out a SourcePolicy update (plus a non-fatal failing source to verify no duplicate error events on Retrieve).
		failingSource := &mockSourcePolicy{name: "failing_src", evalErr: errors.New("broken source")}
		validSource := &mockSourcePolicy{name: "source_a"}
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			PolicySetID: "projects/my-project/locations/us-central1/policySets/ps-1",
			RevisionID:  "rev-proto-source-1",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				filter1.PolicyName(): {PolicyObj: filter1},
				"failing_src":        {PolicyObj: failingSource},
				"source_a":           {PolicyObj: validSource},
			},
		})

		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
			assert.NoError(t, ev.Error)
		case <-time.After(2 * time.Second):
			t.Fatal("expected diff-gated watcher to fire when SourcePolicy changes synthesized confmap")
		}
		assert.Equal(t, int32(1), watcherCalls.Load())

		// Simulate confmap.Resolver.Resolve: close previous Retrieved, then call Retrieve with watcher again.
		require.NoError(t, ret1.Close(context.Background()))
		ret2, err := p.Retrieve(context.Background(), "googlecontrolplane:component:my-config", watcher)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, ret2.Close(context.Background()))
		})

		conf2, err := ret2.AsConf()
		require.NoError(t, err)
		assert.True(t, conf2.IsSet("receivers::otlp/source_a"))

		// Verify failing_src only emitted 1 PolicyEvaluateErrorEvent across the watcher pre-eval + Retrieve.
		evalErrLogs := recorded.FilterMessageSnippet("failed to evaluate source policy \"failing_src\"").All()
		assert.Len(t, evalErrLogs, 1, "consuming pendingConfMap in Retrieve must not double-evaluate or double-log policy errors")

		// 5. Roll out another LogFilterPolicy update after the reload -> new watcher goroutine suppresses it.
		filter3 := makeLogFilterPolicy("filter-3", "drop-3")
		googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
			PolicySetID: "projects/my-project/locations/us-central1/policySets/ps-1",
			RevisionID:  "rev-proto-source-1-plus-filter-3",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				filter1.PolicyName(): {PolicyObj: filter1},
				filter3.PolicyName(): {PolicyObj: filter3},
				"source_a":           {PolicyObj: validSource},
			},
		})
		require.Eventually(t, func() bool {
			return len(recorded.FilterMessage("Synthesized confmap unchanged after policy set update; skipping collector reload").All()) >= 4
		}, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, int32(1), watcherCalls.Load(), "subsequent LogFilterPolicy update after reload must also be suppressed")
	})
}

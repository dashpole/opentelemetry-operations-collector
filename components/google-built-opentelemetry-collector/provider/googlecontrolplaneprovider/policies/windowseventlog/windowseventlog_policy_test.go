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

package windowseventlog

import (
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/windowseventlogreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

func TestWindowsEventLogPolicy_Evaluate_SingleAndDeduplicated(t *testing.T) {
	policy := &WindowsEventLogSourcePolicy{
		ID:       "dc-events",
		Channels: []string{"Security", "Application", "security", "  System  ", "APPLICATION"},
	}

	assert.Equal(t, "dc-events", policy.PolicyName())
	assert.Equal(t, PolicyType, policy.PolicyType())
	assert.Equal(t, googlepolicy.PolicyClassSource, policy.PolicyClass())
	assert.Equal(t, []string{"Security", "Application", "System"}, policy.DeduplicatedChannels())

	conf, err := policy.Evaluate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conf)

	expectedChannels := map[string]string{
		"receivers::windowseventlog/security":    "Security",
		"receivers::windowseventlog/application": "Application",
		"receivers::windowseventlog/system":      "System",
	}

	receiversMap, ok := conf.Get("receivers").(map[string]any)
	require.True(t, ok)
	assert.Len(t, receiversMap, 3)

	for prefix, wantChannel := range expectedChannels {
		assert.Equal(t, wantChannel, conf.Get(prefix+"::channel"))
		assert.Equal(t, true, conf.Get(prefix+"::suppress_rendering_info"))
		assert.Equal(t, true, conf.Get(prefix+"::event_driven_scraping"))
		assert.Equal(t, 500, conf.Get(prefix+"::max_reads"))
		assert.Equal(t, true, conf.Get(prefix+"::ignore_channel_errors"))
	}
}

func TestWindowsEventLogPolicy_Evaluate_CrossPolicyDeduplication(t *testing.T) {
	policy1 := &WindowsEventLogSourcePolicy{
		ID:       "policy-1",
		Channels: []string{"Security", "Application", "security"},
	}
	policy2 := &WindowsEventLogSourcePolicy{
		ID:       "policy-2",
		Channels: []string{"SECURITY", "System", "application"},
	}

	seen := NewSeenChannels()
	ctx := WithSeenChannels(t.Context(), seen)

	conf1, err := policy1.Evaluate(ctx)
	require.NoError(t, err)
	conf2, err := policy2.Evaluate(ctx)
	require.NoError(t, err)

	merged := confmap.New()
	require.NoError(t, merged.Merge(conf1))
	require.NoError(t, merged.Merge(conf2))

	// First-seen casing from policy-1 ("Security", "Application") must be preserved
	// even after merging policy-2 ("SECURITY", "application").
	assert.Equal(t, "Security", merged.Get("receivers::windowseventlog/security::channel"))
	assert.Equal(t, "Application", merged.Get("receivers::windowseventlog/application::channel"))
	assert.Equal(t, "System", merged.Get("receivers::windowseventlog/system::channel"))

	receiversMap, ok := merged.Get("receivers").(map[string]any)
	require.True(t, ok)
	assert.Len(t, receiversMap, 3)
}

func TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal(t *testing.T) {
	factory := windowseventlogreceiver.NewFactory()

	// 1. Baseline: verify upstream v0.162.0 defaults before policy compilation
	baselineCfg, ok := factory.CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)
	require.True(t, ok)
	assert.False(t, baselineCfg.InputConfig.SuppressRenderingInfo, "upstream default suppress_rendering_info must be false")
	assert.False(t, baselineCfg.InputConfig.EventDrivenScraping, "upstream default event_driven_scraping must be false")
	assert.Equal(t, 100, baselineCfg.InputConfig.MaxReads, "upstream default max_reads must be 100")
	assert.False(t, baselineCfg.InputConfig.IgnoreChannelErrors, "upstream default ignore_channel_errors must be false")

	// 2. Change: compile WindowsEventLogSourcePolicy and unmarshal into upstream WindowsLogConfig
	policy := &WindowsEventLogSourcePolicy{
		ID:       "dc-events",
		Channels: []string{"Security"},
	}
	conf, err := policy.Evaluate(t.Context())
	require.NoError(t, err)

	sub, err := conf.Sub("receivers::windowseventlog/security")
	require.NoError(t, err)

	compiledCfg, ok := factory.CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)
	require.True(t, ok)
	require.NoError(t, sub.Unmarshal(compiledCfg))

	assert.Equal(t, "Security", compiledCfg.InputConfig.Channel)
	assert.True(t, compiledCfg.InputConfig.SuppressRenderingInfo)
	assert.True(t, compiledCfg.InputConfig.EventDrivenScraping)
	assert.Equal(t, 500, compiledCfg.InputConfig.MaxReads)
	assert.True(t, compiledCfg.InputConfig.IgnoreChannelErrors)
}

func TestWindowsEventLogPolicy_Pipelines(t *testing.T) {
	policy := &WindowsEventLogSourcePolicy{
		ID:       "dc-events",
		Channels: []string{"Security", "System", "security"},
	}

	googlePolicyProc := component.MustNewID("googlepolicy")
	batchLogsProc := component.MustNewIDWithName("queuebatch", "default_gcp_destination_batch_logs")
	exporter := component.MustNewIDWithName("otlp_grpc", "default_gcp_destination")
	extension := component.MustNewIDWithName("googleclientauth", "default_gcp_destination")

	logsConf, err := policy.LogsPipelines(
		[]component.ID{googlePolicyProc, batchLogsProc},
		[]component.ID{exporter},
		[]component.ID{extension},
	)
	require.NoError(t, err)
	require.NotNil(t, logsConf)

	assert.Equal(t, []any{"windowseventlog/security", "windowseventlog/system"}, logsConf.Get("service::pipelines::logs/windows_event_log::receivers"))
	assert.Equal(t, []any{"googlepolicy", "queuebatch/default_gcp_destination_batch_logs"}, logsConf.Get("service::pipelines::logs/windows_event_log::processors"))
	assert.Equal(t, []any{"otlp_grpc/default_gcp_destination"}, logsConf.Get("service::pipelines::logs/windows_event_log::exporters"))

	metricsConf, err := policy.MetricsPipelines(nil, []component.ID{exporter}, []component.ID{extension})
	require.NoError(t, err)
	assert.Nil(t, metricsConf)

	tracesConf, err := policy.TracesPipelines(nil, []component.ID{exporter}, []component.ID{extension})
	require.NoError(t, err)
	assert.Nil(t, tracesConf)
}

func TestWindowsEventLogPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		policy  *WindowsEventLogSourcePolicy
		wantErr error
	}{
		{
			name:   "valid with id",
			policy: &WindowsEventLogSourcePolicy{ID: "dc-events", Channels: []string{"Security", "System"}},
		},
		{
			name:   "valid with name",
			policy: &WindowsEventLogSourcePolicy{Name: "dc-events", Channels: []string{"Security"}},
		},
		{
			name:   "valid with matching id and name",
			policy: &WindowsEventLogSourcePolicy{ID: "dc-events", Name: "dc-events", Channels: []string{"Security"}},
		},
		{
			name:    "missing id and name",
			policy:  &WindowsEventLogSourcePolicy{Channels: []string{"Security"}},
			wantErr: ErrMissingPolicyName,
		},
		{
			name:    "whitespace-only id",
			policy:  &WindowsEventLogSourcePolicy{ID: "   ", Channels: []string{"Security"}},
			wantErr: ErrMissingPolicyName,
		},
		{
			name:    "conflicting id and name",
			policy:  &WindowsEventLogSourcePolicy{ID: "id-1", Name: "name-2", Channels: []string{"Security"}},
			wantErr: ErrConflictingPolicyNames,
		},
		{
			name:    "empty channels slice",
			policy:  &WindowsEventLogSourcePolicy{ID: "dc-events"},
			wantErr: ErrMissingChannels,
		},
		{
			name:    "empty channel entry",
			policy:  &WindowsEventLogSourcePolicy{ID: "dc-events", Channels: []string{"Security", "  "}},
			wantErr: ErrEmptyChannel,
		},
		{
			name:    "channel with confmap delimiter",
			policy:  &WindowsEventLogSourcePolicy{ID: "dc-events", Channels: []string{"Security::Sub"}},
			wantErr: ErrInvalidChannelName,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				err := tc.policy.Validate()
				if tc.wantErr == nil {
					assert.NoError(t, err)
					return
				}
				require.ErrorIs(t, err, tc.wantErr)
			})
		})
	}
}

func TestWindowsEventLogPolicy_LoadableFromRegistry(t *testing.T) {
	loaded, err := googlepolicy.LoadPolicy(PolicyType, map[string]any{
		"type":     PolicyType,
		"id":       "dc-security-events",
		"channels": []any{"Security", "System"},
	})
	require.NoError(t, err)
	assert.Equal(t, "dc-security-events", loaded.PolicyName())

	_, err = googlepolicy.LoadPolicy(PolicyType, map[string]any{
		"type": PolicyType,
		"id":   "empty-channels",
	})
	require.ErrorIs(t, err, googlepolicy.ErrPolicyFailedValidation)
}

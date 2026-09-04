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

package googlecontrolplane

import (
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPolicyValidator(t *testing.T) {
	reg := driver.NewDefaultRegistry()

	t.Run("Valid policy set without skips", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.WarnLevel)
		logger := zap.New(observedZapCore)
		val := NewPolicyValidator(reg, logger)

		validLogPolicy := &policyv1alpha1.LogFilterPolicy{
			Id:     "keep-critical-logs",
			Action: policyv1alpha1.Action_ACTION_KEEP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "ERROR"},
				},
			},
		}
		anyLog, err := anypb.New(validLogPolicy)
		require.NoError(t, err)

		collector := &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{
				{
					Name: "gcp-destination",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLGcpDestination,
					},
				},
				{
					Name: "otlp-source",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLOtlpSource,
					},
				},
				{
					Name:        "keep-critical-logs",
					TypedConfig: anyLog,
				},
			},
		}

		res := val.Validate(collector)
		assert.False(t, res.HasSkippedPolicies)
		assert.Empty(t, res.Diagnostics)
		assert.Len(t, res.ValidPolicies, 3)
		assert.Equal(t, "accepted", res.PolicyStatuses["gcp-destination"].Status)
		assert.Equal(t, "accepted", res.PolicyStatuses["otlp-source"].Status)
		assert.Equal(t, "accepted", res.PolicyStatuses["keep-critical-logs"].Status)
		assert.Empty(t, observedLogs.All())
	})

	t.Run("Fail-open: Skips unsupported TypeURLs and malformed regex", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.WarnLevel)
		logger := zap.New(observedZapCore)
		val := NewPolicyValidator(reg, logger)

		malformedRegexPolicy := &policyv1alpha1.LogFilterPolicy{
			Id:     "bad-regex-policy",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Regex{Regex: "[unclosed"},
				},
			},
		}
		anyBadRegex, err := anypb.New(malformedRegexPolicy)
		require.NoError(t, err)

		collector := &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{
				{
					Name: "valid-otlp",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLOtlpSource,
					},
				},
				{
					Name: "future-unknown-policy",
					TypedConfig: &anypb.Any{
						TypeUrl: "type.googleapis.com/google.telemetry.future.v2.ExperimentalPolicy",
					},
				},
				{
					Name:        "bad-regex-policy",
					TypedConfig: anyBadRegex,
				},
			},
		}

		res := val.Validate(collector)
		assert.True(t, res.HasSkippedPolicies)
		assert.Len(t, res.Diagnostics, 2)
		assert.Len(t, res.ValidPolicies, 1)
		assert.Equal(t, "valid-otlp", res.ValidPolicies[0].Name)

		assert.Equal(t, "accepted", res.PolicyStatuses["valid-otlp"].Status)
		assert.Equal(t, "unsupported", res.PolicyStatuses["future-unknown-policy"].Status)
		assert.Equal(t, "rejected", res.PolicyStatuses["bad-regex-policy"].Status)

		summary := res.SkippedSummary()
		assert.Contains(t, summary, "future-unknown-policy")
		assert.Contains(t, summary, "bad-regex-policy")

		// Verify structured diagnostic log event emission
		logs := observedLogs.All()
		require.Len(t, logs, 2)

		var foundUnsupported, foundRejected bool
		for _, log := range logs {
			fields := log.ContextMap()
			if fields["event.name"] == EventPolicyUnsupported {
				foundUnsupported = true
				assert.Equal(t, "future-unknown-policy", fields["policy_id"])
			}
			if fields["event.name"] == EventPolicyRejected {
				foundRejected = true
				assert.Equal(t, "bad-regex-policy", fields["policy_id"])
			}
		}
		assert.True(t, foundUnsupported, "Must emit telemetry.policy.unsupported event")
		assert.True(t, foundRejected, "Must emit telemetry.policy.rejected event")
	})

	t.Run("Multiple destination policies rejected", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.WarnLevel)
		logger := zap.New(observedZapCore)
		val := NewPolicyValidator(reg, logger)

		collector := &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{
				{
					Name: "gcp-destination-1",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLGcpDestination,
					},
				},
				{
					Name: "gcp-destination-2",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLGcpDestination,
					},
				},
			},
		}

		res := val.Validate(collector)
		assert.True(t, res.HasSkippedPolicies)
		assert.Len(t, res.ValidPolicies, 1)
		assert.Equal(t, "gcp-destination-1", res.ValidPolicies[0].Name)
		assert.Equal(t, "accepted", res.PolicyStatuses["gcp-destination-1"].Status)
		assert.Equal(t, "rejected", res.PolicyStatuses["gcp-destination-2"].Status)

		logs := observedLogs.All()
		require.Len(t, logs, 1)
		fields := logs[0].ContextMap()
		assert.Equal(t, EventPolicyRejected, fields["event.name"])
		assert.Equal(t, "gcp-destination-2", fields["policy_id"])
		assert.Contains(t, fields["reason"], "multiple destination policies detected")
	})

	t.Run("Duplicate policy IDs rejected", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.WarnLevel)
		logger := zap.New(observedZapCore)
		val := NewPolicyValidator(reg, logger)

		collector := &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{
				{
					Name: "duplicate-id",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLOtlpSource,
					},
				},
				{
					Name: "duplicate-id",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLFilelogSource,
					},
				},
			},
		}

		res := val.Validate(collector)
		assert.True(t, res.HasSkippedPolicies)
		assert.Len(t, res.ValidPolicies, 1)
		assert.Equal(t, "duplicate-id", res.ValidPolicies[0].Name)
		assert.Equal(t, driver.TypeURLOtlpSource, res.ValidPolicies[0].TypedConfig.TypeUrl)

		logs := observedLogs.All()
		require.Len(t, logs, 1)
		fields := logs[0].ContextMap()
		assert.Equal(t, EventPolicyRejected, fields["event.name"])
		assert.Contains(t, fields["reason"], "duplicate policy ID")
	})

	t.Run("Fail-open: Skips policies with missing target or missing predicate", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.WarnLevel)
		logger := zap.New(observedZapCore)
		val := NewPolicyValidator(reg, logger)

		nilTargetPolicy := &policyv1alpha1.LogFilterPolicy{
			Id:     "nil-target-policy",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target:    nil,
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "test"},
				},
			},
		}
		anyNilTarget, err := anypb.New(nilTargetPolicy)
		require.NoError(t, err)

		nilPredicatePolicy := &policyv1alpha1.LogFilterPolicy{
			Id:     "nil-predicate-policy",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: nil,
				},
			},
		}
		anyNilPredicate, err := anypb.New(nilPredicatePolicy)
		require.NoError(t, err)

		collector := &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{
				{
					Name: "valid-source",
					TypedConfig: &anypb.Any{
						TypeUrl: driver.TypeURLOtlpSource,
					},
				},
				{
					Name:        "nil-target-policy",
					TypedConfig: anyNilTarget,
				},
				{
					Name:        "nil-predicate-policy",
					TypedConfig: anyNilPredicate,
				},
			},
		}

		res := val.Validate(collector)
		assert.True(t, res.HasSkippedPolicies)
		assert.Len(t, res.ValidPolicies, 1)
		assert.Equal(t, "valid-source", res.ValidPolicies[0].Name)
		assert.Equal(t, "rejected", res.PolicyStatuses["nil-target-policy"].Status)
		assert.Equal(t, "rejected", res.PolicyStatuses["nil-predicate-policy"].Status)

		logs := observedLogs.All()
		require.Len(t, logs, 2)
		for _, log := range logs {
			fields := log.ContextMap()
			assert.Equal(t, EventPolicyRejected, fields["event.name"])
		}
	})

	t.Run("Empty collector", func(t *testing.T) {
		val := NewPolicyValidator(reg, zap.NewNop())
		res := val.Validate(nil)
		assert.False(t, res.HasSkippedPolicies)
		assert.Empty(t, res.ValidPolicies)
		assert.Empty(t, res.Diagnostics)
	})
}

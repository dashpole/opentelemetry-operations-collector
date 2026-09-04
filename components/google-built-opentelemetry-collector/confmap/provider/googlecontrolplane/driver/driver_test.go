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

package driver

import (
	"testing"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPolicyDriverRegistry(t *testing.T) {
	reg := NewDefaultRegistry()

	t.Run("Default drivers and aliases", func(t *testing.T) {
		assert.NotNil(t, reg.GetDriver(TypeURLGcpDestination))
		assert.NotNil(t, reg.GetDriver(TypeURLGcpDestinationAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLOtlpSource))
		assert.NotNil(t, reg.GetDriver(TypeURLOtlpSourceAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLFilelogSource))
		assert.NotNil(t, reg.GetDriver(TypeURLFilelogSourceAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLSelfObservability))
		assert.NotNil(t, reg.GetDriver(TypeURLSelfObservabilityAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLLogFilterPolicy))
		assert.NotNil(t, reg.GetDriver(TypeURLLogFilterPolicyAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLMetricFilterPolicy))
		assert.NotNil(t, reg.GetDriver(TypeURLMetricFilterPolicyAlias))
		assert.NotNil(t, reg.GetDriver(TypeURLTraceFilterPolicy))
		assert.NotNil(t, reg.GetDriver(TypeURLTraceFilterPolicyAlias))

		assert.Nil(t, reg.GetDriver("unknown/type"))
		assert.NotEmpty(t, reg.ListDrivers())
		assert.NotEmpty(t, reg.SupportedTypeURLs())
	})

	t.Run("Default component factories", func(t *testing.T) {
		assert.NotNil(t, reg.GetReceiverFactory("otlp"))
		assert.NotNil(t, reg.GetReceiverFactory("filelog"))
		assert.Nil(t, reg.GetReceiverFactory("unknown"))

		assert.NotNil(t, reg.GetProcessorFactory("batch"))
		assert.NotNil(t, reg.GetProcessorFactory("policy"))
		assert.Nil(t, reg.GetProcessorFactory("unknown"))

		assert.NotNil(t, reg.GetExporterFactory("otlp"))
		assert.Nil(t, reg.GetExporterFactory("unknown"))

		assert.NotNil(t, reg.GetExtensionFactory("googleclientauth"))
		assert.NotNil(t, reg.GetExtensionFactory("googlecontrolplaneextension"))
		assert.Nil(t, reg.GetExtensionFactory("unknown"))
	})
}

func TestPolicyDrivers(t *testing.T) {
	t.Run("GcpDestinationDriver", TestGcpDestinationDriver)
	t.Run("OtlpSourceDriver", TestOtlpSourceDriver)
	t.Run("FilelogSourceDriver", TestFilelogSourceDriver)
	t.Run("SelfObservabilityDriver", TestSelfObservabilityDriver)
	t.Run("TransformFilterDriver", TestTransformFilterDriver)
}

func TestGcpDestinationDriver(t *testing.T) {
	d := NewGcpDestinationDriver()
	assert.Equal(t, TypeURLGcpDestination, d.TypeURL())
	assert.Equal(t, PolicyClassDestination, d.Class())
	assert.NoError(t, d.Validate(nil))

	ctx := &CompilationContext{
		PolicyID:       "gcp-dest-policy",
		FleetID:        "fleet-1",
		ResolvedTokens: make(map[string]string),
	}

	frag, err := d.GenerateConfig(nil, ctx)
	require.NoError(t, err)
	require.NotNil(t, frag)

	assert.Contains(t, frag.Exporters, "otlp/gcp_destination")
	assert.Contains(t, frag.Extensions, "googleclientauth")
	assert.Contains(t, frag.ServiceExts, "googleclientauth")

	// UTP batch sizing
	metricBatch, ok := frag.Processors["batch/gcp_destination_metrics"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 200, metricBatch["send_batch_size"])
	assert.Equal(t, "5s", metricBatch["timeout"])

	logBatch, ok := frag.Processors["batch/gcp_destination_logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 8192, logBatch["send_batch_size"])
	assert.Equal(t, "1s", logBatch["timeout"])

	traceBatch, ok := frag.Processors["batch/gcp_destination_traces"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 25000, traceBatch["send_batch_size"])
	assert.Equal(t, "5s", traceBatch["timeout"])

	assert.Equal(t, "otlp/gcp_destination", ctx.ResolvedTokens["active_destination_exporter"])
	assert.Equal(t, "batch/gcp_destination_metrics", ctx.ResolvedTokens["active_destination_metric_preprocess"])
}

func TestOtlpSourceDriver(t *testing.T) {
	d := NewOtlpSourceDriver()
	assert.Equal(t, TypeURLOtlpSource, d.TypeURL())
	assert.Equal(t, PolicyClassSource, d.Class())
	assert.NoError(t, d.Validate(nil))

	frag, err := d.GenerateConfig(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, frag)

	assert.Contains(t, frag.Receivers, "otlp")
	assert.Contains(t, frag.Pipelines, "logs")
	assert.Contains(t, frag.Pipelines, "metrics")
	assert.Contains(t, frag.Pipelines, "traces")

	assert.Equal(t, []string{"otlp"}, frag.Pipelines["logs"].Receivers)
	assert.Equal(t, []string{"otlp"}, frag.Pipelines["metrics"].Receivers)
	assert.Equal(t, []string{"otlp"}, frag.Pipelines["traces"].Receivers)
}

func TestFilelogSourceDriver(t *testing.T) {
	d := NewFilelogSourceDriver()
	assert.Equal(t, TypeURLFilelogSource, d.TypeURL())
	assert.Equal(t, PolicyClassSource, d.Class())
	assert.NoError(t, d.Validate(nil))

	frag, err := d.GenerateConfig(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, frag)

	assert.Contains(t, frag.Receivers, "filelog")
	assert.Contains(t, frag.Pipelines, "logs")
	assert.Equal(t, []string{"filelog"}, frag.Pipelines["logs"].Receivers)
	assert.NotContains(t, frag.Pipelines, "metrics")
}

func TestSelfObservabilityDriver(t *testing.T) {
	d := NewSelfObservabilityDriver()
	assert.Equal(t, TypeURLSelfObservability, d.TypeURL())
	assert.Equal(t, PolicyClassSource, d.Class())
	assert.NoError(t, d.Validate(nil))

	frag, err := d.GenerateConfig(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, frag)

	assert.Contains(t, frag.Receivers, "otlp/selfobs_internal")
	assert.Contains(t, frag.Pipelines, "metrics")
	assert.Equal(t, []string{"otlp/selfobs_internal"}, frag.Pipelines["metrics"].Receivers)

	// Verify loopback endpoint is 127.0.0.1:4320
	recvMap, ok := frag.Receivers["otlp/selfobs_internal"].(map[string]any)
	require.True(t, ok)
	protocols, ok := recvMap["protocols"].(map[string]any)
	require.True(t, ok)
	grpcConf, ok := protocols["grpc"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "127.0.0.1:4320", grpcConf["endpoint"])
}

func TestTransformFilterDriver(t *testing.T) {
	logDriver := NewLogFilterDriver()
	assert.Equal(t, TypeURLLogFilterPolicy, logDriver.TypeURL())
	assert.Equal(t, PolicyClassTransformation, logDriver.Class())

	t.Run("Validation: ACTION_UNSPECIFIED fails", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_UNSPECIFIED.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "test"},
				},
			},
		}
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACTION_UNSPECIFIED")
	})

	t.Run("Validation: empty matchers fail", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action:  policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: nil,
		}
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one matcher")
	})

	t.Run("Validation: nil matcher target or empty target oneof fails", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target:    nil,
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "test"},
				},
			},
		}
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a valid target")

		lpEmptyTarget := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target:    &policyv1alpha1.LogFieldSelector{},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "test"},
				},
			},
		}
		err = logDriver.Validate(lpEmptyTarget)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a valid target")
	})

	t.Run("Validation: nil matcher predicate fails", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
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
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a predicate")
	})

	t.Run("Validation: nil Exists struct in Exists predicate fails", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exists{Exists: nil},
				},
			},
		}
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exists predicate cannot be nil")
	})

	t.Run("Validation: malformed regex fails", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Regex{Regex: "[unclosed-regex"},
				},
			},
		}
		err := logDriver.Validate(lp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid regex")
	})

	t.Run("Validation: valid policy succeeds directly and via anypb.Any", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Regex{Regex: "^ERROR:.*"},
				},
			},
		}
		assert.NoError(t, logDriver.Validate(lp))

		anyMsg, err := anypb.New(lp)
		require.NoError(t, err)
		assert.NoError(t, logDriver.Validate(anyMsg))
	})

	t.Run("GenerateConfig for LogFilterPolicy", func(t *testing.T) {
		lp := &policyv1alpha1.LogFilterPolicy{
			Id:     "drop-errors",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "drop-me"},
				},
			},
		}

		ctx := &CompilationContext{
			PolicyID:       "drop-errors",
			ResolvedTokens: make(map[string]string),
		}

		frag, err := logDriver.GenerateConfig(lp, ctx)
		require.NoError(t, err)
		require.NotNil(t, frag)

		policyGlobal, ok := frag.Processors["policy/global"].(map[string]any)
		require.True(t, ok)
		policies, ok := policyGlobal["policies"].([]any)
		require.True(t, ok)
		require.Len(t, policies, 1)

		p0, ok := policies[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "drop-errors", p0["id"])
		assert.Equal(t, TypeURLLogFilterPolicy, p0["type_url"])
		assert.NotNil(t, p0["rule"])

		assert.Equal(t, "policy/global", ctx.ResolvedTokens["global_policy_processor"])
	})

	t.Run("MetricFilterDriver validation and config generation", func(t *testing.T) {
		metricDriver := NewMetricFilterDriver()
		mp := &policyv1alpha1.MetricFilterPolicy{
			Id:     "drop-jvm",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.MetricMatcher{
				{
					Target: &policyv1alpha1.MetricFieldSelector{
						Target: &policyv1alpha1.MetricFieldSelector_DescriptorField{
							DescriptorField: policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME,
						},
					},
					Predicate: &policyv1alpha1.MetricMatcher_Exact{Exact: "jvm.memory.used"},
				},
			},
		}

		require.NoError(t, metricDriver.Validate(mp))

		frag, err := metricDriver.GenerateConfig(mp, &CompilationContext{PolicyID: "drop-jvm"})
		require.NoError(t, err)
		assert.Contains(t, frag.Processors, "policy/global")
	})

	t.Run("TraceFilterDriver validation and config generation", func(t *testing.T) {
		traceDriver := NewTraceFilterDriver()
		tp := &policyv1alpha1.TraceFilterPolicy{
			Id:     "drop-health",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.TraceMatcher{
				{
					Target: &policyv1alpha1.TraceFieldSelector{
						Target: &policyv1alpha1.TraceFieldSelector_RecordField{
							RecordField: policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_NAME,
						},
					},
					Predicate: &policyv1alpha1.TraceMatcher_Exact{Exact: "healthz"},
				},
			},
		}

		require.NoError(t, traceDriver.Validate(tp))

		frag, err := traceDriver.GenerateConfig(tp, &CompilationContext{PolicyID: "drop-health"})
		require.NoError(t, err)
		assert.Contains(t, frag.Processors, "policy/global")
	})
}

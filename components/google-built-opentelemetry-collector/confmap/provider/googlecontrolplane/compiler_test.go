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
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestConfigCompiler(t *testing.T) {
	reg := driver.NewDefaultRegistry()
	logger := zap.NewNop()

	t.Run("Default compilation without policies", func(t *testing.T) {
		compiler := NewConfigCompiler(reg, logger,
			WithCompilerFleetID("fleet-test-1"),
			WithCompilerCollectorID("coll-test-1"),
		)

		confMap, tokens, err := compiler.Compile(nil)
		require.NoError(t, err)
		require.NotNil(t, confMap)

		// Verify tokens
		assert.Equal(t, "policy/global", tokens["global_policy_processor"])
		assert.Equal(t, "otlp/gcp_destination", tokens["active_destination_exporter"])
		assert.Equal(t, "batch/gcp_destination_metrics", tokens["active_destination_metric_preprocess"])

		// Convert to confmap.Conf to verify standard unmarshaling
		conf := confmap.NewFromStringMap(confMap)
		require.NotNil(t, conf)

		// Receivers must include otlp
		assert.True(t, conf.IsSet("receivers::otlp"))

		// Exporters must include otlp/gcp_destination
		assert.True(t, conf.IsSet("exporters::otlp/gcp_destination"))

		// Extensions must include googleclientauth and googlecontrolplaneextension
		assert.True(t, conf.IsSet("extensions::googleclientauth"))
		assert.True(t, conf.IsSet("extensions::googlecontrolplaneextension"))

		// Processors must include policy/global and batch processors
		assert.True(t, conf.IsSet("processors::policy/global"))
		assert.True(t, conf.IsSet("processors::batch/gcp_destination_metrics"))
		assert.True(t, conf.IsSet("processors::batch/gcp_destination_logs"))
		assert.True(t, conf.IsSet("processors::batch/gcp_destination_traces"))

		// Telemetry resource attributes
		assert.True(t, conf.IsSet("service::telemetry::resource::attributes"))
		rawAttrs := conf.Get("service::telemetry::resource::attributes")
		telemetryAttrs, ok := rawAttrs.([]any)
		require.True(t, ok)
		attrsMap := make(map[string]any)
		for _, item := range telemetryAttrs {
			m, isMap := item.(map[string]any)
			require.True(t, isMap)
			attrsMap[m["name"].(string)] = m["value"]
		}
		assert.Equal(t, "coll-test-1", attrsMap["service.instance.id"])
		assert.Equal(t, "fleet-test-1", attrsMap["gcp.fleet_id"])

		// Service pipelines for all 3 signals
		assert.True(t, conf.IsSet("service::pipelines::metrics"))
		assert.True(t, conf.IsSet("service::pipelines::logs"))
		assert.True(t, conf.IsSet("service::pipelines::traces"))

		// Verify PreValidator accepts the synthesized configuration!
		pv := NewPreValidator(reg, logger)
		require.NoError(t, pv.Validate(confMap), "Synthesized configuration must pass PreValidator")
	})

	t.Run("Multi-signal and multi-source compilation with filter policies", func(t *testing.T) {
		compiler := NewConfigCompiler(reg, logger,
			WithCompilerFleetID("fleet-multi"),
			WithCompilerCollectorID("coll-multi"),
		)

		logPolicy := &policyv1alpha1.LogFilterPolicy{
			Id:     "filter-debug-logs",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
				},
			},
		}
		anyLog, err := anypb.New(logPolicy)
		require.NoError(t, err)

		metricPolicy := &policyv1alpha1.MetricFilterPolicy{
			Id:     "filter-noisy-metric",
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.MetricMatcher{
				{
					Target: &policyv1alpha1.MetricFieldSelector{
						Target: &policyv1alpha1.MetricFieldSelector_DescriptorField{
							DescriptorField: policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME,
						},
					},
					Predicate: &policyv1alpha1.MetricMatcher_Exact{Exact: "noisy.metric"},
				},
			},
		}
		anyMetric, err := anypb.New(metricPolicy)
		require.NoError(t, err)

		policies := []*v3.TypedExtensionConfig{
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
				Name: "filelog-source",
				TypedConfig: &anypb.Any{
					TypeUrl: driver.TypeURLFilelogSource,
				},
			},
			{
				Name: "self-obs",
				TypedConfig: &anypb.Any{
					TypeUrl: driver.TypeURLSelfObservability,
				},
			},
			{
				Name:        "filter-debug-logs",
				TypedConfig: anyLog,
			},
			{
				Name:        "filter-noisy-metric",
				TypedConfig: anyMetric,
			},
		}

		confMap, tokens, err := compiler.Compile(policies)
		require.NoError(t, err)
		assert.Equal(t, "policy/global", tokens["global_policy_processor"])

		conf := confmap.NewFromStringMap(confMap)

		// Receivers should have otlp, filelog, and otlp/selfobs_internal
		assert.True(t, conf.IsSet("receivers::otlp"))
		assert.True(t, conf.IsSet("receivers::filelog"))
		assert.True(t, conf.IsSet("receivers::otlp/selfobs_internal"))

		// Processors policy/global should contain 2 policies
		subGpp, err := conf.Sub("processors::policy/global")
		require.NoError(t, err)
		var policyGlobalCfg struct {
			Policies []map[string]any `mapstructure:"policies"`
		}
		require.NoError(t, subGpp.Unmarshal(&policyGlobalCfg))
		assert.Len(t, policyGlobalCfg.Policies, 2)

		// Pipelines verification:
		// Logs pipeline should have receivers [otlp, filelog]
		subLogs, err := conf.Sub("service::pipelines::logs")
		require.NoError(t, err)
		var logsPipe struct {
			Receivers  []string `mapstructure:"receivers"`
			Processors []string `mapstructure:"processors"`
			Exporters  []string `mapstructure:"exporters"`
		}
		require.NoError(t, subLogs.Unmarshal(&logsPipe))
		assert.Contains(t, logsPipe.Receivers, "otlp")
		assert.Contains(t, logsPipe.Receivers, "filelog")
		assert.Equal(t, []string{"policy/global", "batch/gcp_destination_logs"}, logsPipe.Processors)
		assert.Equal(t, []string{"otlp/gcp_destination"}, logsPipe.Exporters)

		// Metrics pipeline should have receivers [otlp, otlp/selfobs_internal]
		subMetrics, err := conf.Sub("service::pipelines::metrics")
		require.NoError(t, err)
		var metricsPipe struct {
			Receivers  []string `mapstructure:"receivers"`
			Processors []string `mapstructure:"processors"`
			Exporters  []string `mapstructure:"exporters"`
		}
		require.NoError(t, subMetrics.Unmarshal(&metricsPipe))
		assert.Contains(t, metricsPipe.Receivers, "otlp")
		assert.Contains(t, metricsPipe.Receivers, "otlp/selfobs_internal")
		assert.Equal(t, []string{"policy/global", "batch/gcp_destination_metrics"}, metricsPipe.Processors)
		assert.Equal(t, []string{"otlp/gcp_destination"}, metricsPipe.Exporters)

		// Passes PreValidator
		pv := NewPreValidator(reg, logger)
		require.NoError(t, pv.Validate(confMap))
	})

	t.Run("Base config merge: policy/global inserted before batch processor", func(t *testing.T) {
		baseConfig := map[string]any{
			"receivers": map[string]any{
				"otlp": map[string]any{
					"protocols": map[string]any{
						"grpc": map[string]any{
							"endpoint": "0.0.0.0:4317",
						},
					},
				},
			},
			"processors": map[string]any{
				"memory_limiter": map[string]any{
					"check_interval": "1s",
				},
				"batch/custom": map[string]any{
					"send_batch_size": 500,
					"timeout":         "2s",
				},
			},
			"exporters": map[string]any{
				"otlp/base": map[string]any{
					"endpoint": "base.example.com:443",
				},
			},
			"service": map[string]any{
				"telemetry": map[string]any{
					"resource": map[string]any{
						"attributes": map[string]any{
							"deployment.environment": "prod",
						},
					},
				},
				"pipelines": map[string]any{
					"logs": map[string]any{
						"receivers":  []any{"otlp"},
						"processors": []any{"memory_limiter", "batch/custom"},
						"exporters":  []any{"otlp/base"},
					},
				},
			},
		}

		compiler := NewConfigCompiler(reg, logger,
			WithCompilerFleetID("fleet-merge"),
			WithCompilerCollectorID("coll-merge"),
			WithCompilerBaseConfig(baseConfig),
		)

		confMap, _, err := compiler.Compile(nil)
		require.NoError(t, err)

		conf := confmap.NewFromStringMap(confMap)

		// Check telemetry attributes merged without overwriting deployment.environment
		assert.True(t, conf.IsSet("service::telemetry::resource::attributes"))
		rawAttrs := conf.Get("service::telemetry::resource::attributes")
		attrsSlice, ok := rawAttrs.([]any)
		require.True(t, ok)
		attrs := make(map[string]any)
		for _, item := range attrsSlice {
			m, isMap := item.(map[string]any)
			require.True(t, isMap)
			attrs[m["name"].(string)] = m["value"]
		}
		assert.Equal(t, "prod", attrs["deployment.environment"])
		assert.Equal(t, "coll-merge", attrs["service.instance.id"])
		assert.Equal(t, "fleet-merge", attrs["gcp.fleet_id"])

		// Verify policy/global inserted directly before batch/custom
		subPipe, err := conf.Sub("service::pipelines::logs")
		require.NoError(t, err)
		var pipe struct {
			Receivers  []string `mapstructure:"receivers"`
			Processors []string `mapstructure:"processors"`
			Exporters  []string `mapstructure:"exporters"`
		}
		require.NoError(t, subPipe.Unmarshal(&pipe))
		assert.Equal(t, []string{"memory_limiter", "policy/global", "batch/custom"}, pipe.Processors)
	})

	t.Run("Base config merge: policy/global appended when batch processor absent", func(t *testing.T) {
		baseConfig := map[string]any{
			"receivers": map[string]any{
				"otlp": map[string]any{
					"protocols": map[string]any{
						"grpc": map[string]any{
							"endpoint": "0.0.0.0:4317",
						},
					},
				},
			},
			"processors": map[string]any{
				"custom_filter": map[string]any{},
			},
			"exporters": map[string]any{
				"otlp": map[string]any{
					"endpoint": "destination.example.com:443",
				},
			},
			"service": map[string]any{
				"pipelines": map[string]any{
					"traces": map[string]any{
						"receivers":  []any{"otlp"},
						"processors": []any{"custom_filter"},
						"exporters":  []any{"otlp"},
					},
				},
			},
		}

		compiler := NewConfigCompiler(reg, logger,
			WithCompilerBaseConfig(baseConfig),
		)

		confMap, _, err := compiler.Compile(nil)
		require.NoError(t, err)

		conf := confmap.NewFromStringMap(confMap)

		// policy/global appended as final processor before exporter
		subPipe, err := conf.Sub("service::pipelines::traces")
		require.NoError(t, err)
		var pipe struct {
			Receivers  []string `mapstructure:"receivers"`
			Processors []string `mapstructure:"processors"`
			Exporters  []string `mapstructure:"exporters"`
		}
		require.NoError(t, subPipe.Unmarshal(&pipe))
		assert.Equal(t, []string{"custom_filter", "policy/global"}, pipe.Processors)
	})

	t.Run("Base config merge: deepMergeMap preserves sub-fields on receivers and components", func(t *testing.T) {
		baseConfig := map[string]any{
			"receivers": map[string]any{
				"otlp": map[string]any{
					"protocols": map[string]any{
						"grpc": map[string]any{
							"endpoint": "0.0.0.0:4317",
							"tls": map[string]any{
								"insecure":  false,
								"cert_file": "/etc/ssl/custom-cert.pem",
							},
						},
					},
				},
			},
			"service": map[string]any{
				"pipelines": map[string]any{
					"logs": map[string]any{
						"receivers":  []any{"otlp"},
						"processors": []any{"batch"},
						"exporters":  []any{"otlp"},
					},
				},
			},
		}

		compiler := NewConfigCompiler(reg, logger,
			WithCompilerBaseConfig(baseConfig),
		)

		// Compile with an OtlpSourceDriver policy, which synthesizes receivers::otlp with grpc endpoint 0.0.0.0:4317 and http endpoint 0.0.0.0:4318
		otlpPolicy := []*v3.TypedExtensionConfig{
			{
				Name: "otlp-src",
				TypedConfig: &anypb.Any{
					TypeUrl: driver.TypeURLOtlpSource,
				},
			},
		}

		confMap, _, err := compiler.Compile(otlpPolicy)
		require.NoError(t, err)

		conf := confmap.NewFromStringMap(confMap)

		// Verify custom TLS was NOT wiped out by synthesized default otlp receiver
		subGrpcTLS, err := conf.Sub("receivers::otlp::protocols::grpc::tls")
		require.NoError(t, err)
		var tlsConf map[string]any
		require.NoError(t, subGrpcTLS.Unmarshal(&tlsConf))
		assert.Equal(t, false, tlsConf["insecure"])
		assert.Equal(t, "/etc/ssl/custom-cert.pem", tlsConf["cert_file"])

		// Verify synthesized HTTP protocol default was still populated into the merged config
		subHTTP, err := conf.Sub("receivers::otlp::protocols::http")
		require.NoError(t, err)
		var httpConf map[string]any
		require.NoError(t, subHTTP.Unmarshal(&httpConf))
		assert.Equal(t, "0.0.0.0:4318", httpConf["endpoint"])
	})

	t.Run("LoadBaseConfig from file and default", func(t *testing.T) {
		compiler := NewConfigCompiler(reg, logger,
			WithCompilerFileReader(func(path string) ([]byte, error) {
				if path == "/etc/otelcol/base.yaml" {
					return []byte(`
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: []
      exporters: [otlp]
`), nil
				}
				return nil, assert.AnError
			}),
		)

		// Load from URI
		loaded, err := compiler.LoadBaseConfig("file:///etc/otelcol/base.yaml")
		require.NoError(t, err)
		assert.NotNil(t, loaded["receivers"])

		// Load default
		def, err := compiler.LoadBaseConfig("")
		require.NoError(t, err)
		assert.NotNil(t, def["receivers"])
		assert.NotNil(t, def["processors"])
	})
}

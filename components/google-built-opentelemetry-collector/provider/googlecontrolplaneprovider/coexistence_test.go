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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/googleclientauthextension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/queuebatchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

type rawMapComponentConfig map[string]any

func pipelineMustID(signal pipeline.Signal, name string) pipeline.ID {
	return pipeline.NewIDWithName(signal, name)
}

type nopComponent struct {
	component.StartFunc
	component.ShutdownFunc
}

type passthroughProcessor struct {
	nopComponent
	stampAttrKey string
	stampAttrVal string
	nextLogs     consumer.Logs
	nextMetrics  consumer.Metrics
	nextTraces   consumer.Traces
}

func (p *passthroughProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: p.stampAttrKey != ""}
}

func (p *passthroughProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if p.stampAttrKey != "" {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					sl.LogRecords().At(k).Attributes().PutStr(p.stampAttrKey, p.stampAttrVal)
				}
			}
		}
	}
	if p.nextLogs != nil {
		return p.nextLogs.ConsumeLogs(ctx, ld)
	}
	return nil
}

func (p *passthroughProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if p.nextMetrics != nil {
		return p.nextMetrics.ConsumeMetrics(ctx, md)
	}
	return nil
}

func (p *passthroughProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if p.nextTraces != nil {
		return p.nextTraces.ConsumeTraces(ctx, td)
	}
	return nil
}

func newNopReceiverFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType("nop"),
		func() component.Config { return &rawMapComponentConfig{} },
		receiver.WithLogs(func(context.Context, receiver.Settings, component.Config, consumer.Logs) (receiver.Logs, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		receiver.WithMetrics(func(context.Context, receiver.Settings, component.Config, consumer.Metrics) (receiver.Metrics, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
		receiver.WithTraces(func(context.Context, receiver.Settings, component.Config, consumer.Traces) (receiver.Traces, error) {
			return &nopComponent{}, nil
		}, component.StabilityLevelStable),
	)
}

func newNopExporterFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType("nop"),
		func() component.Config { return &rawMapComponentConfig{} },
		exporter.WithLogs(func(context.Context, exporter.Settings, component.Config) (exporter.Logs, error) {
			return &passthroughProcessor{}, nil
		}, component.StabilityLevelStable),
		exporter.WithMetrics(func(context.Context, exporter.Settings, component.Config) (exporter.Metrics, error) {
			return &passthroughProcessor{}, nil
		}, component.StabilityLevelStable),
		exporter.WithTraces(func(context.Context, exporter.Settings, component.Config) (exporter.Traces, error) {
			return &passthroughProcessor{}, nil
		}, component.StabilityLevelStable),
	)
}

func newPassthroughProcessorFactory(typeStr string) processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		func() component.Config { return &rawMapComponentConfig{} },
		processor.WithLogs(func(_ context.Context, set processor.Settings, _ component.Config, next consumer.Logs) (processor.Logs, error) {
			pp := &passthroughProcessor{nextLogs: next}
			if typeStr == "transform" && set.ID.Name() == "sep_only" {
				pp.stampAttrKey = "sep.preprocessed"
				pp.stampAttrVal = "true"
			}
			return pp, nil
		}, component.StabilityLevelStable),
		processor.WithMetrics(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Metrics) (processor.Metrics, error) {
			return &passthroughProcessor{nextMetrics: next}, nil
		}, component.StabilityLevelStable),
		processor.WithTraces(func(_ context.Context, _ processor.Settings, _ component.Config, next consumer.Traces) (processor.Traces, error) {
			return &passthroughProcessor{nextTraces: next}, nil
		}, component.StabilityLevelStable),
	)
}

func newNopExtensionFactory(typeStr string) extension.Factory {
	return extension.NewFactory(
		component.MustNewType(typeStr),
		func() component.Config { return &rawMapComponentConfig{} },
		func(context.Context, extension.Settings, component.Config) (extension.Extension, error) {
			return &nopComponent{}, nil
		},
		component.StabilityLevelStable,
	)
}

func wrapCountingProcessorFactory(base processor.Factory, logsCount *atomic.Int32) processor.Factory {
	if logsCount == nil {
		return base
	}
	return processor.NewFactory(
		base.Type(),
		base.CreateDefaultConfig,
		processor.WithLogs(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
			logsCount.Add(1)
			return base.CreateLogs(ctx, set, cfg, next)
		}, base.LogsStability()),
		processor.WithMetrics(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
			return base.CreateMetrics(ctx, set, cfg, next)
		}, base.MetricsStability()),
		processor.WithTraces(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
			return base.CreateTraces(ctx, set, cfg, next)
		}, base.TracesStability()),
	)
}

func buildCoexistenceFactories(googlePolicyLogsCount, queueBatchLogsCount *atomic.Int32, extraReceivers ...receiver.Factory) (otelcol.Factories, error) {
	var err error
	factories := otelcol.Factories{
		Telemetry: otelconftelemetry.NewFactory(),
	}

	factories.Extensions, err = otelcol.MakeFactoryMap[extension.Factory](
		googleclientauthextension.NewFactory(),
		newNopExtensionFactory("googlecontrolplane"),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	recFactories := []receiver.Factory{
		otlpreceiver.NewFactory(),
		newNopReceiverFactory(),
	}
	recFactories = append(recFactories, extraReceivers...)
	factories.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](recFactories...)
	if err != nil {
		return otelcol.Factories{}, err
	}

	factories.Exporters, err = otelcol.MakeFactoryMap[exporter.Factory](
		otlpexporter.NewFactory(),
		newNopExporterFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	factories.Processors, err = otelcol.MakeFactoryMap[processor.Factory](
		wrapCountingProcessorFactory(googlepolicyprocessor.NewFactory(), googlePolicyLogsCount),
		wrapCountingProcessorFactory(queuebatchprocessor.NewFactory(), queueBatchLogsCount),
		newPassthroughProcessorFactory("resourcedetection"),
		newPassthroughProcessorFactory("transform"),
		newPassthroughProcessorFactory("batch"),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	factories.Connectors, err = otelcol.MakeFactoryMap[connector.Factory](
		newForwardConnectorFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	return factories, nil
}

func newForwardConnectorFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType("forward"),
		func() component.Config { return &rawMapComponentConfig{} },
		connector.WithLogsToLogs(func(_ context.Context, _ connector.Settings, _ component.Config, next consumer.Logs) (connector.Logs, error) {
			return &passthroughProcessor{nextLogs: next}, nil
		}, component.StabilityLevelStable),
	)
}

// localFileProvider is a confmap.Provider for "file:" URIs using confmap.NewRetrievedFromYAML.
type localFileProvider struct{}

func newLocalFileProviderFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(func(_ confmap.ProviderSettings) confmap.Provider {
		return &localFileProvider{}
	})
}

func (f *localFileProvider) Retrieve(_ context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	path := strings.TrimPrefix(uri, "file:")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return confmap.NewRetrievedFromYAML(raw)
}

func (f *localFileProvider) Scheme() string { return "file" }

func (f *localFileProvider) Shutdown(context.Context) error { return nil }

// mockMultiComponentProvider simulates a component:// provider that returns a
// single processor ID for "global_policy_processor", a single exporter ID for
// "active_destination_exporter", and a 2-processor slice for
// "active_destination_log_preprocess" (as required by Two-Stage Batch Chaining
// in One-Pager #1 or resourcedetection + queue_batch).
type mockMultiComponentProvider struct{}

func newMockMultiComponentProviderFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(func(_ confmap.ProviderSettings) confmap.Provider {
		return &mockMultiComponentProvider{}
	})
}

func (m *mockMultiComponentProvider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	target := strings.TrimPrefix(uri, "mockcomponent:")
	target = strings.TrimPrefix(target, "component://")
	switch target {
	case "root_config_map":
		// Simulates v0.162.0 provider.go where case innerSchemeComponent delegated
		// directly to evaluateActivePolicySet(ctx) and returned the full root map.
		if err := GenerateCollectorID(); err != nil {
			return nil, err
		}
		p := createProvider().(*provider)
		return p.evaluateActivePolicySet(ctx)
	case "global_policy_processor":
		return confmap.NewRetrieved("googlepolicy")
	case "active_destination_log_preprocess":
		return confmap.NewRetrieved([]any{"batch/gcp_bytes", "batch/gcp_items"})
	case "active_destination_exporter":
		return confmap.NewRetrieved("otlp_grpc/default_gcp_destination")
	default:
		return nil, fmt.Errorf("unknown mockcomponent target %q", target)
	}
}

func (m *mockMultiComponentProvider) Scheme() string { return "mockcomponent" }

func (m *mockMultiComponentProvider) Shutdown(context.Context) error { return nil }

// nonCanonicalBaselineProvider wraps the control-plane evaluation and strips the
// canonical workload pipelines ({logs,metrics,traces}/googlecontrolplane and
// nop/googlecontrolplane) to deterministically reproduce the v0.162.0 baseline
// when zero remote SourcePolicy entries exist.
type nonCanonicalBaselineProvider struct{}

func newNonCanonicalBaselineProviderFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(func(_ confmap.ProviderSettings) confmap.Provider {
		return &nonCanonicalBaselineProvider{}
	})
}

func (n *nonCanonicalBaselineProvider) Retrieve(ctx context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if err := GenerateCollectorID(); err != nil {
		return nil, err
	}
	p := createProvider().(*provider)
	ret, err := p.evaluateActivePolicySet(ctx)
	if err != nil {
		return nil, err
	}
	conf, err := ret.AsConf()
	if err != nil {
		return nil, err
	}
	raw := conf.ToStringMap()
	if recMap, ok := raw["receivers"].(map[string]any); ok {
		delete(recMap, "nop/googlecontrolplane")
	}
	if svcMap, ok := raw["service"].(map[string]any); ok {
		if pipesMap, ok := svcMap["pipelines"].(map[string]any); ok {
			delete(pipesMap, "logs/googlecontrolplane")
			delete(pipesMap, "metrics/googlecontrolplane")
			delete(pipesMap, "traces/googlecontrolplane")
		}
	}
	return confmap.NewRetrieved(raw)
}

func (n *nonCanonicalBaselineProvider) Scheme() string { return "baselinecp" }

func (n *nonCanonicalBaselineProvider) Shutdown(context.Context) error { return nil }

// TestBaselineComponentSchemeFailures empirically proves the architectural failures
// of the ${googlecontrolplane:component://...} escape-hatch design from go/gboc-learns-to-tango.
func TestBaselineComponentSchemeFailures(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")

	t.Run("Failure1_ComponentSchemeInProviderAtV0162OrAfterRemoval", func(t *testing.T) {
		resetGlobals()
		t.Cleanup(resetGlobals)

		// Part A: Post-change googlecontrolplaneprovider rejects component:// URIs with ErrURIInvalidInnerScheme.
		customCfgPath := filepath.Join(t.TempDir(), "custom_config.yaml")
		customYAML := `receivers:
  otlp/sep_sql:
    protocols:
      grpc:
        endpoint: localhost:0
service:
  pipelines:
    logs/custom_sql:
      receivers: [otlp/sep_sql]
      processors:
        - ${googlecontrolplane:component://global_policy_processor}
      exporters:
        - otlp_grpc/default_gcp_destination
`
		require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

		resolver, err := confmap.NewResolver(confmap.ResolverSettings{
			URIs: []string{fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath))},
			ProviderFactories: []confmap.ProviderFactory{
				NewFactory(),
				newLocalFileProviderFactory(),
			},
			DefaultScheme: "googlecontrolplane",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = resolver.Shutdown(context.Background()) })

		_, resolveErr := resolver.Resolve(context.Background())
		require.ErrorIs(t, resolveErr, ErrURIInvalidInnerScheme)
		t.Logf("Resolve failed as expected with removed component:// scheme: %v", resolveErr)

		// Part B: Deterministic v0.162.0 baseline proof — when component:// delegates to
		// evaluateActivePolicySet(ctx) and returns the root config map into processors[0],
		// ConfigProvider.Get fails unmarshaling with invalid keys on processors[0].
		v0162CfgPath := filepath.Join(t.TempDir(), "custom_config_v0162.yaml")
		v0162YAML := `receivers:
  otlp/sep_sql:
    protocols:
      grpc:
        endpoint: localhost:0
service:
  pipelines:
    logs/custom_sql:
      receivers: [otlp/sep_sql]
      processors:
        - ${mockcomponent:component://root_config_map}
      exporters:
        - otlp_grpc/default_gcp_destination
`
		require.NoError(t, os.WriteFile(v0162CfgPath, []byte(v0162YAML), 0644))

		factories, err := buildCoexistenceFactories(nil, nil)
		require.NoError(t, err)
		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{fmt.Sprintf("file:%s", filepath.ToSlash(v0162CfgPath))},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newLocalFileProviderFactory(),
					newMockMultiComponentProviderFactory(),
				},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		_, unmarshalErr := cfgProvider.Get(context.Background(), factories)
		require.Error(t, unmarshalErr, "dumping root config map into processors[0] must fail ConfigProvider.Get")
		t.Logf("Baseline v0.162.0 ConfigProvider.Get error: %v", unmarshalErr)
		assert.Contains(t, unmarshalErr.Error(), "processors[0]")
		assert.Contains(t, unmarshalErr.Error(), "has invalid keys: exporters, extensions, processors, receivers, service")
	})

	t.Run("Failure2_ConfmapCannotSpliceMultiProcessorSliceIntoSequence", func(t *testing.T) {
		customCfgPath := filepath.Join(t.TempDir(), "custom_config_multi_proc.yaml")
		customYAML := `receivers:
  otlp/sep_sql:
    protocols:
      grpc:
        endpoint: localhost:0
service:
  pipelines:
    logs/custom_sql:
      receivers: [otlp/sep_sql]
      processors:
        - ${mockcomponent:component://global_policy_processor}
        - ${mockcomponent:component://active_destination_log_preprocess}
      exporters:
        - ${mockcomponent:component://active_destination_exporter}
`
		require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

		resolver, err := confmap.NewResolver(confmap.ResolverSettings{
			URIs: []string{fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath))},
			ProviderFactories: []confmap.ProviderFactory{
				NewFactory(),
				newLocalFileProviderFactory(),
				newMockMultiComponentProviderFactory(),
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = resolver.Shutdown(context.Background()) })

		conf, err := resolver.Resolve(context.Background())
		require.NoError(t, err)

		// Prove that confmap expands ${mockcomponent:component://active_destination_log_preprocess}
		// into a nested slice inside processors: ["googlepolicy", ["batch/gcp_bytes", "batch/gcp_items"]].
		rawProcs := conf.Get("service::pipelines::logs/custom_sql::processors")
		t.Logf("Resolved processors slice with multi-processor component:// expansion: %#v", rawProcs)
		assert.Equal(t, []any{
			"googlepolicy",
			[]any{"batch/gcp_bytes", "batch/gcp_items"},
		}, rawProcs)

		factories, err := buildCoexistenceFactories(nil, nil)
		require.NoError(t, err)
		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath))},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newLocalFileProviderFactory(),
					newMockMultiComponentProviderFactory(),
				},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		_, unmarshalErr := cfgProvider.Get(context.Background(), factories)
		require.Error(t, unmarshalErr, "nested slice in processors must fail ConfigProvider.Get")
		t.Logf("ConfigProvider.Get error on multi-processor component:// slice: %v", unmarshalErr)
		assert.Contains(t, unmarshalErr.Error(), "processors[1]")
	})

	t.Run("Failure3_PerPipelineProcessorAndQueueDuplication", func(t *testing.T) {
		var googlePolicyInstances atomic.Int32
		var queueBatchInstances atomic.Int32

		factories, err := buildCoexistenceFactories(&googlePolicyInstances, &queueBatchInstances)
		require.NoError(t, err)

		// Two separate workload log pipelines (logs/policy_source + logs/custom_sql)
		// both referencing googlepolicy and queue_batch/default_gcp_destination_batch_logs.
		dupYAML := `receivers:
  nop/policy_source: {}
  nop/custom_sql: {}
processors:
  googlepolicy: {}
  queue_batch/default_gcp_destination_batch_logs: {}
exporters:
  nop/dest: {}
service:
  pipelines:
    logs/policy_source:
      receivers: [nop/policy_source]
      processors: [googlepolicy, queue_batch/default_gcp_destination_batch_logs]
      exporters: [nop/dest]
    logs/custom_sql:
      receivers: [nop/custom_sql]
      processors: [googlepolicy, queue_batch/default_gcp_destination_batch_logs]
      exporters: [nop/dest]
`
		dupCfgPath := filepath.Join(t.TempDir(), "dup_pipelines.yaml")
		require.NoError(t, os.WriteFile(dupCfgPath, []byte(dupYAML), 0644))

		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{fmt.Sprintf("file:%s", filepath.ToSlash(dupCfgPath))},
				ProviderFactories: []confmap.ProviderFactory{
					newLocalFileProviderFactory(),
				},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		cfg, err := cfgProvider.Get(context.Background(), factories)
		require.NoError(t, err)

		srv, err := service.New(context.Background(), service.Settings{
			BuildInfo:           component.NewDefaultBuildInfo(),
			ReceiversConfigs:    cfg.Receivers,
			ReceiversFactories:  factories.Receivers,
			ProcessorsConfigs:   cfg.Processors,
			ProcessorsFactories: factories.Processors,
			ExportersConfigs:    cfg.Exporters,
			ExportersFactories:  factories.Exporters,
			ConnectorsConfigs:   cfg.Connectors,
			ConnectorsFactories: factories.Connectors,
			ExtensionsConfigs:   cfg.Extensions,
			ExtensionsFactories: factories.Extensions,
			TelemetryFactory:    factories.Telemetry,
			AsyncErrorChannel:   make(chan error, 1),
		}, cfg.Service)
		require.NoError(t, err)
		require.NotNil(t, srv)
		require.NoError(t, srv.Shutdown(context.Background()))

		t.Logf("Separate pipelines created googlepolicy instances=%d, queue_batch instances=%d",
			googlePolicyInstances.Load(), queueBatchInstances.Load())
		assert.Equal(t, int32(2), googlePolicyInstances.Load(), "separate pipelines duplicate googlepolicy instances")
		assert.Equal(t, int32(2), queueBatchInstances.Load(), "separate pipelines duplicate queue_batch instances")
	})

	t.Run("BaselineWithoutCanonicalPipelinesFailsValidationOnOneLineCustomReceiver", func(t *testing.T) {
		resetGlobals()
		t.Cleanup(func() {
			resetGlobals()
			for googlepolicy.ActivePolicySet() != nil {
				googlepolicy.RollbackActivePolicySet()
			}
		})

		policiesDir := t.TempDir()
		customCfgPath := filepath.Join(t.TempDir(), "custom_config.yaml")
		customYAML := `receivers:
  otlp/sep_sql:
    protocols:
      grpc:
        endpoint: localhost:0
service:
  pipelines:
    logs/googlecontrolplane:
      receivers: [otlp/sep_sql]
`
		require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

		factories, err := buildCoexistenceFactories(nil, nil)
		require.NoError(t, err)

		// Part A: Deterministic v0.162.0 baseline proof — when canonical workload pipelines
		// are omitted on 0 remote SourcePolicy entries, a 1-line custom receiver attachment
		// fails confmap.Validate(baselineCfg) with "must have at least one exporter".
		baselineCfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("baselinecp:file://%s", filepath.ToSlash(policiesDir)),
					fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath)),
				},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newNonCanonicalBaselineProviderFactory(),
					newLocalFileProviderFactory(),
				},
				DefaultScheme: "googlecontrolplane",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = baselineCfgProvider.Shutdown(context.Background()) })

		baselineCfg, err := baselineCfgProvider.Get(context.Background(), factories)
		require.NoError(t, err)
		baselineValErr := confmap.Validate(baselineCfg)
		require.Error(t, baselineValErr, "without canonical workload pipelines, 1-line custom receiver must fail confmap.Validate")
		t.Logf("Baseline v0.162.0 (without canonical workload pipelines) confmap.Validate(cfg) error: %v", baselineValErr)
		assert.Contains(t, baselineValErr.Error(), "logs/googlecontrolplane")
		assert.Contains(t, baselineValErr.Error(), "must have at least one exporter")

		// Part B: Post-change positive proof — with canonical workload pipelines synthesized,
		// the exact same 1-line custom receiver attachment passes confmap.Validate(cfg).
		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:file://%s", filepath.ToSlash(policiesDir)),
					fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath)),
				},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newLocalFileProviderFactory(),
				},
				DefaultScheme: "googlecontrolplane",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		cfg, err := cfgProvider.Get(context.Background(), factories)
		require.NoError(t, err)
		require.NoError(t, confmap.Validate(cfg))

		pipeCfg := cfg.Service.Pipelines[pipelineMustID(pipeline.SignalLogs, "googlecontrolplane")]
		require.NotNil(t, pipeCfg)
		t.Logf("With canonical workload pipelines implemented, confmap.Validate(cfg) succeeded; receivers=%v, processors=%v, exporters=%v",
			pipeCfg.Receivers,
			pipeCfg.Processors,
			pipeCfg.Exporters,
		)
		assert.NotEmpty(t, pipeCfg.Processors)
		assert.NotEmpty(t, pipeCfg.Exporters)
	})
}

func TestCanonicalWorkloadPipelines(t *testing.T) {
	t.Setenv("FLEET_ID", "1234")

	t.Run("ZeroRemoteSourcesPlusLocalCustomReceiverMergesAndSharesSingleProcessorInstance", func(t *testing.T) {
		resetGlobals()
		t.Cleanup(func() {
			resetGlobals()
			for googlepolicy.ActivePolicySet() != nil {
				googlepolicy.RollbackActivePolicySet()
			}
		})

		policiesDir := t.TempDir()
		customCfgPath := filepath.Join(t.TempDir(), "custom_config.yaml")
		customYAML := `receivers:
  otlp/sep_sql:
    protocols:
      grpc:
        endpoint: localhost:0
service:
  pipelines:
    logs/googlecontrolplane:
      receivers: [otlp/sep_sql]
`
		require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

		var googlePolicyInstances atomic.Int32
		var queueBatchInstances atomic.Int32
		factories, err := buildCoexistenceFactories(&googlePolicyInstances, &queueBatchInstances)
		require.NoError(t, err)

		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:file://%s", filepath.ToSlash(policiesDir)),
					fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath)),
				},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newLocalFileProviderFactory(),
				},
				DefaultScheme: "googlecontrolplane",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		cfg, err := cfgProvider.Get(context.Background(), factories)
		require.NoError(t, err)
		require.NoError(t, confmap.Validate(cfg))

		logsPipe := cfg.Service.Pipelines[pipelineMustID(pipeline.SignalLogs, "googlecontrolplane")]
		require.NotNil(t, logsPipe)
		assert.Equal(t, []component.ID{
			component.MustNewIDWithName("nop", "googlecontrolplane"),
			component.MustNewIDWithName("otlp", "sep_sql"),
		}, logsPipe.Receivers)
		assert.Equal(t, []component.ID{
			component.MustNewID("googlepolicy"),
			component.MustNewIDWithName("queue_batch", "default_gcp_destination_batch_logs"),
		}, logsPipe.Processors)
		assert.Equal(t, []component.ID{
			component.MustNewIDWithName("otlp_grpc", "default_gcp_destination"),
		}, logsPipe.Exporters)

		// Self-metrics pipeline must stay isolated without googlepolicy.
		selfLogsPipe := cfg.Service.Pipelines[pipelineMustID(pipeline.SignalLogs, "default_self_metrics")]
		require.NotNil(t, selfLogsPipe)
		assert.NotContains(t, selfLogsPipe.Processors, component.MustNewID("googlepolicy"))

		// Use default no-op OTLP telemetry export config during unit-test service instantiation
		// so Shutdown does not wait for localhost:18888 without srv.Start.
		cfg.Service.Telemetry = factories.Telemetry.CreateDefaultConfig()

		srv, err := service.New(context.Background(), service.Settings{
			BuildInfo:           component.NewDefaultBuildInfo(),
			ReceiversConfigs:    cfg.Receivers,
			ReceiversFactories:  factories.Receivers,
			ProcessorsConfigs:   cfg.Processors,
			ProcessorsFactories: factories.Processors,
			ExportersConfigs:    cfg.Exporters,
			ExportersFactories:  factories.Exporters,
			ConnectorsConfigs:   cfg.Connectors,
			ConnectorsFactories: factories.Connectors,
			ExtensionsConfigs:   cfg.Extensions,
			ExtensionsFactories: factories.Extensions,
			TelemetryFactory:    factories.Telemetry,
			AsyncErrorChannel:   make(chan error, 1),
		}, cfg.Service)
		require.NoError(t, err)
		require.NotNil(t, srv)
		require.NoError(t, srv.Shutdown(context.Background()))

		// Exactly 1 googlepolicy logs processor instance is created (in logs/googlecontrolplane),
		// and 2 queue_batch logs processor instances (1 in logs/googlecontrolplane + 1 in logs/default_self_metrics).
		assert.Equal(t, int32(1), googlePolicyInstances.Load())
		assert.Equal(t, int32(2), queueBatchInstances.Load())
	})

	t.Run("MultipleRemoteSourcesAndLocalReceiverShareSingleWorkloadPipeline", func(t *testing.T) {
		resetGlobals()
		t.Cleanup(resetGlobals)

		ps := &googlepolicy.PolicySet{
			RevisionID: "rev-multi-sources",
			Policies: map[string]*googlepolicy.PolicySetEntry{
				"source_a": {PolicyObj: &mockSourcePolicy{name: "source_a"}},
				"source_b": {PolicyObj: &mockSourcePolicy{name: "source_b"}},
			},
		}
		googlepolicy.SetActivePolicySet(ps)
		t.Cleanup(func() {
			for googlepolicy.ActivePolicySet() != nil {
				googlepolicy.RollbackActivePolicySet()
			}
		})

		p := createProvider()
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

		ret, err := retrieveForTest(t, p)
		require.NoError(t, err)
		cpConf, err := ret.AsConf()
		require.NoError(t, err)

		localConf := confmap.NewFromStringMap(map[string]any{
			"receivers": map[string]any{
				"otlp/sep_sql": map[string]any{
					"protocols": map[string]any{
						"grpc": map[string]any{"endpoint": "localhost:0"},
					},
				},
			},
			"service": map[string]any{
				"pipelines": map[string]any{
					"logs/googlecontrolplane": map[string]any{
						"receivers": []any{"otlp/sep_sql", "otlp/source_a"}, // duplicate otlp/source_a must be deduplicated
					},
				},
			},
		})
		require.NoError(t, cpConf.Merge(cleanConf(localConf)))

		assert.Equal(t, []any{
			"nop/googlecontrolplane",
			"otlp/source_a",
			"otlp/source_b",
			"otlp/sep_sql",
		}, cpConf.Get("service::pipelines::logs/googlecontrolplane::receivers"))
		assert.Equal(t, []any{
			"googlepolicy",
			"queue_batch/default_gcp_destination_batch_logs",
		}, cpConf.Get("service::pipelines::logs/googlecontrolplane::processors"))
		assert.Equal(t, []any{
			"otlp_grpc/default_gcp_destination",
		}, cpConf.Get("service::pipelines::logs/googlecontrolplane::exporters"))
	})
}

type sqlQuerySEPConfig struct {
	Driver     string `mapstructure:"driver"`
	Datasource string `mapstructure:"datasource"`
	RunID      string `mapstructure:"run_id"`
	ProjectID  string `mapstructure:"project_id"`
	SourceTag  string `mapstructure:"source_tag"`
}

type sqlQuerySEPReceiver struct {
	nopComponent
	cfg      *sqlQuerySEPConfig
	nextLogs consumer.Logs
}

func (r *sqlQuerySEPReceiver) Start(ctx context.Context, _ component.Host) error {
	projectID := r.cfg.ProjectID
	if projectID == "" {
		projectID = "dashpole-dev"
	}
	sourceTag := r.cfg.SourceTag
	if sourceTag == "" {
		sourceTag = "sqlquery/sep"
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("gcp.project_id", projectID)
	rl.Resource().Attributes().PutStr("service.name", "sep-sql-poller")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(sourceTag)

	now := pcommon.NewTimestampFromTime(time.Now().UTC())

	// Record 1: DEBUG watermark/heartbeat log (should be dropped when LogFilterPolicy is active).
	debugRec := sl.LogRecords().AppendEmpty()
	debugRec.SetTimestamp(now)
	debugRec.SetObservedTimestamp(now)
	debugRec.SetSeverityText("DEBUG")
	debugRec.SetSeverityNumber(plog.SeverityNumberDebug)
	debugRec.Body().SetStr(fmt.Sprintf("sep-sql-watermark source=%s run_id=%s", sourceTag, r.cfg.RunID))
	debugRec.Attributes().PutStr("sep.run_id", r.cfg.RunID)
	debugRec.Attributes().PutStr("sep.event_type", "watermark")
	debugRec.Attributes().PutStr("sep.source", sourceTag)

	// Record 2: INFO security alert log (should be kept and exported).
	infoRec := sl.LogRecords().AppendEmpty()
	infoRec.SetTimestamp(now)
	infoRec.SetObservedTimestamp(now)
	infoRec.SetSeverityText("INFO")
	infoRec.SetSeverityNumber(plog.SeverityNumberInfo)
	infoRec.Body().SetStr(fmt.Sprintf("sep-sql-security-alert source=%s run_id=%s", sourceTag, r.cfg.RunID))
	infoRec.Attributes().PutStr("sep.run_id", r.cfg.RunID)
	infoRec.Attributes().PutStr("sep.event_type", "security_alert")
	infoRec.Attributes().PutStr("sep.source", sourceTag)

	return r.nextLogs.ConsumeLogs(ctx, ld)
}

func newSQLQuerySEPReceiverFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType("sqlquery"),
		func() component.Config {
			return &sqlQuerySEPConfig{
				Driver:    "sqlserver",
				ProjectID: "dashpole-dev",
				SourceTag: "sqlquery/sep",
			}
		},
		receiver.WithLogs(func(_ context.Context, _ receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
			return &sqlQuerySEPReceiver{
				cfg:      cfg.(*sqlQuerySEPConfig),
				nextLogs: next,
			}, nil
		}, component.StabilityLevelAlpha),
	)
}

type otlpLogsCaptureServer struct {
	plogotlp.UnimplementedGRPCServer
	srv  *grpc.Server
	mu   sync.Mutex
	logs []plog.Logs
}

func (s *otlpLogsCaptureServer) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := plog.NewLogs()
	req.Logs().CopyTo(cloned)
	s.logs = append(s.logs, cloned)
	return plogotlp.NewExportResponse(), nil
}

func (s *otlpLogsCaptureServer) Records() []plog.LogRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []plog.LogRecord
	for _, ld := range s.logs {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					rec := plog.NewLogRecord()
					sl.LogRecords().At(k).CopyTo(rec)
					out = append(out, rec)
				}
			}
		}
	}
	return out
}

func startOTLPLogsCaptureServer(t *testing.T) (*otlpLogsCaptureServer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &otlpLogsCaptureServer{srv: grpc.NewServer()}
	plogotlp.RegisterGRPCServer(s.srv, s)
	go func() { _ = s.srv.Serve(ln) }()
	t.Cleanup(func() { s.srv.GracefulStop() })
	return s, ln.Addr().String()
}

const dropDebugLogFilterPolicyJSON = `{
  "type": "log_filter",
  "id": "drop-debug-logs",
  "action": "ACTION_DROP",
  "matches": [
    {
      "target": { "record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT" },
      "equals": { "string_value": "DEBUG" }
    }
  ]
}`

const remoteSourcePolicyJSON = `{
  "type": "test_remote_source",
  "id": "remote_policy_source",
  "name": "remote_policy_source"
}`

func init() {
	googlepolicy.RegisterPolicyDriver("test_remote_source", &googlepolicy.GenericDriver[*testRemoteSourcePolicy]{})
}

type testRemoteSourcePolicy struct {
	ID   string `mapstructure:"id"`
	Name string `mapstructure:"name"`
}

func (p *testRemoteSourcePolicy) PolicyName() string                   { return p.Name }
func (p *testRemoteSourcePolicy) PolicyType() string                   { return "test_remote_source" }
func (p *testRemoteSourcePolicy) PolicyClass() googlepolicy.PolicyClass { return googlepolicy.PolicyClassSource }
func (p *testRemoteSourcePolicy) Validate() error                      { return nil }
func (p *testRemoteSourcePolicy) Evaluate(context.Context) (*confmap.Conf, error) {
	return confmap.NewFromStringMap(map[string]any{
		"receivers": map[string]any{
			"otlp/" + p.Name: map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{"endpoint": "127.0.0.1:0"},
				},
			},
		},
	}), nil
}
func (p *testRemoteSourcePolicy) LogsPipelines(preExportProcessors []component.ID, _ []component.ID, _ []component.ID) (*confmap.Conf, error) {
	procStrs := make([]any, 0, len(preExportProcessors))
	for _, id := range preExportProcessors {
		procStrs = append(procStrs, id.String())
	}
	return confmap.NewFromStringMap(map[string]any{
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs/" + p.Name: map[string]any{
					"receivers":  []any{"otlp/" + p.Name},
					"processors": procStrs,
					"exporters":  []any{"otlp_grpc/default_gcp_destination"},
				},
			},
		},
	}), nil
}
func (p *testRemoteSourcePolicy) MetricsPipelines([]component.ID, []component.ID, []component.ID) (*confmap.Conf, error) {
	return nil, nil
}
func (p *testRemoteSourcePolicy) TracesPipelines([]component.ID, []component.ID, []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

func disableInternalTelemetryExport(t *testing.T, cfg *otelcol.Config) {
	t.Helper()
	telConf := confmap.NewFromStringMap(map[string]any{
		"logs": map[string]any{
			"processors": []any{},
		},
		"metrics": map[string]any{
			"level":   "none",
			"readers": []any{},
		},
	})
	require.NoError(t, telConf.Unmarshal(&cfg.Service.Telemetry))
}

func startLocalMockMetadataServer(t *testing.T, tokenJSON []byte) {
	t.Helper()
	metaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Metadata-Flavor", "Google")
		switch {
		case strings.Contains(r.URL.Path, "/service-accounts/") && strings.HasSuffix(r.URL.Path, "/token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(bytes.TrimSpace(tokenJSON))
		case strings.Contains(r.URL.Path, "/service-accounts/") && strings.HasSuffix(r.URL.Path, "/scopes"):
			_, _ = w.Write([]byte("https://www.googleapis.com/auth/cloud-platform\nhttps://www.googleapis.com/auth/logging.write\n"))
		case strings.Contains(r.URL.Path, "/service-accounts/"):
			_, _ = w.Write([]byte("default/\n"))
		case strings.HasSuffix(r.URL.Path, "/project/project-id"):
			_, _ = w.Write([]byte("dashpole-dev"))
		case strings.HasSuffix(r.URL.Path, "/project/numeric-project-id"):
			_, _ = w.Write([]byte("1030812186551"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(metaSrv.Close)
	emptyDir := t.TempDir()
	t.Setenv("CLOUDSDK_CONFIG", emptyDir)
	t.Setenv("HOME", emptyDir)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(metaSrv.URL, "http://"))
}

// TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline verifies:
// - Invariant 1 (Merge & Deduplication)
// - Invariant 2 (Single Shared googlepolicy & queue_batch Processor Instance)
// - Invariant 3 (Receiver-Specific Pre-Processing via forward Connector: logs/sep_pre -> forward/sep -> logs/googlecontrolplane)
func TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline(t *testing.T) {
	resetGlobals()
	t.Cleanup(func() {
		resetGlobals()
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})
	t.Setenv("FLEET_ID", "1234")
	startLocalMockMetadataServer(t, []byte(`{"access_token":"mock-token","expires_in":3600,"token_type":"Bearer"}`))

	policiesDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(policiesDir, "01-drop-debug.json"), []byte(dropDebugLogFilterPolicyJSON), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(policiesDir, "02-remote-source.json"), []byte(remoteSourcePolicyJSON), 0644))

	captureSrv, captureAddr := startOTLPLogsCaptureServer(t)
	runID := uuid.NewString()

	customCfgPath := filepath.Join(t.TempDir(), "custom_config_forward.yaml")
	customYAML := fmt.Sprintf(`receivers:
  sqlquery/sep:
    driver: sqlserver
    datasource: "sqlserver://sa:pass@localhost:1433?database=sem5"
    run_id: %[1]q
    source_tag: "sqlquery/sep"
  sqlquery/direct_app:
    driver: postgres
    datasource: "postgres://localhost:5432/app"
    run_id: %[1]q
    source_tag: "sqlquery/direct_app"
  otlp/default_self_metrics:
    protocols:
      grpc:
        endpoint: 127.0.0.1:0

processors:
  transform/sep_only: {}

connectors:
  forward/sep: {}

exporters:
  otlp_grpc/default_gcp_destination:
    endpoint: %[2]s
    auth:
      enabled: false
    tls:
      insecure: true

service:
  pipelines:
    logs/sep_pre:
      receivers: [sqlquery/sep]
      processors: [transform/sep_only]
      exporters: [forward/sep]
    logs/googlecontrolplane:
      receivers: [sqlquery/direct_app, forward/sep]
`, runID, captureAddr)
	require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

	var googlePolicyInstances atomic.Int32
	var queueBatchInstances atomic.Int32
	factories, err := buildCoexistenceFactories(&googlePolicyInstances, &queueBatchInstances, newSQLQuerySEPReceiverFactory())
	require.NoError(t, err)

	cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
		ResolverSettings: confmap.ResolverSettings{
			URIs: []string{
				fmt.Sprintf("googlecontrolplane:file://%s?project=dashpole-dev", filepath.ToSlash(policiesDir)),
				fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath)),
			},
			ProviderFactories: []confmap.ProviderFactory{
				NewFactory(),
				newLocalFileProviderFactory(),
			},
			DefaultScheme: "googlecontrolplane",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

	cfg, err := cfgProvider.Get(context.Background(), factories)
	require.NoError(t, err)
	require.NoError(t, confmap.Validate(cfg))

	disableInternalTelemetryExport(t, cfg)

	logsPipe := cfg.Service.Pipelines[pipelineMustID(pipeline.SignalLogs, "googlecontrolplane")]
	require.NotNil(t, logsPipe)
	assert.Equal(t, []component.ID{
		component.MustNewIDWithName("nop", "googlecontrolplane"),
		component.MustNewIDWithName("otlp", "remote_policy_source"),
		component.MustNewIDWithName("sqlquery", "direct_app"),
		component.MustNewIDWithName("forward", "sep"),
	}, logsPipe.Receivers)

	srv, err := service.New(context.Background(), service.Settings{
		BuildInfo:           component.NewDefaultBuildInfo(),
		ReceiversConfigs:    cfg.Receivers,
		ReceiversFactories:  factories.Receivers,
		ProcessorsConfigs:   cfg.Processors,
		ProcessorsFactories: factories.Processors,
		ExportersConfigs:    cfg.Exporters,
		ExportersFactories:  factories.Exporters,
		ConnectorsConfigs:   cfg.Connectors,
		ConnectorsFactories: factories.Connectors,
		ExtensionsConfigs:   cfg.Extensions,
		ExtensionsFactories: factories.Extensions,
		TelemetryFactory:    factories.Telemetry,
		AsyncErrorChannel:   make(chan error, 1),
	}, cfg.Service)
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))

	require.Eventually(t, func() bool {
		return len(captureSrv.Records()) >= 2
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, srv.Shutdown(context.Background()))

	// Invariant 2: Only 1 googlepolicy logs processor instance is created (in logs/googlecontrolplane),
	// even though logs/sep_pre and logs/googlecontrolplane both process logs!
	assert.Equal(t, int32(1), googlePolicyInstances.Load(), "must create exactly 1 googlepolicy logs processor instance")
	assert.Equal(t, int32(2), queueBatchInstances.Load(), "must create 1 queue_batch in logs/googlecontrolplane + 1 in logs/default_self_metrics")

	// Invariant 3: Both DEBUG logs (from sqlquery/sep and sqlquery/direct_app) were dropped by googlepolicy;
	// only the sqlquery/sep INFO log has sep.preprocessed == "true", while sqlquery/direct_app INFO log does not.
	records := captureSrv.Records()
	require.Len(t, records, 2, "expected 2 INFO logs and 0 DEBUG logs")
	bySource := map[string]plog.LogRecord{}
	for _, rec := range records {
		assert.Equal(t, "INFO", rec.SeverityText(), "all DEBUG logs must be dropped by LogFilterPolicy")
		srcVal, ok := rec.Attributes().Get("sep.source")
		require.True(t, ok)
		bySource[srcVal.Str()] = rec
	}

	sepRec, ok := bySource["sqlquery/sep"]
	require.True(t, ok, "expected INFO log from sqlquery/sep")
	preVal, exists := sepRec.Attributes().Get("sep.preprocessed")
	require.True(t, exists, "sqlquery/sep log routed via logs/sep_pre -> forward/sep must have sep.preprocessed attribute")
	assert.Equal(t, "true", preVal.Str())

	directRec, ok := bySource["sqlquery/direct_app"]
	require.True(t, ok, "expected INFO log from sqlquery/direct_app")
	_, directHasPre := directRec.Attributes().Get("sep.preprocessed")
	assert.False(t, directHasPre, "sqlquery/direct_app log attached directly to logs/googlecontrolplane must NOT be mutated by logs/sep_pre")
}

func setupLiveGCPAuth(t *testing.T) string {
	t.Helper()
	realHome := os.Getenv("HOME")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch a real GCE VM service account token in dashpole-dev to serve via local GCE_METADATA_HOST
	// so googleclientauth/default_gcp_destination authenticates against live telemetry.googleapis.com:443.
	vmTokenCmd := exec.CommandContext(ctx, "gcloud", "compute", "ssh",
		"gke-obi-demo-cluster-default-pool-3f37b07e-8j8w",
		"--zone=us-central1-a",
		"--project=dashpole-dev",
		"--command=curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
	)
	vmTokenBytes, err := vmTokenCmd.Output()
	require.NoError(t, err, "failed to fetch GCE VM service account token from dashpole-dev")

	startLocalMockMetadataServer(t, vmTokenBytes)
	return realHome
}

type cloudLoggingEntry struct {
	InsertID    string `json:"insertId"`
	LogName     string `json:"logName"`
	Severity    string `json:"severity"`
	TextPayload string `json:"textPayload"`
	Timestamp   string `json:"timestamp"`
}

func queryCloudLoggingByRunID(t *testing.T, realHome, projectID, runID string, since time.Time) []cloudLoggingEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	filter := fmt.Sprintf("timestamp >= %q AND %q", since.Add(-2*time.Minute).UTC().Format(time.RFC3339), runID)
	cmd := exec.CommandContext(ctx, "gcloud", "logging", "read", filter,
		"--project="+projectID,
		"--limit=20",
		"--format=json",
	)
	var cleanEnv []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "CLOUDSDK_CONFIG=") || strings.HasPrefix(kv, "GCE_METADATA_HOST=") || strings.HasPrefix(kv, "GOOGLE_APPLICATION_CREDENTIALS=") {
			continue
		}
		cleanEnv = append(cleanEnv, kv)
	}
	cleanEnv = append(cleanEnv, "HOME="+realHome)
	cmd.Env = cleanEnv

	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "gcloud logging read failed: %s", string(out))

	var entries []cloudLoggingEntry
	require.NoError(t, json.Unmarshal(out, &entries))
	return entries
}

// TestEndToEndCustomConfigCoexistence_LiveGCP verifies:
// - Invariant 1 (Merge & Deduplication)
// - Invariant 2 (Single Shared googlepolicy & queue_batch Processor Instance)
// - Invariant 4 (Live telemetry.googleapis.com:443 Export in dashpole-dev + Negative Control)
func TestEndToEndCustomConfigCoexistence_LiveGCP(t *testing.T) {
	queryToken := setupLiveGCPAuth(t)
	t.Setenv("FLEET_ID", "1234")
	startTime := time.Now().UTC()

	runCollectorToLiveGCP := func(t *testing.T, includeDropDebugPolicy bool) string {
		t.Helper()
		resetGlobals()
		t.Cleanup(func() {
			resetGlobals()
			for googlepolicy.ActivePolicySet() != nil {
				googlepolicy.RollbackActivePolicySet()
			}
		})

		policiesDir := t.TempDir()
		if includeDropDebugPolicy {
			require.NoError(t, os.WriteFile(filepath.Join(policiesDir, "01-drop-debug.json"), []byte(dropDebugLogFilterPolicyJSON), 0644))
		}

		runID := uuid.NewString()
		customCfgPath := filepath.Join(t.TempDir(), "custom_config_live_gcp.yaml")
		customYAML := fmt.Sprintf(`receivers:
  sqlquery/sep:
    driver: sqlserver
    datasource: "sqlserver://sa:pass@localhost:1433?database=sem5"
    run_id: %[1]q
    project_id: "dashpole-dev"
    source_tag: "sqlquery/sep"
  otlp/default_self_metrics:
    protocols:
      grpc:
        endpoint: 127.0.0.1:0

service:
  pipelines:
    logs/googlecontrolplane:
      receivers: [sqlquery/sep]
`, runID)
		require.NoError(t, os.WriteFile(customCfgPath, []byte(customYAML), 0644))

		var googlePolicyInstances atomic.Int32
		var queueBatchInstances atomic.Int32
		factories, err := buildCoexistenceFactories(&googlePolicyInstances, &queueBatchInstances, newSQLQuerySEPReceiverFactory())
		require.NoError(t, err)

		cfgProvider, err := otelcol.NewConfigProvider(otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:file://%s?project=dashpole-dev", filepath.ToSlash(policiesDir)),
					fmt.Sprintf("file:%s", filepath.ToSlash(customCfgPath)),
				},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					newLocalFileProviderFactory(),
				},
				DefaultScheme: "googlecontrolplane",
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cfgProvider.Shutdown(context.Background()) })

		cfg, err := cfgProvider.Get(context.Background(), factories)
		require.NoError(t, err)
		require.NoError(t, confmap.Validate(cfg))

		// Verify Invariant 1: logs/googlecontrolplane merged and deduplicated with live GCP destination.
		logsPipe := cfg.Service.Pipelines[pipelineMustID(pipeline.SignalLogs, "googlecontrolplane")]
		require.NotNil(t, logsPipe)
		assert.Equal(t, []component.ID{
			component.MustNewIDWithName("nop", "googlecontrolplane"),
			component.MustNewIDWithName("sqlquery", "sep"),
		}, logsPipe.Receivers)
		assert.Equal(t, []component.ID{
			component.MustNewID("googlepolicy"),
			component.MustNewIDWithName("queue_batch", "default_gcp_destination_batch_logs"),
		}, logsPipe.Processors)
		assert.Equal(t, []component.ID{
			component.MustNewIDWithName("otlp_grpc", "default_gcp_destination"),
		}, logsPipe.Exporters)

		otlpExpCfg := cfg.Exporters[component.MustNewIDWithName("otlp_grpc", "default_gcp_destination")].(*otlpexporter.Config)
		assert.Equal(t, "telemetry.googleapis.com:443", otlpExpCfg.ClientConfig.Endpoint)

		disableInternalTelemetryExport(t, cfg)

		srv, err := service.New(context.Background(), service.Settings{
			BuildInfo:           component.NewDefaultBuildInfo(),
			ReceiversConfigs:    cfg.Receivers,
			ReceiversFactories:  factories.Receivers,
			ProcessorsConfigs:   cfg.Processors,
			ProcessorsFactories: factories.Processors,
			ExportersConfigs:    cfg.Exporters,
			ExportersFactories:  factories.Exporters,
			ConnectorsConfigs:   cfg.Connectors,
			ConnectorsFactories: factories.Connectors,
			ExtensionsConfigs:   cfg.Extensions,
			ExtensionsFactories: factories.Extensions,
			TelemetryFactory:    factories.Telemetry,
			AsyncErrorChannel:   make(chan error, 1),
		}, cfg.Service)
		require.NoError(t, err)
		require.NoError(t, srv.Start(context.Background()))
		// Wait for queue_batch (200ms flush_timeout) to flush to live telemetry.googleapis.com:443.
		time.Sleep(1500 * time.Millisecond)
		require.NoError(t, srv.Shutdown(context.Background()))

		// Verify Invariant 2: single shared googlepolicy logs processor instance.
		assert.Equal(t, int32(1), googlePolicyInstances.Load())
		assert.Equal(t, int32(2), queueBatchInstances.Load())

		return runID
	}

	changeRunID := runCollectorToLiveGCP(t, true)
	baselineRunID := runCollectorToLiveGCP(t, false)
	t.Logf("Exported to live telemetry.googleapis.com:443 in dashpole-dev: changeRunID=%s (with LogFilterPolicy), baselineRunID=%s (negative control without LogFilterPolicy)", changeRunID, baselineRunID)

	var changeEntries, baselineEntries []cloudLoggingEntry
	require.Eventually(t, func() bool {
		changeEntries = queryCloudLoggingByRunID(t, queryToken, "dashpole-dev", changeRunID, startTime)
		baselineEntries = queryCloudLoggingByRunID(t, queryToken, "dashpole-dev", baselineRunID, startTime)
		return len(changeEntries) >= 1 && len(baselineEntries) >= 2
	}, 45*time.Second, 2*time.Second, "timed out waiting for logs to appear in Cloud Logging in dashpole-dev")

	var changeInfoCount, changeDebugCount int
	for _, e := range changeEntries {
		t.Logf("[Change / With LogFilterPolicy] Cloud Logging entry: insertId=%s severity=%s textPayload=%q timestamp=%s", e.InsertID, e.Severity, e.TextPayload, e.Timestamp)
		switch e.Severity {
		case "INFO":
			changeInfoCount++
		case "DEBUG":
			changeDebugCount++
		}
	}
	assert.Equal(t, 1, changeInfoCount, "with LogFilterPolicy active, expected 1 INFO log from sqlquery/sep in dashpole-dev")
	assert.Equal(t, 0, changeDebugCount, "with LogFilterPolicy active, expected 0 DEBUG logs from sqlquery/sep in dashpole-dev")

	var baselineInfoCount, baselineDebugCount int
	for _, e := range baselineEntries {
		t.Logf("[Baseline / Negative Control Without LogFilterPolicy] Cloud Logging entry: insertId=%s severity=%s textPayload=%q timestamp=%s", e.InsertID, e.Severity, e.TextPayload, e.Timestamp)
		switch e.Severity {
		case "INFO":
			baselineInfoCount++
		case "DEBUG":
			baselineDebugCount++
		}
	}
	assert.Equal(t, 1, baselineInfoCount, "negative control without LogFilterPolicy: expected 1 INFO log in dashpole-dev")
	assert.Equal(t, 1, baselineDebugCount, "negative control without LogFilterPolicy: expected 1 DEBUG log in dashpole-dev")
}


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
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// metricSample records a single scraped OTLP datapoint from SelfMetricsPolicy.
type metricSample struct {
	IntVal         int64
	DoubleVal      float64
	StartTimestamp pcommon.Timestamp
	Timestamp      pcommon.Timestamp
	Attributes     map[string]string
}

type telemetryCaptureSink struct {
	mu          sync.RWMutex
	logBodies   []string
	metricPoints map[string][]metricSample
}

func newTelemetryCaptureSink() *telemetryCaptureSink {
	return &telemetryCaptureSink{
		metricPoints: make(map[string][]metricSample),
	}
}

func (s *telemetryCaptureSink) consumeLogs(_ context.Context, ld plog.Logs) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				s.logBodies = append(s.logBodies, sl.LogRecords().At(k).Body().AsString())
			}
		}
	}
	return nil
}

func (s *telemetryCaptureSink) consumeMetrics(_ context.Context, md pmetric.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				name := m.Name()
				switch m.Type() {
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						s.recordNumberDataPoint(name, dp)
					}
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						s.recordNumberDataPoint(name, dp)
					}
				}
			}
		}
	}
	return nil
}

func (s *telemetryCaptureSink) recordNumberDataPoint(name string, dp pmetric.NumberDataPoint) {
	attrs := make(map[string]string, dp.Attributes().Len())
	dp.Attributes().Range(func(k string, v pcommon.Value) bool {
		attrs[k] = v.AsString()
		return true
	})
	sample := metricSample{
		StartTimestamp: dp.StartTimestamp(),
		Timestamp:      dp.Timestamp(),
		Attributes:     attrs,
	}
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		sample.IntVal = dp.IntValue()
		sample.DoubleVal = float64(dp.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		sample.DoubleVal = dp.DoubleValue()
		sample.IntVal = int64(dp.DoubleValue())
	}
	s.metricPoints[name] = append(s.metricPoints[name], sample)
}

func (s *telemetryCaptureSink) latestMetric(name string, matchAttrs map[string]string) (metricSample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pts := s.metricPoints[name]
	for i := len(pts) - 1; i >= 0; i-- {
		pt := pts[i]
		matched := true
		for k, v := range matchAttrs {
			if pt.Attributes[k] != v {
				matched = false
				break
			}
		}
		if matched {
			return pt, true
		}
	}
	return metricSample{}, false
}

func (s *telemetryCaptureSink) exportedLogs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.logBodies)
}

// captureDestinationPolicy directs all customer and self-metrics pipelines to
// the in-process capture exporter ("capture/test_dest").
type captureDestinationPolicy struct {
	name string
}

func (p *captureDestinationPolicy) PolicyName() string { return p.name }
func (p *captureDestinationPolicy) PolicyType() string { return "capture_destination" }
func (p *captureDestinationPolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassDestination
}
func (p *captureDestinationPolicy) Validate() error { return nil }
func (p *captureDestinationPolicy) Evaluate(context.Context) (*confmap.Conf, error) {
	return confmap.NewFromStringMap(map[string]any{
		"exporters": map[string]any{
			"capture/" + p.name: map[string]any{},
		},
	}), nil
}
func (p *captureDestinationPolicy) ExporterIDs() []component.ID {
	expType, _ := component.NewType("capture")
	return []component.ID{component.NewIDWithName(expType, p.name)}
}
func (p *captureDestinationPolicy) PreProcessMetricIDs() []component.ID { return nil }
func (p *captureDestinationPolicy) PreProcessLogIDs() []component.ID    { return nil }
func (p *captureDestinationPolicy) PreProcessTraceIDs() []component.ID  { return nil }
func (p *captureDestinationPolicy) ExtensionIDs() []component.ID        { return nil }

// otlpLogSourcePolicy configures an OTLP gRPC receiver on 127.0.0.1:<port> and
// wires it into service::pipelines::<pipelineName>.
//   - When pipelineName == "logs", multiple otlpLogSourcePolicy instances merge
//     their receiver IDs into the shared service::pipelines::logs::receivers list
//     via confmap.enableMergeAppendOption.
//   - When pipelineName == "logs/" + name, each policy creates a distinct
//     pipeline key in service::pipelines.
type otlpLogSourcePolicy struct {
	name         string
	port         int
	pipelineName string
}

func (p *otlpLogSourcePolicy) PolicyName() string { return p.name }
func (p *otlpLogSourcePolicy) PolicyType() string { return "otlp_log_source" }
func (p *otlpLogSourcePolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassSource
}
func (p *otlpLogSourcePolicy) Validate() error { return nil }
func (p *otlpLogSourcePolicy) Evaluate(context.Context) (*confmap.Conf, error) {
	return confmap.NewFromStringMap(map[string]any{
		"receivers": map[string]any{
			"otlp/" + p.name: map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", p.port),
					},
				},
			},
		},
	}), nil
}
func (p *otlpLogSourcePolicy) LogsPipelines(preExportProcessors []component.ID, exporters []component.ID, _ []component.ID) (*confmap.Conf, error) {
	procStrs := make([]any, 0, len(preExportProcessors))
	for _, id := range preExportProcessors {
		procStrs = append(procStrs, id.String())
	}
	expStrs := make([]any, 0, len(exporters))
	for _, id := range exporters {
		expStrs = append(expStrs, id.String())
	}
	pipeKey := p.pipelineName
	if pipeKey == "" {
		pipeKey = "logs"
	}
	return confmap.NewFromStringMap(map[string]any{
		"service": map[string]any{
			"pipelines": map[string]any{
				pipeKey: map[string]any{
					"receivers":  []any{"otlp/" + p.name},
					"processors": procStrs,
					"exporters":  expStrs,
				},
			},
		},
	}), nil
}
func (p *otlpLogSourcePolicy) MetricsPipelines([]component.ID, []component.ID, []component.ID) (*confmap.Conf, error) {
	return nil, nil
}
func (p *otlpLogSourcePolicy) TracesPipelines([]component.ID, []component.ID, []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

// selfMetricsLoopbackProxy forwards OTLP gRPC metric exports from the Collector's
// internal PeriodicMetricReader to BuiltInSelfMetricsPolicy's otlp/default_self_metrics
// receiver (127.0.0.1:<selfMetricsPort>) while the receiver is running.
//
// During a full Service.Shutdown(ctx), upstream service.go:300 calls
// Pipelines.ShutdownAll (stopping otlp/default_self_metrics) BEFORE calling
// meterProvider.Shutdown (service.go:315). Without absorbing the retiring flush's
// connection-refused error, col.service.Shutdown(ctx) returns an error and aborts
// col.reloadConfiguration(ctx). Using this proxy in TestReloadContinuity_* allows
// full reloads to complete so we can measure post-restart counter and uptime resets,
// while TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown tests the unproxied
// loopback failure directly.
type selfMetricsLoopbackProxy struct {
	pmetricotlp.UnimplementedGRPCServer
	targetAddr string
}

func (p *selfMetricsLoopbackProxy) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	conn, err := grpc.NewClient(p.targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return pmetricotlp.NewExportResponse(), nil
	}
	defer conn.Close()
	resp, err := pmetricotlp.NewGRPCClient(conn).Export(ctx, req)
	if err != nil {
		return pmetricotlp.NewExportResponse(), nil
	}
	return resp, nil
}

func startSelfMetricsLoopbackProxy(t *testing.T, selfMetricsPort int) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	pmetricotlp.RegisterGRPCServer(srv, &selfMetricsLoopbackProxy{
		targetAddr: fmt.Sprintf("127.0.0.1:%d", selfMetricsPort),
	})
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().(*net.TCPAddr).Port
}

// staticOverrideProvider supplies a fast 100ms PeriodicMetricReader interval for
// SelfMetricsPolicy and disables internal Collector log shipping into the test
// log exporter so otelcol_exporter_sent_log_records reflects customer logs only.
type staticOverrideProvider struct {
	readerEndpointPort int
}

func (s *staticOverrideProvider) Retrieve(context.Context, string, confmap.WatcherFunc) (*confmap.Retrieved, error) {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", s.readerEndpointPort)
	return confmap.NewRetrieved(map[string]any{
		"service": map[string]any{
			"telemetry": map[string]any{
				"logs": map[string]any{
					"processors": []any{},
				},
				"metrics": map[string]any{
					"level": "Normal",
					"readers": []any{
						map[string]any{
							"periodic": map[string]any{
								"interval": 100,
								"timeout":  150,
								"exporter": map[string]any{
									"otlp": map[string]any{
										"endpoint": endpoint,
										"protocol": "grpc",
										"insecure": true,
										"timeout":  150,
									},
								},
							},
						},
					},
				},
			},
		},
	})
}
func (s *staticOverrideProvider) Scheme() string                   { return "testoverride" }
func (s *staticOverrideProvider) Shutdown(context.Context) error { return nil }

type nopComponentConfig struct {
	Raw map[string]any `mapstructure:",remain"`
}

type nopExtension struct{}

func (nopExtension) Start(context.Context, component.Host) error { return nil }
func (nopExtension) Shutdown(context.Context) error              { return nil }

func newPassThroughProcessorFactory(typeStr string) processor.Factory {
	pType, _ := component.NewType(typeStr)
	return processor.NewFactory(
		pType,
		func() component.Config { return &nopComponentConfig{} },
		processor.WithLogs(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
			return processorhelper.NewLogs(ctx, set, cfg, next, func(_ context.Context, ld plog.Logs) (plog.Logs, error) {
				return ld, nil
			})
		}, component.StabilityLevelStable),
		processor.WithMetrics(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
			return processorhelper.NewMetrics(ctx, set, cfg, next, func(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
				return md, nil
			})
		}, component.StabilityLevelStable),
	)
}

func newCaptureExporterFactory(sink *telemetryCaptureSink) exporter.Factory {
	expType, _ := component.NewType("capture")
	return exporter.NewFactory(
		expType,
		func() component.Config { return &nopComponentConfig{} },
		exporter.WithLogs(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
			return exporterhelper.NewLogs(ctx, set, cfg, sink.consumeLogs)
		}, component.StabilityLevelStable),
		exporter.WithMetrics(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
			return exporterhelper.NewMetrics(ctx, set, cfg, sink.consumeMetrics)
		}, component.StabilityLevelStable),
	)
}

func newTestCollectorFactories(sink *telemetryCaptureSink) func() (otelcol.Factories, error) {
	return func() (otelcol.Factories, error) {
		otlpRecvFactory := otlpreceiver.NewFactory()
		gpProcFactory := googlepolicyprocessor.NewFactory()
		rdProcFactory := newPassThroughProcessorFactory("resourcedetection")
		tfProcFactory := newPassThroughProcessorFactory("transform")
		capExpFactory := newCaptureExporterFactory(sink)

		cpExtType, _ := component.NewType("googlecontrolplane")
		cpExtFactory := extension.NewFactory(
			cpExtType,
			func() component.Config { return &nopComponentConfig{} },
			func(context.Context, extension.Settings, component.Config) (extension.Extension, error) {
				return nopExtension{}, nil
			},
			component.StabilityLevelStable,
		)

		return otelcol.Factories{
			Receivers: map[component.Type]receiver.Factory{
				otlpRecvFactory.Type(): otlpRecvFactory,
			},
			Processors: map[component.Type]processor.Factory{
				gpProcFactory.Type(): gpProcFactory,
				rdProcFactory.Type(): rdProcFactory,
				tfProcFactory.Type(): tfProcFactory,
			},
			Exporters: map[component.Type]exporter.Factory{
				capExpFactory.Type(): capExpFactory,
			},
			Extensions: map[component.Type]extension.Factory{
				cpExtFactory.Type(): cpExtFactory,
			},
			Connectors: map[component.Type]connector.Factory{},
			Telemetry:  otelconftelemetry.NewFactory(),
		}, nil
	}
}

func sendOTLPLogs(t *testing.T, port int, bodies []string) {
	t.Helper()
	conn, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := plogotlp.NewGRPCClient(conn)
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, b := range bodies {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(b)
	}

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, rpcErr := client.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
		return rpcErr == nil
	}, 3*time.Second, 25*time.Millisecond)
}

func mustLoadLogFilter(t *testing.T, id, dropBodyRegex string) googlepolicy.Policy {
	t.Helper()
	pol, err := googlepolicy.LoadPolicy("log_filter", map[string]any{
		"type":   "log_filter",
		"id":     id,
		"action": "ACTION_DROP",
		"matches": []any{
			map[string]any{
				"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
				"regex":  dropBodyRegex,
			},
		},
	})
	require.NoError(t, err)
	return pol
}

func TestReloadContinuity_Baseline_ResetsCountersAndUptime(t *testing.T) {
	withFakeGCEMetadata(t)
	t.Setenv("FLEET_ID", "fleet-tab3-baseline")

	origSelfMetricsPort := BuiltInSelfMetricsPolicy.Port
	selfMetricsPort := freeLocalPort(t)
	BuiltInSelfMetricsPolicy.Port = selfMetricsPort
	t.Cleanup(func() {
		BuiltInSelfMetricsPolicy.Port = origSelfMetricsPort
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	sourceAPort1 := freeLocalPort(t)
	sourceAPort2 := freeLocalPort(t)
	destPolicy := &captureDestinationPolicy{name: "test_dest"}
	sourceA1 := &otlpLogSourcePolicy{name: "source_a", port: sourceAPort1, pipelineName: "logs"}

	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-baseline-0-initial",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName(): {PolicyObj: destPolicy},
			sourceA1.PolicyName():   {PolicyObj: sourceA1},
		},
	})

	sink := newTelemetryCaptureSink()
	core, recordedLogs := observer.New(zapcore.InfoLevel)

	// Baseline: unconditional WatcherFunc (diffGateReloads = false) AND upstream
	// partial reload feature gates disabled (v0.162.0 defaults).
	baselineProviderFactory := confmap.NewProviderFactory(func(set confmap.ProviderSettings) confmap.Provider {
		p := newProvider(set).(*provider)
		p.diffGateReloads = false
		_ = featuregate.GlobalRegistry().Set(partialReloadFeatureGateID, false)
		_ = featuregate.GlobalRegistry().Set(partialReloadReceiversGateID, false)
		return p
	})
	proxyPort := startSelfMetricsLoopbackProxy(t, selfMetricsPort)
	overrideFactory := confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &staticOverrideProvider{readerEndpointPort: proxyPort}
	})

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo:               component.NewDefaultBuildInfo(),
		Factories:               newTestCollectorFactories(sink),
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
		LoggingOptions: []zap.Option{
			zap.WrapCore(func(c zapcore.Core) zapcore.Core {
				return zapcore.NewTee(c, core)
			}),
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					"googlecontrolplane:component:baseline",
					"testoverride:fast_metrics",
				},
				ProviderFactories: []confmap.ProviderFactory{
					baselineProviderFactory,
					overrideFactory,
				},
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	var runErr error
	go func() {
		defer close(runDone)
		runErr = col.Run(ctx)
	}()
	t.Cleanup(func() {
		col.Shutdown()
		<-runDone
		_ = runErr
	})

	require.Eventually(t, func() bool {
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 20*time.Millisecond)

	// Initial traffic: send 5 logs to otlp/source_a.
	sendOTLPLogs(t, sourceAPort1, []string{"log-1", "log-2", "log-3", "log-4", "log-5"})

	var expSentBefore, recvAcceptedBefore, uptimeBefore metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3 bool
		expSentBefore, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvAcceptedBefore, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		uptimeBefore, ok3 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 && ok3 && expSentBefore.IntVal == 5 && recvAcceptedBefore.IntVal == 5 && uptimeBefore.DoubleVal >= 0.25
	}, 5*time.Second, 25*time.Millisecond)

	t.Logf("[Baseline Step 0 - Initial] exporter_sent=%d (start=%d), receiver_accepted(source_a)=%d, process_uptime=%.3fs (start=%d)",
		expSentBefore.IntVal, expSentBefore.StartTimestamp, recvAcceptedBefore.IntVal, uptimeBefore.DoubleVal, uptimeBefore.StartTimestamp)

	// Step 1 (Baseline): Roll out a LogFilterPolicy update ("DROP_ME").
	// Because diffGateReloads=false and partial reload is disabled, this triggers a full Service restart!
	filterPolicy := mustLoadLogFilter(t, "drop-filter", "DROP_ME")
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-baseline-1-log-filter",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA1.PolicyName():     {PolicyObj: sourceA1},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Config updated, restart service").All()) >= 1 &&
			len(recordedLogs.FilterMessage("Everything is ready. Begin running and processing data.").All()) >= 2
	}, 5*time.Second, 20*time.Millisecond)

	// Send 3 kept logs + 2 dropped logs after the LogFilterPolicy rollout.
	sendOTLPLogs(t, sourceAPort1, []string{"keep-1", "DROP_ME-1", "keep-2", "DROP_ME-2", "keep-3"})

	var expSentAfterFilter, recvAcceptedAfterFilter, uptimeAfterFilter metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3 bool
		expSentAfterFilter, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvAcceptedAfterFilter, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		uptimeAfterFilter, ok3 = sink.latestMetric("otelcol_process_uptime", nil)
		// Because MeterProvider was recreated on full reload, StartTimestamp increased and
		// exporter_sent reset from 5 to 3 (instead of 5 + 3 = 8)!
		return ok1 && ok2 && ok3 &&
			expSentAfterFilter.StartTimestamp > expSentBefore.StartTimestamp &&
			expSentAfterFilter.IntVal == 3 &&
			recvAcceptedAfterFilter.StartTimestamp > recvAcceptedBefore.StartTimestamp &&
			recvAcceptedAfterFilter.IntVal == 5 &&
			uptimeAfterFilter.StartTimestamp > uptimeBefore.StartTimestamp
	}, 5*time.Second, 25*time.Millisecond)

	t.Logf("[Baseline Step 1 - After LogFilterPolicy Rollout] full_restarts=%d, exporter_sent=%d (RESET from 5 to 3; expected 8; start=%d > %d), receiver_accepted(source_a)=%d (RESET from 5 to 5; expected 10), process_uptime=%.3fs (RESET start=%d > %d)",
		len(recordedLogs.FilterMessage("Config updated, restart service").All()),
		expSentAfterFilter.IntVal, expSentAfterFilter.StartTimestamp, expSentBefore.StartTimestamp,
		recvAcceptedAfterFilter.IntVal,
		uptimeAfterFilter.DoubleVal, uptimeAfterFilter.StartTimestamp, uptimeBefore.StartTimestamp)

	// Step 2 (Baseline): Roll out a SourcePolicy update (changing source_a port).
	// Triggers a second full Service restart and resets counters again!
	sourceA2 := &otlpLogSourcePolicy{name: "source_a", port: sourceAPort2, pipelineName: "logs"}
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-baseline-2-source-update",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA2.PolicyName():     {PolicyObj: sourceA2},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Config updated, restart service").All()) >= 2 &&
			len(recordedLogs.FilterMessage("Everything is ready. Begin running and processing data.").All()) >= 3
	}, 5*time.Second, 20*time.Millisecond)

	sendOTLPLogs(t, sourceAPort2, []string{"after-source-1", "after-source-2"})

	var expSentAfterSource, uptimeAfterSource metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2 bool
		expSentAfterSource, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		uptimeAfterSource, ok2 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 &&
			expSentAfterSource.StartTimestamp > expSentAfterFilter.StartTimestamp &&
			expSentAfterSource.IntVal == 2 &&
			uptimeAfterSource.StartTimestamp > uptimeAfterFilter.StartTimestamp
	}, 5*time.Second, 25*time.Millisecond)

	t.Logf("[Baseline Step 2 - After SourcePolicy Rollout] full_restarts=%d, exporter_sent=%d (RESET from 3 to 2; expected 10; start=%d > %d), process_uptime=%.3fs (RESET start=%d > %d)",
		len(recordedLogs.FilterMessage("Config updated, restart service").All()),
		expSentAfterSource.IntVal, expSentAfterSource.StartTimestamp, expSentAfterFilter.StartTimestamp,
		uptimeAfterSource.DoubleVal, uptimeAfterSource.StartTimestamp, uptimeAfterFilter.StartTimestamp)
}

func TestReloadContinuity_Prototype_PreservesCountersAndUptime(t *testing.T) {
	withFakeGCEMetadata(t)
	t.Setenv("FLEET_ID", "fleet-tab3-prototype")

	origSelfMetricsPort := BuiltInSelfMetricsPolicy.Port
	selfMetricsPort := freeLocalPort(t)
	BuiltInSelfMetricsPolicy.Port = selfMetricsPort
	t.Cleanup(func() {
		BuiltInSelfMetricsPolicy.Port = origSelfMetricsPort
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	sourceAPort := freeLocalPort(t)
	sourceBPort1 := freeLocalPort(t)
	sourceBPort2 := freeLocalPort(t)
	sourceCPort := freeLocalPort(t)

	destPolicy := &captureDestinationPolicy{name: "test_dest"}
	sourceA := &otlpLogSourcePolicy{name: "source_a", port: sourceAPort, pipelineName: "logs"}

	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-proto-0-initial",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName(): {PolicyObj: destPolicy},
			sourceA.PolicyName():    {PolicyObj: sourceA},
		},
	})

	sink := newTelemetryCaptureSink()
	core, recordedLogs := observer.New(zapcore.DebugLevel)

	proxyPort := startSelfMetricsLoopbackProxy(t, selfMetricsPort)
	overrideFactory := confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &staticOverrideProvider{readerEndpointPort: proxyPort}
	})

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo:               component.NewDefaultBuildInfo(),
		Factories:               newTestCollectorFactories(sink),
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
		LoggingOptions: []zap.Option{
			zap.WrapCore(func(c zapcore.Core) zapcore.Core {
				return zapcore.NewTee(c, core)
			}),
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					"googlecontrolplane:component:prototype",
					"testoverride:fast_metrics",
				},
				ProviderFactories: []confmap.ProviderFactory{
					NewFactory(),
					overrideFactory,
				},
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	var runErr error
	go func() {
		defer close(runDone)
		runErr = col.Run(ctx)
	}()
	t.Cleanup(func() {
		col.Shutdown()
		<-runDone
		_ = runErr
	})

	require.Eventually(t, func() bool {
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 20*time.Millisecond)

	// Step 0: Initial traffic — send 5 logs through otlp/source_a.
	sendOTLPLogs(t, sourceAPort, []string{"init-1", "init-2", "init-3", "init-4", "init-5"})

	var expSent0, recvA0, uptime0 metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3 bool
		expSent0, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvA0, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		uptime0, ok3 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 && ok3 && expSent0.IntVal == 5 && recvA0.IntVal == 5 && uptime0.DoubleVal >= 0.15
	}, 5*time.Second, 25*time.Millisecond)

	t.Logf("[Prototype Step 0 - Initial] exporter_sent=%d (start=%d), receiver_accepted(source_a)=%d, process_uptime=%.3fs (start=%d)",
		expSent0.IntVal, expSent0.StartTimestamp, recvA0.IntVal, uptime0.DoubleVal, uptime0.StartTimestamp)

	// Step 1: Roll out a LogFilterPolicy update ("DROP_ME").
	// googlepolicyprocessor hot-swaps its Evaluator in memory; googlecontrolplaneprovider
	// detects an identical synthesized confmap and suppresses WatcherFunc (0 reloads).
	filterPolicy := mustLoadLogFilter(t, "drop-filter", "DROP_ME")
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-proto-1-log-filter",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA.PolicyName():      {PolicyObj: sourceA},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Successfully compiled and updated transformation policy engine").All()) >= 1 &&
			len(recordedLogs.FilterMessage("Synthesized confmap unchanged after policy set update; skipping collector reload").All()) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	// Send 3 kept logs + 2 dropped logs through otlp/source_a.
	sendOTLPLogs(t, sourceAPort, []string{"keep-1", "DROP_ME-1", "keep-2", "DROP_ME-2", "keep-3"})

	var expSent1, recvA1, uptime1 metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3 bool
		expSent1, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvA1, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		uptime1, ok3 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 && ok3 && expSent1.IntVal == 8 && recvA1.IntVal == 10 && uptime1.DoubleVal > uptime0.DoubleVal
	}, 5*time.Second, 25*time.Millisecond)

	assert.Equal(t, 0, len(recordedLogs.FilterMessage("Config updated, restart service").All()))
	assert.Equal(t, 0, len(recordedLogs.FilterMessage("Config updated, performing partial receiver reload").All()))
	assert.Equal(t, expSent0.StartTimestamp, expSent1.StartTimestamp, "exporter_sent StartTimestamp must not reset")
	assert.Equal(t, recvA0.StartTimestamp, recvA1.StartTimestamp, "receiver_accepted(source_a) StartTimestamp must not reset")
	assert.Equal(t, uptime0.StartTimestamp, uptime1.StartTimestamp, "process_uptime StartTimestamp must not reset")
	assert.NotContains(t, sink.exportedLogs(), "DROP_ME-1")
	assert.NotContains(t, sink.exportedLogs(), "DROP_ME-2")

	t.Logf("[Prototype Step 1 - After LogFilterPolicy Rollout] reloads=0, exporter_sent=%d (5->8 monotonic, start=%d), receiver_accepted(source_a)=%d (5->10 monotonic), process_uptime=%.3fs (> %.3fs, start=%d)",
		expSent1.IntVal, expSent1.StartTimestamp, recvA1.IntVal, uptime1.DoubleVal, uptime0.DoubleVal, uptime1.StartTimestamp)

	// Step 2 (SourcePolicy Case B1): Add a brand-new receiver (otlp/source_b) to the
	// existing shared pipeline service::pipelines::logs via confmap.enableMergeAppendOption.
	// Verify that receiversOnlyChanged in otelcol/config_diff.go returns true and
	// invokes Service.UpdateReceivers without resetting source_a, exporter, or uptime!
	sourceB1 := &otlpLogSourcePolicy{name: "source_b", port: sourceBPort1, pipelineName: "logs"}
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-proto-2-add-source-b-shared-pipeline",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA.PolicyName():      {PolicyObj: sourceA},
			sourceB1.PolicyName():     {PolicyObj: sourceB1},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Partial receiver reload completed successfully").All()) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	// Send 4 logs to unchanged otlp/source_a and 6 logs to newly added otlp/source_b.
	sendOTLPLogs(t, sourceAPort, []string{"a-1", "a-2", "a-3", "a-4"})
	sendOTLPLogs(t, sourceBPort1, []string{"b-1", "b-2", "b-3", "b-4", "b-5", "b-6"})

	var expSent2, recvA2, recvB2, uptime2 metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3, ok4 bool
		expSent2, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvA2, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		recvB2, ok3 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_b"})
		uptime2, ok4 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 && ok3 && ok4 &&
			expSent2.IntVal == 18 &&
			recvA2.IntVal == 14 &&
			recvB2.IntVal == 6 &&
			uptime2.DoubleVal > uptime1.DoubleVal
	}, 5*time.Second, 25*time.Millisecond)

	assert.Equal(t, 0, len(recordedLogs.FilterMessage("Config updated, restart service").All()), "must not perform full reload")
	assert.Equal(t, 1, len(recordedLogs.FilterMessage("Config updated, performing partial receiver reload").All()))
	assert.Equal(t, expSent0.StartTimestamp, expSent2.StartTimestamp)
	assert.Equal(t, recvA0.StartTimestamp, recvA2.StartTimestamp)
	assert.Equal(t, uptime0.StartTimestamp, uptime2.StartTimestamp)

	t.Logf("[Prototype Step 2 - Case B1 Add Receiver to Shared Pipeline] partial_reloads=1, full_restarts=0, exporter_sent=%d (8->18 monotonic), receiver_accepted(source_a)=%d (10->14 monotonic), receiver_accepted(source_b)=%d, process_uptime=%.3fs (> %.3fs, start=%d)",
		expSent2.IntVal, recvA2.IntVal, recvB2.IntVal, uptime2.DoubleVal, uptime1.DoubleVal, uptime2.StartTimestamp)

	// Step 3 (SourcePolicy Case A): Modify existing receiver otlp/source_b's config in place
	// (changing its endpoint port from sourceBPort1 to sourceBPort2) while leaving otlp/source_a untouched.
	sourceB2 := &otlpLogSourcePolicy{name: "source_b", port: sourceBPort2, pipelineName: "logs"}
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-proto-3-modify-source-b-in-place",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA.PolicyName():      {PolicyObj: sourceA},
			sourceB2.PolicyName():     {PolicyObj: sourceB2},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Partial receiver reload completed successfully").All()) >= 2
	}, 5*time.Second, 20*time.Millisecond)

	// Send 2 logs to otlp/source_a and 3 logs to otlp/source_b (on its new port).
	sendOTLPLogs(t, sourceAPort, []string{"a-5", "a-6"})
	sendOTLPLogs(t, sourceBPort2, []string{"b2-1", "b2-2", "b2-3"})

	var expSent3, recvA3, recvB3, uptime3 metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2, ok3, ok4 bool
		expSent3, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		recvA3, ok2 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_a"})
		recvB3, ok3 = sink.latestMetric("otelcol_receiver_accepted_log_records", map[string]string{"receiver": "otlp/source_b"})
		uptime3, ok4 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 && ok3 && ok4 &&
			expSent3.IntVal == 23 &&
			recvA3.IntVal == 16 &&
			recvB3.IntVal == 9 &&
			uptime3.DoubleVal > uptime2.DoubleVal
	}, 5*time.Second, 25*time.Millisecond)

	assert.Equal(t, 0, len(recordedLogs.FilterMessage("Config updated, restart service").All()), "must not perform full reload")
	assert.Equal(t, 2, len(recordedLogs.FilterMessage("Config updated, performing partial receiver reload").All()))
	assert.Equal(t, expSent0.StartTimestamp, expSent3.StartTimestamp)
	assert.Equal(t, recvA0.StartTimestamp, recvA3.StartTimestamp)
	assert.Equal(t, recvB2.StartTimestamp, recvB3.StartTimestamp, "because MeterProvider is preserved across UpdateReceivers, the OTel Go SDK reuses the synchronous Int64Counter instrument for otlp/source_b without resetting it")
	assert.Equal(t, uptime0.StartTimestamp, uptime3.StartTimestamp)

	t.Logf("[Prototype Step 3 - Case A Modify Receiver In Place] partial_reloads=2, full_restarts=0, exporter_sent=%d (18->23 monotonic), receiver_accepted(source_a)=%d (14->16 monotonic), receiver_accepted(source_b)=%d (6->9 monotonic via preserved MeterProvider!), process_uptime=%.3fs (> %.3fs, start=%d)",
		expSent3.IntVal, recvA3.IntVal, recvB3.IntVal, uptime3.DoubleVal, uptime2.DoubleVal, uptime3.StartTimestamp)

	// Step 4 (SourcePolicy Case B2): Add a SourcePolicy that introduces a brand-new
	// pipeline key in service.pipelines ("logs/source_c" instead of shared "logs").
	// Empirically verify that in v0.162.0, receiversOnlyChanged (otelcol/config_diff.go:161)
	// checks len(old.pipelines) != len(cur.pipelines), returns false, and falls back
	// to a full service reload ("Config updated, restart service").
	sourceC := &otlpLogSourcePolicy{name: "source_c", port: sourceCPort, pipelineName: "logs/source_c"}
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-proto-4-new-pipeline-key-triggers-full-reload",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA.PolicyName():      {PolicyObj: sourceA},
			sourceB2.PolicyName():     {PolicyObj: sourceB2},
			sourceC.PolicyName():      {PolicyObj: sourceC},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	require.Eventually(t, func() bool {
		return len(recordedLogs.FilterMessage("Config updated, restart service").All()) >= 1 &&
			len(recordedLogs.FilterMessage("Everything is ready. Begin running and processing data.").All()) >= 2
	}, 5*time.Second, 20*time.Millisecond)

	sendOTLPLogs(t, sourceCPort, []string{"c-1", "c-2"})

	var expSent4, uptime4 metricSample
	require.Eventually(t, func() bool {
		var ok1, ok2 bool
		expSent4, ok1 = sink.latestMetric("otelcol_exporter_sent_log_records", map[string]string{"exporter": "capture/test_dest"})
		uptime4, ok2 = sink.latestMetric("otelcol_process_uptime", nil)
		return ok1 && ok2 &&
			expSent4.StartTimestamp > expSent3.StartTimestamp &&
			expSent4.IntVal == 2 &&
			uptime4.StartTimestamp > uptime3.StartTimestamp
	}, 5*time.Second, 25*time.Millisecond)

	t.Logf("[Prototype Step 4 - Case B2 New Pipeline Key (logs/source_c)] full_restarts=1 (config_diff.go:161 len(old.pipelines)!=len(cur.pipelines)), exporter_sent=%d (RESET from 23 to 2, start=%d > %d), process_uptime=%.3fs (RESET start=%d > %d)",
		expSent4.IntVal, expSent4.StartTimestamp, expSent3.StartTimestamp, uptime4.DoubleVal, uptime4.StartTimestamp, uptime3.StartTimestamp)
}

// TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown verifies a newly
// discovered architectural hazard in BuiltInSelfMetricsPolicy (selfmetrics_policy.go)
// when combined with upstream service.Service.Shutdown(ctx) (service.go:289-322):
//   - BuiltInSelfMetricsPolicy configures service.telemetry.metrics.readers[0].periodic
//     to push OTLP gRPC metrics to http://localhost:<port> (otlp/default_self_metrics
//     inside the same Service's pipelines).
//   - During a full collector reload (otelcol.Collector.reloadConfiguration),
//     Service.Shutdown(ctx) executes Pipelines.ShutdownAll (service.go:300, stopping
//     otlp/default_self_metrics) BEFORE meterProvider.Shutdown (service.go:315).
//   - Consequently, on a direct (unproxied) loopback topology, any full reload
//     causes meterProvider.Shutdown to fail with connection refused, which
//     reloadConfiguration (collector.go:313) treats as fatal, terminating col.Run!
func TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown(t *testing.T) {
	withFakeGCEMetadata(t)
	t.Setenv("FLEET_ID", "fleet-tab3-direct-loopback")

	origSelfMetricsPort := BuiltInSelfMetricsPolicy.Port
	selfMetricsPort := freeLocalPort(t)
	BuiltInSelfMetricsPolicy.Port = selfMetricsPort
	t.Cleanup(func() {
		BuiltInSelfMetricsPolicy.Port = origSelfMetricsPort
		for googlepolicy.ActivePolicySet() != nil {
			googlepolicy.RollbackActivePolicySet()
		}
	})

	sourceAPort := freeLocalPort(t)
	destPolicy := &captureDestinationPolicy{name: "test_dest"}
	sourceA := &otlpLogSourcePolicy{name: "source_a", port: sourceAPort, pipelineName: "logs"}

	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-loopback-0",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName(): {PolicyObj: destPolicy},
			sourceA.PolicyName():    {PolicyObj: sourceA},
		},
	})

	sink := newTelemetryCaptureSink()
	baselineProviderFactory := confmap.NewProviderFactory(func(set confmap.ProviderSettings) confmap.Provider {
		p := newProvider(set).(*provider)
		p.diffGateReloads = false
		_ = featuregate.GlobalRegistry().Set(partialReloadFeatureGateID, false)
		_ = featuregate.GlobalRegistry().Set(partialReloadReceiversGateID, false)
		return p
	})
	// Point PeriodicMetricReader directly at selfMetricsPort (no proxy), matching
	// BuiltInSelfMetricsPolicy's production loopback topology.
	overrideFactory := confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &staticOverrideProvider{readerEndpointPort: selfMetricsPort}
	})

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo:               component.NewDefaultBuildInfo(),
		Factories:               newTestCollectorFactories(sink),
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					"googlecontrolplane:component:loopback",
					"testoverride:fast_metrics",
				},
				ProviderFactories: []confmap.ProviderFactory{
					baselineProviderFactory,
					overrideFactory,
				},
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- col.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 20*time.Millisecond)

	// Trigger a full reload by updating ActivePolicySet when diffGateReloads=false.
	filterPolicy := mustLoadLogFilter(t, "drop-filter", "DROP_ME")
	googlepolicy.SetActivePolicySet(&googlepolicy.PolicySet{
		RevisionID: "rev-loopback-1",
		Policies: map[string]*googlepolicy.PolicySetEntry{
			destPolicy.PolicyName():   {PolicyObj: destPolicy},
			sourceA.PolicyName():      {PolicyObj: sourceA},
			filterPolicy.PolicyName(): {PolicyObj: filterPolicy},
		},
	})

	select {
	case runErr := <-runDone:
		require.Error(t, runErr)
		assert.ErrorContains(t, runErr, "failed to shutdown the retiring config")
		assert.ErrorContains(t, runErr, "failed to shutdown meter provider")
		t.Logf("[Direct SelfMetrics Loopback Full Reload Failure] col.Run exited fatally during reloadConfiguration: %v", runErr)
	case <-time.After(5 * time.Second):
		col.Shutdown()
		t.Fatal("expected full reload on direct SelfMetricsPolicy loopback to fail in meterProvider.Shutdown")
	}
}


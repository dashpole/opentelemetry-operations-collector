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

package googlecontrolplane_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/ingestor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/extension/googlecontrolplaneextension"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/policyprocessor"
)

// --- Mock OTLP Destination Service ---

type mockLogsServer struct {
	plogotlp.UnimplementedGRPCServer
	mu   sync.Mutex
	logs []plog.Logs
}

func (s *mockLogsServer) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ld := plog.NewLogs()
	req.Logs().CopyTo(ld)
	s.logs = append(s.logs, ld)
	return plogotlp.NewExportResponse(), nil
}

func (s *mockLogsServer) getLogs() []plog.Logs {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]plog.Logs, len(s.logs))
	copy(res, s.logs)
	return res
}

func (s *mockLogsServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = nil
}

type mockMetricsServer struct {
	pmetricotlp.UnimplementedGRPCServer
	mu      sync.Mutex
	metrics []pmetric.Metrics
}

func (s *mockMetricsServer) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	md := pmetric.NewMetrics()
	req.Metrics().CopyTo(md)
	s.metrics = append(s.metrics, md)
	return pmetricotlp.NewExportResponse(), nil
}

func (s *mockMetricsServer) getMetrics() []pmetric.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]pmetric.Metrics, len(s.metrics))
	copy(res, s.metrics)
	return res
}

func (s *mockMetricsServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = nil
}

type mockTracesServer struct {
	ptraceotlp.UnimplementedGRPCServer
	mu     sync.Mutex
	traces []ptrace.Traces
}

func (s *mockTracesServer) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	td := ptrace.NewTraces()
	req.Traces().CopyTo(td)
	s.traces = append(s.traces, td)
	return ptraceotlp.NewExportResponse(), nil
}

func (s *mockTracesServer) getTraces() []ptrace.Traces {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]ptrace.Traces, len(s.traces))
	copy(res, s.traces)
	return res
}

func (s *mockTracesServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traces = nil
}

type mockOtlpDestination struct {
	logs    *mockLogsServer
	metrics *mockMetricsServer
	traces  *mockTracesServer
	server  *grpc.Server
	port    int
}

func startMockOtlpDestination(t *testing.T) (*mockOtlpDestination, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	logsSrv := &mockLogsServer{}
	metricsSrv := &mockMetricsServer{}
	tracesSrv := &mockTracesServer{}

	plogotlp.RegisterGRPCServer(grpcServer, logsSrv)
	pmetricotlp.RegisterGRPCServer(grpcServer, metricsSrv)
	ptraceotlp.RegisterGRPCServer(grpcServer, tracesSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	dest := &mockOtlpDestination{
		logs:    logsSrv,
		metrics: metricsSrv,
		traces:  tracesSrv,
		server:  grpcServer,
		port:    lis.Addr().(*net.TCPAddr).Port,
	}

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return dest, cleanup
}

func (d *mockOtlpDestination) resetAll() {
	d.logs.reset()
	d.metrics.reset()
	d.traces.reset()
}

// --- Mock xDS Discovery Service ---

type mockDiscoveryServer struct {
	xdsv1alpha1.UnimplementedTelemetryDiscoveryServiceServer

	reqCh       chan *discoveryv3.DiscoveryRequest
	respCh      chan *discoveryv3.DiscoveryResponse
	streamErrCh chan error
}

func newMockDiscoveryServer() *mockDiscoveryServer {
	return &mockDiscoveryServer{
		reqCh:       make(chan *discoveryv3.DiscoveryRequest, 20),
		respCh:      make(chan *discoveryv3.DiscoveryResponse, 20),
		streamErrCh: make(chan error, 1),
	}
}

func (m *mockDiscoveryServer) StreamTelemetryCollectors(stream grpc.BidiStreamingServer[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]) error {
	errCh := make(chan error, 2)

	go func() {
		for {
			select {
			case resp, ok := <-m.respCh:
				if !ok {
					errCh <- nil
					return
				}
				if err := stream.Send(resp); err != nil {
					errCh <- err
					return
				}
			case err := <-m.streamErrCh:
				if err != nil {
					errCh <- err
					return
				}
			case <-stream.Context().Done():
				errCh <- stream.Context().Err()
				return
			}
		}
	}()

	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			m.reqCh <- req
		}
	}()

	return <-errCh
}

func startMockDiscoveryServer(t *testing.T, srv *mockDiscoveryServer) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	xdsv1alpha1.RegisterTelemetryDiscoveryServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	endpoint := lis.Addr().String()
	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return endpoint, cleanup
}

// --- Test Invariant Violation Driver for Phase 3 ---

const typeURLStructuralViolation = "type.googleapis.com/test.StructuralViolationPolicy"

type structuralViolationDriver struct{}

func (d *structuralViolationDriver) TypeURL() string { return typeURLStructuralViolation }
func (d *structuralViolationDriver) Class() driver.PolicyClass {
	return driver.PolicyClassSource
}
func (d *structuralViolationDriver) Validate(proto.Message) error { return nil }
func (d *structuralViolationDriver) GenerateConfig(proto.Message, *driver.CompilationContext) (*driver.ConfigFragment, error) {
	frag := driver.NewConfigFragment()
	// Deliberately reference an undeclared receiver in the pipeline to trigger PreValidator rejection
	frag.Pipelines["logs"] = driver.PipelineConfig{
		Receivers: []string{"undeclared_receiver_that_triggers_prevalidator_rejection"},
	}
	return frag, nil
}

// --- Helper Functions to Generate Policies ---

func buildPolicySetR1() *xdsv1alpha1.TelemetryCollector {
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}

	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}

	// LogFilter: Drop DEBUG logs
	logFilter := &policyv1alpha1.LogFilterPolicy{
		Id:     "log-filter-r1",
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
	anyLog, _ := anypb.New(logFilter)

	// MetricFilter: Drop "dropped_metric"
	metricFilter := &policyv1alpha1.MetricFilterPolicy{
		Id:     "metric-filter-r1",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.MetricMatcher{
			{
				Target: &policyv1alpha1.MetricFieldSelector{
					Target: &policyv1alpha1.MetricFieldSelector_DescriptorField{
						DescriptorField: policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.MetricMatcher_Exact{Exact: "dropped_metric"},
			},
		},
	}
	anyMetric, _ := anypb.New(metricFilter)

	// TraceFilter: Drop "drop_me"
	traceFilter := &policyv1alpha1.TraceFilterPolicy{
		Id:     "trace-filter-r1",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.TraceMatcher{
			{
				Target: &policyv1alpha1.TraceFieldSelector{
					Target: &policyv1alpha1.TraceFieldSelector_RecordField{
						RecordField: policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.TraceMatcher_Exact{Exact: "drop_me"},
			},
		},
	}
	anyTrace, _ := anypb.New(traceFilter)

	return &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "log-filter-r1", TypedConfig: anyLog},
			{Name: "metric-filter-r1", TypedConfig: anyMetric},
			{Name: "trace-filter-r1", TypedConfig: anyTrace},
		},
	}
}

func buildPolicySetR2() *xdsv1alpha1.TelemetryCollector {
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}

	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}

	// LogFilter: Drop INFO logs (keep DEBUG)
	logFilter := &policyv1alpha1.LogFilterPolicy{
		Id:     "log-filter-r2",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "INFO"},
			},
		},
	}
	anyLog, _ := anypb.New(logFilter)

	// MetricFilter: Drop "kept_metric" (keep dropped_metric)
	metricFilter := &policyv1alpha1.MetricFilterPolicy{
		Id:     "metric-filter-r2",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.MetricMatcher{
			{
				Target: &policyv1alpha1.MetricFieldSelector{
					Target: &policyv1alpha1.MetricFieldSelector_DescriptorField{
						DescriptorField: policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.MetricMatcher_Exact{Exact: "kept_metric"},
			},
		},
	}
	anyMetric, _ := anypb.New(metricFilter)

	// TraceFilter: Drop "keep_me" (keep drop_me)
	traceFilter := &policyv1alpha1.TraceFilterPolicy{
		Id:     "trace-filter-r2",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.TraceMatcher{
			{
				Target: &policyv1alpha1.TraceFieldSelector{
					Target: &policyv1alpha1.TraceFieldSelector_RecordField{
						RecordField: policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.TraceMatcher_Exact{Exact: "keep_me"},
			},
		},
	}
	anyTrace, _ := anypb.New(traceFilter)

	return &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "log-filter-r2", TypedConfig: anyLog},
			{Name: "metric-filter-r2", TypedConfig: anyMetric},
			{Name: "trace-filter-r2", TypedConfig: anyTrace},
		},
	}
}

func buildPolicySetR3Violation() *xdsv1alpha1.TelemetryCollector {
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}

	// Violation policy triggers undeclared receiver in PreValidator
	violationAny := &anypb.Any{
		TypeUrl: typeURLStructuralViolation,
	}

	return &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			{Name: "violation-policy", TypedConfig: violationAny},
		},
	}
}

func getFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func writeBaseConfigFile(t *testing.T, destPort, recvGrpcPort, recvHttpPort int) string {
	baseMap := map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", recvGrpcPort),
					},
					"http": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", recvHttpPort),
					},
				},
			},
		},
		"exporters": map[string]any{
			"otlp/gcp_destination": map[string]any{
				"endpoint": fmt.Sprintf("127.0.0.1:%d", destPort),
				"tls": map[string]any{
					"insecure": true,
				},
			},
		},
		"processors": map[string]any{
			"batch/gcp_destination_metrics": map[string]any{
				"send_batch_size":     1,
				"send_batch_max_size": 1,
				"timeout":             "50ms",
			},
			"batch/gcp_destination_logs": map[string]any{
				"send_batch_size":     1,
				"send_batch_max_size": 1,
				"timeout":             "50ms",
			},
			"batch/gcp_destination_traces": map[string]any{
				"send_batch_size":     1,
				"send_batch_max_size": 1,
				"timeout":             "50ms",
			},
		},
		"service": map[string]any{
			"telemetry": map[string]any{
				"logs": map[string]any{
					"level": "warn",
				},
			},
		},
	}

	tmpFile := filepath.Join(t.TempDir(), "base_config.yaml")
	data, err := yaml.Marshal(baseMap)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tmpFile, data, 0o600))
	return tmpFile
}

func createFactories(meterProvider *sdkmetric.MeterProvider, statusReg *googlecontrolplane.StatusRegistry) func() (otelcol.Factories, error) {
	return func() (otelcol.Factories, error) {
		policyFactory := policyprocessor.NewFactory()
		customPolicy := processor.NewFactory(
			component.MustNewType("policy"),
			policyFactory.CreateDefaultConfig,
			processor.WithLogs(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
				if meterProvider != nil {
					set.TelemetrySettings.MeterProvider = meterProvider
				}
				return policyFactory.CreateLogs(ctx, set, cfg, next)
			}, component.StabilityLevelAlpha),
			processor.WithMetrics(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
				if meterProvider != nil {
					set.TelemetrySettings.MeterProvider = meterProvider
				}
				return policyFactory.CreateMetrics(ctx, set, cfg, next)
			}, component.StabilityLevelAlpha),
			processor.WithTraces(func(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
				if meterProvider != nil {
					set.TelemetrySettings.MeterProvider = meterProvider
				}
				return policyFactory.CreateTraces(ctx, set, cfg, next)
			}, component.StabilityLevelAlpha),
		)

		extFactory := googlecontrolplaneextension.NewFactoryWithStatusRegistry(statusReg)
		customExt := extension.NewFactory(
			component.MustNewType("googlecontrolplaneextension"),
			extFactory.CreateDefaultConfig,
			func(ctx context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
				if meterProvider != nil {
					set.TelemetrySettings.MeterProvider = meterProvider
				}
				return extFactory.Create(ctx, set, cfg)
			},
			component.StabilityLevelAlpha,
		)

		return otelcol.Factories{
			Receivers: map[component.Type]receiver.Factory{
				component.MustNewType("otlp"): otlpreceiver.NewFactory(),
			},
			Processors: map[component.Type]processor.Factory{
				component.MustNewType("batch"):  batchprocessor.NewFactory(),
				component.MustNewType("policy"): customPolicy,
			},
			Exporters: map[component.Type]exporter.Factory{
				component.MustNewType("otlp"): otlpexporter.NewFactory(),
			},
			Extensions: map[component.Type]extension.Factory{
				component.MustNewType("googleclientauth"):            driver.NewGoogleClientAuthExtensionFactory(),
				component.MustNewType("googlecontrolplaneextension"): customExt,
			},
			Telemetry: otelconftelemetry.NewFactory(),
		}, nil
	}
}

// --- Multi-Phase Dynamic End-to-End Test Suite ---

func TestMultiPhaseDynamicE2E_XDS(t *testing.T) {
	// 1. Setup Mock OTLP Destination Service
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	// 2. Setup Mock xDS Server
	xdsSrv := newMockDiscoveryServer()
	xdsEndpoint, stopXds := startMockDiscoveryServer(t, xdsSrv)
	defer stopXds()

	recvGrpcPort := getFreePort(t)
	recvHttpPort := getFreePort(t)
	baseConfigFile := writeBaseConfigFile(t, dest.port, recvGrpcPort, recvHttpPort)

	// 3. Driver Registry & Status Registry
	driverReg := driver.NewDefaultRegistry()
	driverReg.RegisterDriver(&structuralViolationDriver{})
	statusReg := googlecontrolplane.NewStatusRegistry()

	meterReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(meterReader))
	defer func() {
		_ = meterProvider.Shutdown(context.Background())
	}()

	providerFactory := googlecontrolplane.NewFactoryWithOptions(
		googlecontrolplane.WithProviderDriverRegistry(driverReg),
		googlecontrolplane.WithProviderStatusRegistry(statusReg),
	)

	// Build collector settings
	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactories(meterProvider, statusReg),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:xds://%s?base_config=%s&startup_timeout=10s", xdsEndpoint, baseConfigFile),
				},
				ProviderFactories: []confmap.ProviderFactory{
					providerFactory,
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	// 4. Send Initial Revision R1 over xDS before starting collector
	colR1 := buildPolicySetR1()
	colAnyR1, err := anypb.New(colR1)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Resources:   []*anypb.Any{colAnyR1},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
	}

	// Start collector
	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()
	defer func() {
		col.Shutdown()
		<-colErrCh
	}()

	// Verify initial DiscoveryRequest from collector
	req1 := <-xdsSrv.reqCh
	assert.Equal(t, "", req1.VersionInfo)

	// Verify ACK for Revision 1
	ack1 := <-xdsSrv.reqCh
	assert.Equal(t, "1", ack1.VersionInfo)
	assert.Equal(t, "nonce-1", ack1.ResponseNonce)
	assert.Nil(t, ack1.ErrorDetail)

	// Wait for collector to reach running state
	select {
	case err := <-colErrCh:
		t.Fatalf("col.Run failed: %v", err)
	default:
		t.Logf("col state before wait: %v", col.GetState())
	}
	require.Eventually(t, func() bool {
		select {
		case err := <-colErrCh:
			t.Fatalf("col.Run failed during wait: %v", err)
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Connect gRPC client to collector OTLP receiver
	recvAddr := fmt.Sprintf("127.0.0.1:%d", recvGrpcPort)
	cc, err := grpc.NewClient(recvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()

	logsClient := plogotlp.NewGRPCClient(cc)
	metricsClient := pmetricotlp.NewGRPCClient(cc)
	tracesClient := ptraceotlp.NewGRPCClient(cc)

	// =========================================================================
	// PHASE 1: Initial Boot & Data Filtering (R1)
	// =========================================================================
	t.Run("Phase 1: Initial Boot & Telemetry Filtering", func(t *testing.T) {
		// Send Logs: 1 DEBUG (drop), 1 INFO (retain)
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberDebug)
		r1.SetSeverityText("DEBUG")
		r1.Body().SetStr("debug-log-r1")

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberInfo)
		r2.SetSeverityText("INFO")
		r2.Body().SetStr("info-log-r1")

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Send Metrics: "dropped_metric" (drop), "kept_metric" (retain)
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()

		m1 := sm.Metrics().AppendEmpty()
		m1.SetName("dropped_metric")
		dp1 := m1.SetEmptyGauge().DataPoints().AppendEmpty()
		dp1.SetIntValue(10)

		m2 := sm.Metrics().AppendEmpty()
		m2.SetName("kept_metric")
		dp2 := m2.SetEmptyGauge().DataPoints().AppendEmpty()
		dp2.SetIntValue(20)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := metricsClient.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Send Traces: "drop_me" (drop), "keep_me" (retain)
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		ss := rs.ScopeSpans().AppendEmpty()

		s1 := ss.Spans().AppendEmpty()
		s1.SetName("drop_me")
		s1.SetTraceID([16]byte{1})
		s1.SetSpanID([8]byte{1})

		s2 := ss.Spans().AppendEmpty()
		s2.SetName("keep_me")
		s2.SetTraceID([16]byte{2})
		s2.SetSpanID([8]byte{2})

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := tracesClient.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Verify Mock Destination captured ONLY retained items
		require.Eventually(t, func() bool {
			return len(dest.logs.getLogs()) > 0 &&
				len(dest.metrics.getMetrics()) > 0 &&
				len(dest.traces.getTraces()) > 0
		}, 5*time.Second, 50*time.Millisecond)

		// Check Logs: Only INFO log retained
		var receivedLogs []string
		for _, l := range dest.logs.getLogs() {
			for i := 0; i < l.ResourceLogs().Len(); i++ {
				r := l.ResourceLogs().At(i)
				for j := 0; j < r.ScopeLogs().Len(); j++ {
					s := r.ScopeLogs().At(j)
					for k := 0; k < s.LogRecords().Len(); k++ {
						receivedLogs = append(receivedLogs, s.LogRecords().At(k).Body().AsString())
					}
				}
			}
		}
		assert.Equal(t, []string{"info-log-r1"}, receivedLogs)

		// Check Metrics: Only "kept_metric" retained
		var receivedMetrics []string
		for _, m := range dest.metrics.getMetrics() {
			for i := 0; i < m.ResourceMetrics().Len(); i++ {
				r := m.ResourceMetrics().At(i)
				for j := 0; j < r.ScopeMetrics().Len(); j++ {
					s := r.ScopeMetrics().At(j)
					for k := 0; k < s.Metrics().Len(); k++ {
						receivedMetrics = append(receivedMetrics, s.Metrics().At(k).Name())
					}
				}
			}
		}
		assert.Equal(t, []string{"kept_metric"}, receivedMetrics)

		// Check Traces: Only "keep_me" retained
		var receivedSpans []string
		for _, tr := range dest.traces.getTraces() {
			for i := 0; i < tr.ResourceSpans().Len(); i++ {
				r := tr.ResourceSpans().At(i)
				for j := 0; j < r.ScopeSpans().Len(); j++ {
					s := r.ScopeSpans().At(j)
					for k := 0; k < s.Spans().Len(); k++ {
						receivedSpans = append(receivedSpans, s.Spans().At(k).Name())
					}
				}
			}
		}
		assert.Equal(t, []string{"keep_me"}, receivedSpans)

		// Verify Self-Observability StatusRegistry & Gauges
		rev, statuses := statusReg.Snapshot()
		assert.Equal(t, int64(1), rev)
		assert.Equal(t, "accepted", statuses["log-filter-r1"].Status)
		assert.Equal(t, "accepted", statuses["metric-filter-r1"].Status)
		assert.Equal(t, "accepted", statuses["trace-filter-r1"].Status)

		var resMetrics metricdata.ResourceMetrics
		err = meterReader.Collect(context.Background(), &resMetrics)
		require.NoError(t, err)

		gaugeRev, gaugeStatuses, evalCounts := parseTestMetrics(&resMetrics)
		assert.Equal(t, int64(1), gaugeRev)
		assert.Equal(t, "accepted", gaugeStatuses["log-filter-r1"])
		assert.Equal(t, "accepted", gaugeStatuses["metric-filter-r1"])
		assert.Equal(t, "accepted", gaugeStatuses["trace-filter-r1"])
		assert.GreaterOrEqual(t, evalCounts["logs:drop"], int64(1))
		assert.GreaterOrEqual(t, evalCounts["metrics:drop"], int64(1))
		assert.GreaterOrEqual(t, evalCounts["traces:drop"], int64(1))
	})

	// =========================================================================
	// PHASE 2: In-Process Dynamic Reload (R2)
	// =========================================================================
	t.Run("Phase 2: In-Process Dynamic Reload", func(t *testing.T) {
		dest.resetAll()

		// Distribute Revision 2 via xDS stream
		colR2 := buildPolicySetR2()
		colAnyR2, err := anypb.New(colR2)
		require.NoError(t, err)

		xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
			VersionInfo: "2",
			Resources:   []*anypb.Any{colAnyR2},
			TypeUrl:     ingestor.TelemetryCollectorTypeURL,
			Nonce:       "nonce-2",
		}

		// Wait for ACK for Revision 2
		ack2 := <-xdsSrv.reqCh
		assert.Equal(t, "2", ack2.VersionInfo)
		assert.Equal(t, "nonce-2", ack2.ResponseNonce)
		assert.Nil(t, ack2.ErrorDetail)

		// Verify StatusRegistry updated to Revision 2 and pruned old R1 policies
		require.Eventually(t, func() bool {
			r, _ := statusReg.Snapshot()
			return r == 2
		}, 5*time.Second, 50*time.Millisecond)

		rev, statuses := statusReg.Snapshot()
		assert.Equal(t, int64(2), rev)
		assert.Contains(t, statuses, "log-filter-r2")
		assert.NotContains(t, statuses, "log-filter-r1")

		// Verify collector process remains running in-process
		require.Eventually(t, func() bool {
			return col.GetState() == otelcol.StateRunning
		}, 5*time.Second, 50*time.Millisecond)

		// Send Telemetry under R2 rules:
		// Logs: INFO should now be dropped, DEBUG should now be retained!
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberInfo)
		r1.SetSeverityText("INFO")
		r1.Body().SetStr("info-log-r2")

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberDebug)
		r2.SetSeverityText("DEBUG")
		r2.Body().SetStr("debug-log-r2")

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Metrics: "kept_metric" should now be dropped, "dropped_metric" retained!
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()

		m1 := sm.Metrics().AppendEmpty()
		m1.SetName("kept_metric")
		dp1 := m1.SetEmptyGauge().DataPoints().AppendEmpty()
		dp1.SetIntValue(100)

		m2 := sm.Metrics().AppendEmpty()
		m2.SetName("dropped_metric")
		dp2 := m2.SetEmptyGauge().DataPoints().AppendEmpty()
		dp2.SetIntValue(200)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := metricsClient.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Traces: "keep_me" should now be dropped, "drop_me" retained!
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		ss := rs.ScopeSpans().AppendEmpty()

		s1 := ss.Spans().AppendEmpty()
		s1.SetName("keep_me")
		s1.SetTraceID([16]byte{3})
		s1.SetSpanID([8]byte{3})

		s2 := ss.Spans().AppendEmpty()
		s2.SetName("drop_me")
		s2.SetTraceID([16]byte{4})
		s2.SetSpanID([8]byte{4})

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := tracesClient.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		// Verify Mock Destination received the newly inverted rule outputs
		require.Eventually(t, func() bool {
			return len(dest.logs.getLogs()) > 0 &&
				len(dest.metrics.getMetrics()) > 0 &&
				len(dest.traces.getTraces()) > 0
		}, 5*time.Second, 50*time.Millisecond)

		var receivedLogs []string
		for _, l := range dest.logs.getLogs() {
			for i := 0; i < l.ResourceLogs().Len(); i++ {
				r := l.ResourceLogs().At(i)
				for j := 0; j < r.ScopeLogs().Len(); j++ {
					s := r.ScopeLogs().At(j)
					for k := 0; k < s.LogRecords().Len(); k++ {
						receivedLogs = append(receivedLogs, s.LogRecords().At(k).Body().AsString())
					}
				}
			}
		}
		assert.Equal(t, []string{"debug-log-r2"}, receivedLogs)

		var receivedMetrics []string
		for _, m := range dest.metrics.getMetrics() {
			for i := 0; i < m.ResourceMetrics().Len(); i++ {
				r := m.ResourceMetrics().At(i)
				for j := 0; j < r.ScopeMetrics().Len(); j++ {
					s := r.ScopeMetrics().At(j)
					for k := 0; k < s.Metrics().Len(); k++ {
						receivedMetrics = append(receivedMetrics, s.Metrics().At(k).Name())
					}
				}
			}
		}
		assert.Equal(t, []string{"dropped_metric"}, receivedMetrics)

		var receivedSpans []string
		for _, tr := range dest.traces.getTraces() {
			for i := 0; i < tr.ResourceSpans().Len(); i++ {
				r := tr.ResourceSpans().At(i)
				for j := 0; j < r.ScopeSpans().Len(); j++ {
					s := r.ScopeSpans().At(j)
					for k := 0; k < s.Spans().Len(); k++ {
						receivedSpans = append(receivedSpans, s.Spans().At(k).Name())
					}
				}
			}
		}
		assert.Equal(t, []string{"drop_me"}, receivedSpans)

		// Verify Gauge metric updated to revision 2
		var resMetrics metricdata.ResourceMetrics
		err = meterReader.Collect(context.Background(), &resMetrics)
		require.NoError(t, err)

		gaugeRev, gaugeStatuses, _ := parseTestMetrics(&resMetrics)
		assert.Equal(t, int64(2), gaugeRev)
		assert.Equal(t, "accepted", gaugeStatuses["log-filter-r2"])
	})

	// =========================================================================
	// PHASE 3: Crash-Proof Runtime Gate Rejection (R3)
	// =========================================================================
	t.Run("Phase 3: Crash-Proof Runtime Gate Rejection", func(t *testing.T) {
		dest.resetAll()

		// Push Revision 3 violating structural invariants
		colR3 := buildPolicySetR3Violation()
		colAnyR3, err := anypb.New(colR3)
		require.NoError(t, err)

		xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
			VersionInfo: "3",
			Resources:   []*anypb.Any{colAnyR3},
			TypeUrl:     ingestor.TelemetryCollectorTypeURL,
			Nonce:       "nonce-3",
		}

		// Wait for NACK from collector
		nack := <-xdsSrv.reqCh
		assert.Equal(t, "2", nack.VersionInfo, "NACK must preserve last valid version 2")
		assert.Equal(t, "nonce-3", nack.ResponseNonce, "NACK must acknowledge rejected nonce-3")
		require.NotNil(t, nack.ErrorDetail, "NACK must contain ErrorDetail")
		assert.Equal(t, int32(codes.InvalidArgument), nack.ErrorDetail.Code)
		assert.Contains(t, nack.ErrorDetail.Message, "Structural validation failed")

		// Verify collector process remains healthy and running
		assert.Equal(t, otelcol.StateRunning, col.GetState())

		// Verify StatusRegistry remains at Revision 2
		rev, statuses := statusReg.Snapshot()
		assert.Equal(t, int64(2), rev)
		assert.Contains(t, statuses, "log-filter-r2")

		// Send telemetry and verify R2 rules are STILL actively enforced
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberInfo)
		r1.SetSeverityText("INFO")
		r1.Body().SetStr("info-log-r3-attempt")

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberDebug)
		r2.SetSeverityText("DEBUG")
		r2.Body().SetStr("debug-log-r3-attempt")

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			return len(dest.logs.getLogs()) > 0
		}, 5*time.Second, 50*time.Millisecond)

		var receivedLogs []string
		for _, l := range dest.logs.getLogs() {
			for i := 0; i < l.ResourceLogs().Len(); i++ {
				r := l.ResourceLogs().At(i)
				for j := 0; j < r.ScopeLogs().Len(); j++ {
					s := r.ScopeLogs().At(j)
					for k := 0; k < s.LogRecords().Len(); k++ {
						receivedLogs = append(receivedLogs, s.LogRecords().At(k).Body().AsString())
					}
				}
			}
		}
		// Under R2, INFO is dropped and DEBUG is retained
		assert.Equal(t, []string{"debug-log-r3-attempt"}, receivedLogs)
	})
}

// =============================================================================
// PHASE 4: Escape Hatch Integration with Custom Configuration
// =============================================================================

func TestMultiPhaseDynamicE2E_Phase4EscapeHatch(t *testing.T) {
	// 1. Setup Mock OTLP Destination Service
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	tmpDir := t.TempDir()

	// 2. Write Policy File (JSON) with LogFilterPolicy dropping DEBUG
	policyCol := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			{
				Name: "gcp-dest",
				TypedConfig: &anypb.Any{
					TypeUrl: driver.TypeURLGcpDestination,
				},
			},
			{
				Name: "log-filter",
				TypedConfig: func() *anypb.Any {
					a, _ := anypb.New(&policyv1alpha1.LogFilterPolicy{
						Id:     "log-filter-escape",
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
					})
					return a
				}(),
			},
		},
	}
	policyData, err := proto.Marshal(policyCol)
	require.NoError(t, err)
	policyPath := filepath.Join(tmpDir, "policies.pb")
	require.NoError(t, os.WriteFile(policyPath, policyData, 0o600))

	// 3. Write Base Config mapping destination exporter to mock server
	otlpGrpcPort := getFreePort(t)
	otlpHttpPort := getFreePort(t)
	baseConfigMap := map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", otlpGrpcPort),
					},
					"http": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", otlpHttpPort),
					},
				},
			},
		},
		"exporters": map[string]any{
			"otlp/gcp_destination": map[string]any{
				"endpoint": fmt.Sprintf("127.0.0.1:%d", dest.port),
				"tls": map[string]any{
					"insecure": true,
				},
			},
		},
		"processors": map[string]any{
			"batch/gcp_destination_logs": map[string]any{
				"send_batch_size":     1,
				"send_batch_max_size": 1,
				"timeout":             "50ms",
			},
		},
		"service": map[string]any{
			"telemetry": map[string]any{
				"logs": map[string]any{
					"level": "warn",
				},
			},
		},
	}
	baseData, err := yaml.Marshal(baseConfigMap)
	require.NoError(t, err)
	basePath := filepath.Join(tmpDir, "base.yaml")
	require.NoError(t, os.WriteFile(basePath, baseData, 0o600))

	// 4. Write Custom Configuration using escape hatch tokens:
	// ${googlecontrolplane:component//global_policy_processor}
	// ${googlecontrolplane:component//active_destination_exporter}
	customRecvPort := getFreePort(t)
	customMap := map[string]any{
		"receivers": map[string]any{
			"otlp/custom": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", customRecvPort),
					},
				},
			},
		},
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs/custom": map[string]any{
					"receivers":  []any{"otlp/custom"},
					"processors": []any{"${googlecontrolplane:component//global_policy_processor}"},
					"exporters":  []any{"${googlecontrolplane:component//active_destination_exporter}"},
				},
			},
		},
	}
	customData, err := yaml.Marshal(customMap)
	require.NoError(t, err)
	customPath := filepath.Join(tmpDir, "custom.yaml")
	require.NoError(t, os.WriteFile(customPath, customData, 0o600))

	// 5. Build Collector with both URIs:
	// --config "googlecontrolplane:file://..." --config "file://..."
	gcpURI := fmt.Sprintf("googlecontrolplane:file://%s?base_config=%s", policyPath, basePath)
	customURI := fmt.Sprintf("file:%s", customPath)

	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactories(nil, nil),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{gcpURI, customURI},
				ProviderFactories: []confmap.ProviderFactory{
					googlecontrolplane.NewFactory(),
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()
	defer func() {
		col.Shutdown()
		<-colErrCh
	}()

	require.Eventually(t, func() bool {
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Send telemetry to custom receiver: 1 DEBUG (drop), 1 INFO (retain)
	recvAddr := fmt.Sprintf("127.0.0.1:%d", customRecvPort)
	cc, err := grpc.NewClient(recvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()

	logsClient := plogotlp.NewGRPCClient(cc)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	r1 := sl.LogRecords().AppendEmpty()
	r1.SetSeverityNumber(plog.SeverityNumberDebug)
	r1.SetSeverityText("DEBUG")
	r1.Body().SetStr("debug-custom-drop")

	r2 := sl.LogRecords().AppendEmpty()
	r2.SetSeverityNumber(plog.SeverityNumberInfo)
	r2.SetSeverityText("INFO")
	r2.Body().SetStr("info-custom-retain")

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
		return err == nil
	}, 5*time.Second, 100*time.Millisecond)

	// Verify Mock Destination received ONLY the INFO log from the custom pipeline
	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)

	var receivedLogs []string
	for _, l := range dest.logs.getLogs() {
		for i := 0; i < l.ResourceLogs().Len(); i++ {
			r := l.ResourceLogs().At(i)
			for j := 0; j < r.ScopeLogs().Len(); j++ {
				s := r.ScopeLogs().At(j)
				for k := 0; k < s.LogRecords().Len(); k++ {
					receivedLogs = append(receivedLogs, s.LogRecords().At(k).Body().AsString())
				}
			}
		}
	}
	assert.Equal(t, []string{"info-custom-retain"}, receivedLogs)
}

// --- Metrics Extraction Helper ---

func parseTestMetrics(rm *metricdata.ResourceMetrics) (int64, map[string]string, map[string]int64) {
	var rev int64
	statuses := make(map[string]string)
	evalCounts := make(map[string]int64)

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
						var policyID, status string
						for _, attr := range dp.Attributes.ToSlice() {
							switch attr.Key {
							case "policy_id":
								policyID = attr.Value.AsString()
							case "status":
								status = attr.Value.AsString()
							}
						}
						statuses[policyID] = status
					}
				}
			case "telemetry_policy_evaluations_total":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						var sig, action string
						for _, attr := range dp.Attributes.ToSlice() {
							switch attr.Key {
							case "signal":
								sig = attr.Value.AsString()
							case "action":
								action = attr.Value.AsString()
							}
						}
						key := sig + ":" + action
						evalCounts[key] += dp.Value
					}
				}
			}
		}
	}

	return rev, statuses, evalCounts
}

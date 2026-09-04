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
	"sync/atomic"
	"testing"

	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/extension/filepolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/extension/googlexdspolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
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
	streamCount atomic.Int32
}

func newMockDiscoveryServer() *mockDiscoveryServer {
	return &mockDiscoveryServer{
		reqCh:       make(chan *discoveryv3.DiscoveryRequest, 20),
		respCh:      make(chan *discoveryv3.DiscoveryResponse, 20),
		streamErrCh: make(chan error, 1),
	}
}

func (m *mockDiscoveryServer) StreamTelemetryCollectors(stream grpc.BidiStreamingServer[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]) error {
	m.streamCount.Add(1)
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

const typeURLStructuralViolation = "type.googleapis.com/test.StructuralViolationSourcePolicy"

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
	return createFactoriesWithInformerRegistry(meterProvider, controlplane.DefaultRegistry)
}

func createFactoriesWithInformerRegistry(meterProvider *sdkmetric.MeterProvider, infReg *controlplane.InformerRegistry) func() (otelcol.Factories, error) {
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

		xdsExtFactory := googlexdspolicy.NewFactory()
		if infReg != nil {
			xdsExtFactory = googlexdspolicy.NewFactoryWithRegistry(infReg)
		}

		return otelcol.Factories{
			Receivers: map[component.Type]receiver.Factory{
				component.MustNewType("otlp"):    otlpreceiver.NewFactory(),
				component.MustNewType("filelog"): filelogreceiver.NewFactory(),
			},
			Processors: map[component.Type]processor.Factory{
				component.MustNewType("batch"):  batchprocessor.NewFactory(),
				component.MustNewType("policy"): customPolicy,
			},
			Exporters: map[component.Type]exporter.Factory{
				component.MustNewType("otlp"): otlpexporter.NewFactory(),
			},
			Extensions: map[component.Type]extension.Factory{
				component.MustNewType("googleclientauth"): driver.NewGoogleClientAuthExtensionFactory(),
				component.MustNewType("googlexdspolicy"):  xdsExtFactory,
				component.MustNewType("filepolicy"):      filepolicy.NewFactory(),
			},
			Telemetry: otelconftelemetry.NewFactory(),
		}, nil
	}
}

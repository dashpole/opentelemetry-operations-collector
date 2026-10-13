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

package selfmetrics

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/queuebatchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// controllableOTLPServer is a local OTLP gRPC server that implements LogsService,
// MetricsService, and TraceService. It allows tests to stall or fail workload
// exports (e.g. LogsService/Export) while immediately accepting self-observability
// metrics on MetricsService/Export.
type controllableOTLPServer struct {
	plogotlp.UnimplementedGRPCServer

	grpcServer *grpc.Server
	endpoint   string

	stallLogsCh   chan struct{}
	logsInFlight  atomic.Int64
	logsExportErr atomic.Pointer[error]
	receivedLogs  atomic.Int64

	stallMetricsWorkloadCh chan struct{}
	stallTracesWorkloadCh  chan struct{}

	mu            sync.Mutex
	latestMetrics []ObservedMetricPoint
}

func (s *controllableOTLPServer) setLogsStalled(stalled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stalled {
		select {
		case <-s.stallLogsCh:
			s.stallLogsCh = make(chan struct{})
		default:
		}
	} else {
		select {
		case <-s.stallLogsCh:
		default:
			close(s.stallLogsCh)
		}
	}
}

func (s *controllableOTLPServer) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	s.logsInFlight.Add(1)
	defer s.logsInFlight.Add(-1)

	s.mu.Lock()
	ch := s.stallLogsCh
	s.mu.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
		return plogotlp.NewExportResponse(), ctx.Err()
	}

	if errPtr := s.logsExportErr.Load(); errPtr != nil && *errPtr != nil {
		return plogotlp.NewExportResponse(), *errPtr
	}

	s.receivedLogs.Add(int64(req.Logs().LogRecordCount()))
	return plogotlp.NewExportResponse(), nil
}

// metricsGRPCAdapter separates pmetricotlp.GRPCServer.Export method signature from plogotlp.
type metricsGRPCAdapter struct {
	pmetricotlp.UnimplementedGRPCServer
	parent *controllableOTLPServer
}

func (m *metricsGRPCAdapter) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	pts := ExtractObservedMetricPoints(req.Metrics())
	// Distinguish workload metrics vs collector self-metrics:
	isWorkload := false
	for _, pt := range pts {
		if pt.Name == "workload_test_metric" {
			isWorkload = true
			break
		}
	}
	if isWorkload {
		m.parent.mu.Lock()
		ch := m.parent.stallMetricsWorkloadCh
		m.parent.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return pmetricotlp.NewExportResponse(), ctx.Err()
		}
		return pmetricotlp.NewExportResponse(), nil
	}

	m.parent.mu.Lock()
	// Merge/update latest observed points by (Name, exporter, data_type) key while retaining history.
	m.parent.latestMetrics = append(m.parent.latestMetrics, pts...)
	m.parent.mu.Unlock()
	return pmetricotlp.NewExportResponse(), nil
}

type tracesGRPCAdapter struct {
	ptraceotlp.UnimplementedGRPCServer
	parent *controllableOTLPServer
}

func (tr *tracesGRPCAdapter) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	tr.parent.mu.Lock()
	ch := tr.parent.stallTracesWorkloadCh
	tr.parent.mu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
		return ptraceotlp.NewExportResponse(), ctx.Err()
	}
	return ptraceotlp.NewExportResponse(), nil
}

func newMultiSignalOTLPServer(t *testing.T) *controllableOTLPServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &controllableOTLPServer{
		grpcServer:             grpc.NewServer(),
		endpoint:               lis.Addr().String(),
		stallLogsCh:            make(chan struct{}),
		stallMetricsWorkloadCh: make(chan struct{}),
		stallTracesWorkloadCh:  make(chan struct{}),
	}
	close(s.stallLogsCh)
	close(s.stallMetricsWorkloadCh)
	close(s.stallTracesWorkloadCh)

	plogotlp.RegisterGRPCServer(s.grpcServer, s)
	pmetricotlp.RegisterGRPCServer(s.grpcServer, &metricsGRPCAdapter{parent: s})
	ptraceotlp.RegisterGRPCServer(s.grpcServer, &tracesGRPCAdapter{parent: s})

	go func() {
		_ = s.grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		s.grpcServer.Stop()
	})
	return s
}

// snapshotLatestMetrics returns the most recent ObservedMetricPoint per (Name, exporter, data_type).
func (s *controllableOTLPServer) snapshotLatestMetrics() []ObservedMetricPoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := make(map[string]ObservedMetricPoint)
	for _, pt := range s.latestMetrics {
		key := pt.Name + "|" + pt.Attributes["exporter"] + "|" + pt.Attributes["data_type"] + "|" + pt.Attributes["error.type"]
		byKey[key] = pt
	}
	out := make([]ObservedMetricPoint, 0, len(byKey))
	for _, pt := range byKey {
		out = append(out, pt)
	}
	return out
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// policyHarness wires an in-process OTel Collector pipeline using SelfMetricsPolicy
// and GCPDestinationPolicy (queue_batch + otlp_grpc + PeriodicMetricReader -> otlpreceiver).
type policyHarness struct {
	server           *controllableOTLPServer
	telSettings      component.TelemetrySettings
	logsQueueProc    processor.Logs
	metricsQueueProc processor.Metrics
	tracesQueueProc  processor.Traces
	otlpLogsExp      exporter.Logs
	selfReceiver     receiver.Metrics
	shutdownFn       func(context.Context) error
}

type harnessOptions struct {
	logsQueueMutator    func(*queuebatchprocessor.Config)
	metricsQueueMutator func(*queuebatchprocessor.Config)
	tracesQueueMutator  func(*queuebatchprocessor.Config)
	otlpExporterMutator func(*otlpexporter.Config)
	enableTracesPipe    bool
}

func startPolicyHarness(t *testing.T, opts harnessOptions) *policyHarness {
	t.Helper()
	ctx := context.Background()
	srv := newMultiSignalOTLPServer(t)
	selfPort := freeLocalPort(t)

	// 1. Evaluate SelfMetricsPolicy to verify policy config generation and extract telemetry config.
	smPolicy := &SelfMetricsPolicy{
		Name: "default_self_metrics",
		Port: selfPort,
	}
	evalCtx := smPolicy.ContextSetup(ctx, "collector-tab2-proto-001", "fleet-tab2")
	evalCtx = context.WithValue(evalCtx, contextKeyProjectID, "dashpole-dev")
	smConf, err := smPolicy.Evaluate(evalCtx)
	require.NoError(t, err)
	require.Equal(t, "Normal", smConf.Get("service::telemetry::metrics::level"))

	// 2. Evaluate GCPDestinationPolicy to verify default queue_batch and otlp_grpc settings.
	destPolicy := &gcpdestination.GCPDestinationPolicy{
		Name:      "default_gcp_destination",
		ProjectID: "dashpole-dev",
	}
	destConf, err := destPolicy.Evaluate(ctx)
	require.NoError(t, err)
	require.Equal(t, true, destConf.Get("processors::queue_batch/default_gcp_destination_batch_logs::block_on_overflow"))
	require.Equal(t, int64(10), destConf.Get("processors::queue_batch/default_gcp_destination_batch_logs::queue_size"))
	require.Equal(t, 5*time.Minute, destConf.Get("exporters::otlp_grpc/default_gcp_destination::retry_on_failure::max_elapsed_time"))

	// 3. Build real OTel SDK MeterProvider & TracerProvider via otelconftelemetry (matching SelfMetricsPolicy)
	// with PeriodicMetricReader interval reduced from 5000ms to 100ms for fast test collection.
	telFactory := otelconftelemetry.NewFactory()
	telCfg := telFactory.CreateDefaultConfig().(*otelconftelemetry.Config)
	require.NotNil(t, smConf.Get("service::telemetry"), "service::telemetry must exist")

	// Unmarshal the evaluated SelfMetricsPolicy telemetry block and override Periodic.Interval to 100ms.
	subTel, err := smConf.Sub("service::telemetry")
	require.NoError(t, err)
	require.NoError(t, subTel.Unmarshal(telCfg))
	fastInterval := 100
	telCfg.Metrics.MeterProvider.Readers[0].Periodic.Interval = &fastInterval
	// Silence internal batch log processor in unit tests so shutdown doesn't wait on self-log loops.
	telCfg.Logs.Processors = nil

	buildInfo := component.NewDefaultBuildInfo()
	baseTelSet := telemetry.Settings{BuildInfo: buildInfo}
	res, schemaURL, err := telFactory.CreateResource(ctx, baseTelSet, telCfg)
	require.NoError(t, err)
	baseTelSet.Resource = &res
	baseTelSet.SchemaURL = schemaURL

	logger, loggerShutdown, err := telFactory.CreateLogger(ctx, telemetry.LoggerSettings{
		Settings:       baseTelSet,
		BuildZapLogger: zap.Config.Build,
	}, telCfg)
	require.NoError(t, err)

	mp, err := telFactory.CreateMeterProvider(ctx, telemetry.MeterSettings{
		Settings: baseTelSet,
		Logger:   logger,
	}, telCfg)
	require.NoError(t, err)

	tp, err := telFactory.CreateTracerProvider(ctx, telemetry.TracerSettings{
		Settings: baseTelSet,
		Logger:   logger,
	}, telCfg)
	require.NoError(t, err)

	telSettings := component.TelemetrySettings{
		Logger:            logger,
		MeterProvider:     mp,
		TracerProvider:    tp,
		Resource:          res,
		ResourceSchemaURL: schemaURL,
	}

	host := componenttest.NewNopHost()

	// 4. Create the downstream otlp_grpc/default_gcp_destination exporter pointing to our local OTLP server.
	otlpExpFactory := otlpexporter.NewFactory()
	otlpExpID := component.MustNewIDWithName("otlp_grpc", "default_gcp_destination")
	otlpExpCfg := otlpExpFactory.CreateDefaultConfig().(*otlpexporter.Config)
	otlpExpCfg.ClientConfig.Endpoint = srv.endpoint
	otlpExpCfg.ClientConfig.TLS = configtls.ClientConfig{Insecure: true}
	// Match GCPDestinationPolicy: disable exporter sending_queue because queue_batch sits in front.
	otlpExpCfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	// Keep retry_on_failure.max_elapsed_time = 5m0s from GCPDestinationPolicy unless overridden.
	if opts.otlpExporterMutator != nil {
		opts.otlpExporterMutator(otlpExpCfg)
	}

	expSet := exporter.Settings{
		ID:                otlpExpID,
		TelemetrySettings: telSettings,
		BuildInfo:         buildInfo,
	}
	otlpLogsExp, err := otlpExpFactory.CreateLogs(ctx, expSet, otlpExpCfg)
	require.NoError(t, err)
	otlpMetricsExp, err := otlpExpFactory.CreateMetrics(ctx, expSet, otlpExpCfg)
	require.NoError(t, err)

	var otlpTracesExp exporter.Traces
	if opts.enableTracesPipe {
		otlpTracesExp, err = otlpExpFactory.CreateTraces(ctx, expSet, otlpExpCfg)
		require.NoError(t, err)
	}

	// 5. Create queue_batch/default_gcp_destination_batch_{logs,metrics,traces} processors matching GCPDestinationPolicy.
	qbFactory := queuebatchprocessor.NewFactory()

	qbLogsID := component.NewIDWithName(qbFactory.Type(), "default_gcp_destination_batch_logs")
	qbLogsCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
	// Fast flush for deterministic test execution (default GCPDestinationPolicy is 200ms, 8192).
	logsBatchCfg := qbLogsCfg.Batch.GetOrInsertDefault()
	logsBatchCfg.FlushTimeout = 20 * time.Millisecond
	logsBatchCfg.MinSize = 1
	logsBatchCfg.MaxSize = 1
	if opts.logsQueueMutator != nil {
		opts.logsQueueMutator(qbLogsCfg)
	}
	logsQueueProc, err := qbFactory.CreateLogs(ctx, processor.Settings{
		ID:                qbLogsID,
		TelemetrySettings: telSettings,
		BuildInfo:         buildInfo,
	}, qbLogsCfg, otlpLogsExp)
	require.NoError(t, err)

	qbMetricsID := component.NewIDWithName(qbFactory.Type(), "default_gcp_destination_batch_metrics")
	qbMetricsCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
	metricsBatchCfg := qbMetricsCfg.Batch.GetOrInsertDefault()
	metricsBatchCfg.FlushTimeout = 20 * time.Millisecond
	metricsBatchCfg.MinSize = 1
	metricsBatchCfg.MaxSize = 200
	if opts.metricsQueueMutator != nil {
		opts.metricsQueueMutator(qbMetricsCfg)
	}
	metricsQueueProc, err := qbFactory.CreateMetrics(ctx, processor.Settings{
		ID:                qbMetricsID,
		TelemetrySettings: telSettings,
		BuildInfo:         buildInfo,
	}, qbMetricsCfg, otlpMetricsExp)
	require.NoError(t, err)

	var tracesQueueProc processor.Traces
	if opts.enableTracesPipe {
		qbTracesID := component.NewIDWithName(qbFactory.Type(), "default_gcp_destination_batch_traces")
		qbTracesCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		tracesBatchCfg := qbTracesCfg.Batch.GetOrInsertDefault()
		tracesBatchCfg.FlushTimeout = 20 * time.Millisecond
		tracesBatchCfg.MinSize = 1
		tracesBatchCfg.MaxSize = 1
		if opts.tracesQueueMutator != nil {
			opts.tracesQueueMutator(qbTracesCfg)
		}
		tracesQueueProc, err = qbFactory.CreateTraces(ctx, processor.Settings{
			ID:                qbTracesID,
			TelemetrySettings: telSettings,
			BuildInfo:         buildInfo,
		}, qbTracesCfg, otlpTracesExp)
		require.NoError(t, err)
	}

	// 6. Create otlp/default_self_metrics receiver (from SelfMetricsPolicy) feeding directly into
	// the metrics export path so PeriodicMetricReader exports flow through the real OTLP receiver.
	otlpRecvFactory := otlpreceiver.NewFactory()
	otlpRecvID := component.MustNewIDWithName("otlp", "default_self_metrics")
	otlpRecvCfg := otlpRecvFactory.CreateDefaultConfig().(*otlpreceiver.Config)
	otlpRecvCfg.Protocols.HTTP = configoptional.None[otlpreceiver.HTTPConfig]()
	otlpRecvCfg.Protocols.GRPC.GetOrInsertDefault().NetAddr.Endpoint = net.JoinHostPort("localhost", itoaPort(selfPort))
	selfReceiver, err := otlpRecvFactory.CreateMetrics(ctx, receiver.Settings{
		ID:                otlpRecvID,
		TelemetrySettings: telSettings,
		BuildInfo:         buildInfo,
	}, otlpRecvCfg, otlpMetricsExp)
	require.NoError(t, err)

	// Start components in collector order: exporters -> processors -> receivers.
	require.NoError(t, otlpLogsExp.Start(ctx, host))
	require.NoError(t, otlpMetricsExp.Start(ctx, host))
	if otlpTracesExp != nil {
		require.NoError(t, otlpTracesExp.Start(ctx, host))
	}
	require.NoError(t, logsQueueProc.Start(ctx, host))
	require.NoError(t, metricsQueueProc.Start(ctx, host))
	if tracesQueueProc != nil {
		require.NoError(t, tracesQueueProc.Start(ctx, host))
	}
	require.NoError(t, selfReceiver.Start(ctx, host))

	shutdownFn := func(shutCtx context.Context) error {
		// Ensure server is unblocked before shutting down so draining queues do not hang.
		srv.setLogsStalled(false)
		// Shut down MeterProvider and TracerProvider BEFORE selfReceiver so the final
		// PeriodicMetricReader flush on Shutdown succeeds immediately against selfReceiver.
		_ = mp.Shutdown(shutCtx)
		_ = tp.Shutdown(shutCtx)
		_ = selfReceiver.Shutdown(shutCtx)
		_ = logsQueueProc.Shutdown(shutCtx)
		_ = metricsQueueProc.Shutdown(shutCtx)
		if tracesQueueProc != nil {
			_ = tracesQueueProc.Shutdown(shutCtx)
		}
		_ = otlpLogsExp.Shutdown(shutCtx)
		_ = otlpMetricsExp.Shutdown(shutCtx)
		if otlpTracesExp != nil {
			_ = otlpTracesExp.Shutdown(shutCtx)
		}
		_ = loggerShutdown.Shutdown(shutCtx)
		return nil
	}
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownFn(shutCtx)
	})

	return &policyHarness{
		server:           srv,
		telSettings:      telSettings,
		logsQueueProc:    logsQueueProc,
		metricsQueueProc: metricsQueueProc,
		tracesQueueProc:  tracesQueueProc,
		otlpLogsExp:      otlpLogsExp,
		selfReceiver:     selfReceiver,
		shutdownFn:       shutdownFn,
	}
}

func itoaPort(port int) string {
	return strconv.Itoa(port)
}

func makeLogBatch(numRecords int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < numRecords; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("test-log-record")
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}
	return ld
}

func makeMetricBatch(numPoints int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("workload_test_metric")
	g := m.SetEmptyGauge()
	for i := 0; i < numPoints; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetIntValue(int64(i + 1))
	}
	return md
}

func makeTraceBatch(numSpans int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < numSpans; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("workload-test-span")
	}
	return td
}

// TestQueueSaturation_BlockingQueue_BaselineVsProposed (Milestone 1, Test 1 — Scenario A)
// Empirically proves that under GCPDestinationPolicy's default block_on_overflow: true
// and max_elapsed_time: 5m0s, a 100% saturated queue (10/10) reports ZERO errors on
// otelcol_exporter_send_failed_log_records and otelcol_receiver_refused_log_records
// (the current 24-metric catalog is 100% blind), while otelcol_exporter_queue_size (10)
// and otelcol_exporter_queue_capacity (10) immediately report 100% (1.0) saturation.
func TestQueueSaturation_BlockingQueue_BaselineVsProposed(t *testing.T) {
	h := startPolicyHarness(t, harnessOptions{})
	h.server.setLogsStalled(true)

	ctx := context.Background()
	// Send 10 single-record log batches synchronously.
	// In memory_queue.go, Read() pops without decrementing mq.size, and onDone() only
	// decrements mq.size after partitionBatcher.flush() completes consumeFunc.
	// Thus all 10 batches (1 in-flight in the stalled gRPC call + 1 waiting on wp.workers
	// + 8 in mq.items) are counted in mq.size, bringing queue_size to 10/10.
	for i := 0; i < 10; i++ {
		require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(1)))
	}

	// Wait until the first batch is actively stalled inside the downstream LogsService/Export handler.
	require.Eventually(t, func() bool {
		return h.server.logsInFlight.Load() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Send an 11th single-record log batch in a background goroutine; because mq.size == 10 == mq.cap
	// and block_on_overflow == true, Offer() blocks in mq.hasMoreSpace.Wait(ctx).
	var eleventhDone atomic.Bool
	go func() {
		err := h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(1))
		assert.NoError(t, err)
		eleventhDone.Store(true)
	}()

	// Wait for PeriodicMetricReader to flush self-metrics and verify that while the 11th batch is blocked:
	// 1. Baseline 24-metric catalog sees 0 errors (send_failed == 0, receiver_refused == 0).
	// 2. Proposed catalog sees queue_size == 10, queue_capacity == 10, utilization == 1.0, enqueue_failed == 0.
	const queueExpID = "queue_batch/default_gcp_destination_batch_logs"
	const otlpExpID = "otlp_grpc/default_gcp_destination"

	var saturatedView CatalogHealthView
	require.Eventually(t, func() bool {
		pts := h.server.snapshotLatestMetrics()
		saturatedView = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
		return saturatedView.ProposedQueueSize == 10 && saturatedView.ProposedQueueCapacity == 10
	}, 3*time.Second, 25*time.Millisecond)

	// Verify the 11th producer is still blocked in Offer()!
	assert.False(t, eleventhDone.Load(), "11th batch must remain blocked in Offer() while queue is 10/10")

	// Assert Baseline-vs-Change while 100% saturated:
	t.Logf("Saturated Queue Health View: %s", saturatedView.String())
	assert.Equal(t, int64(0), saturatedView.BaselineSendFailedLogs, "Baseline send_failed_log_records is 0 while stalled inside 5m0s retry window")
	assert.Equal(t, int64(0), saturatedView.BaselineReceiverRefusedLogs, "Baseline receiver_refused_log_records is 0 when pull receiver blocks on Offer()")
	assert.False(t, saturatedView.BaselineDetectsIssue, "Baseline 24-metric catalog MUST be blind to 100% saturated blocking queue")

	assert.Equal(t, int64(10), saturatedView.ProposedQueueSize)
	assert.Equal(t, int64(10), saturatedView.ProposedQueueCapacity)
	assert.InDelta(t, 1.0, saturatedView.ProposedQueueUtilization, 1e-9)
	assert.Equal(t, int64(0), saturatedView.ProposedEnqueueFailedLogs, "Blocking queue does not increment enqueue_failed while callers wait")
	assert.True(t, saturatedView.ProposedDetectsSaturation, "Proposed catalog MUST detect >=80% queue saturation (1.0)")

	// Now release the downstream LogsService/Export stall and verify the queue drains to 0/10
	// and all 11 records are exported.
	h.server.setLogsStalled(false)
	require.Eventually(t, func() bool {
		return eleventhDone.Load() && h.server.receivedLogs.Load() == 11
	}, 3*time.Second, 20*time.Millisecond)

	var drainedView CatalogHealthView
	require.Eventually(t, func() bool {
		pts := h.server.snapshotLatestMetrics()
		drainedView = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
		var otlpSent, qbSent int64
		for _, pt := range pts {
			if pt.Name == MetricExporterSentLogRecords {
				switch pt.Attributes["exporter"] {
				case otlpExpID:
					otlpSent = pt.IntValue
				case queueExpID:
					qbSent = pt.IntValue
				}
			}
		}
		return drainedView.ProposedQueueSize == 0 && otlpSent == 11 && qbSent == 11
	}, 3*time.Second, 25*time.Millisecond)

	t.Logf("Drained Queue Health View: %s", drainedView.String())
	assert.Equal(t, int64(0), drainedView.ProposedQueueSize)
	assert.Equal(t, int64(10), drainedView.ProposedQueueCapacity)
	assert.InDelta(t, 0.0, drainedView.ProposedQueueUtilization, 1e-9)
}

// TestQueueEntryDrops_EnqueueFailedVsSendFailed (Milestone 1, Test 2 — Scenario B)
// Empirically proves across all 3 queue-entry rejection paths that
// otelcol_exporter_send_failed_log_records stays 0 while
// otelcol_exporter_enqueue_failed_log_records increments by the exact dropped item count.
func TestQueueEntryDrops_EnqueueFailedVsSendFailed(t *testing.T) {
	const queueExpID = "queue_batch/default_gcp_destination_batch_logs"
	const otlpExpID = "otlp_grpc/default_gcp_destination"

	t.Run("2a_NonBlockingOverflow_ErrQueueIsFull", func(t *testing.T) {
		h := startPolicyHarness(t, harnessOptions{
			logsQueueMutator: func(cfg *queuebatchprocessor.Config) {
				cfg.BlockOnOverflow = false
				cfg.QueueSize = 10
			},
		})
		h.server.setLogsStalled(true)

		ctx := context.Background()
		for i := 0; i < 10; i++ {
			require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(1)))
		}
		// Queue is now 10/10. Send 3 additional batches of 5 records each (15 records total).
		for i := 0; i < 3; i++ {
			err := h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(5))
			require.Error(t, err, "Offer() must fail with ErrQueueIsFull when block_on_overflow is false and queue is 10/10")
		}

		var view CatalogHealthView
		require.Eventually(t, func() bool {
			pts := h.server.snapshotLatestMetrics()
			view = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
			return view.ProposedEnqueueFailedLogs == 15
		}, 3*time.Second, 25*time.Millisecond)

		t.Logf("Subtest 2a (block_on_overflow=false) View: %s", view.String())
		assert.Equal(t, int64(0), view.BaselineSendFailedLogs, "send_failed_log_records is 0 on queue overflow drops")
		assert.False(t, view.BaselineDetectsIssue, "Baseline catalog is blind to queue-entry overflow drops")
		assert.Equal(t, int64(15), view.ProposedEnqueueFailedLogs)
		assert.True(t, view.ProposedDetectsDrops)
	})

	t.Run("2b_OversizedBatch_WithBlockOnOverflowTrue_errSizeTooLarge", func(t *testing.T) {
		// Even with GCPDestinationPolicy's default block_on_overflow: true, if sizer is items
		// (or bytes) and a single incoming batch exceeds queue_size capacity (memory_queue.go:75),
		// Offer() immediately rejects the batch with errSizeTooLarge without blocking!
		h := startPolicyHarness(t, harnessOptions{
			logsQueueMutator: func(cfg *queuebatchprocessor.Config) {
				cfg.BlockOnOverflow = true
				cfg.QueueSize = 10
				require.NoError(t, cfg.Sizer.UnmarshalText([]byte("items")))
			},
		})

		ctx := context.Background()
		err := h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(25))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "element size too large")

		var view CatalogHealthView
		require.Eventually(t, func() bool {
			pts := h.server.snapshotLatestMetrics()
			view = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
			return view.ProposedEnqueueFailedLogs == 25
		}, 3*time.Second, 25*time.Millisecond)

		t.Logf("Subtest 2b (block_on_overflow=true, oversized batch 25 > 10) View: %s", view.String())
		assert.Equal(t, int64(0), view.BaselineSendFailedLogs, "send_failed_log_records is 0 when oversized batch is rejected at queue entry")
		assert.False(t, view.BaselineDetectsIssue)
		assert.Equal(t, int64(25), view.ProposedEnqueueFailedLogs)
		assert.True(t, view.ProposedDetectsDrops)
	})

	t.Run("2c_ContextDeadlineExceeded_WhileBlockedInOffer", func(t *testing.T) {
		// With GCPDestinationPolicy's default block_on_overflow: true, when the queue is 10/10
		// and a caller's context deadline expires while waiting in mq.hasMoreSpace.Wait(ctx)
		// (memory_queue.go:116), Offer() returns context.DeadlineExceeded and increments enqueue_failed_*.
		h := startPolicyHarness(t, harnessOptions{})
		h.server.setLogsStalled(true)

		ctx := context.Background()
		for i := 0; i < 10; i++ {
			require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(1)))
		}

		shortCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()
		err := h.logsQueueProc.ConsumeLogs(shortCtx, makeLogBatch(7))
		require.ErrorIs(t, err, context.DeadlineExceeded)

		var view CatalogHealthView
		require.Eventually(t, func() bool {
			pts := h.server.snapshotLatestMetrics()
			view = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
			return view.ProposedEnqueueFailedLogs == 7
		}, 3*time.Second, 25*time.Millisecond)

		t.Logf("Subtest 2c (block_on_overflow=true, ctx.DeadlineExceeded) View: %s", view.String())
		assert.Equal(t, int64(0), view.BaselineSendFailedLogs, "send_failed_log_records is 0 when blocked Offer() times out")
		assert.False(t, view.BaselineDetectsIssue)
		assert.Equal(t, int64(7), view.ProposedEnqueueFailedLogs)
		assert.True(t, view.ProposedDetectsDrops)
	})
}

// TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit (Milestone 1, Test 3 — Scenarios C & D)
func TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit(t *testing.T) {
	t.Run("3a_ExactMetricSchemaAudit_AcrossLogsMetricsTraces", func(t *testing.T) {
		h := startPolicyHarness(t, harnessOptions{
			enableTracesPipe: true,
			logsQueueMutator: func(cfg *queuebatchprocessor.Config) {
				cfg.QueueSize = 10
				require.NoError(t, cfg.Sizer.UnmarshalText([]byte("items")))
			},
			metricsQueueMutator: func(cfg *queuebatchprocessor.Config) {
				cfg.QueueSize = 10
				require.NoError(t, cfg.Sizer.UnmarshalText([]byte("items")))
			},
			tracesQueueMutator: func(cfg *queuebatchprocessor.Config) {
				cfg.QueueSize = 10
				require.NoError(t, cfg.Sizer.UnmarshalText([]byte("items")))
			},
		})

		ctx := context.Background()
		// Trigger enqueue_failed on all 3 signals by sending oversized batches (15 > 10 items).
		require.Error(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(15)))
		require.Error(t, h.metricsQueueProc.ConsumeMetrics(ctx, makeMetricBatch(15)))
		require.Error(t, h.tracesQueueProc.ConsumeTraces(ctx, makeTraceBatch(15)))

		var pts []ObservedMetricPoint
		require.Eventually(t, func() bool {
			pts = h.server.snapshotLatestMetrics()
			found := 0
			for _, spec := range NormativeObsQueueSpecs() {
				for _, pt := range pts {
					if pt.Name == spec.Name && pt.IntValue > 0 {
						found++
						break
					}
				}
			}
			// queue_size may be 0 after oversized rejection; check capacity + 3 enqueue_failed > 0.
			return found >= 4
		}, 3*time.Second, 25*time.Millisecond)

		for _, spec := range NormativeObsQueueSpecs() {
			var matched []ObservedMetricPoint
			for _, pt := range pts {
				if pt.Name == spec.Name {
					matched = append(matched, pt)
				}
			}
			require.NotEmpty(t, matched, "expected metric %s to be emitted", spec.Name)
			for _, pt := range matched {
				t.Logf("Schema Audit: name=%s type=%s unit=%q monotonic=%v val=%d attrs=%v",
					pt.Name, pt.Type.String(), pt.Unit, pt.IsMonotonic, pt.IntValue, pt.Attributes)
				assert.Equal(t, spec.Type, pt.Type, "type mismatch for %s", spec.Name)
				assert.Equal(t, spec.Unit, pt.Unit, "unit mismatch for %s", spec.Name)
				if spec.Type == pmetric.MetricTypeSum {
					assert.Equal(t, spec.IsMonotonic, pt.IsMonotonic, "monotonic mismatch for %s", spec.Name)
					assert.Equal(t, pmetric.AggregationTemporalityCumulative, pt.Temporality, "temporality mismatch for %s", spec.Name)
				}
				for _, reqKey := range spec.RequiredAttrKeys {
					assert.NotEmpty(t, pt.Attributes[reqKey], "metric %s must include attribute %q", spec.Name, reqKey)
				}
				for _, forbidKey := range spec.ForbiddenAttrs {
					_, exists := pt.Attributes[forbidKey]
					assert.False(t, exists, "metric %s must NOT include attribute %q", spec.Name, forbidKey)
				}
			}
		}
	})

	t.Run("3b_ScenarioC_SizerAgnosticUtilization_RequestsItemsBytes", func(t *testing.T) {
		const queueExpID = "queue_batch/default_gcp_destination_batch_logs"
		const otlpExpID = "otlp_grpc/default_gcp_destination"

		testCases := []struct {
			name        string
			sizer       string
			capacity    int64
			fillBatches int
			recordsEach int
			wantUtil    float64
		}{
			{
				name:        "sizer_requests_5_of_10",
				sizer:       "requests",
				capacity:    10,
				fillBatches: 5,
				recordsEach: 4, // 5 requests * 4 records = 5 request units in queue
				wantUtil:    0.50,
			},
			{
				name:        "sizer_items_25_of_50",
				sizer:       "items",
				capacity:    50,
				fillBatches: 5,
				recordsEach: 5, // 5 batches * 5 items = 25 item units in queue
				wantUtil:    0.50,
			},
			{
				name:        "sizer_bytes_half_full",
				sizer:       "bytes",
				capacity:    0, // dynamically set to 2 * singleBatchBytes below
				fillBatches: 1,
				recordsEach: 4,
				wantUtil:    0.50,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sampleBatch := makeLogBatch(tc.recordsEach)
				capVal := tc.capacity
				if tc.sizer == "bytes" {
					// Measure exact proto marshaled byte size of 1 batch so capacity is exactly 2x batch bytes.
					var sizer plog.ProtoMarshaler
					capVal = int64(sizer.LogsSize(sampleBatch) * 2)
				}

				h := startPolicyHarness(t, harnessOptions{
					logsQueueMutator: func(cfg *queuebatchprocessor.Config) {
						cfg.QueueSize = capVal
						require.NoError(t, cfg.Sizer.UnmarshalText([]byte(tc.sizer)))
					},
				})
				h.server.setLogsStalled(true)

				ctx := context.Background()
				for i := 0; i < tc.fillBatches; i++ {
					require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(tc.recordsEach)))
				}

				var view CatalogHealthView
				var sizeUnit, capUnit string
				require.Eventually(t, func() bool {
					pts := h.server.snapshotLatestMetrics()
					view = EvaluateCatalogHealth(pts, queueExpID, otlpExpID, "logs")
					for _, pt := range pts {
						if pt.Name == MetricExporterQueueSize && pt.Attributes["exporter"] == queueExpID {
							sizeUnit = pt.Unit
						}
						if pt.Name == MetricExporterQueueCapacity && pt.Attributes["exporter"] == queueExpID {
							capUnit = pt.Unit
						}
					}
					return view.ProposedQueueSize > 0 && view.ProposedQueueCapacity == capVal
				}, 3*time.Second, 25*time.Millisecond)

				t.Logf("Sizer=%s: queue_size=%d (unit=%q), queue_capacity=%d (unit=%q), utilization=%.2f",
					tc.sizer, view.ProposedQueueSize, sizeUnit, view.ProposedQueueCapacity, capUnit, view.ProposedQueueUtilization)
				assert.Equal(t, "{batch}", sizeUnit, "upstream metadata.yaml hardcodes unit={batch} regardless of sizer")
				assert.Equal(t, "{batch}", capUnit, "upstream metadata.yaml hardcodes unit={batch} regardless of sizer")
				assert.InDelta(t, tc.wantUtil, view.ProposedQueueUtilization, 1e-6,
					"queue_size / queue_capacity must be dimensionless %.2f across %s sizer", tc.wantUtil, tc.sizer)
			})
		}
	})

	t.Run("3c_ScenarioD_SendFailedDoubleCountingBetweenQueueBatchAndOtlpGrpc", func(t *testing.T) {
		const queueExpID = "queue_batch/default_gcp_destination_batch_logs"
		const otlpExpID = "otlp_grpc/default_gcp_destination"

		h := startPolicyHarness(t, harnessOptions{})
		// Configure downstream OTLP gRPC server to return a permanent error (codes.InvalidArgument)
		// so retrySender immediately fails without waiting 5m0s.
		permErr := status.Error(codes.InvalidArgument, "permanent downstream rejection")
		h.server.logsExportErr.Store(&permErr)

		ctx := context.Background()
		require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(6)))

		var qbFailed, otlpFailed int64
		require.Eventually(t, func() bool {
			pts := h.server.snapshotLatestMetrics()
			qbFailed, otlpFailed = 0, 0
			for _, pt := range pts {
				if pt.Name == MetricExporterSendFailedLogRecords {
					switch pt.Attributes["exporter"] {
					case queueExpID:
						qbFailed = pt.IntValue
					case otlpExpID:
						otlpFailed = pt.IntValue
					}
				}
			}
			return qbFailed == 6 && otlpFailed == 6
		}, 3*time.Second, 25*time.Millisecond)

		unfilteredSum := qbFailed + otlpFailed
		t.Logf("Scenario D (Permanent downstream error for 6 log records): send_failed[otlp_grpc]=%d, send_failed[queue_batch]=%d, unfiltered_sum=%d (2x double-counted!)",
			otlpFailed, qbFailed, unfilteredSum)
		assert.Equal(t, int64(6), otlpFailed)
		assert.Equal(t, int64(6), qbFailed)
		assert.Equal(t, int64(12), unfilteredSum,
			"Summing otelcol_exporter_send_failed_log_records without filtering exporter double-counts 2x because queuebatchprocessor wraps exporterhelper.NewLogs")
	})
}

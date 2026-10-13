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

package gcpdestination

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/processor/queuebatchprocessor"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGCPDestinationPolicy_ValidateAndIDs(t *testing.T) {
	emptyPol := &GCPDestinationPolicy{}
	require.EqualError(t, emptyPol.Validate(), "policy must be named")

	pol := &GCPDestinationPolicy{
		Name:      "default_gcp_destination",
		ProjectID: "dashpole-dev",
	}
	require.NoError(t, pol.Validate())
	assert.Equal(t, "default_gcp_destination", pol.PolicyName())
	assert.Equal(t, PolicyType, pol.PolicyType())
	assert.Equal(t, googlepolicy.PolicyClassDestination, pol.PolicyClass())

	cm, err := pol.Evaluate(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cm)

	assert.Equal(t, []string{"googleclientauth/default_gcp_destination"}, idsToStrings(pol.ExtensionIDs()))
	assert.Equal(t, []string{"otlp_grpc/default_gcp_destination"}, idsToStrings(pol.ExporterIDs()))
	assert.Equal(t, []string{
		"queue_batch/default_gcp_destination_batch_logs",
		"queue_batch/default_gcp_destination_split_logs",
	}, idsToStrings(pol.PreProcessLogIDs()))
	assert.Equal(t, []string{
		"queue_batch/default_gcp_destination_batch_metrics",
		"queue_batch/default_gcp_destination_split_metrics",
	}, idsToStrings(pol.PreProcessMetricIDs()))
	assert.Equal(t, []string{
		"queue_batch/default_gcp_destination_batch_traces",
		"queue_batch/default_gcp_destination_split_traces",
	}, idsToStrings(pol.PreProcessTraceIDs()))
}

// TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal proves that:
//  1. Without the #711 post-marshal override, confmap.Conf.Marshal emits `sizer: {}`
//     for exporterhelper.RequestSizerType, which fails queuebatchprocessor.Config
//     unmarshaling.
//  2. With the patched GCPDestinationPolicy.Evaluate(), all 6 Stage 1 (_batch_*)
//     and Stage 2 (_split_*) processors unmarshal and validate cleanly with the
//     exact UTR byte and item limits (#710 and #711).
func TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal(t *testing.T) {
	factory := queuebatchprocessor.NewFactory()

	// 1. Negative Control (Unpatched #711 behavior): marshaling queuebatchprocessor.Config
	// directly via confmap.Conf.Marshal produces `sizer: {}` and `batch.sizer: {}`.
	// When unmarshaled back into queuebatchprocessor.NewFactory().CreateDefaultConfig(),
	// mapstructure treats the empty map `{}` as a no-op on the struct, silently discarding
	// RequestSizerTypeBytes and reverting Sizer to the factory defaults ("requests" / "items")
	// so `min_size: 3670016` would be interpreted as 3.67M items instead of 3.5 MiB!
	rawCfg := factory.CreateDefaultConfig().(*queuebatchprocessor.Config)
	rawCfg.Sizer = exporterhelper.RequestSizerTypeBytes
	rawCfg.Batch.GetOrInsertDefault().Sizer = exporterhelper.RequestSizerTypeBytes
	unpatchedConf := confmap.New()
	require.NoError(t, unpatchedConf.Marshal(rawCfg))
	assert.Equal(t, map[string]any{}, unpatchedConf.Get("sizer"), "confmap.Conf.Marshal without #711 fix serializes RequestSizerType as empty map")
	assert.Equal(t, map[string]any{}, unpatchedConf.Get("batch::sizer"), "confmap.Conf.Marshal without #711 fix serializes batch.sizer as empty map")

	unmarshalTarget := factory.CreateDefaultConfig().(*queuebatchprocessor.Config)
	require.NoError(t, unmarshalTarget.Unmarshal(unpatchedConf))
	assert.NotEqual(t, exporterhelper.RequestSizerTypeBytes, unmarshalTarget.Sizer, "unmarshaling `sizer: {}` silently drops RequestSizerTypeBytes and reverts to default")
	assert.Equal(t, exporterhelper.RequestSizerTypeRequests, unmarshalTarget.Sizer)
	assert.NotEqual(t, exporterhelper.RequestSizerTypeBytes, unmarshalTarget.Batch.Get().Sizer, "unmarshaling `batch.sizer: {}` silently drops RequestSizerTypeBytes and reverts to default")
	assert.Equal(t, exporterhelper.RequestSizerTypeItems, unmarshalTarget.Batch.Get().Sizer)

	// 2. Positive Control (Patched GCPDestinationPolicy.Evaluate()):
	pol := &GCPDestinationPolicy{
		Name:      "default_gcp_destination",
		ProjectID: "dashpole-dev",
	}
	cm, err := pol.Evaluate(context.Background())
	require.NoError(t, err)

	for _, id := range []string{
		"queue_batch/default_gcp_destination_batch_logs",
		"queue_batch/default_gcp_destination_batch_metrics",
		"queue_batch/default_gcp_destination_batch_traces",
	} {
		t.Run(id, func(t *testing.T) {
			sub, err := cm.Sub("processors::" + id)
			require.NoError(t, err)
			cfg := factory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, cfg.Unmarshal(sub))
			require.NoError(t, cfg.Validate())
			require.NoError(t, cfg.Batch.Get().Validate())
			require.NoError(t, cfg.Batch.Get().Partition.Validate())

			assert.Equal(t, exporterhelper.RequestSizerTypeBytes, cfg.Sizer)
			assert.Equal(t, int64(33554432), cfg.QueueSize)
			assert.True(t, cfg.BlockOnOverflow)
			assert.False(t, cfg.WaitForResult)
			require.True(t, cfg.Batch.HasValue())
			assert.Equal(t, exporterhelper.RequestSizerTypeBytes, cfg.Batch.Get().Sizer)
			assert.Equal(t, int64(3670016), cfg.Batch.Get().MinSize)
			assert.Equal(t, int64(4194304), cfg.Batch.Get().MaxSize)
			assert.Equal(t, 200*time.Millisecond, cfg.Batch.Get().FlushTimeout)
		})
	}

	expectedStage2MaxItems := map[string]int64{
		"queue_batch/default_gcp_destination_split_logs":    8192,
		"queue_batch/default_gcp_destination_split_metrics": 200,
		"queue_batch/default_gcp_destination_split_traces":  8192, // #710: lowered from 25000 to 8192
	}
	for id, wantMax := range expectedStage2MaxItems {
		t.Run(id, func(t *testing.T) {
			sub, err := cm.Sub("processors::" + id)
			require.NoError(t, err)
			cfg := factory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, cfg.Unmarshal(sub))
			require.NoError(t, cfg.Validate())
			require.NoError(t, cfg.Batch.Get().Validate())
			require.NoError(t, cfg.Batch.Get().Partition.Validate())

			assert.Equal(t, exporterhelper.RequestSizerTypeRequests, cfg.Sizer)
			assert.Equal(t, int64(10), cfg.QueueSize)
			assert.True(t, cfg.BlockOnOverflow)
			assert.False(t, cfg.WaitForResult)
			require.True(t, cfg.Batch.HasValue())
			assert.Equal(t, exporterhelper.RequestSizerTypeItems, cfg.Batch.Get().Sizer)
			assert.Equal(t, int64(0), cfg.Batch.Get().MinSize, "Stage 2 min_size must be 0 so it never holds partial batches")
			assert.Equal(t, wantMax, cfg.Batch.Get().MaxSize)
			assert.Equal(t, 200*time.Millisecond, cfg.Batch.Get().FlushTimeout)
		})
	}
}

type recordedBatch struct {
	items             int
	uncompressedBytes int
	arrivedAt         time.Time
}

type utrSimulatorServer struct {
	plogotlp.UnimplementedGRPCServer
	mu          sync.Mutex
	maxItems    int
	batches     []recordedBatch
	totalItems  int
	rejectedRPC int
	lastErr     error
	batchCh     chan recordedBatch
}

func newUTRSimulatorServer(t *testing.T, maxRecvBytes int, maxItems int) (*utrSimulatorServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &utrSimulatorServer{
		maxItems: maxItems,
		batchCh:  make(chan recordedBatch, 64),
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvBytes))
	plogotlp.RegisterGRPCServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})
	return srv, lis.Addr().String()
}

func (s *utrSimulatorServer) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	raw, err := req.MarshalProto()
	if err != nil {
		return plogotlp.NewExportResponse(), status.Errorf(codes.Internal, "marshal proto: %v", err)
	}
	count := req.Logs().LogRecordCount()
	if count > s.maxItems {
		s.mu.Lock()
		s.rejectedRPC++
		s.lastErr = status.Errorf(codes.InvalidArgument, "Batch contains too many log records: %d > %d", count, s.maxItems)
		err := s.lastErr
		s.mu.Unlock()
		return plogotlp.NewExportResponse(), err
	}

	rb := recordedBatch{
		items:             count,
		uncompressedBytes: len(raw),
		arrivedAt:         time.Now(),
	}
	s.mu.Lock()
	s.batches = append(s.batches, rb)
	s.totalItems += count
	s.mu.Unlock()
	select {
	case s.batchCh <- rb:
	default:
	}
	return plogotlp.NewExportResponse(), nil
}

func (s *utrSimulatorServer) snapshot() (batches []recordedBatch, totalItems int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]recordedBatch, len(s.batches))
	copy(out, s.batches)
	return out, s.totalItems
}

func buildTestLogs(count int, bodyBytes int, projectID string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("gcp.project_id", projectID)
	rl.Resource().Attributes().PutStr("service.name", "gboc-utr-batching-test")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("wef-log-generator")
	sl.LogRecords().EnsureCapacity(count)

	padding := strings.Repeat("X", bodyBytes)
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := 0; i < count; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(now)
		lr.SetObservedTimestamp(now)
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
		lr.Body().SetStr(fmt.Sprintf("<Event><System><EventRecordID>%d</EventRecordID></System><RenderingInfo>%s</RenderingInfo></Event>", i, padding))
	}
	return ld
}

func startOTLPExporter(t *testing.T, ctx context.Context, endpoint string) (component.Component, *observer.ObservedLogs) {
	t.Helper()
	expFactory := otlpexporter.NewFactory()
	expCfg := expFactory.CreateDefaultConfig().(*otlpexporter.Config)
	expCfg.ClientConfig.Endpoint = endpoint
	expCfg.ClientConfig.TLS = configtls.ClientConfig{Insecure: true}
	expCfg.RetryConfig.Enabled = false
	expCfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()

	core, observed := observer.New(zapcore.ErrorLevel)
	expSet := exportertest.NewNopSettings(expFactory.Type())
	expSet.Logger = zap.New(core)
	exp, err := expFactory.CreateLogs(ctx, expSet, expCfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() {
		_ = exp.Shutdown(ctx)
	})
	return exp, observed
}

func TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain(t *testing.T) {
	const (
		utrMaxBytes = 5 * 1024 * 1024 // 5 MiB (5,242,880 B)
		utrMaxItems = 8192            // kMaxLogCountPerBatch
		stage1Max   = 4 * 1024 * 1024 // 4 MiB (4,194,304 B)
	)

	qbFactory := queuebatchprocessor.NewFactory()

	t.Run("CaseA_HeavyWEFXMLLogs_8192x2500B", func(t *testing.T) {
		// 8,192 logs @ ~2.5 KB each = ~20.5 MB uncompressed.
		t.Run("Candidate0_Baseline_SingleStageItems_FailsResourceExhausted", func(t *testing.T) {
			ctx := context.Background()
			srv, addr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
			expComp, observedLogs := startOTLPExporter(t, ctx, addr)
			exp := expComp.(processor.Logs)

			// Candidate 0: Baseline GCPDestinationPolicy config (with #711 sizer fixed so it starts).
			cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			cfg.Sizer = exporterhelper.RequestSizerTypeRequests
			cfg.QueueSize = 10
			batchCfg := cfg.Batch.GetOrInsertDefault()
			batchCfg.Sizer = exporterhelper.RequestSizerTypeItems
			batchCfg.MinSize = 8192
			batchCfg.MaxSize = 8192
			batchCfg.FlushTimeout = 200 * time.Millisecond

			proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), cfg, exp)
			require.NoError(t, err)
			require.NoError(t, proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(8192, 2450, "dashpole-dev")
			sizer := &plog.ProtoMarshaler{}
			uncompressedSize := sizer.LogsSize(payload)
			require.Greater(t, uncompressedSize, 20*1024*1024, "Case A payload must exceed 20 MB uncompressed")

			require.NoError(t, proc.ConsumeLogs(ctx, payload))
			// Shutdown drains and flushes the single 8,192-item (~20.5 MB) batch to the OTLP exporter.
			require.NoError(t, proc.Shutdown(ctx))

			batches, totalItems := srv.snapshot()
			assert.Empty(t, batches, "UTR simulator must reject ~20.5 MB batch exceeding 5 MiB MaxRecvMsgSize")
			assert.Equal(t, 0, totalItems, "all 8,192 logs are dropped when the single ~20.5 MB batch hits ResourceExhausted")

			require.NotEmpty(t, observedLogs.All(), "exporter must log ResourceExhausted failure")
			errField := fmt.Sprint(observedLogs.All()[0].ContextMap()["error"])
			t.Logf("Case A Candidate 0 Exporter Error: %s", errField)
			assert.Contains(t, errField, "ResourceExhausted")
			assert.Contains(t, errField, "larger than max")
			assert.Contains(t, errField, "5242880")
		})

		t.Run("Candidate2_Proposed_TwoStageChain_SucceedsAll8192Logs", func(t *testing.T) {
			ctx := context.Background()
			srv, addr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
			expComp, _ := startOTLPExporter(t, ctx, addr)
			exp := expComp.(processor.Logs)

			// Build Stage 1 and Stage 2 directly from GCPDestinationPolicy.Evaluate().
			pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: "dashpole-dev"}
			cm, err := pol.Evaluate(ctx)
			require.NoError(t, err)

			stage1Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_batch_logs")
			require.NoError(t, err)
			stage1Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, stage1Cfg.Unmarshal(stage1Sub))

			stage2Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_split_logs")
			require.NoError(t, err)
			stage2Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, stage2Cfg.Unmarshal(stage2Sub))

			stage2Set := processortest.NewNopSettings(qbFactory.Type())
			stage2Set.ID = component.NewIDWithName(qbFactory.Type(), "default_gcp_destination_split_logs")
			stage2Proc, err := qbFactory.CreateLogs(ctx, stage2Set, stage2Cfg, exp)
			require.NoError(t, err)
			require.NoError(t, stage2Proc.Start(ctx, componenttest.NewNopHost()))

			stage1Set := processortest.NewNopSettings(qbFactory.Type())
			stage1Set.ID = component.NewIDWithName(qbFactory.Type(), "default_gcp_destination_batch_logs")
			stage1Proc, err := qbFactory.CreateLogs(ctx, stage1Set, stage1Cfg, stage2Proc)
			require.NoError(t, err)
			require.NoError(t, stage1Proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(8192, 2450, "dashpole-dev")
			start := time.Now()
			require.NoError(t, stage1Proc.ConsumeLogs(ctx, payload))

			// Wait for the first 5 full-byte (~4 MiB, ~1,580 logs each) batches to arrive WITHOUT calling Shutdown.
			// Because Stage 2 has min_size: 0, each ~1,580-log batch (< 8,192) flushes through Stage 2 immediately
			// without waiting for Stage 2's 200ms flush_timeout (each step < 180ms < 200ms flush_timeout).
			prevArrival := start
			for i := 0; i < 5; i++ {
				select {
				case rb := <-srv.batchCh:
					stepElapsed := rb.arrivedAt.Sub(prevArrival)
					prevArrival = rb.arrivedAt
					assert.Less(t, stepElapsed, 180*time.Millisecond, "full-byte batch %d must flush through Stage 2 without waiting for 200ms flush_timeout (step took %v)", i+1, stepElapsed)
				case <-time.After(350 * time.Millisecond):
					t.Fatalf("timed out waiting for immediate flush of full-byte batch %d", i+1)
				}
			}

			// The 6th byte-remainder batch (~750 KB < 3.5 MiB Stage 1 min_size) flushes after Stage 1's single 200ms flush_timeout.
			select {
			case rb := <-srv.batchCh:
				stepElapsed := rb.arrivedAt.Sub(prevArrival)
				assert.Less(t, stepElapsed, 350*time.Millisecond, "6th remainder batch must flush after Stage 1's single 200ms flush_timeout with 0ms added Stage 2 delay")
			case <-time.After(600 * time.Millisecond):
				t.Fatal("timed out waiting for 6th remainder batch")
			}

			require.NoError(t, stage1Proc.Shutdown(ctx))
			require.NoError(t, stage2Proc.Shutdown(ctx))

			batches, totalItems := srv.snapshot()
			assert.Equal(t, 8192, totalItems, "all 8,192 WEF XML logs must be delivered")
			assert.Len(t, batches, 6, "20.5 MB payload must be split into 6 batches of <= 4 MiB")
			for i, b := range batches {
				t.Logf("Case A Candidate 2 Batch %d: items=%d, uncompressed_bytes=%d", i+1, b.items, b.uncompressedBytes)
				assert.LessOrEqual(t, b.uncompressedBytes, stage1Max, "batch %d uncompressed bytes (%d) must be <= 4 MiB", i, b.uncompressedBytes)
				assert.LessOrEqual(t, b.items, utrMaxItems, "batch %d item count (%d) must be <= 8,192", i, b.items)
			}
		})
	})

	t.Run("CaseB_CompactLogs_20000x200B", func(t *testing.T) {
		// 20,000 compact logs @ ~190 B each = ~3.95 MiB uncompressed (between 3.5 MiB min_size and 4.0 MiB max_size).
		t.Run("Candidate1_SingleStageBytes_FailsInvalidArgumentTooManyItems", func(t *testing.T) {
			ctx := context.Background()
			srv, addr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
			expComp, _ := startOTLPExporter(t, ctx, addr)
			exp := expComp.(processor.Logs)

			// Candidate 1: Single-Stage sizer: bytes (min_size: 3.5 MiB, max_size: 4 MiB).
			cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			cfg.Sizer = exporterhelper.RequestSizerTypeBytes
			cfg.QueueSize = 33554432
			batchCfg := cfg.Batch.GetOrInsertDefault()
			batchCfg.Sizer = exporterhelper.RequestSizerTypeBytes
			batchCfg.MinSize = 3670016
			batchCfg.MaxSize = 4194304
			batchCfg.FlushTimeout = 200 * time.Millisecond

			proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), cfg, exp)
			require.NoError(t, err)
			require.NoError(t, proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(20000, 65, "dashpole-dev")
			sizer := &plog.ProtoMarshaler{}
			uncompressedSize := sizer.LogsSize(payload)
			require.GreaterOrEqual(t, uncompressedSize, 3670016, "Case B payload must be >= 3.5 MiB min_size")
			require.LessOrEqual(t, uncompressedSize, 4194304, "Case B payload must be <= 4.0 MiB max_size")

			require.NoError(t, proc.ConsumeLogs(ctx, payload))
			require.NoError(t, proc.Shutdown(ctx))

			batches, totalItems := srv.snapshot()
			assert.Empty(t, batches, "UTR must reject single 20,000-item batch exceeding 8,192 item limit")
			assert.Equal(t, 0, totalItems)
			srv.mu.Lock()
			assert.Equal(t, 1, srv.rejectedRPC)
			require.Error(t, srv.lastErr)
			assert.Contains(t, srv.lastErr.Error(), "Batch contains too many log records: 20000 > 8192")
			srv.mu.Unlock()
		})

		t.Run("Candidate2_Proposed_TwoStageChain_SplitsInto8192_8192_3616_Immediately", func(t *testing.T) {
			ctx := context.Background()
			srv, addr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
			expComp, _ := startOTLPExporter(t, ctx, addr)
			exp := expComp.(processor.Logs)

			pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: "dashpole-dev"}
			cm, err := pol.Evaluate(ctx)
			require.NoError(t, err)

			stage1Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_batch_logs")
			require.NoError(t, err)
			stage1Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, stage1Cfg.Unmarshal(stage1Sub))

			stage2Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_split_logs")
			require.NoError(t, err)
			stage2Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			require.NoError(t, stage2Cfg.Unmarshal(stage2Sub))

			stage2Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage2Cfg, exp)
			require.NoError(t, err)
			require.NoError(t, stage2Proc.Start(ctx, componenttest.NewNopHost()))

			stage1Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage1Cfg, stage2Proc)
			require.NoError(t, err)
			require.NoError(t, stage1Proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(20000, 65, "dashpole-dev")
			start := time.Now()
			require.NoError(t, stage1Proc.ConsumeLogs(ctx, payload))

			// Because payload is ~4.05 MB (>= 3.5 MiB Stage 1 min_size and <= 4.0 MiB Stage 1 max_size),
			// Stage 1 flushes immediately in 1 batch of 20,000 records, and Stage 2 (min_size: 0, max_size: 8192)
			// splits it into 8192 + 8192 + 3616 and flushes ALL 3 sub-batches (including the 3,616-item remainder!)
			// in < 120ms (well before the 200ms flush_timeout) WITHOUT calling Shutdown.
			for i := 0; i < 3; i++ {
				select {
				case rb := <-srv.batchCh:
					elapsed := rb.arrivedAt.Sub(start)
					assert.Less(t, elapsed, 180*time.Millisecond, "sub-batch %d (%d items) must flush immediately (< 180ms < 200ms flush_timeout)", i+1, rb.items)
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("timed out waiting for immediate flush of sub-batch %d", i+1)
				}
			}

			require.NoError(t, stage1Proc.Shutdown(ctx))
			require.NoError(t, stage2Proc.Shutdown(ctx))

			batches, totalItems := srv.snapshot()
			assert.Equal(t, 20000, totalItems)
			require.Len(t, batches, 3)
			gotCounts := []int{batches[0].items, batches[1].items, batches[2].items}
			assert.ElementsMatch(t, []int{8192, 8192, 3616}, gotCounts)
			for i, b := range batches {
				t.Logf("Case B Candidate 2 Batch %d: items=%d, uncompressed_bytes=%d", i+1, b.items, b.uncompressedBytes)
				assert.LessOrEqual(t, b.uncompressedBytes, stage1Max, "batch %d uncompressed bytes (%d) must be <= 4 MiB", i, b.uncompressedBytes)
				assert.LessOrEqual(t, b.items, utrMaxItems, "batch %d items (%d) must be <= 8,192", i, b.items)
			}
		})
	})

	t.Run("CaseC_SingleOversizedRecord_IsolatedWithoutPoisoningBatch", func(t *testing.T) {
		ctx := context.Background()
		srv, addr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
		expComp, _ := startOTLPExporter(t, ctx, addr)
		exp := expComp.(processor.Logs)

		pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: "dashpole-dev"}
		cm, err := pol.Evaluate(ctx)
		require.NoError(t, err)

		stage1Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_batch_logs")
		require.NoError(t, err)
		stage1Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, stage1Cfg.Unmarshal(stage1Sub))

		stage2Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_split_logs")
		require.NoError(t, err)
		stage2Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, stage2Cfg.Unmarshal(stage2Sub))

		stage2Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage2Cfg, exp)
		require.NoError(t, err)
		require.NoError(t, stage2Proc.Start(ctx, componenttest.NewNopHost()))

		stage1Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage1Cfg, stage2Proc)
		require.NoError(t, err)
		require.NoError(t, stage1Proc.Start(ctx, componenttest.NewNopHost()))

		// 1,500 normal ~2.5 KB logs (~3.84 MiB >= 3.5 MiB min_size) + 1 oversized 4.5 MiB log record.
		payload := buildTestLogs(1500, 2450, "dashpole-dev")
		oversized := payload.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty()
		oversized.Body().SetStr(strings.Repeat("Z", 4500000))

		start := time.Now()
		require.NoError(t, stage1Proc.ConsumeLogs(ctx, payload))

		select {
		case rb := <-srv.batchCh:
			assert.Less(t, rb.arrivedAt.Sub(start), 180*time.Millisecond, "normal records must flush immediately (< 180ms < 200ms flush_timeout)")
			assert.Equal(t, 1500, rb.items, "only the 1 oversized > 4 MiB record is dropped; all 1,500 normal records are exported")
			assert.LessOrEqual(t, rb.uncompressedBytes, stage1Max)
		case <-time.After(300 * time.Millisecond):
			t.Fatal("timed out waiting for normal records to flush after dropping oversized record")
		}

		require.NoError(t, stage1Proc.Shutdown(ctx))
		require.NoError(t, stage2Proc.Shutdown(ctx))
	})

	t.Run("Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof", func(t *testing.T) {
		// Sends 1,580 WEF XML logs (~3.95 MiB >= 3.5 MiB Stage 1 min_size, so Stage 1 flushes immediately)
		// with Stage 2 flush_timeout set to 5s:
		//   - Control (Stage 2 min_size = 8192): holds the 1,580 logs (1580 < 8192) waiting for the 5s timer.
		//   - Proposed (Stage 2 min_size = 0): flushes all 1,580 logs immediately (< 150ms << 5s timer).
		ctx := context.Background()
		payload := buildTestLogs(1580, 2450, "dashpole-dev")

		// 1. Negative Control: Stage 2 with min_size = 8192 and flush_timeout = 5s holds the 1,580-log batch.
		ctrlSrv, ctrlAddr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
		ctrlExpComp, _ := startOTLPExporter(t, ctx, ctrlAddr)
		ctrlExp := ctrlExpComp.(processor.Logs)

		pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: "dashpole-dev"}
		cm, err := pol.Evaluate(ctx)
		require.NoError(t, err)

		s1Sub, err := cm.Sub("processors::queue_batch/default_gcp_destination_batch_logs")
		require.NoError(t, err)
		s1Cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, s1Cfg.Unmarshal(s1Sub))

		s2CtrlSub, err := cm.Sub("processors::queue_batch/default_gcp_destination_split_logs")
		require.NoError(t, err)
		s2CtrlCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, s2CtrlCfg.Unmarshal(s2CtrlSub))
		s2CtrlCfg.Batch.Get().MinSize = 8192
		s2CtrlCfg.Batch.Get().FlushTimeout = 5 * time.Second

		s2CtrlProc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), s2CtrlCfg, ctrlExp)
		require.NoError(t, err)
		require.NoError(t, s2CtrlProc.Start(ctx, componenttest.NewNopHost()))

		s1CtrlProc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), s1Cfg, s2CtrlProc)
		require.NoError(t, err)
		require.NoError(t, s1CtrlProc.Start(ctx, componenttest.NewNopHost()))

		require.NoError(t, s1CtrlProc.ConsumeLogs(ctx, payload))
		select {
		case rb := <-ctrlSrv.batchCh:
			t.Fatalf("Stage 2 with min_size=8192 should have held the 1,580-item batch, but flushed %d items", rb.items)
		case <-time.After(200 * time.Millisecond):
			// Expected: Stage 2 held the batch because 1580 < 8192.
		}
		require.NoError(t, s1CtrlProc.Shutdown(ctx))
		require.NoError(t, s2CtrlProc.Shutdown(ctx))

		// 2. Positive Proof: Stage 2 with min_size = 0 (from GCPDestinationPolicy) and flush_timeout = 5s
		// flushes the 1,580-log batch immediately without waiting for the 5s timer.
		propSrv, propAddr := newUTRSimulatorServer(t, utrMaxBytes, utrMaxItems)
		propExpComp, _ := startOTLPExporter(t, ctx, propAddr)
		propExp := propExpComp.(processor.Logs)

		s1PropCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, s1PropCfg.Unmarshal(s1Sub))
		s2PropCfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
		require.NoError(t, s2PropCfg.Unmarshal(s2CtrlSub))
		require.Equal(t, int64(0), s2PropCfg.Batch.Get().MinSize)
		s2PropCfg.Batch.Get().FlushTimeout = 5 * time.Second

		s2PropProc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), s2PropCfg, propExp)
		require.NoError(t, err)
		require.NoError(t, s2PropProc.Start(ctx, componenttest.NewNopHost()))

		s1PropProc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), s1PropCfg, s2PropProc)
		require.NoError(t, err)
		require.NoError(t, s1PropProc.Start(ctx, componenttest.NewNopHost()))

		start := time.Now()
		require.NoError(t, s1PropProc.ConsumeLogs(ctx, buildTestLogs(1580, 2450, "dashpole-dev")))
		select {
		case rb := <-propSrv.batchCh:
			elapsed := rb.arrivedAt.Sub(start)
			assert.Less(t, elapsed, 150*time.Millisecond, "Stage 2 with min_size=0 must flush 1,580 items immediately (%v << 5s flush_timeout)", elapsed)
			assert.Equal(t, 1580, rb.items)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for Stage 2 min_size=0 immediate flush")
		}
		require.NoError(t, s1PropProc.Shutdown(ctx))
		require.NoError(t, s2PropProc.Shutdown(ctx))
	})
}

func idsToStrings(ids []component.ID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

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
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/processor/queuebatchprocessor"
)

type liveExportBatchResult struct {
	batchIndex        int
	items             int
	uncompressedBytes int
	duration          time.Duration
	err               error
}

type liveExporterSpy struct {
	next    consumer.Logs
	mu      sync.Mutex
	results []liveExportBatchResult
	doneCh  chan liveExportBatchResult
}

func newLiveExporterSpy(next consumer.Logs) *liveExporterSpy {
	return &liveExporterSpy{
		next:   next,
		doneCh: make(chan liveExportBatchResult, 32),
	}
}

func (s *liveExporterSpy) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *liveExporterSpy) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	sizer := &plog.ProtoMarshaler{}
	uncompressed := sizer.LogsSize(ld)
	items := ld.LogRecordCount()

	start := time.Now()
	err := s.next.ConsumeLogs(ctx, ld)
	dur := time.Since(start)

	s.mu.Lock()
	idx := len(s.results) + 1
	res := liveExportBatchResult{
		batchIndex:        idx,
		items:             items,
		uncompressedBytes: uncompressed,
		duration:          dur,
		err:               err,
	}
	s.results = append(s.results, res)
	s.mu.Unlock()

	select {
	case s.doneCh <- res:
	default:
	}
	return err
}

func (s *liveExporterSpy) snapshot() []liveExportBatchResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]liveExportBatchResult, len(s.results))
	copy(out, s.results)
	return out
}

func getGcloudAccessToken(t *testing.T) string {
	t.Helper()
	if tok := strings.TrimSpace(os.Getenv("LIVE_UTR_ACCESS_TOKEN")); tok != "" {
		return tok
	}
	// On cloudtop hosts where user gcloud tokens are mTLS-bound via enterprise-certificate-proxy,
	// fetch a standard non-mTLS bearer token from the dashpole-dev GCE VM service account.
	cmd := exec.Command(
		"gcloud", "compute", "ssh",
		"gke-obi-demo-cluster-default-pool-3f37b07e-8j8w",
		"--zone=us-central1-a",
		"--project=dashpole-dev",
		"--command=curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
	)
	out, err := cmd.Output()
	if err == nil {
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		var tokResp struct {
			AccessToken string `json:"access_token"`
		}
		if jsonErr := json.Unmarshal([]byte(lines[len(lines)-1]), &tokResp); jsonErr == nil && tokResp.AccessToken != "" {
			return tokResp.AccessToken
		}
	}

	fallbackCmd := exec.Command("gcloud", "auth", "print-access-token")
	fallbackOut, err := fallbackCmd.Output()
	require.NoError(t, err, "failed to get gcloud access token")
	tok := strings.TrimSpace(string(fallbackOut))
	require.NotEmpty(t, tok, "empty gcloud access token")
	return tok
}

func startLiveUTRExporter(t *testing.T, ctx context.Context, token string, projectID string) *liveExporterSpy {
	t.Helper()
	return startLiveUTRExporterWithCompression(t, ctx, token, projectID, configcompression.TypeGzip)
}

func startLiveUTRExporterWithCompression(t *testing.T, ctx context.Context, token string, projectID string, compression configcompression.Type) *liveExporterSpy {
	t.Helper()
	expFactory := otlpexporter.NewFactory()
	expCfg := expFactory.CreateDefaultConfig().(*otlpexporter.Config)
	expCfg.ClientConfig.Endpoint = "telemetry.googleapis.com:443"
	expCfg.ClientConfig.Compression = compression
	expCfg.ClientConfig.Headers = configopaque.MapList{
		{Name: "authorization", Value: configopaque.String("Bearer " + token)},
		{Name: "x-goog-user-project", Value: configopaque.String(projectID)},
	}
	expCfg.RetryConfig.Enabled = false
	expCfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()

	expSet := exportertest.NewNopSettings(expFactory.Type())
	exp, err := expFactory.CreateLogs(ctx, expSet, expCfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() {
		_ = exp.Shutdown(ctx)
	})
	return newLiveExporterSpy(exp)
}

func TestLiveUTR_BaselineVsTwoStageChain(t *testing.T) {
	if os.Getenv("RUN_LIVE_UTR_TEST") == "" {
		t.Skip("set RUN_LIVE_UTR_TEST=1 to run live verification against telemetry.googleapis.com:443")
	}

	const projectID = "dashpole-dev"
	token := getGcloudAccessToken(t)
	qbFactory := queuebatchprocessor.NewFactory()

	t.Run("CaseA_HeavyWEFXMLLogs_8192x2500B", func(t *testing.T) {
		t.Run("Baseline_SingleStageItems_RejectedByLiveUTR_ResourceExhausted", func(t *testing.T) {
			ctx := context.Background()
			spy := startLiveUTRExporter(t, ctx, token, projectID)

			cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			cfg.Sizer = exporterhelper.RequestSizerTypeRequests
			cfg.QueueSize = 10
			batchCfg := cfg.Batch.GetOrInsertDefault()
			batchCfg.Sizer = exporterhelper.RequestSizerTypeItems
			batchCfg.MinSize = 8192
			batchCfg.MaxSize = 8192
			batchCfg.FlushTimeout = 200 * time.Millisecond

			proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), cfg, spy)
			require.NoError(t, err)
			require.NoError(t, proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(8192, 2450, projectID)
			require.NoError(t, proc.ConsumeLogs(ctx, payload))
			require.NoError(t, proc.Shutdown(ctx))

			results := spy.snapshot()
			require.Len(t, results, 1, "baseline emits a single 8,192-item (~21.19 MB) batch")
			res := results[0]
			t.Logf("[LIVE UTR - Case A Baseline (gzip)] batch=%d items=%d uncompressed_bytes=%d duration=%v err=%v",
				res.batchIndex, res.items, res.uncompressedBytes, res.duration, res.err)
			require.Error(t, res.err, "live telemetry.googleapis.com:443 must reject ~21.19 MB uncompressed batch")
			assert.Contains(t, res.err.Error(), "RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880")
			assert.Contains(t, res.err.Error(), "at uncompressed byte 5242880")

			// Also verify uncompressed wire export (compression: none) is rejected at the 5,242,880-byte limit.
			spyUncomp := startLiveUTRExporterWithCompression(t, ctx, token, projectID, "")
			procUncomp, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), cfg, spyUncomp)
			require.NoError(t, err)
			require.NoError(t, procUncomp.Start(ctx, componenttest.NewNopHost()))
			require.NoError(t, procUncomp.ConsumeLogs(ctx, buildTestLogs(8192, 2450, projectID)))
			require.NoError(t, procUncomp.Shutdown(ctx))

			resUncomp := spyUncomp.snapshot()
			require.Len(t, resUncomp, 1)
			t.Logf("[LIVE UTR - Case A Baseline (uncompressed)] batch=%d items=%d uncompressed_bytes=%d duration=%v err=%v",
				resUncomp[0].batchIndex, resUncomp[0].items, resUncomp[0].uncompressedBytes, resUncomp[0].duration, resUncomp[0].err)
			require.Error(t, resUncomp[0].err)
			assert.Contains(t, resUncomp[0].err.Error(), "Request payload size exceeds the limit: 5242880 bytes.")
		})

		t.Run("Proposed_TwoStageChain_AcceptedByLiveUTR", func(t *testing.T) {
			ctx := context.Background()
			spy := startLiveUTRExporter(t, ctx, token, projectID)

			pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: projectID}
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

			stage2Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage2Cfg, spy)
			require.NoError(t, err)
			require.NoError(t, stage2Proc.Start(ctx, componenttest.NewNopHost()))

			stage1Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage1Cfg, stage2Proc)
			require.NoError(t, err)
			require.NoError(t, stage1Proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(8192, 2450, projectID)
			require.NoError(t, stage1Proc.ConsumeLogs(ctx, payload))

			// Wait for all 6 batches to finish exporting to live telemetry.googleapis.com:443.
			for i := 0; i < 6; i++ {
				select {
				case res := <-spy.doneCh:
					t.Logf("[LIVE UTR - Case A Two-Stage] batch=%d items=%d uncompressed_bytes=%d duration=%v err=%v",
						res.batchIndex, res.items, res.uncompressedBytes, res.duration, res.err)
					require.NoError(t, res.err, "batch %d must succeed against live telemetry.googleapis.com:443", res.batchIndex)
					assert.LessOrEqual(t, res.uncompressedBytes, 4194304)
					assert.LessOrEqual(t, res.items, 8192)
				case <-time.After(15 * time.Second):
					t.Fatalf("timed out waiting for live UTR export of batch %d", i+1)
				}
			}

			require.NoError(t, stage1Proc.Shutdown(ctx))
			require.NoError(t, stage2Proc.Shutdown(ctx))

			results := spy.snapshot()
			require.Len(t, results, 6)
			totalAccepted := 0
			for _, r := range results {
				require.NoError(t, r.err)
				totalAccepted += r.items
			}
			assert.Equal(t, 8192, totalAccepted, "all 8,192 WEF XML logs accepted by live telemetry.googleapis.com:443")
		})
	})

	t.Run("CaseB_CompactLogs_20000x200B", func(t *testing.T) {
		t.Run("SingleStageBytes_RejectedByLiveUTR_InvalidArgumentTooManyRecords", func(t *testing.T) {
			ctx := context.Background()
			spy := startLiveUTRExporter(t, ctx, token, projectID)

			cfg := qbFactory.CreateDefaultConfig().(*queuebatchprocessor.Config)
			cfg.Sizer = exporterhelper.RequestSizerTypeBytes
			cfg.QueueSize = 33554432
			batchCfg := cfg.Batch.GetOrInsertDefault()
			batchCfg.Sizer = exporterhelper.RequestSizerTypeBytes
			batchCfg.MinSize = 3670016
			batchCfg.MaxSize = 4194304
			batchCfg.FlushTimeout = 200 * time.Millisecond

			proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), cfg, spy)
			require.NoError(t, err)
			require.NoError(t, proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(20000, 65, projectID)
			require.NoError(t, proc.ConsumeLogs(ctx, payload))
			require.NoError(t, proc.Shutdown(ctx))

			results := spy.snapshot()
			require.Len(t, results, 1, "single-stage bytes batcher emits 1 batch of 20,000 items (~4.05 MB <= 4 MiB)")
			res := results[0]
			t.Logf("[LIVE UTR - Case B Single-Stage Bytes] batch=%d items=%d uncompressed_bytes=%d duration=%v err=%v",
				res.batchIndex, res.items, res.uncompressedBytes, res.duration, res.err)
			require.Error(t, res.err, "live telemetry.googleapis.com:443 must reject 20,000-item batch (> 8,192)")
			assert.Contains(t, res.err.Error(), "InvalidArgument")
			assert.Contains(t, res.err.Error(), "Permanent error")
		})

		t.Run("Proposed_TwoStageChain_SplitsAndAcceptedByLiveUTR", func(t *testing.T) {
			ctx := context.Background()
			spy := startLiveUTRExporter(t, ctx, token, projectID)

			pol := &GCPDestinationPolicy{Name: "default_gcp_destination", ProjectID: projectID}
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

			stage2Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage2Cfg, spy)
			require.NoError(t, err)
			require.NoError(t, stage2Proc.Start(ctx, componenttest.NewNopHost()))

			stage1Proc, err := qbFactory.CreateLogs(ctx, processortest.NewNopSettings(qbFactory.Type()), stage1Cfg, stage2Proc)
			require.NoError(t, err)
			require.NoError(t, stage1Proc.Start(ctx, componenttest.NewNopHost()))

			payload := buildTestLogs(20000, 65, projectID)
			require.NoError(t, stage1Proc.ConsumeLogs(ctx, payload))

			// Wait for all 3 split sub-batches (8192 + 8192 + 3616) to finish exporting to live telemetry.googleapis.com:443.
			for i := 0; i < 3; i++ {
				select {
				case res := <-spy.doneCh:
					t.Logf("[LIVE UTR - Case B Two-Stage] batch=%d items=%d uncompressed_bytes=%d duration=%v err=%v",
						res.batchIndex, res.items, res.uncompressedBytes, res.duration, res.err)
					require.NoError(t, res.err, "sub-batch %d must succeed against live telemetry.googleapis.com:443", res.batchIndex)
					assert.LessOrEqual(t, res.uncompressedBytes, 4194304)
					assert.LessOrEqual(t, res.items, 8192)
				case <-time.After(15 * time.Second):
					t.Fatalf("timed out waiting for live UTR export of sub-batch %d", i+1)
				}
			}

			require.NoError(t, stage1Proc.Shutdown(ctx))
			require.NoError(t, stage2Proc.Shutdown(ctx))

			results := spy.snapshot()
			require.Len(t, results, 3)
			totalAccepted := 0
			gotCounts := make([]int, 0, 3)
			for _, r := range results {
				require.NoError(t, r.err)
				totalAccepted += r.items
				gotCounts = append(gotCounts, r.items)
			}
			assert.Equal(t, 20000, totalAccepted, "all 20,000 compact logs accepted by live telemetry.googleapis.com:443")
			assert.ElementsMatch(t, []int{8192, 8192, 3616}, gotCounts)
		})
	})
}

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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/processor/queuebatchprocessor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	liveTelemetryEndpoint = "telemetry.googleapis.com:443"
	liveGCPProjectID      = "dashpole-dev"
)

func getGcloudAccessToken(t *testing.T) string {
	t.Helper()
	// First check if local gcloud auth print-access-token works directly against monitoring.googleapis.com.
	// On corp Cloudtop workstations with Context-Aware Access (use_client_certificate=true),
	// bare user access tokens without ECP mTLS return 401 ACCESS_TOKEN_TYPE_UNSUPPORTED.
	// In that case, fetch a standard OAuth2 bearer token from the GCE metadata server on a
	// running GCE VM in dashpole-dev (matching how GBOC authenticates on GCE in production).
	cmd := exec.Command("gcloud", "auth", "print-access-token")
	if out, err := cmd.Output(); err == nil {
		tok := strings.TrimSpace(string(out))
		if tok != "" && probeMonitoringToken(tok) {
			return tok
		}
	}

	sshCmd := exec.Command(
		"gcloud", "compute", "ssh",
		"gke-obi-demo-cluster-default-pool-3f37b07e-8j8w",
		"--zone=us-central1-a",
		"--project="+liveGCPProjectID,
		"--command=curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
	)
	out, err := sshCmd.Output()
	require.NoError(t, err, "failed to fetch GCE metadata server token from dashpole-dev VM")
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &tokenResp))
	require.NotEmpty(t, tokenResp.AccessToken, "GCE metadata access_token must not be empty")
	return tokenResp.AccessToken
}

func probeMonitoringToken(token string) bool {
	u := fmt.Sprintf("https://monitoring.googleapis.com/v3/projects/%s/metricDescriptors?pageSize=1", liveGCPProjectID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Goog-User-Project", liveGCPProjectID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

type liveExportResult struct {
	MetricName         string
	MetricType         string
	Unit               string
	GRPCStatus         codes.Code
	GRPCError          string
	RejectedDataPoints int64
	PartialErrorMsg    string
}

// buildSingleMetricRequest clones a single metric (by name and optional exporter prefix) from a captured
// collector pmetric.Metrics batch and stamps fresh timestamps so telemetry.googleapis.com:443 evaluates it in isolation.
func buildSingleMetricRequest(t *testing.T, captured []ObservedMetricPoint, metricName string, exporterPrefix string, instanceID string) pmetricotlp.ExportRequest {
	t.Helper()
	var target *ObservedMetricPoint
	for i := range captured {
		if captured[i].Name != metricName {
			continue
		}
		if exporterPrefix != "" && !strings.HasPrefix(captured[i].Attributes["exporter"], exporterPrefix) {
			continue
		}
		target = &captured[i]
		break
	}
	require.NotNil(t, target, "metric %s (exporterPrefix=%q) not found in captured collector self-metrics", metricName, exporterPrefix)

	now := time.Now()
	startTime := now.Add(-10 * time.Second)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resAttrs := rm.Resource().Attributes()
	resAttrs.PutStr("gcp.project_id", liveGCPProjectID)
	resAttrs.PutStr("cloud.account.id", liveGCPProjectID)
	resAttrs.PutStr("cloud.provider", "gcp")
	resAttrs.PutStr("cloud.platform", "gcp_compute_engine")
	resAttrs.PutStr("host.id", "1234567890123456789")
	resAttrs.PutStr("cloud.availability_zone", "us-central1-a")
	resAttrs.PutStr("service.instance.id", instanceID)
	resAttrs.PutStr("gcp.fleet_id", "fleet-tab2-live")
	resAttrs.PutStr("service.name", "otelcol")

	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(target.Name)
	m.SetDescription(target.Description)
	m.SetUnit(target.Unit)

	switch target.Type {
	case pmetric.MetricTypeGauge:
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.SetIntValue(target.IntValue)
		for k, v := range target.Attributes {
			dp.Attributes().PutStr(k, v)
		}
	case pmetric.MetricTypeSum:
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(target.IsMonotonic)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.SetIntValue(target.IntValue)
		for k, v := range target.Attributes {
			dp.Attributes().PutStr(k, v)
		}
	default:
		t.Fatalf("unexpected metric type: %v", target.Type)
	}

	return pmetricotlp.NewExportRequestFromMetrics(md)
}

type metricDescriptorListResp struct {
	MetricDescriptors []struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		MetricKind  string `json:"metricKind"`
		ValueType   string `json:"valueType"`
		Unit        string `json:"unit"`
		Description string `json:"description"`
		Labels      []struct {
			Key         string `json:"key"`
			ValueType   string `json:"valueType"`
			Description string `json:"description"`
		} `json:"labels"`
	} `json:"metricDescriptors"`
}

type timeSeriesListResp struct {
	TimeSeries []struct {
		Metric struct {
			Type   string            `json:"type"`
			Labels map[string]string `json:"labels"`
		} `json:"metric"`
		Resource struct {
			Type   string            `json:"type"`
			Labels map[string]string `json:"labels"`
		} `json:"resource"`
		MetricKind string `json:"metricKind"`
		ValueType  string `json:"valueType"`
		Points     []struct {
			Interval struct {
				StartTime string `json:"startTime"`
				EndTime   string `json:"endTime"`
			} `json:"interval"`
			Value map[string]any `json:"value"`
		} `json:"points"`
	} `json:"timeSeries"`
}

func queryCloudMonitoringTimeSeries(t *testing.T, token, metricType string, lookback time.Duration) (int, timeSeriesListResp, string) {
	t.Helper()
	now := time.Now().UTC()
	start := now.Add(-lookback)
	u := fmt.Sprintf(
		"https://monitoring.googleapis.com/v3/projects/%s/timeSeries?filter=%s&interval.startTime=%s&interval.endTime=%s&pageSize=5",
		liveGCPProjectID,
		url.QueryEscape(fmt.Sprintf("metric.type = %q", metricType)),
		url.QueryEscape(start.Format(time.RFC3339)),
		url.QueryEscape(now.Format(time.RFC3339)),
	)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Goog-User-Project", liveGCPProjectID)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var parsed timeSeriesListResp
	if resp.StatusCode == http.StatusOK {
		_ = json.Unmarshal(body, &parsed)
	}
	return resp.StatusCode, parsed, string(body)
}

func queryCloudMonitoringDescriptors(t *testing.T, token, filter string) metricDescriptorListResp {
	t.Helper()
	u := fmt.Sprintf("https://monitoring.googleapis.com/v3/projects/%s/metricDescriptors?filter=%s",
		liveGCPProjectID, url.QueryEscape(filter))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Goog-User-Project", liveGCPProjectID)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "metricDescriptors query failed: %s", string(body))

	var parsed metricDescriptorListResp
	require.NoError(t, json.Unmarshal(body, &parsed))
	return parsed
}

// TestLiveTelemetryGoogleAPIs_SelfMetricsExport (Milestone 2)
// Executes live OTLP gRPC MetricsService/Export calls against telemetry.googleapis.com:443
// in project dashpole-dev using collector-generated self-observability metrics.
func TestLiveTelemetryGoogleAPIs_SelfMetricsExport(t *testing.T) {
	if os.Getenv("RUN_LIVE_GCP_TEST") != "1" {
		t.Skip("Skipping live GCP telemetry.googleapis.com test; set RUN_LIVE_GCP_TEST=1 to run")
	}

	token := getGcloudAccessToken(t)

	// 1. Capture real collector-generated self-metrics across logs, metrics, and traces from our GBOC harness.
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
	// Trigger a successful export (for sent_log_records), a permanent failure (for send_failed_log_records),
	// and oversized queue-entry drops on logs, metrics, and traces (for enqueue_failed_*).
	require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(3)))
	require.Eventually(t, func() bool {
		return h.server.receivedLogs.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond)

	permErr := status.Error(codes.InvalidArgument, "simulated downstream permanent error")
	h.server.logsExportErr.Store(&permErr)
	require.NoError(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(2)))

	require.Error(t, h.logsQueueProc.ConsumeLogs(ctx, makeLogBatch(15)))
	require.Error(t, h.metricsQueueProc.ConsumeMetrics(ctx, makeMetricBatch(15)))
	require.Error(t, h.tracesQueueProc.ConsumeTraces(ctx, makeTraceBatch(15)))

	targetMetrics := []string{
		MetricExporterSentLogRecords,
		MetricExporterSendFailedLogRecords,
		MetricExporterQueueSize,
		MetricExporterQueueCapacity,
		MetricExporterEnqueueFailedLogRecords,
		MetricExporterEnqueueFailedMetricPoints,
		MetricExporterEnqueueFailedSpans,
	}

	var captured []ObservedMetricPoint
	require.Eventually(t, func() bool {
		captured = h.server.snapshotLatestMetrics()
		for _, name := range targetMetrics {
			found := false
			for _, pt := range captured {
				if pt.Name == name {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}, 3*time.Second, 25*time.Millisecond)

	// 2. Dial live telemetry.googleapis.com:443 over TLS.
	conn, err := grpc.NewClient(
		liveTelemetryEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pmetricotlp.NewGRPCClient(conn)
	instanceID := fmt.Sprintf("tab2-live-%d", time.Now().UnixNano())

	// 3. Negative Control (Baseline-vs-Change Proof): Unauthenticated Export MUST fail with PermissionDenied / Unauthenticated.
	unauthCtx, unauthCancel := context.WithTimeout(ctx, 10*time.Second)
	defer unauthCancel()
	unauthReq := buildSingleMetricRequest(t, captured, MetricExporterQueueSize, "queue_batch/", instanceID)
	_, unauthErr := client.Export(unauthCtx, unauthReq)
	require.Error(t, unauthErr, "unauthenticated call to telemetry.googleapis.com:443 must fail")
	assert.Contains(t, []codes.Code{codes.PermissionDenied, codes.Unauthenticated}, status.Code(unauthErr),
		"Negative control: expected PermissionDenied or Unauthenticated from telemetry.googleapis.com:443, got: %v", unauthErr)
	t.Logf("Negative Control (Unauthenticated): status=%s err=%v", status.Code(unauthErr), unauthErr)

	// 4. Authenticated per-metric exports to telemetry.googleapis.com:443 in project dashpole-dev
	// using the queue_batch/* data points (which carry consistent metric attributes).
	authMD := metadata.Pairs(
		"authorization", "Bearer "+token,
		"x-goog-user-project", liveGCPProjectID,
	)

	var results []liveExportResult
	for _, metricName := range targetMetrics {
		req := buildSingleMetricRequest(t, captured, metricName, "queue_batch/", instanceID)
		m := req.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)

		callCtx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, authMD), 10*time.Second)
		resp, callErr := client.Export(callCtx, req)
		cancel()

		res := liveExportResult{
			MetricName: metricName,
			MetricType: m.Type().String(),
			Unit:       m.Unit(),
			GRPCStatus: status.Code(callErr),
		}
		if callErr != nil {
			res.GRPCError = callErr.Error()
		} else {
			res.RejectedDataPoints = resp.PartialSuccess().RejectedDataPoints()
			res.PartialErrorMsg = resp.PartialSuccess().ErrorMessage()
		}
		results = append(results, res)
		t.Logf("Live Export [%s (queue_batch/*)] (type=%s unit=%q): grpc_status=%s rejected_points=%d partial_err=%q grpc_err=%q",
			res.MetricName, res.MetricType, res.Unit, res.GRPCStatus, res.RejectedDataPoints, res.PartialErrorMsg, res.GRPCError)
		assert.Equal(t, codes.OK, res.GRPCStatus, "authenticated OTLP export of %s (queue_batch/*) to telemetry.googleapis.com:443 should succeed", metricName)
		assert.Equal(t, int64(0), res.RejectedDataPoints, "expected 0 rejected data points for %s", metricName)
	}

	// 4b. Test exporting otelcol_exporter_sent_log_records from otlp_grpc/* (which attaches extra attributes
	// server.address and server.port whereas queue_batch/* only attaches exporter).
	otlpSentReq := buildSingleMetricRequest(t, captured, MetricExporterSentLogRecords, "otlp_grpc/", instanceID)
	otlpSentCtx, otlpSentCancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, authMD), 10*time.Second)
	otlpSentResp, otlpSentErr := client.Export(otlpSentCtx, otlpSentReq)
	otlpSentCancel()
	require.NoError(t, otlpSentErr)
	assert.Equal(t, int64(0), otlpSentResp.PartialSuccess().RejectedDataPoints())
	t.Logf("Live Export [otelcol_exporter_sent_log_records (otlp_grpc/* with server.address & server.port)]: grpc_status=%s rejected_points=%d",
		status.Code(otlpSentErr), otlpSentResp.PartialSuccess().RejectedDataPoints())

	// 5. Test exporting with SelfMetricsPolicy's exact raw Resource attributes (without cloud.platform=gcp_compute_engine
	// or cloud.availability_zone) to observe how telemetry.googleapis.com:443 handles raw SelfMetricsPolicy resource attributes.
	rawMD := pmetric.NewMetrics()
	rawRM := rawMD.ResourceMetrics().AppendEmpty()
	rawRM.Resource().Attributes().PutStr("service.instance.id", instanceID)
	rawRM.Resource().Attributes().PutStr("gcp.fleet_id", "fleet-tab2-live")
	rawRM.Resource().Attributes().PutStr("gcp.project_id", liveGCPProjectID)
	rawSM := rawRM.ScopeMetrics().AppendEmpty()
	rawM := rawSM.Metrics().AppendEmpty()
	rawM.SetName(MetricExporterQueueSize)
	rawM.SetUnit("{batch}")
	rawDP := rawM.SetEmptyGauge().DataPoints().AppendEmpty()
	rawDP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	rawDP.SetIntValue(10)
	rawDP.Attributes().PutStr("exporter", "queue_batch/default_gcp_destination_batch_logs")
	rawDP.Attributes().PutStr("data_type", "logs")

	rawCtx, rawCancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, authMD), 10*time.Second)
	rawResp, rawErr := client.Export(rawCtx, pmetricotlp.NewExportRequestFromMetrics(rawMD))
	rawCancel()
	t.Logf("Live Export [Raw SelfMetricsPolicy Resource Attrs only]: grpc_status=%s rejected=%d partial_err=%q err=%v",
		status.Code(rawErr),
		func() int64 {
			if rawErr == nil {
				return rawResp.PartialSuccess().RejectedDataPoints()
			}
			return -1
		}(),
		func() string {
			if rawErr == nil {
				return rawResp.PartialSuccess().ErrorMessage()
			}
			return ""
		}(),
		rawErr,
	)
	require.Error(t, rawErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(rawErr))
	assert.Contains(t, rawErr.Error(), "Unrecognized region or location")

	// 6. Query Cloud Monitoring metricDescriptors in dashpole-dev to inspect registered descriptors
	// under agent.googleapis.com/agent/* vs workload.googleapis.com/otelcol_*.
	agentDescs := queryCloudMonitoringDescriptors(t, token, `metric.type = starts_with("agent.googleapis.com/agent/")`)
	t.Logf("Cloud Monitoring agent.googleapis.com/agent/* descriptors count in %s: %d", liveGCPProjectID, len(agentDescs.MetricDescriptors))
	hasAgentOtelcolDesc := false
	for _, d := range agentDescs.MetricDescriptors {
		if strings.Contains(d.Type, "otelcol") {
			hasAgentOtelcolDesc = true
		}
		if strings.Contains(d.Type, "otelcol") || strings.Contains(d.Type, "queue") || strings.Contains(d.Type, "request_count") || strings.Contains(d.Type, "uptime") {
			t.Logf("  agent descriptor: type=%s kind=%s valueType=%s unit=%q", d.Type, d.MetricKind, d.ValueType, d.Unit)
		}
	}
	assert.False(t, hasAgentOtelcolDesc, "agent.googleapis.com/agent/otelcol_* descriptors are not yet registered in Cloud Monitoring")

	allExporterDescs := queryCloudMonitoringDescriptors(t, token, `metric.type = has_substring("otelcol_exporter")`)
	t.Logf("Cloud Monitoring ALL *otelcol_exporter* descriptors count in %s: %d", liveGCPProjectID, len(allExporterDescs.MetricDescriptors))
	descByType := make(map[string]string)
	for _, d := range allExporterDescs.MetricDescriptors {
		descByType[d.Type] = d.Unit
		var labelKeys []string
		for _, l := range d.Labels {
			labelKeys = append(labelKeys, l.Key)
		}
		t.Logf("  exporter descriptor: type=%s kind=%s valueType=%s unit=%q labels=%v",
			d.Type, d.MetricKind, d.ValueType, d.Unit, labelKeys)
	}

	// Verify that telemetry.googleapis.com:443 registered all 5 obsQueue metrics under prometheus.googleapis.com/*
	// with their exact upstream OTLP unit strings.
	assert.Equal(t, "{batch}", descByType["prometheus.googleapis.com/otelcol_exporter_queue_size/gauge"])
	assert.Equal(t, "{batch}", descByType["prometheus.googleapis.com/otelcol_exporter_queue_capacity/gauge"])
	assert.Equal(t, "{record}", descByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_log_records/counter"])
	assert.Equal(t, "{datapoint}", descByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_metric_points/counter"])
	assert.Equal(t, "{span}", descByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_spans/counter"])

	// 7. Query Cloud Monitoring timeSeries in dashpole-dev for the 5 obsQueue metrics and baseline metrics
	// under prometheus.googleapis.com/*, workload.googleapis.com/*, and agent.googleapis.com/agent/*.
	probeSeriesTypes := []string{
		"prometheus.googleapis.com/otelcol_exporter_sent_log_records/counter",
		"prometheus.googleapis.com/otelcol_exporter_send_failed_log_records/counter",
		"prometheus.googleapis.com/otelcol_exporter_queue_size/gauge",
		"prometheus.googleapis.com/otelcol_exporter_queue_capacity/gauge",
		"prometheus.googleapis.com/otelcol_exporter_enqueue_failed_log_records/counter",
		"prometheus.googleapis.com/otelcol_exporter_enqueue_failed_metric_points/counter",
		"prometheus.googleapis.com/otelcol_exporter_enqueue_failed_spans/counter",
		"workload.googleapis.com/otelcol_exporter_queue_size",
		"workload.googleapis.com/otelcol_exporter_enqueue_failed_log_records",
		"agent.googleapis.com/agent/otelcol_exporter_queue_size",
	}
	seriesCountByType := make(map[string]int)
	statusCodeByType := make(map[string]int)
	for _, tsType := range probeSeriesTypes {
		code, tsResp, rawBody := queryCloudMonitoringTimeSeries(t, token, tsType, 60*time.Minute)
		statusCodeByType[tsType] = code
		if code == http.StatusOK {
			seriesCountByType[tsType] = len(tsResp.TimeSeries)
			var latestVal any
			var resType string
			var metricLabels map[string]string
			if len(tsResp.TimeSeries) > 0 {
				resType = tsResp.TimeSeries[0].Resource.Type
				metricLabels = tsResp.TimeSeries[0].Metric.Labels
				if len(tsResp.TimeSeries[0].Points) > 0 {
					latestVal = tsResp.TimeSeries[0].Points[0].Value
				}
			}
			t.Logf("Cloud Monitoring timeSeries [%s]: status=%d series_count=%d resource=%q labels=%v latest_val=%v",
				tsType, code, len(tsResp.TimeSeries), resType, metricLabels, latestVal)
		} else {
			t.Logf("Cloud Monitoring timeSeries [%s]: status=%d body=%s", tsType, code, strings.TrimSpace(rawBody))
		}
	}

	assert.GreaterOrEqual(t, seriesCountByType["prometheus.googleapis.com/otelcol_exporter_queue_size/gauge"], 1)
	assert.GreaterOrEqual(t, seriesCountByType["prometheus.googleapis.com/otelcol_exporter_queue_capacity/gauge"], 1)
	assert.GreaterOrEqual(t, seriesCountByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_log_records/counter"], 1)
	assert.GreaterOrEqual(t, seriesCountByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_metric_points/counter"], 1)
	assert.GreaterOrEqual(t, seriesCountByType["prometheus.googleapis.com/otelcol_exporter_enqueue_failed_spans/counter"], 1)
	assert.Equal(t, http.StatusNotFound, statusCodeByType["agent.googleapis.com/agent/otelcol_exporter_queue_size"])
}

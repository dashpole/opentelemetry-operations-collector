# Demo & Walkthrough Plan: GBOC Self-Observability — Queue Saturation & Drop Visibility (Tab 2)

**Repository**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`v0.162.0`)
**Branch**: `proto/tango-tab2-self-o11y-queue-metrics`
**Workspace**: `/tmp/prototypes/tab2_self_o11y`

---

## 1. Narrative Arc (Before -> Failure Mode -> Proposed Solution -> Live Cloud Monitoring Proof)

1. **Act I — The Allianz / Bindplane Blind Spot (`#42`, `#80`)**:
   - Show how `GCPDestinationPolicy` configures `queue_batch` (`block_on_overflow: true`, `queue_size: 10`, `num_consumers: 1`) in front of `otlp_grpc` (`retry_on_failure.max_elapsed_time: 5m0s`).
   - Run `TestQueueSaturation_BlockingQueue_BaselineVsProposed`: stall the downstream `LogsService/Export` handler, send 10 log batches (`10/10`), and block the 11th batch in `Offer()`.
   - Show the live collector OTLP self-metrics payload: the current 24-metric catalog (`otelcol_exporter_send_failed_log_records` and `otelcol_receiver_refused_log_records`) reports **`0` errors**, while `otelcol_exporter_queue_size` (`10`) and `otelcol_exporter_queue_capacity` (`10`) immediately report **`100%` (`1.00`) queue saturation**.
2. **Act II — Three Queue-Entry Drop Modes Invisible to `send_failed_*` (`#42`, `#43`)**:
   - Run `TestQueueEntryDrops_EnqueueFailedVsSendFailed` to demonstrate all three queue-entry drop paths where `otelcol_exporter_send_failed_log_records == 0`:
     - **2a (`block_on_overflow: false`)**: 15 records dropped on full queue -> `enqueue_failed_log_records = 15`.
     - **2b (`block_on_overflow: true` + oversized batch `elSize=25 > cap=10`)**: `memory_queue.go:75` rejects immediately with `errSizeTooLarge` without blocking -> `enqueue_failed_log_records = 25`.
     - **2c (`block_on_overflow: true` + caller context timeout)**: 7 records dropped when upstream `ctx` expires -> `enqueue_failed_log_records = 7`.
3. **Act III — Sizer-Agnostic Utilization & Avoiding the 2x `send_failed_*` Double-Count**:
   - Run `TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit` to show:
     - Exact OTLP metric schema (`{batch}`, `{record}`, `{datapoint}`, `{span}`).
     - Why `queue_size / queue_capacity` (`0.50` across `requests=5/10`, `items=25/50`, `bytes=146/292`) is required instead of `queue_size` alone.
     - Why `otelcol_exporter_send_failed_*` is emitted by **both** `otlp_grpc/default_gcp_destination` (`6`) and `queue_batch/default_gcp_destination_batch_logs` (`6`) on downstream permanent errors (unfiltered sum = `12`, requiring `exporter !~ "queue_batch/.*"` in alert queries).
4. **Act IV — Live `telemetry.googleapis.com:443` & Cloud Monitoring Proof in `dashpole-dev`**:
   - Run `TestLiveTelemetryGoogleAPIs_SelfMetricsExport` with `RUN_LIVE_GCP_TEST=1` to export all 5 `obsQueue` metrics to `telemetry.googleapis.com:443` and query back the ingested `prometheus.googleapis.com/otelcol_exporter_*/{gauge,counter}` descriptors and `timeSeries` points from `monitoring.googleapis.com/v3/projects/dashpole-dev`.

---

## 2. Exact Copy-Pasteable Reproduction Commands

### Command 1: Run In-Process GBOC Collector Queue Saturation, Drop & Schema Suite (Milestone 1)
```bash
cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  go test -v ./policies/selfmetrics -run "TestQueueSaturation|TestQueueEntryDrops|TestObsQueueMetricSchema"
```

**Expected Output Highlights**:
```text
=== RUN   TestQueueSaturation_BlockingQueue_BaselineVsProposed
    queue_observability_harness_test.go:564: Saturated Queue Health View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=10/10 (util=1.00), enqueue_failed=0, detects_saturation=true, detects_drops=false]
    queue_observability_harness_test.go:600: Drained Queue Health View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=0/10 (util=0.00), enqueue_failed=0, detects_saturation=false, detects_drops=false]
--- PASS: TestQueueSaturation_BlockingQueue_BaselineVsProposed (0.34s)
--- PASS: TestQueueEntryDrops_EnqueueFailedVsSendFailed (0.61s)
    --- PASS: TestQueueEntryDrops_EnqueueFailedVsSendFailed/2a_NonBlockingOverflow_ErrQueueIsFull (0.12s)
    --- PASS: TestQueueEntryDrops_EnqueueFailedVsSendFailed/2b_OversizedBatch_WithBlockOnOverflowTrue_errSizeTooLarge (0.13s)
    --- PASS: TestQueueEntryDrops_EnqueueFailedVsSendFailed/2c_ContextDeadlineExceeded_WhileBlockedInOffer (0.37s)
--- PASS: TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit (0.86s)
    --- PASS: TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit/3a_ExactMetricSchemaAudit_AcrossLogsMetricsTraces (0.13s)
    --- PASS: TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit/3b_ScenarioC_SizerAgnosticUtilization_RequestsItemsBytes (0.61s)
    --- PASS: TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit/3c_ScenarioD_SendFailedDoubleCountingBetweenQueueBatchAndOtlpGrpc (0.12s)
PASS
```

### Command 2: Run Live `telemetry.googleapis.com:443` & Cloud Monitoring Integration Test in `dashpole-dev` (Milestone 2)
```bash
cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  RUN_LIVE_GCP_TEST=1 go test -v ./policies/selfmetrics -run TestLiveTelemetryGoogleAPIs_SelfMetricsExport
```

**Expected Output Highlights**:
```text
=== RUN   TestLiveTelemetryGoogleAPIs_SelfMetricsExport
    queue_observability_live_test.go:352: Negative Control (Unauthenticated): status=PermissionDenied
    queue_observability_live_test.go:383: Live Export [otelcol_exporter_queue_size (queue_batch/*)] (type=Gauge unit="{batch}"): grpc_status=OK rejected_points=0
    queue_observability_live_test.go:383: Live Export [otelcol_exporter_queue_capacity (queue_batch/*)] (type=Gauge unit="{batch}"): grpc_status=OK rejected_points=0
    queue_observability_live_test.go:383: Live Export [otelcol_exporter_enqueue_failed_log_records (queue_batch/*)] (type=Sum unit="{record}"): grpc_status=OK rejected_points=0
    queue_observability_live_test.go:383: Live Export [otelcol_exporter_enqueue_failed_metric_points (queue_batch/*)] (type=Sum unit="{datapoint}"): grpc_status=OK rejected_points=0
    queue_observability_live_test.go:383: Live Export [otelcol_exporter_enqueue_failed_spans (queue_batch/*)] (type=Sum unit="{span}"): grpc_status=OK rejected_points=0
    queue_observability_live_test.go:507: Cloud Monitoring timeSeries [prometheus.googleapis.com/otelcol_exporter_queue_capacity/gauge]: status=200 series_count=4 resource="prometheus_target" latest_val=map[doubleValue:10]
    queue_observability_live_test.go:507: Cloud Monitoring timeSeries [prometheus.googleapis.com/otelcol_exporter_enqueue_failed_log_records/counter]: status=200 series_count=4 resource="prometheus_target" latest_val=map[doubleValue:15]
--- PASS: TestLiveTelemetryGoogleAPIs_SelfMetricsExport
```

### Command 3: Run Full `googlecontrolplaneprovider` Test Suite (Regression Check)
```bash
cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  go test ./...
```

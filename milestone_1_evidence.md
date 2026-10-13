# Milestone 1 Evidence: End-to-End Self-Observability Queue Saturation & Drop Harness in GBOC

**Deliverable Files**:
- `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics/queue_observability_harness.go`
- `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics/queue_observability_harness_test.go`

**Verification Command**:
```bash
cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v ./policies/selfmetrics -run "TestQueueSaturation|TestQueueEntryDrops|TestObsQueueMetricSchema"
```

---

## 1. Summary of Empirical Findings & Baseline-vs-Change Proof

### Test 1: `TestQueueSaturation_BlockingQueue_BaselineVsProposed` (Scenario A — Findings `#42`, `#80`)
- **Setup**: Real OTel Collector (`v0.162.0`) pipeline configured from `SelfMetricsPolicy` (`Normal` level, OTLP gRPC `PeriodicMetricReader` -> `otlp/default_self_metrics`) and `GCPDestinationPolicy` (`queue_batch/default_gcp_destination_batch_logs` with `block_on_overflow: true`, `queue_size: 10`, `num_consumers: 1`, `sizer: requests`, and `otlp_grpc/default_gcp_destination` with `retry_on_failure.max_elapsed_time: 5m0s`).
- **Execution**:
  - Downstream `LogsService/Export` gRPC handler is stalled while `MetricsService/Export` remains open.
  - 10 single-record log batches are offered synchronously. Because `memoryQueue.Read()` pops items without decrementing `mq.size` (`memory_queue.go:146`) and `mq.onDone()` only decrements `mq.size` after `partitionBatcher.flush()` completes `consumeFunc` (`memory_queue.go:164`, `partition_batcher.go:292`), all 10 unacknowledged batches (1 in-flight in the stalled gRPC call + 1 waiting on `wp.workers` + 8 in `mq.items`) are counted in `mq.size`, bringing `mq.size` to `10/10`.
  - An 11th single-record log batch is offered on a background goroutine and blocks in `mq.hasMoreSpace.Wait(ctx)` (`memory_queue.go:116`).
- **Observed Self-Metrics Output (While Saturated vs. After Drain)**:
  ```text
  === RUN   TestQueueSaturation_BlockingQueue_BaselineVsProposed
      queue_observability_harness_test.go:564: Saturated Queue Health View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=10/10 (util=1.00), enqueue_failed=0, detects_saturation=true, detects_drops=false]
      queue_observability_harness_test.go:600: Drained Queue Health View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=0/10 (util=0.00), enqueue_failed=0, detects_saturation=false, detects_drops=false]
  --- PASS: TestQueueSaturation_BlockingQueue_BaselineVsProposed (0.34s)
  ```
- **Baseline-vs-Change Proof**:
  - **Baseline (24-metric catalog)**: `otelcol_exporter_send_failed_log_records == 0`, `otelcol_receiver_refused_log_records == 0` (`detects_issue = false`). A collector with a 100% full blocking queue and stalled pull receivers reports zero errors.
  - **Proposed (`obsQueue` metrics)**: `otelcol_exporter_queue_size == 10`, `otelcol_exporter_queue_capacity == 10`, `utilization == 1.00` (`detects_saturation = true`). Once the downstream stall is released, `queue_size` drains to `0/10` (`utilization == 0.00`) and all `11` log records are exported (`otelcol_exporter_sent_log_records == 11` for both `otlp_grpc/default_gcp_destination` and `queue_batch/default_gcp_destination_batch_logs`).

---

### Test 2: `TestQueueEntryDrops_EnqueueFailedVsSendFailed` (Scenario B — Findings `#42`, `#43`)
Empirically proves across **all three** queue-entry rejection paths in `exporterhelper/internal/queue/memory_queue.go` and `obs_queue.go` that `otelcol_exporter_send_failed_log_records` stays **`0`** while `otelcol_exporter_enqueue_failed_log_records` increments by the exact dropped record count:

| Subtest | Queue Configuration | Fault Injected | Baseline `send_failed_log_records` | Baseline `detects_issue` | Proposed `enqueue_failed_log_records` | Proposed `detects_drops` |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: |
| **2a (`NonBlockingOverflow_ErrQueueIsFull`)** | `block_on_overflow: false`, `queue_size: 10`, `sizer: requests` | Queue filled to `10/10`, then 3 batches of 5 records (`15` records) offered -> `ErrQueueIsFull` (`memory_queue.go:113`) | `0` | `false` | **`15`** | `true` |
| **2b (`OversizedBatch_WithBlockOnOverflowTrue_errSizeTooLarge`)** | `block_on_overflow: true`, `queue_size: 10`, `sizer: items` | Single batch of `25` records offered (`elSize = 25 > cap = 10`) -> rejected immediately with `errSizeTooLarge` (`memory_queue.go:75`) **without blocking!** | `0` | `false` | **`25`** | `true` |
| **2c (`ContextDeadlineExceeded_WhileBlockedInOffer`)** | `block_on_overflow: true`, `queue_size: 10`, `sizer: requests` | Queue filled to `10/10`, then 1 batch of `7` records offered with `80ms` caller context timeout -> `context.DeadlineExceeded` (`memory_queue.go:116`) | `0` | `false` | **`7`** | `true` |

**Concrete Test Output**:
```text
=== RUN   TestQueueEntryDrops_EnqueueFailedVsSendFailed
=== RUN   TestQueueEntryDrops_EnqueueFailedVsSendFailed/2a_NonBlockingOverflow_ErrQueueIsFull
    queue_observability_harness_test.go:638: Subtest 2a (block_on_overflow=false) View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=10/10 (util=1.00), enqueue_failed=15, detects_saturation=true, detects_drops=true]
=== RUN   TestQueueEntryDrops_EnqueueFailedVsSendFailed/2b_OversizedBatch_WithBlockOnOverflowTrue_errSizeTooLarge
    queue_observability_harness_test.go:669: Subtest 2b (block_on_overflow=true, oversized batch 25 > 10) View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=0/10 (util=0.00), enqueue_failed=25, detects_saturation=false, detects_drops=true]
=== RUN   TestQueueEntryDrops_EnqueueFailedVsSendFailed/2c_ContextDeadlineExceeded_WhileBlockedInOffer
    queue_observability_harness_test.go:700: Subtest 2c (block_on_overflow=true, ctx.DeadlineExceeded) View: Baseline[send_failed=0, receiver_refused=0, detects_issue=false] | Proposed[queue=10/10 (util=1.00), enqueue_failed=7, detects_saturation=true, detects_drops=true]
--- PASS: TestQueueEntryDrops_EnqueueFailedVsSendFailed (0.61s)
```

---

### Test 3: `TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit` (Scenarios C & D)

1. **Subtest 3a (`ExactMetricSchemaAudit_AcrossLogsMetricsTraces`)**:
   Verifies the exact OTLP `pmetric.Metric` schema emitted by `go.opentelemetry.io/collector/exporter/exporterhelper@v0.162.0`:
   ```text
   Schema Audit: name=otelcol_exporter_queue_size type=Gauge unit="{batch}" monotonic=false val=0 attrs=map[data_type:logs exporter:queue_batch/default_gcp_destination_batch_logs]
   Schema Audit: name=otelcol_exporter_queue_size type=Gauge unit="{batch}" monotonic=false val=0 attrs=map[data_type:metrics exporter:queue_batch/default_gcp_destination_batch_metrics]
   Schema Audit: name=otelcol_exporter_queue_size type=Gauge unit="{batch}" monotonic=false val=0 attrs=map[data_type:traces exporter:queue_batch/default_gcp_destination_batch_traces]
   Schema Audit: name=otelcol_exporter_queue_capacity type=Gauge unit="{batch}" monotonic=false val=10 attrs=map[data_type:metrics exporter:queue_batch/default_gcp_destination_batch_metrics]
   Schema Audit: name=otelcol_exporter_queue_capacity type=Gauge unit="{batch}" monotonic=false val=10 attrs=map[data_type:logs exporter:queue_batch/default_gcp_destination_batch_logs]
   Schema Audit: name=otelcol_exporter_queue_capacity type=Gauge unit="{batch}" monotonic=false val=10 attrs=map[data_type:traces exporter:queue_batch/default_gcp_destination_batch_traces]
   Schema Audit: name=otelcol_exporter_enqueue_failed_log_records type=Sum unit="{record}" monotonic=true val=15 attrs=map[exporter:queue_batch/default_gcp_destination_batch_logs]
   Schema Audit: name=otelcol_exporter_enqueue_failed_metric_points type=Sum unit="{datapoint}" monotonic=true val=15 attrs=map[exporter:queue_batch/default_gcp_destination_batch_metrics]
   Schema Audit: name=otelcol_exporter_enqueue_failed_spans type=Sum unit="{span}" monotonic=true val=15 attrs=map[exporter:queue_batch/default_gcp_destination_batch_traces]
   ```
   - **Critical Discrepancy Found Against Tab 2 (`tango_onepager_2_gboc_self_o11y.md` lines 53–55)**:
     * Tab 2's table lists plural units `{log_records}`, `{datapoints}`, and `{spans}` for `otelcol_exporter_enqueue_failed_{log_records,metric_points,spans}`.
     * In reality, `exporter/exporterhelper/metadata.yaml` (`v0.162.0` lines 31, 40, 58) emits singular units **`{record}`**, **`{datapoint}`**, and **`{span}`**!
     * Meanwhile, `otelcol_exporter_queue_size` and `otelcol_exporter_queue_capacity` DO carry both `exporter` and `data_type` (`logs`, `metrics`, `traces`) with unit `{batch}`, whereas `otelcol_exporter_enqueue_failed_*` carries ONLY `exporter` (no `data_type`), matching Tab 2's attribute column.

2. **Subtest 3b (`ScenarioC_SizerAgnosticUtilization_RequestsItemsBytes`)**:
   Proves why **Alt 2** (registering `otelcol_exporter_queue_size` without `otelcol_exporter_queue_capacity`) fails across `requests`, `items`, and `bytes` sizers while `queue_size / queue_capacity` remains dimensionless:
   ```text
   Sizer=requests: queue_size=5 (unit="{batch}"), queue_capacity=10 (unit="{batch}"), utilization=0.50
   Sizer=items: queue_size=25 (unit="{batch}"), queue_capacity=50 (unit="{batch}"), utilization=0.50
   Sizer=bytes: queue_size=146 (unit="{batch}"), queue_capacity=292 (unit="{batch}"), utilization=0.50
   ```

3. **Subtest 3c (`ScenarioD_SendFailedDoubleCountingBetweenQueueBatchAndOtlpGrpc`)**:
   - **New Architectural Finding**: Because `queuebatchprocessor` is implemented by calling `exporterhelper.NewLogs` (`queuebatchprocessor@v0.162.0/processor.go:54`), it instantiates its own `obsReportSender` with `exporter="queue_batch/default_gcp_destination_batch_logs"` in front of `otlp_grpc`'s `obsReportSender` (`exporter="otlp_grpc/default_gcp_destination"`).
   - When `otlp_grpc` fails to export a batch of `6` log records (e.g., permanent downstream error `codes.InvalidArgument` or after `5m0s` retry exhaustion) and returns the error to `queue_batch`, **both** components increment `otelcol_exporter_send_failed_log_records` by `6` (`unfiltered_sum = 12`, a **2x double-count**):
     ```text
     Scenario D (Permanent downstream error for 6 log records): send_failed[otlp_grpc]=6, send_failed[queue_batch]=6, unfiltered_sum=12 (2x double-counted!)
     ```
   - Therefore, Tab 2's proposed Drop Rate formula (`rate(otelcol_exporter_send_failed_*) + rate(otelcol_exporter_enqueue_failed_*) + rate(otelcol_receiver_refused_*)`) in Section *Proposed Design* (items 2 and 3) MUST filter `otelcol_exporter_send_failed_*` by `exporter !~ "queue_batch/.*"` (or scope `send_failed_*` to actual destination exporters like `otlp_grpc/*`) so downstream export failures are not counted twice!

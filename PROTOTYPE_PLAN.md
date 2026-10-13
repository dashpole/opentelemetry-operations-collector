# Prototype Plan: GBOC Self-Observability — Queue Saturation and Drop Visibility (Tab 2)

**Repository**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`v0.162.0`)
**Branch**: `proto/tango-tab2-self-o11y-queue-metrics`
**Companion Design Document**: `tango_onepager_2_gboc_self_o11y.md` (*GBOC Self-Observability: Queue Saturation and Drop Visibility*)
**Related Allianz / Bindplane Findings**: `#42` (Missing UI Visibility for Overload, Drops, & Queue Saturation), `#43` (Missing Health & Drop Alerting), `#80` (High-Volume Collection Lag Under Saturated Queue)

---

## 1. Core Question / Hypothesis

Can we empirically prove using an in-process OpenTelemetry Collector (`v0.162.0`) configured by GBOC's `SelfMetricsPolicy` and `GCPDestinationPolicy` (`queue_batch` + `otlp_grpc` + OTel SDK periodic metric reader) and live Cloud Monitoring (`telemetry.googleapis.com:443` in `dashpole-dev`) that:

1. **Hypothesis H1 (100% Saturated Blocking Queue Blind Spot — Findings `#42`, `#80`)**:
   Under `GCPDestinationPolicy`'s default queue configuration (`block_on_overflow: true`, `queue_size: 10`, `num_consumers: 1`) and `otlp_grpc` retry configuration (`retry_on_failure.enabled: true`, `max_elapsed_time: 5m0s`), when a downstream OTLP receiver stalls or throttles so the `queue_batch` queue fills to 100% capacity (`queue_size == 10`, `queue_capacity == 10`), the current 24-metric catalog (`otelcol_exporter_send_failed_*` and `otelcol_receiver_refused_*`) reports **`0` errors**, whereas `obsQueue`'s `otelcol_exporter_queue_size` (`10`) and `otelcol_exporter_queue_capacity` (`10`) immediately surface `100%` (`1.0`) queue saturation.
2. **Hypothesis H2 (Queue-Entry Drop Blind Spot — Findings `#42`, `#43`)**:
   Whenever a batch is rejected at queue entry—either (a) when `block_on_overflow: false` is configured and the queue is full (`ErrQueueIsFull`), (b) when `block_on_overflow: true` is configured and a single incoming request exceeds `queue_size` capacity (`errSizeTooLarge`), or (c) when `block_on_overflow: true` is configured and the upstream caller's context expires while blocked in `Offer()`—`otelcol_exporter_send_failed_*` remains **`0`** while `otelcol_exporter_enqueue_failed_{log_records,metric_points,spans}` increments by the exact number of dropped items.
3. **Hypothesis H3 (Exact Upstream Metric Schema & Live `telemetry.googleapis.com:443` Ingestion Behavior)**:
   Verify the exact metric names, OTLP kinds (`Gauge` vs. monotonic cumulative `Sum`), unit strings (`{batch}`, `{record}`, `{datapoint}`, `{span}` — noting that upstream `exporter/exporterhelper/metadata.yaml` uses singular `{record}`, `{datapoint}`, `{span}`, correcting the plural `{log_records}`, `{datapoints}`, `{spans}` listed in `tango_onepager_2_gboc_self_o11y.md` lines 53–55), and attributes (`exporter`, `data_type`) emitted by `go.opentelemetry.io/collector/exporter/exporterhelper@v0.162.0` (`internal/queue/obs_queue.go`), and test live `MetricsService/Export` calls against `telemetry.googleapis.com:443` in project `dashpole-dev` to observe how Cloud Monitoring handles `otelcol_exporter_queue_size`, `otelcol_exporter_queue_capacity`, and `otelcol_exporter_enqueue_failed_*` compared to pre-registered `otelcol_exporter_send_failed_*`.

---

## 2. Scope & Intentional Simplifications

### In-Scope
- Real in-process OpenTelemetry Collector (`v0.162.0`) components wired using `SelfMetricsPolicy`, `GCPDestinationPolicy`, `queuebatchprocessor`, `otlpexporter`, and `otlpreceiver`.
- Real OTel SDK `PeriodicMetricReader` (with `Interval` overridden from `5000ms` to `100ms` for fast deterministic test collection) + OTLP gRPC metric export capturing the collector's actual `otelcol_*` self-observability metrics.
- Quantitative verification of:
  - **Scenario A**: 100% saturated blocking queue (`block_on_overflow: true`) with stalled downstream `LogsService/Export` receiver while `MetricsService/Export` remains unblocked for self-metrics delivery.
  - **Scenario B**: Queue-entry drops across (2a) non-blocking overflow (`block_on_overflow: false`), (2b) oversized batch with `block_on_overflow: true` (`errSizeTooLarge`), and (2c) caller context deadline expiration while blocked in `block_on_overflow: true` (`context.DeadlineExceeded`).
  - **Scenario C**: Sizer-agnostic utilization ratio (`queue_size / queue_capacity` in `[0.0, 1.0]`) across `requests`, `items`, and `bytes` sizers.
  - **Scenario D**: Downstream permanent error (`codes.InvalidArgument`) showing `queue_batch/*` + `otlp_grpc/*` 2x double-reporting on `otelcol_exporter_send_failed_*` when not filtered by `exporter`.
- Live gRPC calls to `telemetry.googleapis.com:443` (`opentelemetry.proto.collector.metrics.v1.MetricsService/Export`) authenticated via `gcloud auth print-access-token` with `x-goog-user-project: dashpole-dev` against GCP project `dashpole-dev`.

### Out-of-Scope (Intentional PoC Simplifications)
- Modifying server-side Unified Telemetry Router (UTR) / Cloud Monitoring metric descriptor registration tables in Google3 (this prototype tests the GBOC collector emission contract and probes the live `telemetry.googleapis.com:443` endpoint as currently deployed).
- Building the frontend Tango Console UI badges (`#42`) or Terraform module packaging (`#43`).
- Waiting a full wall-clock 5 minutes in unit tests (in Scenario A, we hold the downstream `LogsService/Export` gRPC handler blocked during the active export/retry window while `max_elapsed_time: 5m0s` is configured on the exporter, proving that during the stall window `send_failed == 0` while `queue_size == 10`).

---

## 3. Candidate Approaches Evaluated (Symmetric Rubric)

We evaluate the three observability designs from Tab 2 (`tango_onepager_2_gboc_self_o11y.md` Section *Alternatives Considered*) against identical workload scenarios:

| Approach | Metric Set Evaluated | Scenario A: 100% Saturated Blocking Queue (`block_on_overflow: true`) | Scenario B: Queue-Entry Drops (`ErrQueueIsFull` / `errSizeTooLarge` / `ctx.DeadlineExceeded`) | Scenario C: Sizer-Agnostic Utilization Ratio (`requests`, `items`, `bytes`) | Scenario D: Downstream Permanent Export Failure (`send_failed_*` Attribution) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Baseline (Alt 1: Current 24-Metric Catalog)** | `otelcol_exporter_send_failed_*`, `otelcol_receiver_refused_*` | Expected: `0` errors reported (false healthy / blind spot) | Expected: `otelcol_exporter_send_failed_* == 0` (blind to queue-entry drops) | Expected: N/A (no queue metrics) | Expected: Emits `send_failed_*` for both `otlp_grpc/*` and `queue_batch/*` (2x double-count if summed without `exporter` filter) |
| **Alt 2: `queue_size` Only** | Baseline + `otelcol_exporter_queue_size` | Expected: Raw integer `10` (or bytes/items), no capacity denominator | Expected: `0` drop counters | Expected: Fails to compute `%` utilization when `queue_size` or `sizer` (`requests` vs. `items` vs. `bytes`) changes | Same as Baseline |
| **Proposed: 5 `obs_queue` Metrics** | Baseline + `otelcol_exporter_queue_size`, `otelcol_exporter_queue_capacity`, `otelcol_exporter_enqueue_failed_{log_records,metric_points,spans}` | Expected: `queue_size = 10`, `queue_capacity = 10` (`utilization = 1.0`) | Expected: `enqueue_failed_*` equals exact dropped record/point/span count | Expected: `queue_size / queue_capacity` is dimensionless `[0.0, 1.0]` across `requests`, `items`, and `bytes` sizers | Same as Baseline (requires `exporter` filter on `send_failed_*` in alert query) |

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: End-to-End Self-Observability Queue Saturation & Drop Harness in GBOC

- **Deliverable**:
  - `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics/queue_observability_harness.go` and `queue_observability_harness_test.go`:
    1. Helper/catalog definitions (`ObsQueueMetricNames`, `ComputeQueueUtilization`, `EvaluateCatalogHealth`) and an end-to-end test harness that runs a real OTel Collector pipeline configured from `SelfMetricsPolicy` + `GCPDestinationPolicy` (`queue_batch` + `otlp_grpc`).
       - The harness's controllable local OTLP gRPC server implements `LogsService`, `MetricsService`, and `TraceService`. It selectively stalls or fails **only** the workload signal under test (e.g., `LogsService/Export` in Test 1) while immediately accepting `MetricsService/Export` so the collector's `PeriodicMetricReader` (configured with `Interval: 100ms`) delivers `otelcol_*` self-metrics without contention.
       - Because `SelfMetricsPolicy.TracesPipelines` returns `nil, nil` (`selfmetrics_policy.go:271-273`), the harness explicitly wires a workload `traces` pipeline using `GCPDestinationPolicy.PreProcessTraceIDs()` (`queue_batch/default_gcp_destination_batch_traces`) and `ExporterIDs()` (`otlp_grpc/default_gcp_destination`) when testing all three signals (`logs`, `metrics`, `traces`).
    2. **Test 1 (`TestQueueSaturation_BlockingQueue_BaselineVsProposed` — Scenario A)**:
       - Configures `queue_batch/default_gcp_destination_batch_logs` (`block_on_overflow: true`, `queue_size: 10`, `num_consumers: 1`, `sizer: requests`, `batch.flush_timeout: 50ms`, `batch.min_size: 1`, `batch.max_size: 1`) and `otlp_grpc/default_gcp_destination` (`retry_on_failure.max_elapsed_time: 5m0s`) exporting to the controllable local OTLP gRPC server with `LogsService/Export` stalled.
       - Sends **10** single-record log batches synchronously. Because `memoryQueue.Read()` pops items without decrementing `mq.size` (`memory_queue.go:146`) and `mq.onDone()` only decrements `mq.size` after `partitionBatcher.flush()` completes `consumeFunc` (`memory_queue.go:164`, `partition_batcher.go:292`), all 10 unacknowledged batches (1 in-flight in the stalled worker + 1 waiting on `wp.workers` + 8 in `mq.items`) are counted in `mq.size`, bringing `mq.size` to `10/10` (`otelcol_exporter_queue_size == 10`).
       - Spawns an **11th** single-record log batch in a background goroutine, which hits `mq.size+elSize > mq.cap` (`10+1 > 10` at `memory_queue.go:111`) and blocks in `mq.hasMoreSpace.Wait(ctx)`.
       - Waits for a `PeriodicMetricReader` self-metrics export cycle and asserts from the actual emitted OTLP `pmetric.Metrics`:
         * **Baseline (24-metric catalog)**: `otelcol_exporter_send_failed_log_records == 0` AND `otelcol_receiver_refused_log_records == 0` AND `otelcol_exporter_enqueue_failed_log_records == 0`.
         * **Proposed (`obsQueue` metrics)**: `otelcol_exporter_queue_size == 10` and `otelcol_exporter_queue_capacity == 10` (`utilization == 1.0`), with attributes `exporter="queue_batch/default_gcp_destination_batch_logs"` and `data_type="logs"`.
         * Releases the downstream `LogsService/Export` stall and verifies `otelcol_exporter_queue_size` drains back to `0` (`utilization == 0.0`) and `otelcol_exporter_sent_log_records == 11` for `exporter="otlp_grpc/default_gcp_destination"` (and `11` for `exporter="queue_batch/default_gcp_destination_batch_logs"`).
    3. **Test 2 (`TestQueueEntryDrops_EnqueueFailedVsSendFailed` — Scenario B)**:
       - Subtest 2a (**Non-blocking overflow: `block_on_overflow: false`**): Stalls downstream `LogsService/Export`, sends 10 batches to fill `mq.size` to `10/10`, and sends 3 additional batches (each with 5 log records = 15 log records). Proves `otelcol_exporter_send_failed_log_records == 0` while `otelcol_exporter_enqueue_failed_log_records == 15` (with attribute `exporter="queue_batch/default_gcp_destination_batch_logs"` and **no** `data_type` attribute).
       - Subtest 2b (**Oversized batch with `block_on_overflow: true` and `sizer: items`**): Configures `GCPDestinationPolicy`'s `block_on_overflow: true` with `sizer: items` and `queue_size: 10`, then sends a single batch of `25` log records (`elSize = 25 > cap = 10`, triggering `errSizeTooLarge` in `memory_queue.go:75`). Proves that **even with `block_on_overflow: true`**, `Offer()` rejects the batch immediately without blocking, `otelcol_exporter_send_failed_log_records == 0`, and `otelcol_exporter_enqueue_failed_log_records == 25`!
       - Subtest 2c (**Caller context deadline expiry while blocked in `block_on_overflow: true`**): With `block_on_overflow: true` and queue full (`10/10`), sends a batch of `7` log records with a `100ms` context timeout (`mq.hasMoreSpace.Wait(ctx)` returns `context.DeadlineExceeded`). Proves that **even with `block_on_overflow: true`**, `otelcol_exporter_send_failed_log_records == 0` while `otelcol_exporter_enqueue_failed_log_records == 7`!
    4. **Test 3 (`TestObsQueueMetricSchema_Sizers_AndDoubleCountingAudit` — Scenarios C & D)**:
       - Subtest 3a (**Metric Schema Audit across `logs`, `metrics`, and `traces`**): Audits the exact emitted OTLP `pmetric.Metric` schema across all three signals:
         * `otelcol_exporter_queue_size`: `MetricTypeGauge`, unit `"{batch}"`, attributes `{"exporter": ..., "data_type": "logs"|"metrics"|"traces"}`.
         * `otelcol_exporter_queue_capacity`: `MetricTypeGauge`, unit `"{batch}"`, attributes `{"exporter": ..., "data_type": "logs"|"metrics"|"traces"}`.
         * `otelcol_exporter_enqueue_failed_log_records`: `MetricTypeSum` (monotonic, cumulative), unit `"{record}"`, attributes `{"exporter": ...}` (no `data_type`).
         * `otelcol_exporter_enqueue_failed_metric_points`: `MetricTypeSum` (monotonic, cumulative), unit `"{datapoint}"`, attributes `{"exporter": ...}` (no `data_type`).
         * `otelcol_exporter_enqueue_failed_spans`: `MetricTypeSum` (monotonic, cumulative), unit `"{span}"`, attributes `{"exporter": ...}` (no `data_type`).
       - Subtest 3b (**Scenario C — Sizer-Agnostic Utilization across `requests`, `items`, and `bytes`**): Configures queues with `sizer: requests` (`queue_size: 10`), `sizer: items` (`queue_size: 50`), and `sizer: bytes` (`queue_size: 2048`), fills each to 50% capacity while stalled, and proves that `otelcol_exporter_queue_size` reports raw requests (`5`), items (`25`), or bytes (with unit string still `"{batch}"`), so Alt 2 (`queue_size` alone) cannot determine utilization without hardcoding the sizer and capacity, whereas `otelcol_exporter_queue_size / otelcol_exporter_queue_capacity` yields the exact dimensionless ratio (`0.50`) across all three sizers.
       - Subtest 3c (**Scenario D — `send_failed_*` 2x Double-Counting between `queue_batch/*` and `otlp_grpc/*`**): Demonstrates that when downstream `otlp_grpc` returns a permanent error (`codes.InvalidArgument`), **both** `otlp_grpc/default_gcp_destination` AND `queue_batch/default_gcp_destination_batch_logs` increment `otelcol_exporter_send_failed_log_records` (because `queuebatchprocessor` wraps `exporterhelper.NewLogs`), proving that alert queries on `otelcol_exporter_send_failed_*` MUST filter by `exporter` (or exclude `queue_batch/*`) to avoid 2x double-counting!
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v ./policies/selfmetrics -run "TestQueueSaturation|TestQueueEntryDrops|TestObsQueueMetricSchema"
  ```
- **Baseline-vs-Change Proof**:
  - In every test, we compute both the **Baseline 24-Metric Catalog Health View** (`send_failed_* + receiver_refused_*`) and the **Proposed Extended Catalog Health View** (`queue_size / queue_capacity` + `enqueue_failed_*`) on the exact same emitted OTLP metric payload. Under Scenario A and Scenario B, the Baseline Health View reports `0` errors / `0%` saturation (failing to detect the fault), while the Proposed Health View reports `100%` saturation (`10/10`) and exact dropped item counts (`15`, `25`, `7`).

---

### Milestone 2: Live `telemetry.googleapis.com:443` / Cloud Monitoring Verification in `dashpole-dev`

- **Deliverable**:
  - Live integration test (`TestLiveTelemetryGoogleAPIs_SelfMetricsExport` in `queue_observability_live_test.go`, gated by `RUN_LIVE_GCP_TEST=1` so offline CI remains hermetic) that:
    1. Obtains an OAuth2 access token via `gcloud auth print-access-token` for project `dashpole-dev` and attaches `authorization: Bearer <token>` and `x-goog-user-project: dashpole-dev` gRPC metadata headers.
    2. **Negative Control (Baseline-vs-Change Proof)**: Sends an unauthenticated `ExportMetricsServiceRequest` to `telemetry.googleapis.com:443` and falsifiably asserts that the live server rejects it with `codes.Unauthenticated`.
    3. Sends isolated per-metric `ExportMetricsServiceRequest` calls to `telemetry.googleapis.com:443` (`opentelemetry.proto.collector.metrics.v1.MetricsService/Export`) with resource attributes `gcp.project_id = "dashpole-dev"`, `service.instance.id`, and `gcp.fleet_id` for:
       - Pre-registered baseline metric: `otelcol_exporter_send_failed_log_records`
       - Candidate queue metrics: `otelcol_exporter_queue_size`, `otelcol_exporter_queue_capacity`, `otelcol_exporter_enqueue_failed_log_records`, `otelcol_exporter_enqueue_failed_metric_points`, `otelcol_exporter_enqueue_failed_spans`
    4. Queries the Cloud Monitoring REST API (`https://monitoring.googleapis.com/v3/projects/dashpole-dev/metricDescriptors` with filter `metric.type = starts_with("agent.googleapis.com/agent/")` and `metric.type = starts_with("workload.googleapis.com/otelcol_")`) and `timeSeries` to deterministically assert and record:
       - The exact gRPC status code and `ExportMetricsServiceResponse.PartialSuccess` returned by `telemetry.googleapis.com:443` for each metric.
       - Which `otelcol_*` metric descriptors exist in Cloud Monitoring (`agent.googleapis.com/agent/*` vs. `workload.googleapis.com/*`) in `dashpole-dev`.
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab2_self_o11y/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && RUN_LIVE_GCP_TEST=1 go test -v ./policies/selfmetrics -run TestLiveTelemetryGoogleAPIs_SelfMetricsExport
  ```
- **Baseline-vs-Change Proof**:
  - Compares live `telemetry.googleapis.com:443` response and Cloud Monitoring descriptor/time-series behavior across the unauthenticated negative control (`codes.Unauthenticated`), the baseline metric (`otelcol_exporter_send_failed_log_records`), and the 5 `obsQueue` metrics (`otelcol_exporter_queue_size`, `otelcol_exporter_queue_capacity`, `otelcol_exporter_enqueue_failed_*`) in `dashpole-dev`.

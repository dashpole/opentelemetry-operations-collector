# Prototype Plan: GBOC Two-Stage `QueueBatch` Chain for UTR Dual Request Limits

**Author**: dashpole@google.com  
**Target Repository**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`v0.162.0`, branch `proto/tango-tab1-two-stage-batching`)  
**Companion Design Doc**: `tango_onepager_1_gboc_batching_utr_limits.md` (Tab 1: GBOC Batch Sizing for UTR Dual Request Limits)

---

## 1. Core Question & Hypothesis

The Unified Telemetry Router (`telemetry.googleapis.com:443`) enforces **two independent hard limits** on every OTLP export request:
1. **Byte-size limit**: `5 MiB` (`5,242,880` bytes) maximum uncompressed protobuf and wire message size (`--grpc_max_uncompressed_bytes=5242880`, `--grpc_max_message_size=5242880`).
2. **Item-count limit**: `8,192` log records (`kMaxLogCountPerBatch`), `8,192` spans (`kMaxSpanCountPerBatch`), or `200` metric data points (`kMaxPointsPerRequest`) per batch.

However, upstream OpenTelemetry Collector `v0.162.0`'s `queuebatch` (`exporterhelper/internal/queuebatch`) supports only a **single** `sizer` per `BatchConfig` (`items` **or** `bytes`, not both).

**Core Hypothesis**:
1. **Config Unmarshaling & Prerequisite Fixes (`opentelemetry-operations-collector#710`, `#711`)**: In upstream `exporterhelper/internal/request/sizer.go:18-51`, `request.SizerType` is a struct with an unexported `val string` field and a pointer-receiver `(*SizerType).MarshalText()`; `confmap.Conf.Marshal` therefore serializes value fields of type `request.SizerType` as an empty map (`sizer: {}`). When `confmap.Conf.Unmarshal` subsequently decodes `sizer: {}` into `queuebatchprocessor.NewFactory().CreateDefaultConfig()`, `mapstructure` treats the empty map `{}` as a no-op on the struct—**silently discarding `RequestSizerTypeBytes` and reverting `Sizer` back to the factory defaults (`"requests"` for queue and `"items"` for batch)**, which would cause Stage 1's `min_size: 3670016` to be evaluated as 3.67M items instead of 3.5 MiB! Fixing `GCPDestinationPolicy.Evaluate()` so both queue `sizer` and `batch.sizer` are serialized as strings (`"bytes"`, `"items"`, `"requests"`, `#711`) and lowering trace `max_size` from `25000` to `8192` (`#710`) allows the synthesized `confmap.Conf` to cleanly unmarshal into upstream `queuebatchprocessor.Config` and `otlpexporter.Config`.
2. **Per-Signal Two-Stage Chaining in `GCPDestinationPolicy`**: Because `GCPDestinationPolicy` declares a single `otlp_grpc/<name>` exporter shared across `logs`, `metrics`, and `traces` pipelines (`DestinationPolicy.ExporterIDs()` is shared across all three signals in `provider.go:464-484`), while UTR's item limits are signal-specific (`8,192` for logs/traces vs. `200` for metrics), chaining two per-signal `queue_batch` processors in `PreProcess{Log,Metric,Trace}IDs()`—
   - **Stage 1 (`queue_batch/<name>_batch_{logs,metrics,traces}`)**: Queue `sizer: bytes`, `queue_size: 33554432` (32 MiB), `batch: { sizer: bytes, min_size: 3670016 (3.5 MiB), max_size: 4194304 (4 MiB), flush_timeout: 200ms }`
   - **Stage 2 (`queue_batch/<name>_split_{logs,metrics,traces}`)**: Queue `sizer: requests`, `queue_size: 10` (so `memoryQueue.Offer` counts each Stage 1 batch as `1` request rather than comparing item count against `10`), `batch: { sizer: items, min_size: 0, max_size: 8192 (logs/traces) | 200 (metrics), flush_timeout: 200ms }`
   —simultaneously enforces `<= 4 MiB` uncompressed bytes AND `<= 8,192` items (`<= 200` for metrics) on every exported OTLP request.
3. **Zero Holding Delay in Stage 2 (`min_size: 0`)**: Because `partitionBatcher.consumeInternal` checks `qb.sizer.Sizeof(lastReq) < qb.cfg.MinSize`, configuring `min_size: 0` in Stage 2 ensures `Sizeof(lastReq) < 0` is permanently false. Stage 2 never holds a partial batch in `qb.currentBatch` and never waits for `flush_timeout: 200ms`—it flushes `<= 8,192`-item batches immediately (`< 50ms` traversal latency, well below `flush_timeout: 200ms`) and immediately splits `> 8,192`-item batches into `<= 8,192`-item sub-batches.

---

## 2. Scope & Intentional Simplifications

| Category | In-Scope for Prototype | Out-of-Scope (Intentional Simplifications) |
| :--- | :--- | :--- |
| **GBOC Policy Generation (`gcpdestination_policy.go`)** | Full production-quality implementation in `GCPDestinationPolicy.Evaluate()`, fixing `#710`, `#711`, and wiring the Two-Stage `QueueBatch` Chain (`_batch_*` + `_split_*`) into `PreProcess{Log,Metric,Trace}IDs()`. | Modifying `DestinationPolicy` interface in `pkg/googlepolicy` to return per-signal exporter IDs (unnecessary when Stage 2 uses `_split_*` processors). |
| **Upstream OTel Collector (`v0.162.0`)** | Running unmodified upstream `queuebatchprocessor` (`v0.162.0`) and `otlpexporter` (`v0.162.0`) wired via the exact `confmap.Conf` produced by `GCPDestinationPolicy.Evaluate()`. | Modifying upstream `go.opentelemetry.io/collector/exporter/exporterhelper` (tracked as a future upstream enhancement in `tango_onepager_1_gboc_batching_utr_limits.md`). |
| **Live Cloud Verification (`telemetry.googleapis.com:443`)** | End-to-end OTLP/gRPC export against live production `telemetry.googleapis.com:443` in project `dashpole-dev` using `gcloud auth print-access-token` for both Case A (heavy WEF XML logs) and Case B (compact 200-byte logs). | Multi-hour soak testing or disk persistence crash testing (`file_storage`). |

---

## 3. Symmetric Candidate Evaluation Rubric

All three batching configurations are evaluated using identical `plog.Logs` payloads, identical upstream `queuebatchprocessor` + `otlpexporter` (`v0.162.0`) components, and identical OTLP/gRPC targets (both deterministic local gRPC test server enforcing UTR's 5 MiB + 8,192-item limits and live `telemetry.googleapis.com:443` in `dashpole-dev`):

1. **Candidate 0 (Baseline — Current `GCPDestinationPolicy` Single-Stage Items Batcher)**:
   - Queue `sizer: requests`, `queue_size: 10`, `batch: { sizer: items, min_size: 8192, max_size: 8192, flush_timeout: 200ms }` (with `#711` `sizer: "requests"` / `batch.sizer: "items"` fixed so the baseline can actually start; unpatched `sizer: {}` fails `confmap.Unmarshal` immediately).
2. **Candidate 1 (Alternative 1 — Single-Stage Bytes Batcher)**:
   - Queue `sizer: bytes`, `queue_size: 33554432` (32 MiB), `batch: { sizer: bytes, min_size: 3670016 (3.5 MiB), max_size: 4194304 (4 MiB), flush_timeout: 200ms }`.
3. **Candidate 2 (Proposed — Two-Stage `QueueBatch` Chain)**:
   - Stage 1 (`_batch_logs`): Queue `sizer: bytes`, `queue_size: 33554432` (32 MiB), `batch: { sizer: bytes, min_size: 3670016 (3.5 MiB), max_size: 4194304 (4 MiB), flush_timeout: 200ms }`.
   - Stage 2 (`_split_logs`): Queue `sizer: requests`, `queue_size: 10`, `batch: { sizer: items, min_size: 0, max_size: 8192, flush_timeout: 200ms }`.

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Code Implementation in `GCPDestinationPolicy` & Pipeline Integration Tests

- **Deliverable**:
  1. Update `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination/gcpdestination_policy.go` to:
     - Fix `#711` (`sizer` marshaling as `sizer: {}`) by explicitly setting string sizer values (`"bytes"`, `"requests"`, `"items"`) in the marshaled `confmap.Conf`.
     - Fix `#710` (set trace item `max_size: 8192` instead of `25000`).
     - Configure Stage 1 (`queue_batch/<name>_batch_{logs,metrics,traces}`) with queue `sizer: bytes`, `queue_size: 33554432` (32 MiB), `batch: { sizer: bytes, min_size: 3670016, max_size: 4194304, flush_timeout: 200ms }` and Stage 2 (`queue_batch/<name>_split_{logs,metrics,traces}`) with queue `sizer: requests`, `queue_size: 10`, `batch: { sizer: items, min_size: 0, max_size: 8192 (logs/traces) | 200 (metrics), flush_timeout: 200ms }`, returning both in order in `PreProcess{Log,Metric,Trace}IDs()`.
  2. Update golden file `testdata/policysets/builtin/result.yaml`.
  3. Add unit and integration tests in `policies/gcpdestination/gcpdestination_policy_test.go` that:
     - Verify `GCPDestinationPolicy.Evaluate()` output unmarshals cleanly into `queuebatchprocessor.NewFactory().CreateDefaultConfig()` for all 6 processors without error.
     - Feed real `plog.Logs` through the actual upstream `queuebatchprocessor` + `otlpexporter` pipeline for Case A (`8,192` logs @ ~2.5 KB = ~20.5 MB), Case B (`20,000` logs @ ~200 B = ~3.95 MiB), and Case C (single oversized `> 4 MiB` log record mixed with normal logs totaling `>= 3.5 MiB`), asserting:
       * Every exported batch has `uncompressed_bytes <= 4,194,304` AND `log_record_count <= 8,192`.
       * In Case B (`20,000` logs ≈ `3.86 MiB >= 3.5 MiB`) and in the first 5 full-byte batches of Case A (~1,583 items each), batches traverse Stage 1 + Stage 2 in `elapsed < 180ms` (`< 200ms flush_timeout`) without calling `Shutdown` (and in `Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof` with Stage 2 `flush_timeout: 5s`, `min_size: 0` flushes 1,580 logs in `< 150ms << 5s`, whereas `min_size: 8192` holds them), while Case A's 6th byte-remainder batch (`< 3.5 MiB`) flushes after Stage 1's single `200ms` `flush_timeout` (`stepElapsed < 350ms`, proving Stage 2 adds zero holding delay on top of Stage 1).
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v ./policies/gcpdestination/... .
  ```
- **Baseline-vs-Change Proof (Non-Tautological Control)**:
  1. **Bug `#711` Control**: Prove in `TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal` that unmarshaling the unpatched `sizer: {}` map into `queuebatchprocessor.NewFactory().CreateDefaultConfig()` silently discards `exporterhelper.RequestSizerTypeBytes` and reverts `Sizer` back to `"requests"` / `"items"`, whereas the patched `GCPDestinationPolicy.Evaluate()` output unmarshals cleanly with `Sizer == exporterhelper.RequestSizerTypeBytes` / `Batch.Get().Sizer == exporterhelper.RequestSizerTypeBytes` (Stage 1) and `Sizer == exporterhelper.RequestSizerTypeRequests` / `Batch.Get().Sizer == exporterhelper.RequestSizerTypeItems` (Stage 2).
  2. **Pipeline Control**: Prove in `gcpdestination_policy_test.go` against a local gRPC OTLP server configured with UTR's exact limits (`grpc.MaxRecvMsgSize(5*1024*1024)` and `maxItems = 8192`) that:
     - Candidate 0 (Single-Stage `items`, `8192`) **fails** Case A (`8,192` x 2.5 KB logs) with `rpc error: code = ResourceExhausted desc = grpc: received message [after decompression] larger than max (... vs. 5242880)`.
     - Candidate 1 (Single-Stage `bytes`, `4 MiB`) **fails** Case B (`20,000` x 200 B logs) with `rpc error: code = InvalidArgument desc = Batch contains too many log records: ... > 8192`.
     - Candidate 2 (Two-Stage Chain from `GCPDestinationPolicy.Evaluate()`) **succeeds** on both Case A (delivering all `8,192` logs across 6 batches, each `<= 4 MiB`) and Case B (delivering all `20,000` logs across 3 batches of `8192 + 8192 + 3616` items, each `<= 4 MiB` and `<= 8,192` items).

### Milestone 2: Empirical Baseline-vs-Change Verification Against Live `telemetry.googleapis.com:443` (`dashpole-dev`)

- **Deliverable**:
  - Execute a live integration test (`TestLiveUTR_BaselineVsTwoStageChain` in `policies/gcpdestination/gcpdestination_live_utr_test.go`) that authenticates via `gcloud auth print-access-token` and sends real OTLP/gRPC `ExportLogsServiceRequest` batches through the upstream `queuebatchprocessor` + `otlpexporter` pipeline directly to `telemetry.googleapis.com:443` with `gcp.project_id = "dashpole-dev"`.
  - Capture the exact wire/uncompressed sizes, batch item counts, latencies, gRPC status codes, and error strings in `milestone_2_evidence.md`.
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && RUN_LIVE_UTR_TEST=1 go test -count=1 -v ./policies/gcpdestination -run TestLiveUTR_BaselineVsTwoStageChain
  ```
- **Baseline-vs-Change Proof (Live Production UTR API)**:
  1. **Case A (Heavy WEF XML Logs — 8,192 logs @ ~2.5 KB each = `21,191,703` B uncompressed)**:
     - **Baseline (`batch.sizer: items, min_size: 8192, max_size: 8192`)**: Emits 1 batch of `8,192` logs (`21,191,703` B uncompressed) to `telemetry.googleapis.com:443` -> rejected by live UTR with `Permanent error: rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880` (with default `compression: gzip`) and `Permanent error: rpc error: code = InvalidArgument desc = Request payload size exceeds the limit: 5242880 bytes.` (with `compression: none`), dropping all `8,192` logs.
     - **Prototype (Two-Stage Chain)**: Stage 1 caps each batch at `<= 4 MiB` (`5 x 1,621` logs + `1 x 87` logs) and Stage 2 (`min_size: 0, max_size: 8192`) flushes immediately without waiting for Stage 2 `flush_timeout` -> all 6 batches (`<= 4 MiB` and `<= 8,192` items each, total `8,192` logs) return HTTP/2 gRPC `OK` (`<nil>` error) from live `telemetry.googleapis.com:443` (`dashpole-dev`)!
  2. **Case B (Compact ~200-Byte Logs — 20,000 logs @ ~202 B each = `4,048,999` B uncompressed)**:
     - **Single-Stage `sizer: bytes` (`max_size: 4 MiB`)**: Emits 1 batch of `20,000` logs (`4,048,999` B `<= 4 MiB` uncompressed) to `telemetry.googleapis.com:443` -> rejected by live UTR (`kMaxLogCountPerBatch = 8192` in `otlp_to_log_entry.cc:81-85`, sanitized externally by ESF) with `Permanent error: rpc error: code = InvalidArgument desc = Request contains an invalid argument.` (`0` logs ingested, `20,000` dropped).
     - **Prototype (Two-Stage Chain)**: Stage 1 emits the `4,048,999` B batch of `20,000` logs, and Stage 2 (`batch.sizer: items, min_size: 0, max_size: 8192`) immediately splits it into 3 sub-batches (`8,192 + 8,192 + 3,616` items), all of which return gRPC `OK` (`<nil>` error) from live `telemetry.googleapis.com:443` (`dashpole-dev`)!

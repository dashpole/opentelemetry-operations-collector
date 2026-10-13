# Post-Prototype Learnings & Design-Discrepancy Report: GBOC Two-Stage `QueueBatch` Chain

**Prototype Branch**: `proto/tango-tab1-two-stage-batching` (`GoogleCloudPlatform/opentelemetry-operations-collector` @ `v0.162.0`)  
**Companion Design Doc**: `tango_onepager_1_gboc_batching_utr_limits.md` (Tab 1: GBOC Batch Sizing for UTR Dual Request Limits)

---

## 1. What Worked vs. What Surprised Us

### What Worked as Hypothesized
1. **Simultaneous Enforcement of UTR Dual Limits (`<= 4 MiB` Uncompressed Bytes AND `<= 8,192` / `200` Items)**:
   - Chaining Stage 1 (`queue_batch/<name>_batch_{logs,metrics,traces}`, `sizer: bytes`, `queue_size: 33554432`, `min_size: 3670016`, `max_size: 4194304`, `flush_timeout: 200ms`) into Stage 2 (`queue_batch/<name>_split_{logs,metrics,traces}`, `sizer: requests`, `queue_size: 10`, `batch: { sizer: items, min_size: 0, max_size: 8192 | 200, flush_timeout: 200ms }`) enforced both UTR limits across all synthetic and live cloud workloads without modifying upstream OpenTelemetry Collector `v0.162.0`.
2. **Live `telemetry.googleapis.com:443` (`dashpole-dev`) End-to-End Verification**:
   - **Case A (`8,192` WEF XML logs @ ~2.5 KB = `21,191,703` B uncompressed)**: Baseline Single-Stage `items` (`8,192`) was rejected by live `telemetry.googleapis.com:443` (`0 / 8,192` logs ingested). The Two-Stage Chain split the payload into 6 batches (`5 x 1,621` items @ `4,192,526`–`4,193,636` B + `1 x 87` items @ `225,176` B), and **all 6 batches succeeded (`8,192 / 8,192` logs ingested)** against live `telemetry.googleapis.com:443`.
   - **Case B (`20,000` compact ~202 B logs = `4,048,999` B uncompressed)**: Single-Stage `bytes` (`max_size: 4 MiB`) emitted a single `20,000`-item batch (`4,048,999` B `<= 4 MiB`) and was rejected by live `telemetry.googleapis.com:443` with `codes.InvalidArgument` (`0 / 20,000` logs ingested). The Two-Stage Chain split the `4,048,999` B batch into 3 sub-batches (`8,192` items @ `1,653,781` B, `8,192` items @ `1,661,275` B, `3,616` items @ `734,155` B), and **all 3 sub-batches succeeded (`20,000 / 20,000` logs ingested)** against live `telemetry.googleapis.com:443`.
3. **Zero Holding Delay in Stage 2 (`min_size: 0`)**:
   - In `Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof` (`gcpdestination_policy_test.go:578-658`), setting Stage 2 `flush_timeout: 5s` with `min_size: 8192` held a `1,580`-log (`~3.95 MiB`) batch waiting for the 5s timer, whereas setting `min_size: 0` flushed the `1,580`-log batch in `< 150ms` (`<< 5s`), confirming `qb.sizer.Sizeof(lastReq) < qb.cfg.MinSize` (`Sizeof(lastReq) < 0`) is permanently false.
4. **Oversized Single-Record Isolation**:
   - In `CaseC_SingleOversizedRecord_IsolatedWithoutPoisoningBatch` (`gcpdestination_policy_test.go:529-576`), mixing 1 oversized `4.5 MiB` log record with `1,500` normal `2.5 KB` logs dropped only the single `4.5 MiB` record in `extractScopeLogs` (`logs_batch.go:181-186`) while delivering all `1,500` normal records.

### What Surprised Us (Key Technical Discoveries)
1. **Why Stage 2 in `GCPDestinationPolicy` Must Be Per-Signal `queue_batch/<name>_split_*` Processors (Not `otlp_grpc/<name>.sending_queue.batch`)**:
   - `GCPDestinationPolicy` defines a single shared `otlp_grpc/<name>` exporter across `logs`, `metrics`, and `traces` (`DestinationPolicy.ExporterIDs()` is passed identically to `LogsPipelines`, `MetricsPipelines`, and `TracesPipelines` in `provider.go:464-484`). Because `otlpexporter.Config.QueueConfig` has only one `Batch.MaxSize` field for the entire exporter, `otlp_grpc/<name>.sending_queue.batch` cannot simultaneously enforce `max_size: 8192` for logs/traces and `max_size: 200` for metrics. Using per-signal `queue_batch/<name>_split_{logs,metrics,traces}` processors in `PreProcess{Log,Metric,Trace}IDs()` enforces signal-specific item limits cleanly.
2. **Stage 2 Top-Level Queue `sizer` Must Be `requests` (Not `items`) When `queue_size: 10`**:
   - In `exporterhelper/internal/queue/memory_queue.go:63-77`, `memoryQueue.Offer(ctx, el)` checks `elSize := mq.sizer.Sizeof(el)` against `mq.cap` (`queue_size`) using the **top-level queue `sizer`** *before* handing the batch to `partitionBatcher.Consume`. If Stage 2 sets top-level `sizer: items` with the default `queue_size: 10`, `memoryQueue.Offer` immediately rejects every Stage 1 batch containing `> 10` items with `errSizeTooLarge`! Keeping Stage 2's top-level queue `sizer: requests` with `queue_size: 10` (counting each Stage 1 batch as `1` request) while setting `batch.sizer: items` avoids queue rejection.
3. **Live `telemetry.googleapis.com:443` Surfaces `> 5 MiB` Rejections as Top-Level `InvalidArgument` Wrapping Decompressor `RESOURCE_EXHAUSTED`**:
   - With `compression: gzip` (GBOC's default), live `telemetry.googleapis.com:443` aborts gunzip decompression at `uncompressed byte 5242880` (`5 MiB`) and returns:
     `Permanent error: rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880`
   - With `compression: none`, live `telemetry.googleapis.com:443` returns:
     `Permanent error: rpc error: code = InvalidArgument desc = Request payload size exceeds the limit: 5242880 bytes.`
   - Because `otlpexporter` (`processError` and `shouldRetry` in `exporter/otlpexporter/otlp.go:173-222`) treats `codes.InvalidArgument` as an unconditional `Permanent error` (without even consulting `RetryInfo`), oversized batches are immediately and permanently dropped.
4. **Silent Default-Reversion Mechanism of `#711` (`sizer: {}`)**:
   - When `cm.Marshal(conf)` serializes `exporterhelper.RequestSizerType` as `sizer: {}` (`map[string]any{}`), unmarshaling `sizer: {}` into `queuebatchprocessor.NewFactory().CreateDefaultConfig()` does not return an unmarshal error—instead, `mapstructure` decodes the empty map `{}` as a no-op on the `RequestSizerType` struct, **silently discarding `RequestSizerTypeBytes` and reverting `Sizer` to the factory defaults (`"requests"` for queue and `"items"` for batch)**. Without the `#711` post-marshal override, Stage 1's `min_size: 3670016` would be evaluated as 3.67 million items instead of 3.5 MiB!

---

## 2. Design-Doc Discrepancies & Recommended Spec Updates (`tango_onepager_1_gboc_batching_utr_limits.md`)

| Section in `tango_onepager_1_gboc_batching_utr_limits.md` | Current Design-Doc Text / Assumption | Empirical Prototype Finding & Recommended Update |
| :--- | :--- | :--- |
| **`## Context and Motivation` (`L27`) & `## Alternatives Considered` (`L99`)** | States that ESF rejects `> 5 MiB` batches with `RESOURCE_EXHAUSTED` (without `RetryInfo`). | Clarify that with `compression: gzip`, ESF's streaming decompressor aborts at `uncompressed byte 5242880` with `decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880` and surfaces it to the gRPC client as **top-level `codes.InvalidArgument`** (`Permanent error`, unconditionally non-retryable in `exporter/otlpexporter/otlp.go:173-222`). |
| **`## Proposed Design: Two-Stage Chaining` (`L40-L41`, `L60-L61`, `L71`)** | Presents Stage 2 as either `otlp_grpc/<name>`'s built-in `sending_queue.batch` or a second `queue_batch` processor (`sizer: items, min_size: 0, max_size: 8192`). | Specify that in `GCPDestinationPolicy`, Stage 2 **must** be per-signal `queue_batch/<name>_split_{logs,metrics,traces}` processors (because `otlp_grpc/<name>` is shared across all 3 signals in `provider.go:464-484` while metrics require `max_size: 200` vs. `8,192` for logs/traces), and specify that Stage 2's top-level queue uses `sizer: requests, queue_size: 10` while `batch` uses `sizer: items, min_size: 0, max_size: 8192 \| 200`. |
| **`### Upfront Trade-Offs and Required Prerequisites` (`L74`)** | Describes `#711` as `GCPDestinationPolicy` marshaling `request.SizerType` as `sizer: {}`. | Add the critical detail discovered in Milestone 1: unmarshaling `sizer: {}` into `queuebatchprocessor.CreateDefaultConfig()` silently no-ops and reverts `Sizer` from `"bytes"` back to `"items"`, which would cause `min_size: 3670016` to wait for 3.67M items unless `#711` is fixed. |

---

## 3. Productionization Gap Ledger

| Item | Prototype Status | Production Hardening Action Required |
| :--- | :--- | :--- |
| **`#711` Post-Marshal `sizer` Override in `gcpdestination_policy.go`** | Implemented in `gcpdestination_policy.go:180-218` via `postMarshalOverrides` merge after `cm.Marshal(conf)`. | Upstream a value-receiver `MarshalText() ([]byte, error)` fix on `request.SizerType` in `open-telemetry/opentelemetry-collector` (`exporter/exporterhelper/internal/request/sizer.go`) so `confmap.Conf.Marshal` natively serializes `RequestSizerType` as a string without post-marshal map overrides. |
| **Upstream Native Dual-Limit Batching (`MaxItems` + `MaxBytes`)** | Enforced via Two-Stage `QueueBatch` Chain (`_batch_*` + `_split_*`) in `GCPDestinationPolicy`. | Propose adding dual-limit (`items` + `bytes`) support to `BatchConfig` in `open-telemetry/opentelemetry-collector` (`exporter/exporterhelper/internal/queuebatch`) so Stage 1 and Stage 2 can eventually collapse into a single `QueueBatch` instance. |
| **Single Log Record `> 256 KiB` Cloud Logging Per-Entry Limit** | Stage 1 isolates single records `> 4 MiB` (`extractScopeLogs`), preventing OTLP request rejection. | UTR enforces a per-`LogEntry` limit of `256 KiB` in `otlp_to_log_entry.cc`; records between `256 KiB` and `4 MiB` require a transform/truncation policy or upstream `max_record_size` guard if customers emit single `> 256 KiB` events. |

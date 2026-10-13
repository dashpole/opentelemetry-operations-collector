# Demo & Walkthrough Plan: GBOC Two-Stage `QueueBatch` Chain for UTR Dual Request Limits

**Prototype Branch**: `proto/tango-tab1-two-stage-batching` (`GoogleCloudPlatform/opentelemetry-operations-collector` @ `v0.162.0`)  
**Workspace**: `/tmp/prototypes/tab1_batching_utr_limits`

---

## 1. 60-Second Executive Framing (Why This Matters, What Was Tested, What the Decision Is)

- **Why It Matters**: The Unified Telemetry Router (`telemetry.googleapis.com:443`) enforces two simultaneous hard limits on every OTLP export request: **`<= 5 MiB` (`5,242,880` B)** uncompressed protobuf/wire size AND **`<= 8,192` log records/spans** (`<= 200` metric data points). Upstream OpenTelemetry Collector `v0.162.0`'s `queuebatch` supports only a single `sizer` (`items` **or** `bytes`), causing GBOC's current single-stage `sizer: items` (`8,192`) batcher to drop 100% of heavy Windows Event Forwarding (WEF) XML batches (~20.2 MiB uncompressed) with a non-retryable `Permanent error`, while a single-stage `sizer: bytes` (`4 MiB`) batcher drops 100% of compact 200-byte log bursts (~20,000 items in 4 MiB) with `InvalidArgument`.
- **What Was Tested**: We implemented the **Two-Stage `QueueBatch` Chain** in `GCPDestinationPolicy` (Stage 1 `_batch_*` byte batcher `3.5–4.0 MiB` -> Stage 2 `_split_*` zero-delay item splitter `min_size: 0, max_size: 8192 | 200`) along with fixes for `#710` and `#711`, and tested both failure modes end-to-end against an in-process UTR simulator and **live production `telemetry.googleapis.com:443` in `dashpole-dev`**.
- **Decision / Next Step**: Adopt the per-signal Two-Stage `QueueBatch` Chain (`_batch_{logs,metrics,traces}` + `_split_{logs,metrics,traces}`) and `#710`/`#711` fixes in `GCPDestinationPolicy` immediately for Project Tango, while pursuing native dual-limit batching upstream as a medium-term optimization.

---

## 2. Option A: Interactive Terminal Walkthrough (Reproducible Step-by-Step Commands)

### Step 1: Inspect the `GCPDestinationPolicy` Implementation & Golden Config Diff
```bash
cd /tmp/prototypes/tab1_batching_utr_limits
git diff --stat v0.162.0
git diff v0.162.0 -- \
  components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination/gcpdestination_policy.go \
  components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/builtin/result.yaml
```
**What to highlight**:
- Stage 1 (`queue_batch/default_gcp_destination_batch_{logs,metrics,traces}`): `sizer: bytes`, `queue_size: 33554432` (32 MiB), `batch: { sizer: bytes, min_size: 3670016, max_size: 4194304, flush_timeout: 200ms }`.
- Stage 2 (`queue_batch/default_gcp_destination_split_{logs,metrics,traces}`): `sizer: requests`, `queue_size: 10`, `batch: { sizer: items, min_size: 0, max_size: 8192 (logs/traces) | 200 (metrics), flush_timeout: 200ms }`.
- Explicit string serialization of `sizer` and `batch.sizer` fixing `#711` (`sizer: {}`).

### Step 2: Run Local Unit & UTR-Simulator Pipeline Integration Tests (Milestone 1)
```bash
cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -count=1 -v ./policies/gcpdestination -run "TestGCPDestinationPolicy_"
```
**Expected Output Highlights**:
- `TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal`: Proves unpatched `sizer: {}` silently reverts `RequestSizerTypeBytes` back to `"requests"` / `"items"`, whereas patched `GCPDestinationPolicy.Evaluate()` unmarshals and validates all 6 processors cleanly.
- `CaseA_HeavyWEFXMLLogs_8192x2500B`: Baseline fails with `Permanent error: rpc error: code = ResourceExhausted desc = grpc: received message after decompression larger than max 5242880`; Two-Stage Chain splits into 6 batches (`5 x 1,621` items @ `4,192,526`–`4,193,636` B + `1 x 87` items @ `225,176` B) and delivers all `8,192` logs.
- `CaseB_CompactLogs_20000x200B`: Single-Stage `bytes` fails with `Batch contains too many log records: 20000 > 8192`; Two-Stage Chain immediately splits into `8,192 + 8,192 + 3,616` items (`1,653,781` B, `1,661,275` B, `734,155` B) in `< 180ms` without calling `Shutdown`.
- `Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof`: Proves `min_size: 0` flushes a `1,580`-item batch in `< 150ms` even when Stage 2's `flush_timeout` is `5s` (whereas `min_size: 8192` holds it).

### Step 3: Run Live Production `telemetry.googleapis.com:443` Verification in `dashpole-dev` (Milestone 2)
```bash
cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
RUN_LIVE_UTR_TEST=1 go test -count=1 -v ./policies/gcpdestination -run TestLiveUTR_BaselineVsTwoStageChain
```
**Expected Output Highlights**:
- **Case A Baseline (`8,192` WEF XML logs = `21,191,703` B uncompressed)**: Rejected by live `telemetry.googleapis.com:443` with `Permanent error: rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880` (`gzip`) and `Request payload size exceeds the limit: 5242880 bytes.` (`uncompressed`).
- **Case A Two-Stage Chain**: All 6 batches (`1,621 x 5 + 87 = 8,192` items, each `<= 4,193,636` B) accepted by live `telemetry.googleapis.com:443` (`err=<nil>`).
- **Case B Single-Stage Bytes (`20,000` compact logs = `4,048,999` B uncompressed)**: Rejected by live `telemetry.googleapis.com:443` with `Permanent error: rpc error: code = InvalidArgument desc = Request contains an invalid argument.`.
- **Case B Two-Stage Chain**: All 3 split sub-batches (`8,192 + 8,192 + 3,616 = 20,000` items) accepted by live `telemetry.googleapis.com:443` (`err=<nil>`).

---

## 3. Option B: AI-Narrated Walkthrough Video Storyboard & Script

*(All spoken narration text contains zero raw symbols, backticks, or URLs so TTS narration renders naturally.)*

| Scene | Visual / Terminal Display | Duration | Spoken Narration Script (TTS-Ready) |
| :--- | :--- | :--- | :--- |
| **Scene 1: The Dual-Limit Dilemma in Project Tango** | Architecture slide showing `GCPDestinationPolicy` exporting OTLP gRPC to the Unified Telemetry Router with two red guardrails: 5 mebibytes maximum uncompressed bytes and 8,192 maximum log records per batch. | 18s | In Project Tango, the Google Built OpenTelemetry Collector exports logs, metrics, and traces to the Unified Telemetry Router. Every single export request must obey two strict limits at the same time: five mebibytes maximum uncompressed payload size, and eight thousand one hundred ninety-two maximum log records or spans, or two hundred metric points. However, the upstream OpenTelemetry Collector queue batcher only lets you configure a single sizer: either items, or bytes, never both. |
| **Scene 2: Why Single-Stage Batching Fails in Both Directions** | Split-screen terminal output from `TestLiveUTR_BaselineVsTwoStageChain` showing Case A failing at byte 5,242,880 and Case B failing with 20,000 log records. | 20s | When we batch by item count at eight thousand one hundred ninety-two records, heavy Windows Event Forwarding XML logs averaging two and a half kilobytes reach over twenty-one megabytes uncompressed. Live telemetry dot googleapis dot com aborts gunzip decompression at five mebibytes and permanently drops the entire batch. Conversely, if we switch a single batcher to four mebibytes by byte size, compact two-hundred-byte syslog bursts pack twenty thousand records into a single four-mebibyte batch, which the router rejects with an invalid argument error. |
| **Scene 3: The Two-Stage QueueBatch Chain & Zero-Delay Splitter** | Code diff of `gcpdestination_policy.go` and `result.yaml` showing Stage 1 (`_batch_*`, `sizer: bytes`, `3.5–4.0 MiB`) feeding Stage 2 (`_split_*`, `sizer: requests`, `batch.sizer: items`, `min_size: 0`, `max_size: 8192`). | 22s | Our prototype solves both limits today in G B O C Destination Policy using a two-stage queue batch chain, after fixing the sizer marshaling bug. Stage one batches by bytes between three point five and four mebibytes, guaranteeing every emitted batch is under four mebibytes. Stage two configures an item batcher with minimum size set to zero and maximum size set to eight thousand one hundred ninety-two for logs and traces, or two hundred for metrics. Because minimum size is zero, stage two never holds or waits for a timeout—it flushes small batches immediately and only splits batches that exceed the item limit. |
| **Scene 4: Live Verification Against Production Telemetry API** | Terminal running `RUN_LIVE_UTR_TEST=1 go test -v ./policies/gcpdestination -run TestLiveUTR_BaselineVsTwoStageChain` with green `PASS` on all 6 batches of Case A and all 3 sub-batches of Case B. | 18s | Running our live verification suite against production telemetry dot googleapis dot com in project dashpole dev proves both cases end to end. The twenty-one megabyte Windows XML burst is cleanly split into six batches under four mebibytes, and the twenty-thousand compact log burst is immediately split into two batches of eight thousand one hundred ninety-two and one batch of three thousand six hundred sixteen. Every single log record is ingested with zero drops. |

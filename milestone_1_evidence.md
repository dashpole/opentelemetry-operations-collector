# Milestone 1 Evidence: Code Implementation in `GCPDestinationPolicy` & Pipeline Integration Tests

## 1. Summary of Changes Implemented

1. **`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination/gcpdestination_policy.go`**:
   - **Fixed `opentelemetry-operations-collector#711` (`RequestSizerType` marshaling as `sizer: {}`)**: Explicitly writes string representations (`"bytes"`, `"requests"`, `"items"`) for `sizer` and `batch.sizer` in `postMarshalOverrides` after `cm.Marshal(conf)`, preventing `confmap.Conf.Unmarshal` from treating `sizer: {}` as an empty struct map and reverting `Sizer` to factory defaults.
   - **Fixed `opentelemetry-operations-collector#710` (Trace item limit)**: Lowered trace batch item `max_size` from `25000` to `8192` (`utrMaxTraceItems = 8192`), matching UTR's `kMaxSpanCountPerBatch = 8192`.
   - **Implemented the Two-Stage `QueueBatch` Chain across `logs`, `metrics`, and `traces`**:
     - **Stage 1 (`queue_batch/<name>_batch_{logs,metrics,traces}`)**:
       - Queue: `sizer: bytes`, `queue_size: 33554432` (32 MiB), `block_on_overflow: true`, `wait_for_result: false`
       - Batch: `sizer: bytes`, `min_size: 3670016` (3.5 MiB), `max_size: 4194304` (4 MiB), `flush_timeout: 200ms`
     - **Stage 2 (`queue_batch/<name>_split_{logs,metrics,traces}`)**:
       - Queue: `sizer: requests`, `queue_size: 10`, `block_on_overflow: true`, `wait_for_result: false`
       - Batch: `sizer: items`, `min_size: 0`, `max_size: 8192` (logs & traces) / `200` (metrics), `flush_timeout: 200ms`
     - **Pipeline Wiring (`PreProcess{Log,Metric,Trace}IDs()`)**: Returns `[]component.ID{queueBatch<Signal>ID, queueSplit<Signal>ID}` so every pipeline traverses Stage 1 (`_batch_*`) followed immediately by Stage 2 (`_split_*`) before `otlp_grpc/<name>`.
2. **`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/builtin/result.yaml`**:
   - Updated golden configuration output reflecting the 6 `queue_batch` processors (`_batch_*` and `_split_*`), explicit string `sizer` values, `max_size: 8192` for traces, and two-stage processor lists in pipelines.
3. **`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination/gcpdestination_policy_test.go`**:
   - Added unit and end-to-end pipeline integration tests feeding real `plog.Logs` through upstream `queuebatchprocessor` (`v0.162.0`) + `otlpexporter` (`v0.162.0`) into an in-process gRPC OTLP server enforcing UTR's exact dual limits (`grpc.MaxRecvMsgSize(5*1024*1024)` = `5,242,880` B and `maxItems = 8192`).

---

## 2. Baseline-vs-Change Control Results

| Control Check | Baseline / Negative Control Behavior | Proposed Two-Stage Chain Behavior |
| :--- | :--- | :--- |
| **Control 1: Bug `#711` (`sizer: {}` Unmarshal)** | Marshaling `queuebatchprocessor.Config` without `#711` override emits `sizer: {}` and `batch.sizer: {}`. Unmarshaling into `CreateDefaultConfig()` treats `{}` as a no-op on `RequestSizerType`, silently dropping `RequestSizerTypeBytes` and reverting to `"requests"` / `"items"` (which would interpret `min_size: 3670016` as 3.67M items). | All 6 processors in `GCPDestinationPolicy.Evaluate()` unmarshal cleanly with `Sizer == "bytes"` / `Batch.Sizer == "bytes"` (Stage 1) and `Sizer == "requests"` / `Batch.Sizer == "items"` (Stage 2) and pass `Validate()`. |
| **Control 2: Case A (`8,192` WEF XML Logs @ ~2.5 KB = `21,191,703` B)** | **Candidate 0 (Single-Stage `items`, `8192`)**: Emits 1 batch (`8,192` items, `21,191,703` B / `20.21 MiB` uncompressed) -> fails with `Permanent error: rpc error: code = ResourceExhausted desc = grpc: received message after decompression larger than max 5242880` (`0` logs delivered, `8,192` dropped). | **Candidate 2 (Two-Stage Chain)**: Stage 1 splits into 6 batches (`5 x 1,621` items @ `4,192,526`–`4,193,636` B + `1 x 87` items @ `225,176` B, all `<= 4,194,304` B), Stage 2 flushes each immediately (`<= 8,192` items). All `8,192` logs delivered. |
| **Control 3: Case B (`20,000` Compact Logs @ ~202 B = `4,048,999` B)** | **Candidate 1 (Single-Stage `bytes`, `3.5–4.0 MiB`)**: Emits 1 batch (`20,000` items, `4,048,999` B `<= 4 MiB`) -> fails with `rpc error: code = InvalidArgument desc = Batch contains too many log records: 20000 > 8192` (`0` logs delivered, `20,000` dropped). | **Candidate 2 (Two-Stage Chain)**: Stage 1 flushes the `4,048,999` B batch immediately, and Stage 2 (`min_size: 0, max_size: 8192`) splits it into 3 sub-batches (`8,192` items @ `1,653,781` B, `8,192` items @ `1,661,275` B, `3,616` items @ `734,155` B) in `< 180ms` without calling `Shutdown`. All `20,000` logs delivered. |
| **Control 4: Case C (Single `4.5 MiB` Record + `1,500` Normal Logs)** | Single oversized `> 4 MiB` log record mixed with `1,500` normal `2.5 KB` logs (`~3.88 MiB`). | Stage 1 (`extractScopeLogs`) drops **only** the 1 oversized `4.5 MiB` record and exports all `1,500` normal records (`<= 4 MiB` and `<= 8,192` items) in `< 180ms`. |
| **Control 5: Stage 2 `min_size: 0` Zero-Delay Control (`flush_timeout: 5s`)** | With Stage 2 `min_size: 8192` and `flush_timeout: 5s`, a `1,580`-log (`~3.95 MiB`) batch flushed immediately by Stage 1 is **held** by Stage 2 (`1580 < 8192`), with `0` batches arriving within `200ms`. | With Stage 2 `min_size: 0` and `flush_timeout: 5s`, the `1,580`-log batch flushes through Stage 1 + Stage 2 immediately (`< 150ms << 5s`), proving `min_size: 0` adds zero holding delay. |

---

## 3. Exact Verification Command & Raw Output

Command:
```bash
cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -count=1 -v ./policies/gcpdestination/... .
```

Output:
```text
=== RUN   TestGCPDestinationPolicy_ValidateAndIDs
--- PASS: TestGCPDestinationPolicy_ValidateAndIDs (0.01s)
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_logs
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_metrics
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_traces
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_logs
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_metrics
=== RUN   TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_traces
--- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal (0.01s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_logs (0.00s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_metrics (0.00s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_batch_traces (0.00s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_logs (0.00s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_metrics (0.00s)
    --- PASS: TestGCPDestinationPolicy_Bug711_BaselineVsPatchedSizerUnmarshal/queue_batch/default_gcp_destination_split_traces (0.00s)
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Candidate0_Baseline_SingleStageItems_FailsResourceExhausted
    gcpdestination_policy_test.go:345: Case A Candidate 0 Exporter Error: Permanent error: rpc error: code = ResourceExhausted desc = grpc: received message after decompression larger than max 5242880
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Candidate2_Proposed_TwoStageChain_SucceedsAll8192Logs
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 1: items=1621, uncompressed_bytes=4192526
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 2: items=1621, uncompressed_bytes=4193636
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 3: items=1621, uncompressed_bytes=4193636
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 4: items=1621, uncompressed_bytes=4193636
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 5: items=1621, uncompressed_bytes=4193636
    gcpdestination_policy_test.go:419: Case A Candidate 2 Batch 6: items=87, uncompressed_bytes=225176
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Candidate1_SingleStageBytes_FailsInvalidArgumentTooManyItems
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Candidate2_Proposed_TwoStageChain_SplitsInto8192_8192_3616_Immediately
    gcpdestination_policy_test.go:522: Case B Candidate 2 Batch 1: items=8192, uncompressed_bytes=1653781
    gcpdestination_policy_test.go:522: Case B Candidate 2 Batch 2: items=8192, uncompressed_bytes=1661275
    gcpdestination_policy_test.go:522: Case B Candidate 2 Batch 3: items=3616, uncompressed_bytes=734155
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseC_SingleOversizedRecord_IsolatedWithoutPoisoningBatch
=== RUN   TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof
--- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain (0.83s)
    --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B (0.35s)
        --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Candidate0_Baseline_SingleStageItems_FailsResourceExhausted (0.13s)
        --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Candidate2_Proposed_TwoStageChain_SucceedsAll8192Logs (0.23s)
    --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B (0.15s)
        --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Candidate1_SingleStageBytes_FailsInvalidArgumentTooManyItems (0.07s)
        --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Candidate2_Proposed_TwoStageChain_SplitsInto8192_8192_3616_Immediately (0.08s)
    --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/CaseC_SingleOversizedRecord_IsolatedWithoutPoisoningBatch (0.04s)
    --- PASS: TestGCPDestinationPolicy_Pipeline_BaselineVsTwoStageChain/Stage2MinSizeZero_NeverHoldsPartialItemBatch_ControlProof (0.29s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination	1.009s
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	10.574s
```

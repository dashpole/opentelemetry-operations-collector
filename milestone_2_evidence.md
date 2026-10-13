# Milestone 2 Evidence: Empirical Baseline-vs-Change Proof Against Live `telemetry.googleapis.com:443` (`dashpole-dev`)

## 1. Summary of Live Cloud Verification (`TestLiveUTR_BaselineVsTwoStageChain`)

We executed `TestLiveUTR_BaselineVsTwoStageChain` in [`gcpdestination_live_utr_test.go`](file:///tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination/gcpdestination_live_utr_test.go) using upstream `queuebatchprocessor` (`v0.162.0`) and `otlpexporter` (`v0.162.0`) exporting directly over OTLP/gRPC to live production `telemetry.googleapis.com:443` in GCP project `dashpole-dev`.

---

## 2. Empirical Baseline-vs-Change Results Against Live `telemetry.googleapis.com:443`

| Workload & Scenario | Batching Configuration | Batches Sent to `telemetry.googleapis.com:443` | Uncompressed Protobuf Bytes per Batch | Items per Batch | Live `telemetry.googleapis.com:443` Response | Total Logs Ingested |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Case A: Heavy WEF XML Logs** (`8,192` logs @ ~2.5 KB = `21,191,703` B) | **Candidate 0 (Baseline Single-Stage `items`, `8,192`, `compression: gzip`)** | `1` | `21,191,703` B (`20.21 MiB`) | `8,192` | `Permanent error: rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880` | **`0` / `8,192` (100% dropped)** |
| **Case A: Heavy WEF XML Logs** (`8,192` logs @ ~2.5 KB = `21,191,703` B) | **Candidate 0 (Baseline Single-Stage `items`, `8,192`, `compression: none`)** | `1` | `21,191,703` B (`20.21 MiB`) | `8,192` | `Permanent error: rpc error: code = InvalidArgument desc = Request payload size exceeds the limit: 5242880 bytes.` | **`0` / `8,192` (100% dropped)** |
| **Case A: Heavy WEF XML Logs** (`8,192` logs @ ~2.5 KB = `21,191,703` B) | **Candidate 2 (Proposed Two-Stage Chain)** | `6` | Batch 1: `4,192,526` B<br>Batch 2: `4,193,636` B<br>Batch 3: `4,193,636` B<br>Batch 4: `4,193,636` B<br>Batch 5: `4,193,636` B<br>Batch 6: `225,176` B<br>*(all `<= 4,194,304` B)* | `1,621`<br>`1,621`<br>`1,621`<br>`1,621`<br>`1,621`<br>`87`<br>*(sum = `8,192`)* | **All 6 batches: HTTP/2 gRPC `OK` (`err=<nil>`)** (`249.6ms`, `189.8ms`, `181.4ms`, `179.2ms`, `148.1ms`, `64.9ms`) | **`8,192` / `8,192` (100% ingested)** |
| **Case B: Compact Logs** (`20,000` logs @ ~202 B = `4,048,999` B) | **Candidate 1 (Single-Stage `bytes`, `max_size: 4 MiB`)** | `1` | `4,048,999` B (`3.86 MiB <= 4 MiB`) | `20,000` (`> 8,192`) | `Permanent error: rpc error: code = InvalidArgument desc = Request contains an invalid argument.` *(UTR `otlp_to_log_entry.cc:81-85` `kMaxLogCountPerBatch = 8192` rejection, sanitized by ESF)* | **`0` / `20,000` (100% dropped)** |
| **Case B: Compact Logs** (`20,000` logs @ ~202 B = `4,048,999` B) | **Candidate 2 (Proposed Two-Stage Chain)** | `3` | Batch 1: `1,653,781` B<br>Batch 2: `1,661,275` B<br>Batch 3: `734,155` B<br>*(all `<= 4,194,304` B)* | `8,192`<br>`8,192`<br>`3,616`<br>*(sum = `20,000`)* | **All 3 sub-batches: HTTP/2 gRPC `OK` (`err=<nil>`)** (`226.1ms`, `213.8ms`, `168.4ms`) | **`20,000` / `20,000` (100% ingested)** |

---

## 3. Key Empirical Discovery from Live `telemetry.googleapis.com:443`

When `otlpexporter` exports a `> 5 MiB` batch to `telemetry.googleapis.com:443`, Google's Envoy/ESF front-end maps the `5,242,880`-byte limit violation to **top-level gRPC status `code = InvalidArgument`** (not top-level `code = ResourceExhausted`):
- **With `compression: gzip` (GBOC default)**: ESF's streaming gunzip decompressor aborts at byte `5,242,880` with internal status `RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880` and surfaces it to the gRPC client as:
  `rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880`
- **With `compression: none`**: ESF surfaces:
  `rpc error: code = InvalidArgument desc = Request payload size exceeds the limit: 5242880 bytes.`
- Because `otlpexporter` (`processError` and `shouldRetry` in `exporter/otlpexporter/otlp.go:173-222`) classifies `codes.InvalidArgument` as an unconditional `Permanent error` (non-retryable regardless of `RetryInfo`), any batch exceeding `5,242,880` uncompressed bytes is immediately and permanently dropped by `otlpexporter`!

---

## 4. Exact Verification Command & Raw Output

Command:
```bash
cd /tmp/prototypes/tab1_batching_utr_limits/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && RUN_LIVE_UTR_TEST=1 go test -count=1 -v ./policies/gcpdestination -run TestLiveUTR_BaselineVsTwoStageChain
```

Output:
```text
=== RUN   TestLiveUTR_BaselineVsTwoStageChain
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Baseline_SingleStageItems_RejectedByLiveUTR_ResourceExhausted
    gcpdestination_live_utr_test.go:199: [LIVE UTR - Case A Baseline (gzip)] batch=1 items=8192 uncompressed_bytes=21191703 duration=265.477558ms err=Permanent error: rpc error: code = InvalidArgument desc = Data decompression failed with decompression status: RESOURCE_EXHAUSTED: Maximum length exceeded: 5242880; at byte 15794; at uncompressed byte 5242880
    gcpdestination_live_utr_test.go:215: [LIVE UTR - Case A Baseline (uncompressed)] batch=1 items=8192 uncompressed_bytes=21191703 duration=248.38675ms err=Permanent error: rpc error: code = InvalidArgument desc = Request payload size exceeds the limit: 5242880 bytes.
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Proposed_TwoStageChain_AcceptedByLiveUTR
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=1 items=1621 uncompressed_bytes=4192526 duration=249.588624ms err=<nil>
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=2 items=1621 uncompressed_bytes=4193636 duration=189.814221ms err=<nil>
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=3 items=1621 uncompressed_bytes=4193636 duration=181.392408ms err=<nil>
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=4 items=1621 uncompressed_bytes=4193636 duration=179.198619ms err=<nil>
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=5 items=1621 uncompressed_bytes=4193636 duration=148.1062ms err=<nil>
    gcpdestination_live_utr_test.go:254: [LIVE UTR - Case A Two-Stage] batch=6 items=87 uncompressed_bytes=225176 duration=64.941547ms err=<nil>
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/SingleStageBytes_RejectedByLiveUTR_InvalidArgumentTooManyRecords
    gcpdestination_live_utr_test.go:303: [LIVE UTR - Case B Single-Stage Bytes] batch=1 items=20000 uncompressed_bytes=4048999 duration=107.727996ms err=Permanent error: rpc error: code = InvalidArgument desc = Request contains an invalid argument.
=== RUN   TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Proposed_TwoStageChain_SplitsAndAcceptedByLiveUTR
    gcpdestination_live_utr_test.go:343: [LIVE UTR - Case B Two-Stage] batch=1 items=8192 uncompressed_bytes=1653781 duration=226.124323ms err=<nil>
    gcpdestination_live_utr_test.go:343: [LIVE UTR - Case B Two-Stage] batch=2 items=8192 uncompressed_bytes=1661275 duration=213.844047ms err=<nil>
    gcpdestination_live_utr_test.go:343: [LIVE UTR - Case B Two-Stage] batch=3 items=3616 uncompressed_bytes=734155 duration=168.426217ms err=<nil>
--- PASS: TestLiveUTR_BaselineVsTwoStageChain (7.87s)
    --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B (1.65s)
        --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Baseline_SingleStageItems_RejectedByLiveUTR_ResourceExhausted (0.59s)
        --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseA_HeavyWEFXMLLogs_8192x2500B/Proposed_TwoStageChain_AcceptedByLiveUTR (1.05s)
    --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B (0.78s)
        --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/SingleStageBytes_RejectedByLiveUTR_InvalidArgumentTooManyRecords (0.13s)
        --- PASS: TestLiveUTR_BaselineVsTwoStageChain/CaseB_CompactLogs_20000x200B/Proposed_TwoStageChain_SplitsAndAcceptedByLiveUTR (0.65s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination	8.076s
```

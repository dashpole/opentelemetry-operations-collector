# Prototype Plan: High-Throughput `WindowsEventLogSourcePolicy` Defaults in GBOC (Tab 4)

**Author**: dashpole@google.com  
**Target Repository**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`v0.162.0`)  
**Target Branch**: `proto/tango-tab4-windowseventlog-defaults`  
**Companion Design Document**: `tango_onepager_4_windowseventlog_throughput_defaults.md`  
**Target OS / Cloud Environment**: Windows Server 2022 GCE VM (`tango-win2022-proto` in `--project=dashpole-gke-dev --zone=us-central1-a`)

---

## 1. Core Question & Hypothesis

In Project Tango (`go/tango-preview-policies`), customers configure Windows Event Log collection declaratively via `WindowsEventLogSourcePolicy`:
```protobuf
message WindowsEventLogSourcePolicy {
  string id = 1;
  repeated string channels = 2;
}
```
Because `WindowsEventLogSourcePolicy` intentionally omits low-level receiver tuning knobs (`suppress_rendering_info`, `event_driven_scraping`, `poll_interval`, `max_reads`, `ignore_channel_errors`), every GBOC collector's Windows Event Log throughput and startup resilience is determined by the defaults synthesized by `googlecontrolplaneprovider`.

During the Allianz Google SecOps / Bindplane evaluation, **Finding `#80` (*Critical — Agent can't keep up with event generation, 90-minute lag*)** showed that upstream `windowseventlogreceiver` defaults (`suppress_rendering_info: false`, `event_driven_scraping: false`, `poll_interval: 1s`, `max_reads: 100`) bottleneck on synchronous Win32 `EvtFormatMessage` IPC calls (`RenderDeep`, ~3.56 ms/event, capping single-channel throughput at ~280 events/sec) and 1-second timer polling delays.

### Hypotheses to Test
1. **H1 (Policy Compilation & Multi-Policy Deduplication)**: `googlecontrolplaneprovider` can compile one or more `WindowsEventLogSourcePolicy` instances into `windowseventlog/<lowercase_channel>` receiver configurations with high-throughput defaults (`suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, `ignore_channel_errors: true`), deduplicating channels case-insensitively both within a single policy and across multiple `WindowsEventLogSourcePolicy` instances in the same `PolicySet` without creating duplicate subscriptions or duplicate pipeline fanout.
2. **H2 (End-to-End Collector Receiver Throughput & Latency on Windows Server 2022)**: Running the full upstream `windowseventlogreceiver` (`v0.162.0`) end-to-end on a live Windows Server 2022 VM (`tango-win2022-proto`) with the Prototype Config (`suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, `ignore_channel_errors: true`) achieves $\ge 5,000\text{ events/sec}$ sustained single-channel ingestion throughput (initial hypothesis: $\ge 15\times$ over Upstream Baseline Config based on the `45x` single-event `EvtFormatMessage` vs. `EvtRender` micro-benchmark; empirically validated in Milestone 2 at **`5.07x–6.17x`** end-to-end Stanza receiver speedup [`1,003.57–1,082.71 events/sec` $\to$ `5,488.09–6,195.91 events/sec`] because Stanza's `publisherCache` in `input.go:415` caches `EvtOpenPublisherMetadata` handles across events and both paths share Go `xml.Unmarshal`, `BatchingLogEmitter`, and `bbolt` checkpointing) and eliminates the 1-second timer polling delay on newly arriving events in steady state (`769.94 ms` $\to$ `48.60 ms` median, after the initial `5.0s` `awaitAndReadEvents` startup wait).
3. **H3 (Payload Completeness for SecOps / Structured Log Consumers)**: Setting `suppress_rendering_info: true` (`EvtRenderEventXML` / `RenderSimple`) preserves 100% of `<System>` (`Provider`, `EventID`, `Level`, `Task`, `Opcode`, `Keywords`, `TimeCreated`, `EventRecordID`, `Channel`, `Computer`, `Security`) and `<EventData>` / `<UserData>` fields in raw XML (`log.record.original` / `raw: true`) and preserves all structured fields (`event_id`, `provider`, `system_time`, `computer`, `channel`, `record_id`, `event_data`/`user_data`) in default `raw: false` mode while omitting `<RenderingInfo>` (and preserves `plog.LogRecord.SeverityNumber()` for `<Level>1..4</Level>` providers while revealing an upstream Stanza `parseSeverity("", "0")` gap on `Microsoft-Windows-Security-Auditing` `<Level>0</Level>` events).

---

## 2. Scope & Intentional Simplifications

### In-Scope
1. **`WindowsEventLogSourcePolicy` Implementation in `googlecontrolplaneprovider`**:
   - Package `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog` (`windowseventlog_policy.go` and `windowseventlog_policy_test.go`).
   - Registration of policy type `"windows_event_log"` in `googlepolicy` and integration into `provider.go`.
   - Case-insensitive channel deduplication across both a single policy (`channels: ["Security", "security"]`) and multiple `WindowsEventLogSourcePolicy` instances in an active `PolicySet` via a context-scoped seen-channels tracker (`windowseventlog.WithSeenChannels(ctx)`) initialized in `provider.go` before evaluating `sourcePolicies`. Subsequent policies skip already-claimed channels so `confmap.Merge` does not overwrite the first-seen `receivers::windowseventlog/<lowercase_channel>::channel` scalar value while `confmap.enableMergeAppendOption` unions and deduplicates `service::pipelines::logs/windows_event_log::receivers`.
   - Pipeline wiring into `service::pipelines::logs/windows_event_log` with `googlepolicy` and destination pre-export processors (`queue_batch/default_gcp_destination_batch_logs`) and destination exporters (`otlp_grpc/default_gcp_destination`).
   - Validation that unmarshaling the generated `confmap.Conf` into upstream `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` (`*windowseventlogreceiver.WindowsLogConfig`) populates `SuppressRenderingInfo: true`, `EventDrivenScraping: true`, `MaxReads: 500`, and `IgnoreChannelErrors: true`.
2. **Empirical End-to-End `windowseventlogreceiver` Benchmark on Windows Server 2022 (`tango-win2022-proto`)**:
   - Cross-compiled Go benchmark harness (`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main.go`) importing `github.com/open-telemetry/opentelemetry-collector-contrib/receiver/windowseventlogreceiver v0.162.0` and `github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage v0.162.0`.
   - Serial execution on `tango-win2022-proto` (`Microsoft Windows Server 2022`, `e2-standard-4`, `pd-ssd` in `dashpole-gke-dev`) comparing Baseline Config vs. Prototype Config across:
     1. Backlog drain throughput (`events/sec`) and per-batch read latency (`ms/batch`, `us/event`).
     2. Live event arrival-to-consumer delivery latency (`ms`).
     3. Fail-open startup behavior on a non-existent channel (`ignore_channel_errors: false` vs. `true`).
     4. Emitted `plog.LogRecord` inspection verifying both raw XML (`log.record.original` via `include_log_record_original: true`) and default `raw: false` structured `Body().Map()` field preservation (`<System>` and `<EventData>`).

### Out-of-Scope (Intentional Simplifications)
1. **Upstream Fix for `opentelemetry-collector-contrib#51962`**: Fixing `awaitAndReadEvents` reconnect retry on transient `subscription.Open()` startup errors in upstream `pkg/stanza/operator/input/windows` is tracked separately via upstream issue `#51962` and documented as a rollout prerequisite in the one-pager.
2. **Protobuf Code Generation in `proto/policy/v1alpha1/`**: As `selfmetrics` and `gcpdestination` in `googlecontrolplaneprovider` use `googlepolicy.GenericDriver` over `mapstructure` structs at `v0.162.0`, `WindowsEventLogSourcePolicy` follows the same `googlecontrolplaneprovider/policies/*` pattern while supporting both `id` (proto JSON key) and `name` (YAML/struct key).

---

## 3. Candidate Approaches & Symmetric Evaluation Rubric

| Dimension | Baseline Config (Upstream `v0.162.0` Defaults) | Prototype Config (Proposed `googlecontrolplaneprovider` Defaults) |
| :--- | :--- | :--- |
| **`suppress_rendering_info`** | `false` (`EvtFormatMessage` / `RenderDeep`) | `true` (`EvtRender` / `RenderSimple`) |
| **`event_driven_scraping`** | `false` (`poll_interval: 1s` timer) | `true` (Win32 `WaitForMultipleObjects` signal + `5s` `wait_timeout` fallback) |
| **`max_reads`** | `100` | `500` |
| **`ignore_channel_errors`** | `false` | `true` |
| **Bookmark Persistence** | `file_storage` (`bbolt`, `fsync: false` default) | `file_storage` (`bbolt`, `fsync: false` default) |
| **Downstream Consumer** | Identical in-process `consumer.Logs` counter/timestamp sink | Identical in-process `consumer.Logs` counter/timestamp sink |

### Symmetric Benchmark Rules (No Asymmetric Optimization)
1. **Strictly Serial Execution**: Baseline Config and Prototype Config are never run concurrently; each run starts with a fresh `file_storage` bookmark directory and reads the exact same pre-populated Windows Event Log records (`start_at: beginning`) on `tango-win2022-proto`.
2. **Identical Shared Infrastructure**: Both configurations use the exact same `windowseventlogreceiver` (`v0.162.0`), the same `filestorage` extension (`bbolt` with default `fsync: false`), the same default `raw: false` structured body handling (plus `include_log_record_original: true` in the payload comparison check to inspect raw XML alongside the structured map body), and the same in-memory `consumer.Logs` sink.

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Implement `WindowsEventLogSourcePolicy` with High-Throughput Defaults in `googlecontrolplaneprovider`

- **Deliverables**:
  1. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy.go`
  2. `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy_test.go`
  3. Registration and cross-policy channel deduplication (`windowseventlog.WithSeenChannels(ctx)`) in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go` and `provider_test.go`
  4. Golden policy set test in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/windowseventlog/`
  5. Updated `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/go.mod` and `go.sum`
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -race ./...
  ```
- **Falsifiable Exit Gates**:
  1. `googlepolicy.LoadPolicy("windows_event_log", ...)` loads `WindowsEventLogSourcePolicy` with `id` (or `name`) and `channels`, and rejects empty policy ID/name, empty `channels`, whitespace-only channel names, or channel names containing confmap delimiter `::`.
  2. Evaluating a `PolicySet` with multiple `WindowsEventLogSourcePolicy` instances having overlapping channels in different cases (e.g., Policy 1 `channels: ["Security", "Application", "security"]` and Policy 2 `channels: ["SECURITY", "System", "application"]`) synthesizes:
     - Exactly 3 receiver blocks: `receivers::windowseventlog/security` (`channel: "Security"`), `receivers::windowseventlog/application` (`channel: "Application"`), and `receivers::windowseventlog/system` (`channel: "System"`), without Policy 2 overwriting `"Security"` with `"SECURITY"`.
     - Each receiver block has `suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, and `ignore_channel_errors: true`.
     - A single merged `service::pipelines::logs/windows_event_log` pipeline with `receivers: [windowseventlog/security, windowseventlog/application, windowseventlog/system]`, `processors: [googlepolicy, queue_batch/default_gcp_destination_batch_logs]`, and `exporters: [otlp_grpc/default_gcp_destination]`.
  3. Unmarshaling each synthesized `receivers::windowseventlog/<channel>` block into upstream `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` (`*windowseventlogreceiver.WindowsLogConfig`) succeeds with zero unmapped/invalid fields and yields `InputConfig.SuppressRenderingInfo == true`, `InputConfig.EventDrivenScraping == true`, `InputConfig.MaxReads == 500`, and `InputConfig.IgnoreChannelErrors == true`.
- **Baseline-vs-Change Proof**:
  - **Baseline (`v0.162.0` before change)**: `googlepolicy.LoadPolicy("windows_event_log", ...)` returns `ErrPolicyTypeNotFound`, and `windowseventlogreceiver.NewFactory().CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)` has `SuppressRenderingInfo == false`, `EventDrivenScraping == false`, `MaxReads == 100`, `IgnoreChannelErrors == false`.
  - **Change (after Milestone 1)**: `googlepolicy.LoadPolicy("windows_event_log", ...)` succeeds, and `TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal` verifies both the baseline upstream default struct values `(false, false, 100, false)` and the post-compilation unmarshaled struct values `(true, true, 500, true)`.

---

### Milestone 2: Empirical End-to-End Collector Throughput, Latency & Payload Benchmark on Windows Server 2022 (`tango-win2022-proto`)

- **Deliverables**:
  1. Benchmark harness `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main.go` running real `windowseventlogreceiver` + `filestorage` on Windows Server 2022.
  2. `milestone_2_evidence.md` capturing raw command outputs from `tango-win2022-proto` (`--project=dashpole-gke-dev --zone=us-central1-a`).
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
  GOOS=windows GOARCH=amd64 go build -o /tmp/winbench.exe ./internal/winbench
  gcloud compute scp /tmp/winbench.exe tango-win2022-proto:C:/proto/winbench.exe --project=dashpole-gke-dev --zone=us-central1-a
  gcloud compute ssh tango-win2022-proto --project=dashpole-gke-dev --zone=us-central1-a --command="C:\proto\winbench.exe -events=3000"
  ```
- **Falsifiable Exit Gates**:
  1. **Sustained Backlog Throughput & Batch Latency (3,000 Events)**:
     - Pre-populate 3,000 real Windows Event Log events into a dedicated channel (`Application` or custom event source on `tango-win2022-proto`) using Win32 `ReportEvent` / PowerShell `Write-EventLog`.
     - Run **Baseline Config** (`suppress_rendering_info: false`, `event_driven_scraping: false`, `poll_interval: 1s`, `max_reads: 100`) to drain all 3,000 events from `start_at: beginning` with `filestorage` bookmarking enabled. Record total elapsed time, `events/sec`, and per-batch latency (`ms/batch`, `us/event`).
     - Run **Prototype Config** (`suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, `ignore_channel_errors: true`) serially on the exact same 3,000 events with a fresh `filestorage` directory. Verify end-to-end receiver throughput is $\ge 5,000\text{ events/sec}$ (initial pre-benchmark expectation: $\ge 15\times$ over Baseline; empirically measured in Milestone 2 at **`5,488.09–6,195.91 events/sec`** vs. **`1,003.57–1,082.71 events/sec`** Baseline = **`5.07x–6.17x`** sustained active drain speedup, or **`5.41x`** post-first-batch drain speedup, because `publisherCache` in `input.go:415` caches provider metadata handles across events and both paths share Go `xml.Unmarshal`, `BatchingLogEmitter`, and `bbolt` checkpointing).
  2. **Live Arrival-to-Delivery Latency (`event_driven_scraping: false` vs. `true`)**:
     - With the receiver caught up (`start_at: end`), inject 10 individual events spaced at non-integer intervals (e.g. every 230 ms) and measure the wall-clock delay from `Write-EventLog` / `ReportEvent` completion to `consumer.ConsumeLogs` invocation.
     - Verify **Baseline Config** (`poll_interval: 1s`, `event_driven_scraping: false`) averages hundreds of milliseconds of polling lag ($\ge 300\text{ ms}$ mean), whereas **Prototype Config** (`event_driven_scraping: true`) delivers events in $< 50\text{ ms}$ median latency.
  3. **Fail-Open Missing Channel (`ignore_channel_errors: false` vs. `true`)**:
     - Verify `windowseventlogreceiver` startup on a non-existent channel (`TangoNonExistentChannel_9999`): `r.Start(ctx, host)` returns a non-nil error wrapping `windows.ERROR_EVT_CHANNEL_NOT_FOUND` (`failed to open local subscription`) under **Baseline Config** (`ignore_channel_errors: false`), and returns `nil` (while logging a `WARN`) under **Prototype Config** (`ignore_channel_errors: true`).
  4. **Structured Map & Raw XML Field Preservation (`suppress_rendering_info: true`)**:
     - Capture and compare a sample event emitted by Baseline Config vs. Prototype Config with `include_log_record_original: true` and default `raw: false`:
       - Verify in raw XML (`log.record.original`) that `<System>` and `<EventData>` are preserved byte-for-byte identically while only `<RenderingInfo>` is omitted (and record the exact bytes/event payload reduction).
       - Verify in the default `raw: false` structured `Body().Map()` that `event_id`, `provider`, `system_time`, `computer`, `channel`, `record_id`, `event_data`, and `plog.LogRecord.SeverityNumber()` are preserved identically while `rendering_info` is omitted and `level`/`task`/`opcode`/`keywords` retain their raw `<System>` values.
- **Baseline-vs-Change Proof**:
  - Side-by-side serial measurements from the same `winbench.exe` binary on `tango-win2022-proto` showing Baseline Config vs. Prototype Config across backlog throughput, per-batch latency, live arrival latency, missing-channel startup error vs. fail-open success, and raw XML + structured map event payloads.

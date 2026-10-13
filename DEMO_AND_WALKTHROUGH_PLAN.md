# Demo & Walkthrough Plan: High-Throughput `WindowsEventLogSourcePolicy` Defaults in GBOC (Tab 4)

## 1. Audience & Core Narrative

- **Target Audience**: Google Cloud Observability / SecOps Engineering & Product leads reviewing the Tango (`googlecontrolplaneprovider`) Windows Event Log Source Policy design (`tango_onepager_4_windowseventlog_throughput_defaults.md`).
- **Core Narrative**:
  1. **The Problem**: In the Allianz Google SecOps / Bindplane evaluation (**Finding `#80`**), Windows Event Log collection on active Domain Controllers lagged by **90 minutes** because upstream `windowseventlogreceiver` (`v0.162.0`) defaults (`suppress_rendering_info: false`, `event_driven_scraping: false`, `poll_interval: 1s`, `max_reads: 100`, `ignore_channel_errors: false`) invoke synchronous Win32 `EvtFormatMessage` IPC calls on every event (`~924 µs/event` end-to-end in Stanza, capping single-channel throughput at `~1,004–1,083 events/sec`), poll on a 1-second timer (`~770 ms` median live delivery latency), and fail collector startup if any configured channel is missing on a host.
  2. **The GBOC Policy Solution**: Because `WindowsEventLogSourcePolicy` (`{ id, channels }`) intentionally exposes only high-level channel names, `googlecontrolplaneprovider` must synthesize high-throughput, fail-open defaults (`suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, `ignore_channel_errors: true`) and deduplicate channels case-insensitively across both single and stacked policies.
  3. **Empirical Proof on Windows Server 2022 (`tango-win2022-proto`)**: Running the real upstream `windowseventlogreceiver` (`v0.162.0`) + `filestorage` on `tango-win2022-proto` against `3,000+` real `Microsoft-Windows-Security-Auditing` events in the `Security` channel demonstrates a **`5.07x–6.17x` sustained active drain speedup** (`1,003.57–1,082.71 events/sec` $\to$ **`5,488.09–6,195.91 events/sec`**), a **`15.8x` reduction in median live event delivery latency** (`769.94 ms` $\to$ **`48.60 ms`**), **`60.3%` smaller raw XML payloads** (`3,207 bytes` $\to$ **`1,273 bytes`** on `EventID 4688`), fail-open startup on missing channels, and uncovers two concrete upstream `pkg/stanza` bugs (`awaitAndReadEvents` 5-second initial wait and `parseSeverity("", "0")` on `<Level>0</Level>` Security events).

---

## 2. Step-by-Step Live Demo Script

### Part A: Policy Compilation, Multi-Policy Deduplication & Upstream Config Unmarshaling (Linux)
Run the unit and golden integration test suites in `googlecontrolplaneprovider`:
```bash
cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -race -count=1 ./policies/windowseventlog/... .
```
**What to highlight in the output**:
- `TestWindowsEventLogPolicy_Evaluate_SingleAndDeduplicated`: Intra-policy case-insensitive channel deduplication (`["Security", "Application", "security", "  System  ", "APPLICATION"]` $\to$ `windowseventlog/security`, `windowseventlog/application`, `windowseventlog/system`).
- `TestWindowsEventLogPolicy_Evaluate_CrossPolicyDeduplication` & `TestPolicySets/windowseventlog`: Cross-policy deduplication across `01_dc_events.json` (`["Security", "Application", "security"]`) and `02_wec_events.json` (`["SECURITY", "System", "application"]`) via `windowseventlog.NewSeenChannels()` and `windowseventlog.WithSeenChannels(ctx, seenWinEventLogChannels)` into [`testdata/policysets/windowseventlog/result.yaml`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/windowseventlog/result.yaml).
- `TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal`: Direct unmarshaling into upstream `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` proving `SuppressRenderingInfo: false -> true`, `EventDrivenScraping: false -> true`, `MaxReads: 100 -> 500`, and `IgnoreChannelErrors: false -> true`.

### Part B: Live End-to-End `windowseventlogreceiver` Benchmark on Windows Server 2022 (`tango-win2022-proto`)
Cross-compile `winbench.exe` and execute it on the Windows Server 2022 GCE VM:
```bash
cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
GOOS=windows GOARCH=amd64 go build -o /tmp/winbench.exe ./internal/winbench
gcloud compute scp --project=dashpole-gke-dev --zone=us-central1-a --scp-flag="-O" /tmp/winbench.exe tango-win2022-proto:C:/proto/winbench.exe
gcloud compute ssh --project=dashpole-gke-dev --zone=us-central1-a tango-win2022-proto --command="C:\proto\winbench.exe -events 3000 -live 10"
```
**What to highlight in the live output**:
1. **Check 4 (Payload & Schema Comparison on `Security` `EventID 4688`)**:
   - Raw XML drops from `3,207 bytes` to `1,273 bytes` (`60.3%` smaller, `2.52x` compression) with 100% identical `<System>` and 15 `<EventData>` attributes.
   - Default `raw: false` structured `Body().Map()` preserves `event_data`, `event_id`, `provider`, `record_id`, `system_time`, `computer`, and `channel`, and exposes the upstream `parseSeverity("", "0")` severity mapping nuance (`SeverityNumber=9` vs. `SeverityNumber=0`).
2. **Check 1 (Serial Backlog Throughput on `3,000+` `Security` Events)**:
   - Sustained active drain rate increases from **`1,082.71 events/sec`** (`923.61 µs/event`, `81.15 ms` median batch) to **`5,488.09 events/sec`** (`182.21 µs/event`, `14.74 ms` median batch) — a **`5.07x` speedup** (and shows the `5.047s` initial startup wait caused by `awaitAndReadEvents` calling `Wait()` before `i.read(ctx)`).
3. **Check 2 (Steady-State Live Event Arrival-to-Delivery Latency)**:
   - Median latency drops from **`769.94 ms`** (`poll_interval: 1s`) to **`48.60 ms`** (`event_driven_scraping: true`, **`15.8x` faster**).
4. **Check 3 (Fail-Open Missing Channel `TangoNonExistentChannel_9999`)**:
   - Baseline returns fatal `r.Start()` error (`The specified channel could not be found.`), while Prototype returns `<nil>` and logs a `WARN`.

---

## 3. Code Walkthrough Map

1. **[`policies/windowseventlog/windowseventlog_policy.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy.go)**:
   - `WindowsEventLogSourcePolicy` struct (`ID`, `Name`, `Channels`), `NewSeenChannels()` and `WithSeenChannels(ctx, seen)` / `seenChannelsFromContext(ctx)` for cross-policy case-insensitive channel deduplication, `Validate()` (rejecting empty channels and `::` confmap delimiters), `Evaluate(ctx)` (synthesizing `receivers::windowseventlog/<lowercase_channel>` with the 4 high-throughput defaults), and `LogsPipelines(preExportProcessors, exporters, _)` (paired with nil-returning `MetricsPipelines` and `TracesPipelines` to wire `service::pipelines::logs/windows_event_log`).
2. **[`provider.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go)**:
   - Direct import of `policies/windowseventlog` (which registers the `"windows_event_log"` policy type via `init()`), `seenWinEventLogChannels := windowseventlog.NewSeenChannels()` initialization in `evaluatePolicySet`, and `evalCtx := windowseventlog.WithSeenChannels(ctx, seenWinEventLogChannels)` wiring in `evaluateSingleSourcePolicy`.
3. **[`testdata/policysets/windowseventlog/result.yaml`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/windowseventlog/result.yaml)**:
   - Golden end-to-end GBOC collector configuration showing two stacked `windows_event_log` policies (`01_dc_events.json` and `02_wec_events.json`) compiled and deduplicated alongside `default_gcp_destination` and `default_self_metrics`.
4. **[`internal/winbench/main_windows.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main_windows.go)**:
   - Self-contained Windows Server 2022 benchmark harness exercising real `windowseventlogreceiver.NewFactory().CreateLogs(...)` and `filestorage.NewFactory()` across Check 1 (backlog throughput), Check 2 (live delivery latency), Check 3 (missing channel fail-open), and Check 4 (raw XML + structured map payload comparison).

---

## 4. Talking Points & Q&A Prep

- **Q1: Why is the end-to-end Stanza receiver speedup `5.07x–6.17x` (`1,083/s -> 5,488/s`) when the isolated `EvtFormatMessage` vs. `EvtRender` syscall micro-benchmark showed a `45x` ratio (`280/s -> 12,800/s`)?**
  - **Answer**: Two reasons in `pkg/stanza@v0.162.0/operator/input/windows/input.go`:
    1. Stanza caches open `EvtOpenPublisherMetadata` handles per provider name in `i.publisherCache` (`input.go:415`), so `EvtFormatMessage` does not reopen the provider manifest DLL on every event (`~820 µs/event` instead of `3,560 µs/event`).
    2. Both Baseline and Prototype execute the same downstream Stanza pipeline (`xml.Unmarshal`, `formattedBody` map allocation, `BatchingLogEmitter` 100-event batching, and `bbolt` `filestorage` bookmark writes), which adds `~104 µs/event` of fixed non-rendering overhead to both paths (`78 µs` `EvtRender` + `104 µs` Stanza/bbolt $\approx 182\text{ µs/event}$ = `5,488 events/sec`). Crucially, `5,488–6,196 events/sec` per channel comfortably exceeds the Domain Controller burst rate (`2,000–5,000 events/sec`) that caused Allianz Finding `#80`.
- **Q2: Why did Prototype take `5.05s` before reading its first batch on startup (`totalWall = 5.56s` vs. `3.80s` for 3,000 events)?**
  - **Answer**: Our live Windows Server 2022 benchmark uncovered an upstream bug in `pkg/stanza@v0.162.0/operator/input/windows/input.go:338-356`: `awaitAndReadEvents` calls `i.subscription.Wait(i.cancelEvent, timeoutMs)` (`5,000 ms`) *before* calling `i.read(ctx)`. Per Microsoft's Win32 `EvtSubscribe` documentation, Windows Event Log does not arm `SignalEvent` until the caller first drains `EvtNext` to `ERROR_NO_MORE_ITEMS`. Calling `i.eventsReadInPollCycle = 0; i.read(ctx)` once before the `for` loop in `awaitAndReadEvents` eliminates the 5-second startup delay completely.
- **Q3: Does `suppress_rendering_info: true` affect downstream SecOps parsers or structured log fields?**
  - **Answer**: For raw XML (`raw: true` or `log.record.original`), `<System>` and `<EventData>` / `<UserData>` are 100% byte-for-byte identical while payload size drops by `60.3%` on `EventID 4688` (`3,207 -> 1,273 bytes`). For default `raw: false` structured `Body().Map()`, all structured `<EventData>` key-value pairs are preserved identically, while `rendering_info`/`details` are omitted, `message` is `""`, and we uncovered that `parseSeverity("", "0")` in `xml.go:52-66` maps `<Level>0</Level>` (`win:LogAlways`, used by `Microsoft-Windows-Security-Auditing`) to `SeverityNumber = 0` (`UNSPECIFIED`) instead of `9` (`INFO`) — a 1-line upstream fix (`case "0", "4": return entry.Info`).

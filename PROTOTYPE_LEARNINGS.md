# Prototype Learnings: High-Throughput `WindowsEventLogSourcePolicy` Defaults in GBOC (Tab 4)

## 1. Validated Hypotheses

1. **H1 — Policy Compilation, High-Throughput Defaults & Case-Insensitive Multi-Policy Deduplication (`VALIDATED`)**:
   - **Evidence**: [`milestone_1_evidence.md`](milestone_1_evidence.md), [`windowseventlog_policy.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy.go), [`windowseventlog_policy_test.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy_test.go), and [`provider_test.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider_test.go).
   - `WindowsEventLogSourcePolicy` compiles each unique channel into `receivers::windowseventlog/<lowercase_channel>` with `suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, and `ignore_channel_errors: true`, wired into `service::pipelines::logs/windows_event_log` alongside `googlepolicy`, `queue_batch/default_gcp_destination_batch_logs`, and `otlp_grpc/default_gcp_destination`.
   - Unmarshaling the generated `confmap.Conf` sub-map directly into upstream `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` (`*windowseventlogreceiver.WindowsLogConfig`, `v0.162.0`) succeeds cleanly and overrides all 4 upstream defaults (`SuppressRenderingInfo: false -> true`, `EventDrivenScraping: false -> true`, `MaxReads: 100 -> 500`, `IgnoreChannelErrors: false -> true`).
   - Both intra-policy (`["Security", "Application", "security", "  System  ", "APPLICATION"]`) and cross-policy (`01_dc_events.json` `["Security", "Application", "security"]` + `02_wec_events.json` `["SECURITY", "System", "application"]`) case-insensitive channel deduplication produce exactly one receiver instance per unique channel (`windowseventlog/security` with `channel: Security`, `windowseventlog/application` with `channel: Application`, and `windowseventlog/system` with `channel: System`), preserving the first-seen channel casing.

2. **H2 — End-to-End Collector Receiver Throughput (`>= 5,000 events/sec`), Live Delivery Latency & Fail-Open Startup on Windows Server 2022 (`VALIDATED WITH IMPORTANT ARCHITECTURAL NUANCES`)**:
   - **Evidence**: [`milestone_2_evidence.md`](milestone_2_evidence.md) and [`internal/winbench/main_windows.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main_windows.go) executed on GCE VM `tango-win2022-proto` (`Windows Server 2022 Datacenter 10.0.20348`, `amd64`, `--project=dashpole-gke-dev --zone=us-central1-a`).
   - **Sustained Active Backlog Throughput**: Across `3,000+` real events in the Windows `Security` channel, Prototype Config achieves **`5,488.09 events/sec`** (`task-184`, `182.21 µs/event`, median batch `14.74 ms`) to **`6,195.91 events/sec`** (`task-163`, `161.40 µs/event`, median batch `15.76 ms`), compared to **`1,003.57–1,082.71 events/sec`** (`923.61–996.45 µs/event`, median batch `81.15–89.31 ms`) under Upstream Baseline Config — a **`5.07x–6.17x`** sustained active drain speedup (and **`5.41x`** post-first-batch drain speedup in `task-184`). On the custom `TangoBenchLog` channel (`task-140`, `3,000` events), derived active drain throughput increases from **`1,758.12 events/sec`** (`45.87 ms` median batch) to **`7,394.99 events/sec`** (`10.68 ms` median batch, **`4.21x`** active drain speedup and **`4.30x`** batch latency reduction).
   - **Steady-State Live Event Arrival-to-Delivery Latency**: Once caught up (`start_at: end`), Prototype Config (`event_driven_scraping: true`) reduces end-to-end arrival-to-consumer delivery latency from **`769.94 ms` median / `736.67 ms` mean** (`poll_interval: 1s`) to **`48.60 ms` median / `48.34 ms` mean** (**`15.8x` faster median**, bounded only by Stanza's `100ms` `BatchingLogEmitter` flush interval).
   - **Fail-Open Missing Channel (`ignore_channel_errors: true`)**: On a non-existent channel (`TangoNonExistentChannel_9999`), Baseline Config fails `r.Start()` with a fatal error (`start stanza: failed to open local subscription, error: failed to subscribe to TangoNonExistentChannel_9999 channel: The specified channel could not be found.`), whereas Prototype Config returns `nil` from `r.Start()` and logs a `WARN`.

3. **H3 — Raw XML Payload Reduction & `<System>` / `<EventData>` Preservation (`VALIDATED WITH `<Level>0</Level>` SEVERITY CAVEAT`)**:
   - **Evidence**: [`milestone_2_evidence.md`](milestone_2_evidence.md) (Check 4).
   - In raw XML (`raw: true` or `include_log_record_original: true`), `suppress_rendering_info: true` preserves `<System>` and `<EventData>` / `<UserData>` **100% byte-for-byte identically** while reducing event XML size by **`60.3%` (`2.52x` compression)** on `Microsoft-Windows-Security-Auditing` EventID `4688` (`3,207 bytes -> 1,273 bytes`), **`30.7%` (`1.44x`)** on `Microsoft-Windows-Eventlog` EventID `1102` (`1,422 bytes -> 985 bytes`), and **`33.7%` (`1.51x`)** on `TangoBenchLog` EventID `100` (`974 bytes -> 646 bytes`).

---

## 2. Refuted Assumptions & Surprises Discovered During Prototyping

### Surprise 1: Upstream Bug in `pkg/stanza` `awaitAndReadEvents` Causes a 5-Second (`wait_timeout`) Startup Delay Before the First Read
- **What We Assumed**: Enabling `event_driven_scraping: true` would begin draining existing backlog events immediately at `r.Start()` (`t = 0s`), beating `poll_interval: 1s` from the very first event.
- **What Actually Happened on Windows Server 2022**:
  - In [`pkg/stanza@v0.162.0/operator/input/windows/input.go:338-356`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/pkg/stanza/operator/input/windows/input.go#L338-L356), `awaitAndReadEvents` calls `ready, err := i.subscription.Wait(i.cancelEvent, timeoutMs)` (`timeoutMs = 5000` by default) **at the top of the loop before calling `i.read(ctx)`**:
    ```go
    func (i *Input) awaitAndReadEvents(ctx context.Context) {
    	defer i.wg.Done()

    	timeoutMs := uint32(i.waitTimeout.Milliseconds())
    	for {
    		ready, err := i.subscription.Wait(i.cancelEvent, timeoutMs)
    		...
    		i.eventsReadInPollCycle = 0
    		i.read(ctx)
    	}
    }
    ```
  - Per Microsoft's Win32 [`EvtSubscribe` contract](https://learn.microsoft.com/en-us/windows/win32/api/winevt/nf-winevt-evtsubscribe), the Windows Event Log service **does not signal `SignalEvent` on a pull subscription until the caller first drains `EvtNext` until `ERROR_NO_MORE_ITEMS`**.
  - Consequently, at collector startup, `i.subscription.Wait(i.cancelEvent, 5000)` always times out (`WAIT_TIMEOUT` after `5,000 ms`: measured `5.047s` in `task-184` and `5.028s` in `task-163`) before `i.read(ctx)` executes its first batch read!
  - Once that first `i.read(ctx)` drains `EvtNext` to `ERROR_NO_MORE_ITEMS`, `SignalEvent` is armed and all subsequent live events wake `WaitForMultipleObjects` immediately (`48.60 ms` median end-to-end delivery).
  - Because of the initial `5.0s` startup wait vs. `1.0s` `poll_interval`, total cold-start wall-clock time from `r.Start()` only crosses over in Prototype's favor at **`N ≈ 5,395` backlogged events** (`1.0s + N / 1082.71 = 5.0s + N / 5488.09`), explaining why at `N = 3,000` events total cold-start wall-clock time is `3.80s` (Baseline) vs. `5.56s` (Prototype) despite Prototype draining `5.07x` faster once reading begins (`559.76 ms` vs. `2,799.47 ms`).

### Surprise 2: `suppress_rendering_info: true` Produces `SeverityNumber = 0` (`UNSPECIFIED`) on `Microsoft-Windows-Security-Auditing` Events with `<Level>0</Level>`
- **What We Assumed**: `plog.LogRecord.SeverityNumber()` would be identical (`SeverityNumberInfo = 9`) between `suppress_rendering_info: false` and `suppress_rendering_info: true` for all Windows Event Log channels, and `suppress_rendering_info: true` would only affect raw XML `<RenderingInfo>`.
- **What Actually Happened on Windows Server 2022**:
  1. **Default `raw: false` Structured `Body().Map()` Differences**:
     - Upstream `windowseventlogreceiver` defaults to `raw: false`, which constructs a structured map via `formattedBody()` (`pkg/stanza@v0.162.0/operator/input/windows/xml.go:81-168`).
     - When `suppress_rendering_info: true`, `event_data` (e.g., all 15 named `<Data Name='...'>` fields on EventID `4688`), `user_data`, `event_id`, `provider`, `record_id`, `system_time`, `computer`, `channel`, `execution`, `correlation`, and `version` are preserved 100% identically, while `rendering_info` and `details` (parsed from `<RenderingInfo><Message>`) are omitted, `message` is `""`, and `level`/`task`/`opcode`/`keywords` retain their raw `<System>` values (`"0"`, `"13312"`, `"0"`, `["0x8020000000000000"]`) instead of localized `<RenderingInfo>` strings (`"Information"`, `"Process Creation"`, `"Info"`, `["Audit Success"]`).
  2. **`<Level>0</Level>` (`win:LogAlways`) Severity Mapping Gap in `parseSeverity()`**:
     - Standard providers (`Application`, `System`, `Microsoft-Windows-Eventlog` EventID `1102`) write `<Level>4</Level>` in `<System>`, which `parseSeverity("", "4")` (`xml.go:52-66`) maps to `entry.Info` (`SeverityNumber = 9`, `SeverityText = "INFO"`).
     - However, `Microsoft-Windows-Security-Auditing` events (`4624`, `4688`, `5156`, etc.) write `<Level>0</Level>` (`win:LogAlways`) in `<System>` and rely on `<RenderingInfo><Level>Information</Level></RenderingInfo>` for the rendered level string. Because `parseSeverity("", level)` in `xml.go:55-66` only maps `"1"` (`Fatal`), `"2"` (`Error`), `"3"` (`Warn`), and `"4"` (`Info`), `<Level>0</Level>` falls through to `entry.Default` (`SeverityNumber = 0`, `SeverityText = ""`) when `suppress_rendering_info: true`!

### Surprise 3: End-to-End Stanza Receiver Speedup Is `5.07x–6.17x` (`5,488–6,196 events/sec`) Rather Than `15x–45x` Due to `publisherCache` and Shared XML/Checkpointing Overhead
- **What We Assumed**: Because isolated `EvtFormatMessage` (`RenderDeep`) takes `3.56 ms/event` (`~280 events/sec`) vs. `77.9 µs/event` (`~12,800 events/sec`) for `EvtRender` (`RenderSimple`) in micro-benchmarks (`45x`), the end-to-end receiver would see a `>= 15x` relative speedup.
- **What Actually Happened**:
  - In `pkg/stanza@v0.162.0/operator/input/windows/input.go:415`, Stanza maintains a per-provider `publisherCache` that caches open `EvtOpenPublisherMetadata` handles across events (`EvtFormatMessage` with a cached publisher handle takes `~820 µs/event` instead of `3,560 µs/event`), lifting Baseline `Security` channel throughput from `~280 events/sec` to **`1,003.57–1,082.71 events/sec`** (`923.61–996.45 µs/event`).
  - Meanwhile, both Baseline and Prototype share `~104 µs/event` of downstream Stanza pipeline work (`xml.Unmarshal`, `formattedBody` map allocation, `BatchingLogEmitter` batching, and `bbolt` `filestorage` bookmark writes), bringing Prototype per-event cost to `161.40–182.21 µs/event` (**`5,488.09–6,195.91 events/sec`**, a **`5.07x–6.17x`** end-to-end active drain speedup).

### Surprise 4: Cross-Policy Case-Insensitive Channel Deduplication Requires Context-Scoped State Across `Policy.Evaluate(ctx)` Calls
- **What We Discovered**: `confmap.enableMergeAppendOption` appends slice fields (`service::pipelines::logs/windows_event_log::receivers`) and deduplicates identical lowercase receiver IDs (`windowseventlog/security`), but overwrites scalar map fields (`receivers::windowseventlog/security::channel`). Without a context-scoped tracker (`seenWinEventLogChannels := windowseventlog.NewSeenChannels()` passed via `windowseventlog.WithSeenChannels(ctx, seenWinEventLogChannels)`) initialized in `provider.go` before looping over `sourcePolicies`, a second `WindowsEventLogSourcePolicy` specifying `"SECURITY"` or `"security"` would overwrite the first policy's `"Security"` channel name.

---

## 3. Consolidated Performance & Operational Metrics

| Metric / Dimension | Upstream Baseline (`v0.162.0` Defaults) | GBOC Prototype (`WindowsEventLogSourcePolicy` Defaults) | Measured Improvement / Delta |
| :--- | :--- | :--- | :--- |
| **`Security` Channel Active Backlog Drain Rate (`task-184`, ~3,000 Events)** | `1,082.71 events/sec` (`2,799.47 ms` active drain; `2,707.69 ms` post-first-batch) | **`5,488.09 events/sec`** (`559.76 ms` active drain; `512.74 ms` post-first-batch) | **`5.07x` faster active drain** (**`5.41x`** post-first-batch) |
| **`Security` Channel Active Backlog Drain Rate (`task-163`, ~3,100–3,500 Events)** | `1,003.57 events/sec` (`3,088.98 ms` active drain) | **`6,195.91 events/sec`** (`564.89 ms` active drain) | **`6.17x` faster active drain** |
| **`TangoBenchLog` Active Backlog Drain Rate (`task-140`, 3,000 Events)** | `1,108.49 events/sec` wall (`2,706.37 ms` wall; `~1,758.12 events/sec` derived active drain) | `554.97 events/sec` wall (`5,405.68 ms` wall; **`~7,394.99 events/sec`** derived active drain) | **`4.21x` faster derived active drain** |
| **`Security` Channel Steady-State Batch Time (`task-184`, 100 events/batch)** | Mean `58.86 ms`, Median **`81.15 ms`**, p95 `101.69 ms` | Mean `16.02 ms`, Median **`14.74 ms`**, p95 `30.67 ms` | **`5.51x` lower median batch time** |
| **`Security` Channel Active Per-Event Cost (`task-184`)** | `923.61 µs/event` | **`182.21 µs/event`** | **`5.07x` lower CPU/syscall cost per event** |
| **Live Event Arrival-to-Delivery Latency (`task-184`, 10 Events, `start_at: end`)** | Median **`769.94 ms`**, Mean `736.67 ms`, p95 `842.41 ms` | Median **`48.60 ms`**, Mean `48.34 ms`, p95 `91.57 ms` | **`15.8x` lower median latency** (`15.2x` lower mean) |
| **Initial Startup Wait (`firstBatchTime - r.Start`)** | `1.092s` (`poll_interval: 1s`) | `5.047s` (`wait_timeout: 5s` due to `awaitAndReadEvents` `Wait`-before-`read` bug) | Cold-start wall-clock crossover at **`N ≈ 5,395` events** (`0s` wait once upstream fix lands) |
| **Raw XML Payload Size — `Security` EventID `4688` (Process Creation)** | `3,207 bytes` (`<System>` + `<EventData>` + `<RenderingInfo>`) | **`1,273 bytes`** (`<System>` + `<EventData>`) | **`60.3%` smaller (`2.52x` compression)** |
| **Raw XML Payload Size — `Security` EventID `1102` (Audit Log Cleared)** | `1,422 bytes` | **`985 bytes`** | **`30.7%` smaller (`1.44x` compression)** |
| **Raw XML Payload Size — `TangoBenchLog` EventID `100`** | `974 bytes` | **`646 bytes`** | **`33.7%` smaller (`1.51x` compression)** |
| **Missing Channel Startup (`TangoNonExistentChannel_9999`)** | Fatal error from `r.Start()` (`ERROR_EVT_CHANNEL_NOT_FOUND`) | **`r.Start() == nil` + `WARN` log** | **Fail-open startup resilience** |

---

## 4. Productionization Gap Analysis

| Component / Area | Prototype Implementation | Production Requirement |
| :--- | :--- | :--- |
| **1. Upstream `pkg/stanza` `awaitAndReadEvents` Startup Delay** | Documented & empirically measured (`5.047s` initial wait before first `i.read(ctx)`). | File upstream PR in `open-telemetry/opentelemetry-collector-contrib` (`pkg/stanza/operator/input/windows/input.go:338-356`) to call `i.eventsReadInPollCycle = 0; i.read(ctx)` once immediately before entering the `for { ready, err := i.subscription.Wait(i.cancelEvent, timeoutMs) ... }` loop so `EvtNext` drains to `ERROR_NO_MORE_ITEMS` on startup and arms `SignalEvent` with `0s` delay. |
| **2. Upstream `pkg/stanza` `parseSeverity("", "0")` Mapping** | Documented & empirically measured (`SeverityNumber = 0` on `Microsoft-Windows-Security-Auditing` `<Level>0</Level>` events when `suppress_rendering_info: true`). | File upstream PR in `open-telemetry/opentelemetry-collector-contrib` (`pkg/stanza/operator/input/windows/xml.go:52-66`) to map `case "0", "4": return entry.Info` when `renderedLevel == ""` so `win:LogAlways` (`<Level>0</Level>`) Security Audit events retain `SeverityNumberInfo` (`9`) under `suppress_rendering_info: true`. |
| **3. OnePlatform Proto & xDS Policy Translation** | `WindowsEventLogSourcePolicy` JSON/struct registered in `googlecontrolplaneprovider/policies/windowseventlog`. | Wire OnePlatform `WindowsEventLogSourcePolicy` proto (`google/cloud/otel/v1/oneplatform.proto`) in the xDS control plane translator to emit `"type": "windows_event_log"` policies in `ActivePolicySet`. |

---

## 5. Specific Recommendations for Updating `tango_onepager_4_windowseventlog_throughput_defaults.md`

1. **Update Throughput Speedup Claims with End-to-End Receiver Measurements Alongside Micro-Benchmarks**:
   - Keep the existing `45x` isolated syscall micro-benchmark (`3.56 ms` `EvtFormatMessage` vs. `77.9 µs` `EvtRender` = `280/s` vs. `12,800/s`), and add the end-to-end Stanza `windowseventlogreceiver` (`v0.162.0`) measurements on Windows Server 2022 (`tango-win2022-proto`): **`1,003.57–1,082.71 events/sec` -> `5,488.09–6,195.91 events/sec` (`5.07x–6.17x` active drain speedup)** on `Security` (explaining how `publisherCache` in `input.go:415` caches provider metadata handles and how both paths share `~104 µs/event` of XML/Stanza/bbolt overhead).
2. **Add Upstream Companion Fix #1 (`awaitAndReadEvents` 5-Second Startup Delay)**:
   - Document that `awaitAndReadEvents` in `pkg/stanza@v0.162.0/operator/input/windows/input.go:338-356` calls `i.subscription.Wait(i.cancelEvent, timeoutMs)` (`5,000 ms`) before calling `i.read(ctx)`, violating Win32 `EvtSubscribe`'s requirement to drain `EvtNext` to `ERROR_NO_MORE_ITEMS` before waiting on `SignalEvent`, and recommend a 2-line upstream fix (`i.eventsReadInPollCycle = 0; i.read(ctx)` before the `for` loop).
3. **Add Upstream Companion Fix #2 (`parseSeverity("", "0")` & Structured `Body().Map()` Behavior under `suppress_rendering_info: true`)**:
   - Clarify in the design doc that `windowseventlogreceiver` defaults to `raw: false` (structured `Body().Map()` via `formattedBody()`), where `suppress_rendering_info: true` omits `rendering_info` and `details`, sets `message: ""`, leaves `level`/`task`/`opcode`/`keywords` as raw `<System>` values, and causes `parseSeverity("", "0")` (`xml.go:52-66`) to emit `SeverityNumber = 0` (`UNSPECIFIED`) instead of `9` (`INFO`) on `Microsoft-Windows-Security-Auditing` `<Level>0</Level>` events unless `case "0": return entry.Info` is added upstream.
4. **Specify Cross-Policy Case-Insensitive Channel Deduplication via `NewSeenChannels()` + `WithSeenChannels(ctx, seenWinEventLogChannels)`**:
   - Explicitly note in Section 4 of the one-pager that `provider.go` initializes a context-scoped seen-channels map (`seenWinEventLogChannels := windowseventlog.NewSeenChannels()` passed via `windowseventlog.WithSeenChannels(ctx, seenWinEventLogChannels)`) across all `WindowsEventLogSourcePolicy` instances in an active `PolicySet` so that `confmap.Merge` preserves the first-seen channel casing (`receivers::windowseventlog/<lowercase_channel>::channel`).

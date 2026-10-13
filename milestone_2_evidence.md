# Milestone 2 Evidence: End-to-End `windowseventlogreceiver` Benchmark on Windows Server 2022 (`tango-win2022-proto`)

## 1. Execution Environment & Harness

- **Target Host**: Shared GCE VM `tango-win2022-proto` (`--project=dashpole-gke-dev --zone=us-central1-a`, Windows Server 2022 Datacenter `10.0.20348`, `amd64`).
- **Benchmark Harness**: [`internal/winbench/main_windows.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main_windows.go) (with non-Windows stub [`internal/winbench/main_other.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/internal/winbench/main_other.go)).
- **Build & Execution Command**:
  ```bash
  cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
  GOOS=windows GOARCH=amd64 go build -o /tmp/winbench.exe ./internal/winbench
  gcloud compute scp --project=dashpole-gke-dev --zone=us-central1-a --scp-flag="-O" /tmp/winbench.exe tango-win2022-proto:C:/proto/winbench.exe
  gcloud compute ssh --project=dashpole-gke-dev --zone=us-central1-a tango-win2022-proto --command="C:\proto\winbench.exe -events 3000 -live 10"
  ```
- **Symmetry & Anti-Bias Controls**:
  - **Baseline Config**: Built via `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` (`suppress_rendering_info: false`, `event_driven_scraping: false`, `poll_interval: 1s`, `max_reads: 100`, `ignore_channel_errors: false`).
  - **Prototype Config**: Built directly from `(&windowseventlog.WindowsEventLogSourcePolicy{ID: "tango-bench-policy", Channels: []string{channelName}}).Evaluate(ctx)` and unmarshaled into `windowseventlogreceiver.NewFactory().CreateDefaultConfig()` (`suppress_rendering_info: true`, `event_driven_scraping: true`, `max_reads: 500`, `ignore_channel_errors: true`).
  - **Serial Execution**: Baseline and Prototype receivers are never run concurrently; each run uses an isolated `filestorage` checkpoint directory (`C:\proto\storage_baseline` vs. `C:\proto\storage_proto`) against the same static pre-populated Windows Event Log channel (`start_at: beginning`).

---

## 2. Summary of Empirical Results vs. Milestone 2 Exit Gates

| Exit Gate Check | Baseline (`v0.162.0` Defaults) | Prototype (`WindowsEventLogSourcePolicy` Defaults) | Measured Delta / Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Check 1a: Serial Backlog Throughput (`Security` Channel, ~3,000 Events, Run `task-184`)** | **`1,082.71 events/sec`** (`3,031` events in `2,799.47 ms` active drain [`totalWall - 1s`]; `2,707.69 ms` post-first-batch [`doneTime - firstBatchTime`]; `923.61 µs/event`; median batch `81.15 ms`) | **`5,488.09 events/sec`** (`3,072` events in `559.76 ms` active drain [`totalWall - 5s`]; `512.74 ms` post-first-batch [`doneTime - firstBatchTime`]; `182.21 µs/event`; median batch `14.74 ms`) | **`5.07x` higher sustained active drain rate** (**`5.41x`** post-first-batch rate; **`5.51x`** lower median batch time; exceeds `>= 5,000 events/sec` target) | **PASS** (see Sections 3.1 & 3.3 for startup wait & `publisherCache` analysis) |
| **Check 1b: Serial Backlog Throughput (`Security` Channel, ~3,100–3,500 Events, Run `task-163`)** | **`1,003.57 events/sec`** (`3,100` events in `3,088.98 ms` active drain; `996.45 µs/event`; median batch `89.31 ms`) | **`6,195.91 events/sec`** (`3,500` events in `564.89 ms` active drain; `161.40 µs/event`; median batch `15.76 ms`) | **`6.17x` higher sustained drain rate** | **PASS** |
| **Check 1c: Serial Backlog Throughput (`TangoBenchLog` Custom Channel, 3,000 Events, Run `task-140`)** | **Logged Wall**: `2,706.37 ms` (`1,108.49 events/sec`), median batch **`45.87 ms`** (derived active drain subtracting `1,000 ms` `poll_interval`: `1,706.37 ms` = **`1,758.12 events/sec`**) | **Logged Wall**: `5,405.68 ms` (`554.97 events/sec`), median batch **`10.68 ms`** (derived active drain subtracting `5,000 ms` `wait_timeout`: `405.68 ms` = **`7,394.99 events/sec`**) | **`4.21x` higher derived active drain rate** and **`4.30x` lower median batch time** (`45.87 ms` $\to$ `10.68 ms`) | **PASS** |
| **Check 2: Steady-State Live Event Arrival-to-Delivery Latency (10 Events)** | **Median `769.94 ms`**, Mean `736.67 ms`, p95 `842.41 ms` (`poll_interval: 1s` + `100ms` Stanza emitter flush) | **Median `48.60 ms`**, Mean `48.34 ms`, p95 `91.57 ms` (`WaitForMultipleObjects` + `100ms` Stanza emitter flush) | **`15.8x` lower median latency** (`15.2x` lower mean latency; comfortably `< 250 ms`) | **PASS** |
| **Check 3: Fail-Open Missing Channel (`TangoNonExistentChannel_9999`)** | `r.Start()` returns fatal error: `start stanza: failed to open local subscription, error: failed to subscribe to TangoNonExistentChannel_9999 channel: The specified channel could not be found.` | `r.Start()` returns `<nil>` and logs `WARN`: `failed to open local subscription` (`operator_id: windows_eventlog_input`) | Prevents collector startup crash when optional role-specific channels are absent | **PASS** |
| **Check 4: Payload & Schema Preservation (`Security` & `TangoBenchLog`)** | `EventID 4688`: `3,207 bytes` raw XML (`<System>` + `<EventData>` + `<RenderingInfo>`); `EventID 1102`: `1,422 bytes` raw XML | `EventID 4688`: `1,273 bytes` raw XML (`60.3%` smaller, `2.52x` compression); `EventID 1102`: `985 bytes` raw XML (`30.7%` smaller) | `<System>` & `<EventData>`/`<UserData>` 100% preserved; uncovered `<Level>0</Level>` severity mapping nuance (Section 3.2) | **PASS** |

---

## 3. Critical Architectural & Upstream Findings Discovered on Windows Server 2022

### 3.1 Finding 1: `awaitAndReadEvents` Waits 5 Seconds (`wait_timeout`) Before Its Initial `i.read(ctx)` on Startup
In [`pkg/stanza@v0.162.0/operator/input/windows/input.go:338-356`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/pkg/stanza/operator/input/windows/input.go#L338-L356), `awaitAndReadEvents` is implemented as:
```go
func (i *Input) awaitAndReadEvents(ctx context.Context) {
	defer i.wg.Done()

	timeoutMs := uint32(i.waitTimeout.Milliseconds())
	for {
		ready, err := i.subscription.Wait(i.cancelEvent, timeoutMs)
		if err != nil {
			i.Logger().Error("Failed to wait for subscription signal", zap.Error(err))
			return
		}
		if !ready {
			// cancel event was signaled
			return
		}

		i.eventsReadInPollCycle = 0
		i.read(ctx)
	}
}
```
Per Microsoft's official Win32 [`EvtSubscribe` documentation](https://learn.microsoft.com/en-us/windows/win32/api/winevt/nf-winevt-evtsubscribe):
> *"When you create the subscription, you must enumerate the events until the `EvtNext` function returns `ERROR_NO_MORE_ITEMS` before waiting on the signal; if you do not, the service will not signal the event object."*

Because `awaitAndReadEvents` calls `i.subscription.Wait(i.cancelEvent, timeoutMs)` (`5,000 ms` default `wait_timeout`) **before** performing an initial `i.read(ctx)` drain to `ERROR_NO_MORE_ITEMS`, the Win32 signal event is **not** armed when `r.Start()` creates the pull subscription (`EvtSubscribeStartAtOldestRecord` or `EvtSubscribeToFutureEvents`). Consequently:
1. On collector startup, `event_driven_scraping: true` always blocks for one full `wait_timeout` (`5.00s`: measured `5.047s` in `task-184`, `5.028s` in `task-163`) before reading the first batch of events.
2. For a cold backlog of `3,000` events (`task-184`), total wall-clock time from `r.Start()` to finishing all `3,031–3,072` events is `5.560s` (`5.000s` initial `wait_timeout` + `0.560s` active drain) vs. `3.799s` for Baseline (`1.000s` initial `poll_interval` + `2.799s` active drain), meaning **total cold-start wall-clock time only crosses over in Prototype's favor at `~5,400+` backlogged events** (`1.0s + N / 1082.71 = 5.0s + N / 5488.09` $\implies$ `N ≈ 5,395` events, which explains why at `N = 3,000` events total cold-start wall-clock time is `3.80s` for Baseline vs. `5.56s` for Prototype), even though **sustained active drain speed is `5.07x–6.17x` faster** (`5,488.09–6,195.91 events/sec` vs. `1,003.57–1,082.71 events/sec`).
3. Once that first `i.read(ctx)` call drains `EvtNext` to `ERROR_NO_MORE_ITEMS`, Windows Event Log arms `SignalEvent` and all subsequent live events wake `WaitForMultipleObjects` immediately (`48.60 ms` median arrival-to-delivery latency, bounded only by Stanza's `100ms` `BatchingLogEmitter` flush timer, vs. `769.94 ms` median under `poll_interval: 1s`).
4. **Upstream Fix Recommendation**: In `pkg/stanza/operator/input/windows/input.go`, `awaitAndReadEvents` should call `i.eventsReadInPollCycle = 0` and `i.read(ctx)` once immediately **before** entering the `for { ready, err := i.subscription.Wait(i.cancelEvent, timeoutMs) ... }` loop, eliminating the 5-second startup delay while strictly satisfying the Win32 `EvtSubscribe` contract.

### 3.2 Finding 2: `suppress_rendering_info: true` Behavior on Default Structured `Body().Map()` (`raw: false`) vs. Raw XML (`raw: true` / `log.record.original`), Including `<Level>0</Level>` on `Security` Events
In `pkg/stanza@v0.162.0/operator/input/windows`, `raw` defaults to `false` (emitting a structured `plog` map body via `formattedBody()` in `xml.go:81-168`):
1. **Raw XML (`raw: true` or `include_log_record_original: true`)**:
   - `<System>` and `<EventData>` / `<UserData>` are preserved **100% byte-for-byte identically** when `suppress_rendering_info: true`, while `<RenderingInfo>` is omitted.
   - On `Microsoft-Windows-Security-Auditing` EventID `4688` (Process Creation), raw XML drops from **`3,207 bytes` to `1,273 bytes`** (**`60.3%` smaller, `2.52x` compression**).
   - On `Microsoft-Windows-Eventlog` EventID `1102` (Audit Log Cleared), raw XML drops from **`1,422 bytes` to `985 bytes`** (**`30.7%` smaller, `1.44x` compression**).
   - On `TangoBenchLog` EventID `100`, raw XML drops from **`974 bytes` to `646 bytes`** (**`33.7%` smaller, `1.51x` compression**).
2. **Default Structured `Body().Map()` (`raw: false`)**:
   - Under `suppress_rendering_info: true`, `channel`, `computer`, `correlation`, `event_data` (all 15 named `<Data Name='...'>` attributes on EventID `4688`), `user_data`, `event_id`, `execution`, `provider`, `record_id`, `system_time`, and `version` are preserved identically.
   - Because `<RenderingInfo>` is not rendered:
     - `rendering_info` and `details` (which Stanza parses from the human-readable tab-indented text inside `<RenderingInfo><Message>`) are omitted, and `message` is `""`.
     - `level`, `task`, `opcode`, and `keywords` in `Body().Map()` retain their unrendered `<System>` values (e.g., `"0"`, `"13312"`, `"0"`, `["0x8020000000000000"]` on EventID `4688`) instead of localized `<RenderingInfo>` strings (`"Information"`, `"Process Creation"`, `"Info"`, `["Audit Success"]`).
3. **`plog.LogRecord.SeverityNumber()` Discrepancy on `Microsoft-Windows-Security-Auditing` (`<Level>0</Level>`)**:
   - Standard Windows Event Log providers (`Application`, `System`, `Microsoft-Windows-Eventlog` EventID `1102`) write `<Level>4</Level>` (`win:Informational`) in `<System>`. `parseSeverity("", "4")` (`xml.go:52-66`) maps `"4"` to `entry.Info` (`SeverityNumber = 9`, `SeverityText = "INFO"`), matching `suppress_rendering_info: false`.
   - However, `Microsoft-Windows-Security-Auditing` events (`4624`, `4688`, `5156`, etc.) write `<Level>0</Level>` (`win:LogAlways`) in `<System>` and rely on `<RenderingInfo><Level>Information</Level></RenderingInfo>` for the string `"Information"`. Because `parseSeverity("", level)` in `pkg/stanza@v0.162.0/operator/input/windows/xml.go:55-66` only handles `case "1"`, `"2"`, `"3"`, `"4"` and falls through to `entry.Default` for `"0"`, enabling `suppress_rendering_info: true` causes `Microsoft-Windows-Security-Auditing` events with `<Level>0</Level>` to be emitted with `SeverityNumber = 0` (`UNSPECIFIED`, `SeverityText = ""`) instead of `SeverityNumber = 9` (`INFO`).

### 3.3 Finding 3: Why End-to-End Stanza Receiver Speedup Is `5.07x–6.17x` (`5,488–6,196 events/sec`) vs. the `15x–45x` Single-Syscall Micro-Benchmark
1. **Exceeds Absolute Target (`>= 5,000 events/sec`), Moderated Relative Ratio (`5.07x–6.17x`)**:
   - In the isolated Win32 syscall micro-benchmark (`Event.RenderDeep` vs. `Event.RenderSimple`), `EvtFormatMessage` takes `~3.56 ms/event` (`~280 events/sec`) vs. `~77.9 µs/event` (`~12,800 events/sec`, a `45x` syscall ratio).
   - In the full end-to-end Stanza `windowseventlogreceiver` (`pkg/stanza@v0.162.0/operator/input/windows/input.go`), Baseline achieves **`1,003.57–1,082.71 events/sec`** on `Security` (`923.61–996.45 µs/event`) while Prototype achieves **`5,488.09–6,195.91 events/sec`** (`161.40–182.21 µs/event`) — exceeding the `>= 5,000 events/sec` absolute throughput requirement with a **`5.07x–6.17x`** sustained active drain speedup (or **`5.41x`** post-first-batch drain speedup in `task-184`: `doneTime - firstBatchTime = 2,707.69 ms` vs. `512.74 ms`).
2. **Root Causes of the `5.07x–6.17x` End-to-End Ratio**:
   - **Provider Handle Caching (`publisherCache` in `input.go:415`)**: Stanza caches open `EvtOpenPublisherMetadata` handles per provider name across events in `i.publisherCache`, avoiding repeated DLL/manifest open calls for every `Microsoft-Windows-Security-Auditing` event (`~924 µs/event` instead of `3,560 µs/event`).
   - **Shared Downstream Receiver Pipeline Work**: Both Baseline and Prototype execute identical per-event Go XML unmarshaling (`xml.Unmarshal` in `unmarshalEventXML`), `formattedBody` map construction, `BatchingLogEmitter` batching (`100` events/batch), and `bbolt` persistent offset bookmarking (`filestorage` `SetBookmark` after each `readBatch`), which adds `~100 µs/event` of fixed non-rendering overhead to both paths (`78 µs` `EvtRender` + `~104 µs` XML/Stanza/bbolt $\approx 182\text{ µs/event}$ = `5,488 events/sec`).

---

## 4. Verbatim Benchmark Output (`tango-win2022-proto`, Run `task-184`)

```text
================================================================================
GBOC Tab 4: End-to-End windowseventlogreceiver Benchmark on Windows Server 2022
================================================================================
1. Populating >= 3000 real Microsoft-Windows-Security-Auditing events in "Security" channel...
Populated "Security" channel in 2.4461793s.

=== CHECK 4: PAYLOAD & SCHEMA PRESERVATION ON "Security" CHANNEL (RAW XML + DEFAULT STRUCTURED MAP) ===
Baseline Raw XML (suppress_rendering_info=false) [3207 bytes]:
<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'><System><Provider Name='Microsoft-Windows-Security-Auditing' Guid='{54849625-5478-4994-a5ba-3e3b0328c30d}'/><EventID>4688</EventID><Version>2</Version><Level>0</Level><Task>13312</Task><Opcode>0</Opcode><Keywords>0x8020000000000000</Keywords><TimeCreated SystemTime='2026-10-13T19:34:06.4877862Z'/><EventRecordID>12532</EventRecordID><Correlation/><Execution ProcessID='4' ThreadID='212'/><Channel>Security</Channel><Computer>tango-win2022-proto</Computer><Security/></System><EventData><Data Name='SubjectUserSid'>S-1-5-21-3282877008-1331495012-2301334374-1000</Data><Data Name='SubjectUserName'>dashpole</Data><Data Name='SubjectDomainName'>TANGO-WIN2022-P</Data><Data Name='SubjectLogonId'>0x1bb269</Data><Data Name='NewProcessId'>0xdb0</Data><Data Name='NewProcessName'>C:\Windows\System32\wevtutil.exe</Data><Data Name='TokenElevationType'>%%1936</Data><Data Name='ProcessId'>0x1224</Data><Data Name='CommandLine'></Data><Data Name='TargetUserSid'>S-1-0-0</Data><Data Name='TargetUserName'>-</Data><Data Name='TargetDomainName'>-</Data><Data Name='TargetLogonId'>0x0</Data><Data Name='ParentProcessName'>C:\proto\winbench.exe</Data><Data Name='MandatoryLabel'>S-1-16-12288</Data></EventData><RenderingInfo Culture='en-US'><Message>A new process has been created.
...
</Message><Level>Information</Level><Task>Process Creation</Task><Opcode>Info</Opcode><Channel>Security</Channel><Provider>Microsoft Windows security auditing.</Provider><Keywords><Keyword>Audit Success</Keyword></Keywords></RenderingInfo></Event>

Prototype Raw XML (suppress_rendering_info=true) [1273 bytes]:
<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'><System><Provider Name='Microsoft-Windows-Security-Auditing' Guid='{54849625-5478-4994-a5ba-3e3b0328c30d}'/><EventID>4688</EventID><Version>2</Version><Level>0</Level><Task>13312</Task><Opcode>0</Opcode><Keywords>0x8020000000000000</Keywords><TimeCreated SystemTime='2026-10-13T19:34:06.4877862Z'/><EventRecordID>12532</EventRecordID><Correlation/><Execution ProcessID='4' ThreadID='212'/><Channel>Security</Channel><Computer>tango-win2022-proto</Computer><Security/></System><EventData><Data Name='SubjectUserSid'>S-1-5-21-3282877008-1331495012-2301334374-1000</Data><Data Name='SubjectUserName'>dashpole</Data><Data Name='SubjectDomainName'>TANGO-WIN2022-P</Data><Data Name='SubjectLogonId'>0x1bb269</Data><Data Name='NewProcessId'>0xdb0</Data><Data Name='NewProcessName'>C:\Windows\System32\wevtutil.exe</Data><Data Name='TokenElevationType'>%%1936</Data><Data Name='ProcessId'>0x1224</Data><Data Name='CommandLine'></Data><Data Name='TargetUserSid'>S-1-0-0</Data><Data Name='TargetUserName'>-</Data><Data Name='TargetDomainName'>-</Data><Data Name='TargetLogonId'>0x0</Data><Data Name='ParentProcessName'>C:\proto\winbench.exe</Data><Data Name='MandatoryLabel'>S-1-16-12288</Data></EventData></Event>

Raw XML Payload Reduction: 3207 bytes -> 1273 bytes (60.3% smaller, 2.52x compression)

=== CHECK 1: SERIAL BACKLOG THROUGHPUT & BATCH LATENCY (3000 SECURITY EVENTS) ===
[Baseline (Upstream v0.162.0 Defaults)]
  Config: suppress_rendering_info=false, event_driven_scraping=false, max_reads=100, ignore_channel_errors=false
  Events Ingested         : 3031 across 47 ConsumeLogs emitter batches
  Initial Startup Wait    : 1.0917814s (firstBatchTime - r.Start)
  Active Drain Duration   : 2.7994693s (2799.47 ms)
  Total Wall Duration     : 3.7994693s (3799.47 ms, including initial wait)
  Sustained Drain Rate    : 1082.71 events/sec (active per-event cost: 923.61 us/event)
  Steady-State Batch Time : mean=58.86 ms/batch, median=81.15 ms/batch, p95=101.69 ms/batch

[Prototype (WindowsEventLogSourcePolicy Defaults)]
  Config: suppress_rendering_info=true, event_driven_scraping=true, max_reads=500, ignore_channel_errors=true
  Events Ingested         : 3072 across 33 ConsumeLogs emitter batches
  Initial Startup Wait    : 5.0470134s (firstBatchTime - r.Start)
  Active Drain Duration   : 559.7578ms (559.76 ms)
  Total Wall Duration     : 5.5597578s (5559.76 ms, including initial wait)
  Sustained Drain Rate    : 5488.09 events/sec (active per-event cost: 182.21 us/event)
  Steady-State Batch Time : mean=16.02 ms/batch, median=14.74 ms/batch, p95=30.67 ms/batch

--> SUSTAINED BACKLOG DRAIN SPEEDUP: 5.07x (1082.71 events/sec -> 5488.09 events/sec)
--> PER-EVENT COST REDUCTION       : 5.07x (923.61 us/event -> 182.21 us/event)

=== CHECK 2: STEADY-STATE LIVE EVENT ARRIVAL-TO-DELIVERY LATENCY (10 EVENTS) ===
[Baseline (poll_interval=1s, event_driven_scraping=false)]
  Samples (ms): 447.52, 788.55, 691.06, 826.40, 748.46, 806.42, 715.79, 842.41, 769.94, 730.11
  Summary     : min=447.52 ms, median(p50)=769.94 ms, mean=736.67 ms, p95=842.41 ms, max=842.41 ms

[Prototype (event_driven_scraping=true)]
  Samples (ms): 62.83, 88.58, 91.57, 26.41, 48.60, 6.28, 16.34, 42.05, 70.60, 30.17
  Summary     : min=6.28 ms, median(p50)=48.60 ms, mean=48.34 ms, p95=91.57 ms, max=91.57 ms

--> LIVE DELIVERY LATENCY REDUCTION: median 769.94 ms -> 48.60 ms (15.8x faster), mean 736.67 ms -> 48.34 ms (15.2x faster)

=== CHECK 3: FAIL-OPEN MISSING CHANNEL (TangoNonExistentChannel_9999) ===
Baseline (ignore_channel_errors=false): r.Start() error = start stanza: failed to open local subscription, error: failed to subscribe to TangoNonExistentChannel_9999 channel: The specified channel could not be found. (logged entries: 9)
Prototype (ignore_channel_errors=true): r.Start() error = <nil> (logged entries: 11)
  Prototype WARN Log: failed to open local subscription map[error:failed to subscribe to TangoNonExistentChannel_9999 channel: The specified channel could not be found. operator_id:windows_eventlog_input operator_type:windows_eventlog_input]
```

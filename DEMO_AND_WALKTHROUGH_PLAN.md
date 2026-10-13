# Demo and Walkthrough Plan: Preventing Metric Counter and Uptime Resets Across Policy Rollouts (Tab 3)

## 1. 60-Second Executive Pitch (Why This Matters, What Was Tested, What Next)

- **Why It Matters**: During the Allianz SecOps / Bindplane evaluation (**Finding `#37`**), operators reported that metric counters (`otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`) and process uptime (`otelcol_process_uptime`) reset to zero after every policy update. Worse, our prototype uncovered that in GBOC `v0.162.0`, a full collector reload on `BuiltInSelfMetricsPolicy`'s loopback OTLP topology actually crashes `otelcol.Collector.Run` during shutdown (`connect: connection refused` when `meterProvider.Shutdown` flushes after `otlp/default_self_metrics` has stopped).
- **What Was Tested**: We implemented diff-gated `confmap.WatcherFunc` notifications and enabled upstream `service.partialReload` + `service.partialReloadReceivers` in `googlecontrolplaneprovider`, then ran an end-to-end `otelcol.Collector` harness across `LogFilterPolicy` and `SourcePolicy` rollouts (`Case B1` shared pipeline addition, `Case A` in-place receiver modification, and `Case B2` new pipeline key fallback).
- **What We Proved & Next Step**:
  - **Filter/Transform Rollouts**: `0` collector reloads; `otelcol_exporter_sent_log_records` (`5 -> 8`) and `otelcol_receiver_accepted_log_records` (`5 -> 10`) advance monotonically without resetting.
  - **SourcePolicy Rollouts (Shared Pipeline Key & In-Place Edits)**: `Service.UpdateReceivers` hot-swaps only changed receivers (`partial_reloads = 2, full_restarts = 0`), preserving `otelcol_exporter_sent_log_records` (`8 -> 18 -> 23`), unchanged receiver counters (`10 -> 14 -> 16`), **and even modified receiver counters** (`6 -> 9` via the preserved `MeterProvider`)!
  - **Next Step**: Merge diff-gated `WatcherFunc` + `service.partialReloadReceivers` in `googlecontrolplaneprovider`, enforce shared `service::pipelines::logs` keys across all Tango `SourcePolicy` drivers (with a pre-seeded no-op receiver for the `0 -> 1` transition), and decouple `BuiltInSelfMetricsPolicy` shutdown flushing from `otlp/default_self_metrics`.

---

## 2. Option A: Reproducible Live Terminal Walkthrough

Run these commands directly in `/tmp/prototypes/tab3_reload_continuity`:

### Step 1: Verify Diff-Gated `confmap.WatcherFunc` & Partial Reload Feature Gates (Milestone 1)
```bash
cd /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -race -run "TestPartialReloadFeatureGatesEnabled|TestWatcher_DiffGatedVsBaseline" .
```
**What to Point Out**:
- `TestPartialReloadFeatureGatesEnabled` confirms `confmap.enableMergeAppendOption`, `service.partialReload`, and `service.partialReloadReceivers` are enabled by `NewFactory()`.
- `TestWatcher_DiffGatedVsBaseline` shows that rolling out `LogFilterPolicy` fires `WatcherFunc` in Baseline (`1` reload notification), whereas Prototype suppresses `WatcherFunc` (`0` reload notifications) and only fires `WatcherFunc` when a `SourcePolicy` changes the synthesized `confmap`.

### Step 2: Run End-to-End `otelcol.Collector` Baseline vs. Prototype Comparison (Milestone 2)
```bash
cd /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider
go test -v -race -run "TestReloadContinuity|TestSelfMetricsLoopback" .
```
**What to Point Out in the Output**:
1. **`TestReloadContinuity_Baseline_ResetsCountersAndUptime`**:
   - Step 0: `exporter_sent=5`, `receiver_accepted(source_a)=5`, `process_uptime=0.288s`.
   - Step 1 (After `LogFilterPolicy` rollout): `full_restarts=1`, `exporter_sent=3` (**RESET from 5 to 3**; expected `8`), `receiver_accepted(source_a)=5` (**RESET from 5 to 5**; expected `10`), `process_uptime=0.083s` (**RESET**).
   - Step 2 (After `SourcePolicy` rollout): `full_restarts=2`, `exporter_sent=2` (**RESET from 3 to 2**; expected `10`), `process_uptime=0.086s` (**RESET**).
2. **`TestReloadContinuity_Prototype_PreservesCountersAndUptime`**:
   - Step 0: `exporter_sent=5`, `receiver_accepted(source_a)=5`, `process_uptime=0.186s` (`start=1791920224021890211`).
   - Step 1 (`LogFilterPolicy` rollout): `reloads=0`, `exporter_sent=8` (`5->8` monotonic), `receiver_accepted(source_a)=10` (`5->10` monotonic), `process_uptime=0.286s` (`start` unchanged).
   - Step 2 (`SourcePolicy` Case B1 — Add `otlp/source_b` to shared `logs` pipeline): `partial_reloads=1, full_restarts=0`, `exporter_sent=18` (`8->18` monotonic), `receiver_accepted(source_a)=14` (`10->14` monotonic), `receiver_accepted(source_b)=6`, `process_uptime=0.486s` (`start` unchanged).
   - Step 3 (`SourcePolicy` Case A — Modify `otlp/source_b` port in place): `partial_reloads=2, full_restarts=0`, `exporter_sent=23` (`18->23` monotonic), `receiver_accepted(source_a)=16` (`14->16` monotonic), `receiver_accepted(source_b)=9` (`6->9` monotonic via preserved `MeterProvider`!), `process_uptime=0.686s` (`start` unchanged).
   - Step 4 (`SourcePolicy` Case B2 — Add `otlp/source_c` with new pipeline key `logs/source_c`): `full_restarts=1`, `exporter_sent=2` (**RESET from 23 to 2** because `len(old.pipelines) != len(cur.pipelines)` in `otelcol/config_diff.go:161`).
3. **`TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown`**:
   - Shows the unproxied `BuiltInSelfMetricsPolicy` loopback failing fatally during `col.service.Shutdown(ctx)` (`failed to shutdown the retiring config: failed to shutdown meter provider: ... connect: connection refused`).

---

## 3. Option B: AI-Narrated Walkthrough Video Storyboard & Script

*(Note: All spoken narration below is written in natural spoken English with zero raw symbols, backticks, file paths, or URLs so TTS engines render it cleanly.)*

| Scene | Duration | Visual / Screen Content | Spoken Narration Script (TTS-Ready) |
| :--- | :--- | :--- | :--- |
| **Scene 1: The Problem (Allianz Finding 37 & Loopback Crash)** | `0:00 - 0:20` | Split screen showing Allianz Finding 37 quote on the left and the `TestReloadContinuity_Baseline` + `TestSelfMetricsLoopback` terminal logs on the right (`exporter_sent` dropping `5 -> 3 -> 2` and `meterProvider.Shutdown` connection refused). | During the Allianz evaluation, operators reported Finding thirty-seven: every time a policy update rolls out, collector metric counters and process uptime reset back to zero. Even worse, our prototype uncovered that in the current Google Built OpenTelemetry Collector, a full service reload shuts down the internal self-metrics receiver before flushing the meter provider, causing full reloads to fail with connection refused. |
| **Scene 2: Two-Part Architecture (Diff-Gated Watcher + Partial Receiver Reload)** | `0:20 - 0:45` | Architecture flowchart from `PROTOTYPE_PLAN.md` highlighting `googlecontrolplaneprovider.Retrieve`, `pendingConfMap` caching, and upstream `Service.UpdateReceivers`. | Our prototype solves this with two targeted changes in the Google Control Plane config provider. First, when a policy set revision arrives, the provider diffs the newly synthesized configuration map against the last emitted map. When only filter or transform policies change, the policy processor updates in memory and the provider suppresses the collector reload event entirely. Second, when a source policy changes, we enable upstream partial receiver reload so the collector updates only the changed receivers in place. |
| **Scene 3: Empirical Results (Monotonic Counters Across Filter & Source Rollouts)** | `0:45 - 1:15` | Side-by-side table from `milestone_2_evidence.md` animating Steps 0 through 3 (`exporter_sent: 5 -> 8 -> 18 -> 23`, `receiver_accepted(source_a): 5 -> 10 -> 14 -> 16`, `receiver_accepted(source_b): 6 -> 9`, `uptime: 0.186s -> 0.686s`). | Here are the end-to-end results on a live collector instance. Across a log filter policy rollout, an addition of a second log source policy, and an in-place port change on that second source policy, the prototype triggers zero full restarts. Exporter sent log records climb smoothly from five to eight, eighteen, and twenty-three. Even more surprisingly, when the second receiver restarts on a new port in Step three, its own accepted log counter continues monotonically from six to nine because the preserved meter provider reuses the existing counter instrument. |
| **Scene 4: Key Boundary Condition & Next Steps** | `1:15 - 1:35` | Step 4 terminal log (`logs/source_c` triggering full reload) alongside the 4 Productionization Gaps table from `PROTOTYPE_LEARNINGS.md`. | Finally, Step four demonstrates a critical boundary condition: upstream partial reload only succeeds when the set of pipeline keys remains constant. Therefore, all Tango source policy drivers must merge their receivers into the shared logs pipeline rather than creating per-policy pipeline keys, and we must decouple the self-metrics loopback shutdown before enabling fallback full reloads in production. |

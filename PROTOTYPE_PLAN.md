# Prototype Plan: Preventing Metric Counter and Uptime Resets Across Policy Rollouts (Tab 3)

**Companion Design Doc**: `tango_onepager_3_gboc_reload_continuity.md` (Project Tango — Bindplane Finding `#37`)
**Target Repository & Tag**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`v0.162.0`, commit `d273bac6672e1532c9f0db2f87dbf65aa39704ac`)
**Prototype Workspace**: `/tmp/prototypes/tab3_reload_continuity` (branch `proto/tango-tab3-reload-continuity`)

---

## 1. Core Question & Hypothesis

When Google-Built OpenTelemetry Collector (GBOC) is managed dynamically via `googlecontrolplaneprovider` (`googlepolicy.SetActivePolicySet`), rolling out policy updates can reset cumulative telemetry counters (`otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`) and process uptime (`otelcol_process_uptime`) back to `0` (Bindplane Finding `#37`).

This prototype answers three concrete technical questions:

1. **Diff-Gated `confmap.WatcherFunc` for Transformation Policies**:
   - Can `googlecontrolplaneprovider` subscribe to `googlepolicy.RegisterWatcherChannel()`, re-evaluate `evaluateActivePolicySet(ctx)` on each `PolicySet` revision, compare the synthesized `confmap` against the previously emitted `confmap`, and suppress `watcher(&confmap.ChangeEvent{})` when only `PolicyClassTransformation` policies (`LogFilterPolicy`, `MetricFilterPolicy`, `TraceFilterPolicy`) change—while `googlepolicyprocessor` still hot-swaps its filter evaluator in memory with **zero** Collector reloads and **zero** counter/uptime resets?
2. **Upstream Receiver Partial Reload (`service.partialReload` + `service.partialReloadReceivers`) for Source Policies**:
   - When `googlecontrolplaneprovider` enables upstream feature gates `service.partialReload` (`alpha`) and `service.partialReloadReceivers` (`beta`) alongside `confmap.enableMergeAppendOption`, does rolling out a `SourcePolicy` update invoke `Service.UpdateReceivers(...)` instead of shutting down and recreating `service.Service`, thereby preserving `MeterProvider`, `otelcol_process_uptime`, `otelcol_exporter_sent_log_records`, and unchanged receivers' `otelcol_receiver_accepted_log_records` counters?
3. **Pipeline-Topology Sensitivity in Upstream `otelcol/config_diff.go` (`v0.162.0`)**:
   - How does `fingerprintForPartialReload` / `receiversOnlyChanged` in `go.opentelemetry.io/collector/otelcol@v0.162.0/config_diff.go` behave across three distinct `SourcePolicy` update shapes:
     - **(Case A) Modifying an existing receiver's configuration** under `receivers:` (e.g., changing `include` globs or endpoint/attributes on an existing receiver in `service.pipelines.logs`);
     - **(Case B1) Adding a brand-new receiver ID** (`otlp/source_b`) to an **existing** shared pipeline (`service.pipelines.logs.receivers` merged via `confmap.enableMergeAppendOption`);
     - **(Case B2) Adding a brand-new pipeline ID** (`service.pipelines.logs/source_c`) when each `SourcePolicy` emits a per-policy pipeline key (`logs/<policy_name>`) or when transitioning from `0` to `1` customer log `SourcePolicy` instances?

---

## 2. Scope & Intentional Simplifications

### In Scope
- Production-quality implementation of diff-gated `confmap.WatcherFunc` lifecycle management (`googlepolicy.RegisterWatcherChannel()`, `confmap.WithRetrievedClose`, clean `Shutdown` teardown verified by `goleak`) and feature-gate enablement (`service.partialReload`, `service.partialReloadReceivers`) in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go`.
- Unit tests in `provider_test.go` covering feature gate enablement, diff-gated `WatcherFunc` suppression on transformation-only policy updates, `WatcherFunc` invocation on `SourcePolicy` updates, and goroutine leak checks (`goleak`).
- End-to-end Collector integration test (`reload_continuity_test.go`) running a real `otelcol.Collector` with `googlecontrolplaneprovider`, `googlepolicyprocessor`, `SelfMetricsPolicy` (scraping internal OTLP metrics `otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`, and `otelcol_process_uptime`), and OTLP log receivers/exporters to compare **Baseline** vs. **Prototype** across `LogFilterPolicy` and `SourcePolicy` rollouts (Cases A, B1, and B2).

### Out of Scope (Intentional PoC Simplifications)
- Modifying upstream `go.opentelemetry.io/collector/service` (`proctelemetry.RegisterProcessMetrics`) in an external repository (since `go.mod` consumes upstream `v0.162.0` as an immutable module dependency; we document the exact upstream 1-line `processStartTimeUnixNano` fix and `config_diff.go` pipeline-addition limitation in `PROTOTYPE_LEARNINGS.md`).
- Live xDS control-plane server deployment on GCP (we drive `googlepolicy.SetActivePolicySet` directly and via `file:` policy sets in deterministic local tests, which exercises the exact same `WatcherChannel` notification path as `xdsPolicyManager.handleResponse`).

---

## 3. Symmetric Evaluation Rubric (Baseline vs. Prototype)

Both configurations run inside the **exact same `otelcol.Collector` harness** with identical telemetry scrape intervals, identical `SelfMetricsPolicy` OTLP metric capture, and identical policy revision sequences:

| Dimension | Baseline Mode | Prototype Mode |
| :--- | :--- | :--- |
| **`WatcherFunc` Behavior** | Unconditional: fires `watcher(&confmap.ChangeEvent{})` on every `PolicySet` revision | Diff-Gated: fires `watcher(&confmap.ChangeEvent{})` only when synthesized `confmap.Conf.ToStringMap()` changes |
| **`service.partialReload` & `service.partialReloadReceivers`** | `false` (`v0.162.0` default) | `true` (enabled in `newProvider`) |
| **Metrics Observed** | `otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`, `otelcol_process_uptime` | `otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`, `otelcol_process_uptime` |

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Diff-Gated `confmap.WatcherFunc` & Partial Reload Feature Gates in `googlecontrolplaneprovider`

- **Deliverable**:
  1. Update `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go`:
     - Enable `service.partialReload` and `service.partialReloadReceivers` in `newProvider` alongside `confmap.enableMergeAppendOption`.
     - Implement diff-gated policy watching in `Retrieve`: when `watcher != nil`, subscribe to `googlepolicy.RegisterWatcherChannel()`, re-evaluate `evaluateActivePolicySet(ctx)` on policy updates, compare the newly synthesized `confmap` (`ToStringMap()`) against `lastConfMap` using `reflect.DeepEqual`, and invoke `watcher(&confmap.ChangeEvent{})` **only** when the synthesized `confmap` differs (or when `diffGateReloads` is explicitly disabled for baseline testing).
     - Cache the watcher-evaluated raw config map (`pendingConfMap map[string]any`, `pendingRevID string`, `hasPending bool`) so the subsequent `Collector` reload `Retrieve(ctx, uri, watcher)` call consumes `pendingConfMap` without double-evaluating or emitting duplicate `PolicyEvaluateError` events for the same revision (including `pendingRevID == ""` when rolled back to the built-in fallback), while **always** spawning a fresh watcher goroutine and wrapping the returned `*confmap.Retrieved` with `confmap.WithRetrievedClose`.
     - Ensure clean watcher goroutine and `WatcherChannel` teardown on both `confmap.Retrieved.Close` and `provider.Shutdown` (verified by `goleak.VerifyTestMain`).
  2. Add unit tests in `provider_test.go` testing:
     - `service.partialReload` and `service.partialReloadReceivers` are enabled by `NewFactory().Create(...)`.
     - Updating `ActivePolicySet` with a `LogFilterPolicy` change does **not** invoke `watcher`.
     - Updating `ActivePolicySet` with a `SourcePolicy` change **does** invoke `watcher` and returns the updated `confmap` on subsequent `Retrieve`.
     - Updating `ActivePolicySet` with an invalid `PolicySet` (e.g., 2 destination policies) rolls back to the previous `PolicySet`, sees an identical synthesized `confmap`, and does **not** trigger a spurious reload.
- **Exact Verification Command**:
  ```bash
  go -C /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider test -v -race -run "TestRetrieve|TestWatcher|TestPartialReload" ./...
  ```
- **Baseline-vs-Change Proof**:
  - With `p.diffGateReloads = false` (Baseline), a `LogFilterPolicy`-only `SetActivePolicySet` update increments `watcherCalls` from `0` to `1`.
  - With `p.diffGateReloads = true` (Prototype default), the identical `LogFilterPolicy`-only `SetActivePolicySet` update keeps `watcherCalls == 0`, while a subsequent `SourcePolicy` update increments `watcherCalls` to `1`.

---

### Milestone 2: Empirical End-to-End Collector Verification of Counter & Uptime Continuity + `config_diff.go` Topology Analysis

- **Deliverable**:
  1. Write end-to-end Collector integration tests in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/reload_continuity_test.go` running a real `otelcol.Collector` instance with `googlecontrolplaneprovider`, `googlepolicyprocessor`, `SelfMetricsPolicy`, and customer OTLP log receivers/exporters:
     - **Hermetic Collector Test Harness**:
       - Because `SelfMetricsPolicy.Evaluate` (`selfmetrics_policy.go:120-253`) binds both its `PeriodicMetricReader` OTLP exporter (`http://localhost:<Port>`, hardcoded `interval: 5000`) and its `otlp/<Name>` receiver to `localhost:<Port>` and emits component configs for `googlecontrolplane` (extension), `resourcedetection/<Name>`, and `transform/<Name>` (processors), the test harness:
         1. Allocates a free local port for `BuiltInSelfMetricsPolicy.Port` so `otlp/default_self_metrics` receives internal OTLP metrics and forwards them down `service::pipelines::metrics/default_self_metrics` to `destPolicy.ExporterIDs()`.
         2. Supplies a hermetic test `DestinationPolicy` in `ActivePolicySet` whose exporter captures all exported OTLP logs and internal metrics in memory (avoiding external calls to `telemetry.googleapis.com:443`), and registers lightweight pass-through/nop component factories in `otelcol.Factories` for `googlecontrolplane`, `resourcedetection`, and `transform`.
         3. Passes a second `confmap` URI in `ResolverSettings.URIs` after `googlecontrolplane:...` that overrides `service::telemetry::metrics::readers` to `interval: 100` (100ms, still exporting to `http://localhost:<SelfMetricsPort>`) and sets `service::telemetry::logs::processors: []` so internal Collector `INFO` logs are not shipped into `destPolicy.ExporterIDs()` where they would contaminate `otelcol_exporter_sent_log_records`.
     - **`TestReloadContinuity_Baseline_ResetsCountersAndUptime`**:
       - Runs with unconditional `WatcherFunc` (`diffGateReloads = false`) and partial reload gates disabled (`service.partialReload = false`, `service.partialReloadReceivers = false`).
       - Sends initial batch of 5 logs (`otelcol_exporter_sent_log_records == 5`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"} == 5`, `otelcol_process_uptime >= 0.25s`).
       - Rolls out a `LogFilterPolicy` update (dropping logs with body `"DROP_ME"`), then sends 3 kept logs + 2 dropped logs through `otlp/source_a`.
       - Verifies that `Collector.reloadConfiguration` performs a full service restart (`"Config updated, restart service"`), resetting `otelcol_exporter_sent_log_records` (re-initialized `MeterProvider` starts a new counter stream from `0`, so sending 3 kept + 2 dropped logs reports `otelcol_exporter_sent_log_records == 3` instead of `8`, and `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"} == 5` instead of `10`) and resetting `otelcol_process_uptime` (`StartTimestamp` resets to a newer timestamp after reload and uptime drops back toward `0`).
       - Rolls out a `SourcePolicy` update and verifies a second full service restart and counter/uptime reset.
     - **`TestReloadContinuity_Prototype_PreservesCountersAndUptime`**:
       - Runs with diff-gated `WatcherFunc` (`diffGateReloads = true`) and partial reload gates enabled (`service.partialReload = true`, `service.partialReloadReceivers = true`).
       - Sends initial batch of 5 logs through `otlp/source_a` (`otelcol_exporter_sent_log_records == 5`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"} == 5`, `otelcol_process_uptime` recorded as $U_1$).
       - **Step 1 (`LogFilterPolicy` rollout)**: Rolls out a `LogFilterPolicy` update (dropping `"DROP_ME"`), sends 3 kept logs + 2 dropped logs through `otlp/source_a`. Verifies:
         - `googlepolicyprocessor` drops the 2 `"DROP_ME"` logs and forwards the 3 kept logs;
         - `WatcherFunc` is **not** called (`0` reloads);
         - `otelcol_exporter_sent_log_records` increases monotonically from `5` to `8` (`5 + 3`, never resetting to `3`);
         - `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` increases monotonically from `5` to `10` (`5 + 5`, never resetting to `5`);
         - `otelcol_process_uptime` ($U_2 > U_1$) increases monotonically with an unchanged `StartTimestamp`.
       - **Step 2 (`SourcePolicy` Case B1 — Adding a brand-new receiver `otlp/source_b` to existing pipeline `service::pipelines::logs`)**: Rolls out a second `SourcePolicy` (`source_b`) that appends `otlp/source_b` to `service::pipelines::logs::receivers` via `confmap.enableMergeAppendOption`. Sends 4 logs to `otlp/source_a` and 6 logs to `otlp/source_b`. Verifies:
         - `Collector.reloadConfiguration` logs `"Config updated, performing partial receiver reload"` and calls `Service.UpdateReceivers` (0 full restarts);
         - `otelcol_exporter_sent_log_records` increases monotonically from `8` to `18` (`8 + 4 + 6`);
         - Unchanged receiver counter `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` increases monotonically from `10` to `14` (`10 + 4`, **zero reset**);
         - New receiver counter `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` reports `6`;
         - `otelcol_process_uptime` ($U_3 > U_2 > U_1$) increases monotonically with the exact same `StartTimestamp`.
       - **Step 3 (`SourcePolicy` Case A — Modifying existing receiver `otlp/source_b` config in place)**: Modifies `source_b`'s receiver config (`otlp/source_b` endpoint port) while leaving `otlp/source_a` untouched. Sends 2 logs to `otlp/source_a` and 3 logs to `otlp/source_b` (on its new port). Verifies:
         - `Service.UpdateReceivers` restarts **only** `otlp/source_b`;
         - `otelcol_exporter_sent_log_records` increases monotonically from `18` to `23` (`18 + 2 + 3`);
         - Unchanged receiver `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` increases monotonically from `14` to `16` (`14 + 2`, **zero reset**);
         - Rebuilt receiver `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` is also recorded (observing how the preserved `MeterProvider` aggregates synchronous counter instruments across receiver rebuilds);
         - `otelcol_process_uptime` ($U_4 > U_3$) increases monotonically with the exact same `StartTimestamp`.
       - **Step 4 (`SourcePolicy` Case B2 — Adding a brand-new pipeline key `service::pipelines::logs/source_c`)**: Empirically demonstrates that when a `SourcePolicy` introduces a **new pipeline key** (`logs/source_c`) in `service.pipelines` instead of merging into `service.pipelines.logs`, `receiversOnlyChanged` in `otelcol/config_diff.go:161` (`len(old.pipelines) != len(cur.pipelines)`) returns `false` and falls back to a full service reload (`"Config updated, restart service"`), resetting `MeterProvider` and `otelcol_process_uptime`.
- **Exact Verification Command**:
  ```bash
  go -C /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider test -v -race -run "TestReloadContinuity" ./...
  ```
- **Baseline-vs-Change Proof**:
  - `TestReloadContinuity_Baseline_ResetsCountersAndUptime` proves that without diff-gated `WatcherFunc` and partial reload gates, the exact same `LogFilterPolicy` and `SourcePolicy` rollouts reset `otelcol_exporter_sent_log_records` (`5 -> 3` instead of `5 -> 8`), reset `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` (`5 -> 5` instead of `5 -> 10`), and reset `otelcol_process_uptime` (`StartTimestamp` resets and value drops toward `0`).
  - `TestReloadContinuity_Prototype_PreservesCountersAndUptime` proves that with diff-gated `WatcherFunc` and partial reload gates enabled, `otelcol_exporter_sent_log_records` (`5 -> 8 -> 18 -> 23`), `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` (`5 -> 10 -> 14 -> 16`), and `otelcol_process_uptime` ($U_1 < U_2 < U_3 < U_4$) remain strictly monotonic across `LogFilterPolicy` and `SourcePolicy` (Case B1 & Case A) rollouts, while Case B2 falsifies partial reload when pipeline keys change.

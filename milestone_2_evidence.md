# Milestone 2 Evidence: End-to-End `otelcol.Collector` Verification & Baseline vs. Prototype Comparison

## 1. Summary of Changes

- **File Added**: `/tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/reload_continuity_test.go` (`1033` lines)
- **Tests Implemented**:
  1. `TestReloadContinuity_Baseline_ResetsCountersAndUptime`:
     - Runs a real in-process `otelcol.Collector` (`col.Run(ctx)`) with `BuiltInSelfMetricsPolicy` (`selfmetrics_policy.go`), `googlepolicyprocessor`, `otlpreceiver`, an OTLP log source policy (`otlp/source_a`), and a capture destination policy (`capture/test_dest`).
     - Configures baseline behavior (`p.diffGateReloads = false`, `service.partialReload = false`, `service.partialReloadReceivers = false`).
     - Demonstrates that both a transformation-only `LogFilterPolicy` rollout (Step 1) and a `SourcePolicy` update (Step 2) trigger full `Service` restarts (`"Config updated, restart service"`), recreating `srv.meterProvider` and resetting `otelcol_exporter_sent_log_records`, `otelcol_receiver_accepted_log_records`, and `otelcol_process_uptime` (`StartTimestamp` advances and cumulative counters drop back to post-reload batch counts).
  2. `TestReloadContinuity_Prototype_PreservesCountersAndUptime`:
     - Runs the same end-to-end `otelcol.Collector` with `googlecontrolplaneprovider.NewFactory()` (`diffGateReloads = true`, `service.partialReload = true`, `service.partialReloadReceivers = true`, `confmap.enableMergeAppendOption = true`).
     - **Step 0 (Initial)**: Sends 5 logs through `otlp/source_a` -> `otelcol_exporter_sent_log_records = 5`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"} = 5`, `otelcol_process_uptime = 0.186s` (`StartTimestamp = 1791920224021890211`).
     - **Step 1 (`LogFilterPolicy` Rollout)**: Hot-reloads `LogFilterPolicy` (`"DROP_ME"`) and sends 5 logs (`3` kept, `2` dropped). `googlepolicyprocessor` hot-swaps its `Evaluator` in memory while `googlecontrolplaneprovider` suppresses `WatcherFunc` (`reloads = 0`). `otelcol_exporter_sent_log_records` advances monotonically from `5 -> 8`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` advances from `5 -> 10`, and `otelcol_process_uptime` increases monotonically (`0.186s -> 0.286s`) with an unchanged `StartTimestamp`.
     - **Step 2 (`SourcePolicy` Case B1 — Add `otlp/source_b` to Shared Pipeline `service::pipelines::logs`)**: Adds `otlp/source_b` wired into the existing shared `logs` pipeline via `confmap.enableMergeAppendOption`. Upstream `receiversOnlyChanged` (`otelcol/config_diff.go:130-191`) returns `true` and executes `Service.UpdateReceivers` (`"Partial receiver reload completed successfully"`). Sending 4 logs to `otlp/source_a` and 6 logs to `otlp/source_b` advances `otelcol_exporter_sent_log_records` monotonically from `8 -> 18`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` from `10 -> 14`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` to `6`, and `otelcol_process_uptime` to `0.486s` with an unchanged `StartTimestamp`.
     - **Step 3 (`SourcePolicy` Case A — Modify `otlp/source_b` Endpoint Port In Place)**: Updates `otlp/source_b` port in place while leaving `otlp/source_a` untouched. `Service.UpdateReceivers` stops and restarts only `otlp/source_b` (`partial_reloads = 2, full_restarts = 0`). Sending 2 logs to `otlp/source_a` and 3 logs to `otlp/source_b` advances `otelcol_exporter_sent_log_records` from `18 -> 23`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` from `14 -> 16`, `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` from **`6 -> 9`** (because `srv.meterProvider` is preserved across `Service.UpdateReceivers`, the OTel Go SDK `MeterProvider` returns the same underlying synchronous `Int64Counter` instrument and preserves cumulative state for `{receiver="otlp/source_b", transport="grpc"}`!), and `otelcol_process_uptime` to `0.686s` with an unchanged `StartTimestamp`.
     - **Step 4 (`SourcePolicy` Case B2 — Add `otlp/source_c` Introducing a New Pipeline Key `service::pipelines::logs/source_c`)**: Empirically falsifies partial reload when a `SourcePolicy` introduces a new pipeline key (`logs/source_c`). `receiversOnlyChanged` (`otelcol/config_diff.go:161`) checks `len(old.pipelines) != len(cur.pipelines)`, returns `false`, and falls back to a full service reload (`"Config updated, restart service"`), resetting `otelcol_exporter_sent_log_records` from `23` to `2` and resetting `otelcol_process_uptime` (`StartTimestamp` advances from `1791920224021890211` to `1791920225100983537`).
  3. `TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown`:
     - Empirically proves a newly uncovered P0 architectural interaction between `BuiltInSelfMetricsPolicy` (`selfmetrics_policy.go:157-173`) and upstream `service.Service.Shutdown(ctx)` (`service@v0.162.0/service.go:289-322`):
       - `BuiltInSelfMetricsPolicy` configures `service.telemetry.metrics.readers[0].periodic` to push OTLP gRPC metrics over loopback (`http://localhost:<port>`) to `receivers.otlp/default_self_metrics` inside the same `Service`'s pipeline graph.
       - During any full collector reload (`otelcol.Collector.reloadConfiguration`, `collector.go:313`), `Service.Shutdown(ctx)` executes `srv.host.Pipelines.ShutdownAll` (`service.go:300`, stopping `otlp/default_self_metrics`) **before** `srv.meterProvider.Shutdown(ctx)` (`service.go:315`).
       - Consequently, on an unproxied `BuiltInSelfMetricsPolicy` loopback topology, `meterProvider.Shutdown(ctx)` fails its retiring flush with `connect: connection refused`, and `reloadConfiguration` (`collector.go:313-315`) treats that error as fatal, terminating `col.Run(ctx)` instead of starting the new config!

---

## 2. Empirical Comparison Table (Captured from `task-206` / `task-213`)

| Step / Event | Metric / Signal | Baseline (`diffGateReloads=false`, Gates Disabled) | Prototype (`diffGateReloads=true`, Gates Enabled) |
| :--- | :--- | :--- | :--- |
| **Step 0: Initial (`5` logs to `otlp/source_a`)** | `otelcol_exporter_sent_log_records` | `5` (`start=1791920222799226432`) | `5` (`start=1791920224027006342`) |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` | `5` | `5` |
| | `otelcol_process_uptime` | `0.288s` (`start=1791920222795920879`) | `0.186s` (`start=1791920224021890211`) |
| **Step 1: `LogFilterPolicy` Rollout (`3` kept, `2` dropped to `otlp/source_a`)** | Collector Reload Action | Full `Service` restart (`full_restarts=1`) | Suppressed (`reloads=0`) |
| | `otelcol_exporter_sent_log_records` | **`3` (RESET from `5`; new `start=1791920223365511108`)** | **`8` (`5 -> 8` monotonic; `start` unchanged)** |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` | **`5` (RESET from `5`; expected `10`)** | **`10` (`5 -> 10` monotonic; `start` unchanged)** |
| | `otelcol_process_uptime` | **`0.083s` (RESET; new `start=1791920223362006425`)** | **`0.286s` (`> 0.186s` monotonic; `start` unchanged)** |
| **Step 2: `SourcePolicy` Case B1 (Add `otlp/source_b` to Shared `logs` Pipeline; `4` logs to `source_a`, `6` logs to `source_b`)** | Collector Reload Action | *(In Baseline Step 2, `SourcePolicy` update triggered `full_restarts=2`, resetting `exporter_sent` to `2` and `uptime` to `0.086s`)* | Partial receiver reload (`partial_reloads=1, full_restarts=0`) |
| | `otelcol_exporter_sent_log_records` | **`2` (RESET; new `start=1791920223692511001`)** | **`18` (`8 -> 18` monotonic; `start` unchanged)** |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` | Reset on full restart | **`14` (`10 -> 14` monotonic; `start` unchanged)** |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` | N/A | **`6`** |
| | `otelcol_process_uptime` | **`0.086s` (RESET; new `start=1791920223689490132`)** | **`0.486s` (`> 0.286s` monotonic; `start` unchanged)** |
| **Step 3: `SourcePolicy` Case A (Modify `otlp/source_b` Port In Place; `2` logs to `source_a`, `3` logs to `source_b`)** | Collector Reload Action | Would trigger full `Service` restart | Partial receiver reload (`partial_reloads=2, full_restarts=0`) |
| | `otelcol_exporter_sent_log_records` | Reset on full restart | **`23` (`18 -> 23` monotonic; `start` unchanged)** |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_a"}` | Reset on full restart | **`16` (`14 -> 16` monotonic; `start` unchanged)** |
| | `otelcol_receiver_accepted_log_records{receiver="otlp/source_b"}` | Reset on full restart | **`9` (`6 -> 9` monotonic via preserved `MeterProvider`!)** |
| | `otelcol_process_uptime` | Reset on full restart | **`0.686s` (`> 0.486s` monotonic; `start` unchanged)** |
| **Step 4: `SourcePolicy` Case B2 (Add `otlp/source_c` with New Pipeline Key `logs/source_c`; `2` logs to `source_c`)** | Collector Reload Action | Full `Service` restart | Full `Service` restart fallback (`full_restarts=1`, `len(old.pipelines) != len(cur.pipelines)`) |
| | `otelcol_exporter_sent_log_records` | Reset | **`2` (RESET from `23`; new `start=1791920225105046342`)** |
| | `otelcol_process_uptime` | Reset | **`0.083s` (RESET; new `start=1791920225100983537`)** |

---

## 3. Verbatim Command & Test Output (`go test -v -race ./...`)

```text
$ cd /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -race -run "TestReloadContinuity|TestSelfMetricsLoopback" .
=== RUN   TestReloadContinuity_Baseline_ResetsCountersAndUptime
    reload_continuity_test.go:580: [Baseline Step 0 - Initial] exporter_sent=5 (start=1791920222799226432), receiver_accepted(source_a)=5, process_uptime=0.288s (start=1791920222795920879)
    reload_continuity_test.go:619: [Baseline Step 1 - After LogFilterPolicy Rollout] full_restarts=1, exporter_sent=3 (RESET from 5 to 3; expected 8; start=1791920223365511108 > 1791920222799226432), receiver_accepted(source_a)=5 (RESET from 5 to 5; expected 10), process_uptime=0.083s (RESET start=1791920223362006425 > 1791920222795920879)
    reload_continuity_test.go:655: [Baseline Step 2 - After SourcePolicy Rollout] full_restarts=2, exporter_sent=2 (RESET from 3 to 2; expected 10; start=1791920223692511001 > 1791920223365511108), process_uptime=0.086s (RESET start=1791920223689490132 > 1791920223362006425)
--- PASS: TestReloadContinuity_Baseline_ResetsCountersAndUptime (1.21s)
=== RUN   TestReloadContinuity_Prototype_PreservesCountersAndUptime
    reload_continuity_test.go:754: [Prototype Step 0 - Initial] exporter_sent=5 (start=1791920224027006342), receiver_accepted(source_a)=5, process_uptime=0.186s (start=1791920224021890211)
    reload_continuity_test.go:795: [Prototype Step 1 - After LogFilterPolicy Rollout] reloads=0, exporter_sent=8 (5->8 monotonic, start=1791920224027006342), receiver_accepted(source_a)=10 (5->10 monotonic), process_uptime=0.286s (> 0.186s, start=1791920224021890211)
    reload_continuity_test.go:841: [Prototype Step 2 - Case B1 Add Receiver to Shared Pipeline] partial_reloads=1, full_restarts=0, exporter_sent=18 (8->18 monotonic), receiver_accepted(source_a)=14 (10->14 monotonic), receiver_accepted(source_b)=6, process_uptime=0.486s (> 0.286s, start=1791920224021890211)
    reload_continuity_test.go:886: [Prototype Step 3 - Case A Modify Receiver In Place] partial_reloads=2, full_restarts=0, exporter_sent=23 (18->23 monotonic), receiver_accepted(source_a)=16 (14->16 monotonic), receiver_accepted(source_b)=9 (6->9 monotonic via preserved MeterProvider!), process_uptime=0.686s (> 0.486s, start=1791920224021890211)
    reload_continuity_test.go:924: [Prototype Step 4 - Case B2 New Pipeline Key (logs/source_c)] full_restarts=1 (config_diff.go:161 len(old.pipelines)!=len(cur.pipelines)), exporter_sent=2 (RESET from 23 to 2, start=1791920225105046342 > 1791920224027006342), process_uptime=0.083s (RESET start=1791920225100983537 > 1791920224021890211)
--- PASS: TestReloadContinuity_Prototype_PreservesCountersAndUptime (1.41s)
=== RUN   TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown
    reload_continuity_test.go:1027: [Direct SelfMetrics Loopback Full Reload Failure] col.Run exited fatally during reloadConfiguration: failed to shutdown the retiring config: failed to shutdown meter provider: failed to upload metrics: reader shutdown timeout: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:37401: connect: connection refused"
--- PASS: TestSelfMetricsLoopback_DirectFullReloadFailsOnShutdown (0.45s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	4.432s
```

Full package suite (`go test -v -race ./...` in `googlecontrolplaneprovider`):
- `ok github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider 16.477s`
- `ok github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics 1.353s`

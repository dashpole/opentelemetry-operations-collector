# Milestone 1 Evidence: Diff-Gated `confmap.WatcherFunc` & Partial Reload Feature Gates in `googlecontrolplaneprovider`

## 1. Deliverable Summary

Implemented Milestone 1 in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/`:
1. **Upstream Partial Reload Feature Gates Enabled in `newProvider` (`provider.go:41-47, 108-127`)**:
   - Enabled `service.partialReload` (`partialReloadFeatureGateID`) and `service.partialReloadReceivers` (`partialReloadReceiversGateID`) alongside `confmap.enableMergeAppendOption` when `googlecontrolplaneprovider.NewFactory().Create(...)` constructs the provider.
2. **Diff-Gated `confmap.WatcherFunc` & Single-Evaluation Pending Cache (`provider.go:84-102, 244-409, 726-733`)**:
   - Wired `Retrieve(ctx, uri, watcher)` (`provider.go:211, 242`) to `retrieveAndWatch(ctx, watcher)`.
   - When `watcher != nil`, `startPolicyWatcher` subscribes to `googlepolicy.RegisterWatcherChannel()` using `context.WithoutCancel(ctx)` and re-evaluates `evaluateActivePolicySet(watchCtx)` on every `PolicySet` change.
   - Compares the newly synthesized `confmap.Conf.ToStringMap()` (`newMap`) against `p.lastConfMap` via `reflect.DeepEqual`. When `p.diffGateReloads` is `true` (default) and `newMap` is identical to `p.lastConfMap` (e.g. when only `PolicyClassTransformation` policies such as `LogFilterPolicy` changed, or when an invalid `PolicySet` rolled back to the previous revision), `watcher(&confmap.ChangeEvent{})` is **suppressed** (`0` Collector reloads).
   - When `newMap` differs from `p.lastConfMap` (e.g. when a `SourcePolicy` or `DestinationPolicy` changes), caches `p.pendingConfMap` (`p.pendingRevID`, `p.hasPending = true`) and invokes `watcher(&confmap.ChangeEvent{})` once for that `Retrieve` generation.
   - On the subsequent `Resolver.Resolve(ctx)` -> `Retrieve(ctx, uri, watcher)` call, consumes `p.pendingConfMap` if `p.pendingRevID == googlepolicy.ActivePolicySetRevisionID()` (avoiding duplicate policy evaluation or duplicate `PolicyEvaluateError` events) and starts a fresh watcher goroutine wrapped with `confmap.WithRetrievedClose`.
   - Both `confmap.Retrieved.Close` and `provider.Shutdown` cleanly stop the watcher goroutine and unregister the `googlepolicy.WatcherChannel` (verified by `goleak.VerifyTestMain` in `generated_package_test.go`).

---

## 2. Exact Verification Command & Output

Command executed:
```bash
go -C /tmp/prototypes/tab3_reload_continuity/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider test -v -race -run "TestRetrieve|TestWatcher|TestPartialReload" ./...
```

Output:
```text
=== RUN   TestRetrieve
--- PASS: TestRetrieve (0.14s)
=== RUN   TestRetrieve_NoFleetID
--- PASS: TestRetrieve_NoFleetID (0.04s)
=== RUN   TestRetrieve_XDSServerUnreachable
--- PASS: TestRetrieve_XDSServerUnreachable (5.12s)
=== RUN   TestRetrieve_XDSNumericProjectDoesNotClobberProjectID
--- PASS: TestRetrieve_XDSNumericProjectDoesNotClobberProjectID (5.11s)
=== RUN   TestRetrieve_MultipleDestinationPolicies
--- PASS: TestRetrieve_MultipleDestinationPolicies (0.12s)
=== RUN   TestRetrieve_ActivePolicySetWithCustomDestination
--- PASS: TestRetrieve_ActivePolicySetWithCustomDestination (0.07s)
=== RUN   TestRetrieve_ActivePolicySetWithCustomSelfMetrics
--- PASS: TestRetrieve_ActivePolicySetWithCustomSelfMetrics (0.12s)
=== RUN   TestRetrieve_ActivePolicySetWithOtherSource
--- PASS: TestRetrieve_ActivePolicySetWithOtherSource (0.13s)
=== RUN   TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure
--- PASS: TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure (0.12s)
=== RUN   TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet
--- PASS: TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet (0.04s)
=== RUN   TestPartialReloadFeatureGatesEnabled
--- PASS: TestPartialReloadFeatureGatesEnabled (0.00s)
=== RUN   TestWatcher_DiffGatedVsBaseline
=== RUN   TestWatcher_DiffGatedVsBaseline/baseline_unconditional_watcher_fires_on_log_filter_only
=== RUN   TestWatcher_DiffGatedVsBaseline/prototype_diff_gated_watcher_suppresses_filter_only_and_fires_on_source
--- PASS: TestWatcher_DiffGatedVsBaseline (1.03s)
    --- PASS: TestWatcher_DiffGatedVsBaseline/baseline_unconditional_watcher_fires_on_log_filter_only (0.23s)
    --- PASS: TestWatcher_DiffGatedVsBaseline/prototype_diff_gated_watcher_suppresses_filter_only_and_fires_on_source (0.81s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	13.427s
```

---

## 3. Baseline-vs-Change Proof

In `TestWatcher_DiffGatedVsBaseline` (`provider_test.go:650-817`):
- **Baseline (`diffGateReloads = false`)**:
  - Calling `googlepolicy.SetActivePolicySet` with a `LogFilterPolicy`-only revision (`rev-baseline-filter-1`) immediately invokes `watcher(&confmap.ChangeEvent{})` (`watcherCalls == 1`), even though the synthesized `confmap` is identical.
- **Prototype (`diffGateReloads = true`, default)**:
  - Calling `googlepolicy.SetActivePolicySet` with `rev-proto-filter-1` (`LogFilterPolicy`), `rev-proto-filter-2` (2 `LogFilterPolicy` entries), and `rev-proto-invalid-dest` (invalid 2-destination `PolicySet` that rolls back to `rev-proto-filter-2`) all produce `watcherCalls == 0` (`Synthesized confmap unchanged after policy set update; skipping collector reload`).
  - Calling `googlepolicy.SetActivePolicySet` with `rev-proto-source-1` (adding `mockSourcePolicy` `source_a`) changes the synthesized `confmap` (`receivers::otlp/source_a` and `service::pipelines::logs/source_a`) and fires `watcher` (`watcherCalls == 1`).
  - After closing `ret1` and calling `p.Retrieve` again, `ret2` returns the new `confmap` without double-evaluating `failing_src` (`len(evalErrLogs) == 1`), and a subsequent `LogFilterPolicy` update (`rev-proto-source-1-plus-filter-3`) is again suppressed (`watcherCalls == 1`).

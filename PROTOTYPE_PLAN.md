# Prototype Plan: Integrating Custom Local `config.yaml` Receivers with Control-Plane Policies in GBOC (Tab 7)

**Author**: dashpole@google.com  
**Target Design Doc**: `tango_onepager_7_custom_config_coexistence.md` (*Integrating Custom Local `config.yaml` Receivers with Control-Plane Policies in GBOC*)  
**Repository & Branch**: `GoogleCloudPlatform/opentelemetry-operations-collector` (`proto/tango-tab7-custom-config-coexistence`, based on `v0.162.0`)  
**Target Packages**:
- `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider`
- `integration_test/policy_test`

---

## 1. Core Question & Hypothesis

Can GBOC (`otelcol-google` / `googlecontrolplaneprovider`) support zero-boilerplate coexistence between remote Control-Plane policies (`LogFilterPolicy`, `MetricFilterPolicy`, `TraceFilterPolicy`, `GCPDestinationPolicy`) and local `/etc/otelcol-google/config.yaml` custom receivers (such as `sqlqueryreceiver` for Symantec Endpoint Protection / MS SQL and Oracle Unified Audit Trail in Allianz Findings `#32`, `#33`, `#40`, `#71`, `#72`) by always synthesizing **canonical shared workload pipelines** (`logs/googlecontrolplane`, `metrics/googlecontrolplane`, `traces/googlecontrolplane`) pre-wired with `receivers: [nop/googlecontrolplane]` and merged via `confmap.enableMergeAppendOption`—replacing the broken `${googlecontrolplane:component://...}` URI expansion design in `go/gboc-learns-to-tango`?

Specifically, we test two falsifiable hypotheses:
1. **Why `component://` Fails (`Alt 1` in One-Pager #7)**:
   - **Failure 1a (`v0.162.0` Stub Behavior)**: At `v0.162.0`, `case innerSchemeComponent:` in `provider.go:210` is an unimplemented stub that falls through to `p.evaluateActivePolicySet(ctx)` and dumps the entire root collector configuration map into `processors:`, failing `otelcol.Config` unmarshaling.
   - **Failure 1b (`confmap` Sequence-Splicing Blocker)**: Even if `component://active_destination_log_preprocess` is implemented to return a slice of multiple processor IDs `[]any{"batch/gcp_bytes", "batch/gcp_items"}` (e.g., Two-Stage Batch Chaining from One-Pager #1 or `resourcedetection` + `queue_batch`), `confmap.Resolver` expands `- ${googlecontrolplane:component://active_destination_log_preprocess}` inside `processors:` into a nested slice `["googlepolicy", ["batch/gcp_bytes", "batch/gcp_items"]]`, which fails `otelcol.Config` unmarshaling (`expected a string, got 'slice'`).
   - **Failure 1c (Per-Pipeline Processor & Queue Duplication)**: In OTel Collector's `service/internal/graph`, processors are instantiated once per pipeline. Defining a separate `logs/custom_sql` pipeline in `config.yaml` alongside a remote `SourcePolicy` pipeline instantiates duplicate `googlepolicy` and `queue_batch` instances in memory.
2. **Why Canonical Workload Pipelines + `confmap.enableMergeAppendOption` Succeeds (`Proposed Design` in One-Pager #7)**:
   - Synthesizing `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` pre-wired with `receivers: [nop/googlecontrolplane]`, `processors: [googlepolicy, ...destPreProcessIDs]`, and `exporters: [otlp_grpc/default_gcp_destination]` allows any local `file:/etc/otelcol-google/config.yaml` to attach custom receivers (`receivers: [sqlquery/sep]`) with a single line.
   - `confmap.enableMergeAppendOption` appends and deduplicates receivers across `googlecontrolplane:` and `file:` providers while preserving `processors:` and `exporters:`, creating **exactly 1** `googlepolicy` instance and **exactly 1** `queue_batch/default_gcp_destination_batch_logs` instance in the collector graph, and keeping `logs/default_self_metrics` and `metrics/default_self_metrics` isolated from user transformation policies.

---

## 2. Scope, Environment, & Intentional Simplifications

### In-Scope
- Empirical unit and integration tests in `components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider` reproducing Failures 1a, 1b, and 1c of `component://`.
- Modifying `provider.go` to:
  1. Remove `innerSchemeComponent` (`component://`) from all three locations in `provider.go` (`provider.go:75` constant declaration, `provider.go:180` `if target.Scheme != innerSchemeComponent && p.manager != nil` guard, and `provider.go:210` `case innerSchemeComponent:` switch branch) so `googlecontrolplane:component://...` fails fast with `ErrURIInvalidInnerScheme`.
  2. Always synthesize `receivers.nop/googlecontrolplane` and the three canonical workload pipelines (`logs/googlecontrolplane`, `metrics/googlecontrolplane`, `traces/googlecontrolplane`) via `synthesizeCanonicalWorkloadPipelines` in `evaluatePolicySet`, pre-wired with `receivers: [nop/googlecontrolplane]`, `processors: [googlepolicy, ...destPreProcessIDs]`, and `exporters: destPolicy.ExporterIDs()`.
  3. In `evaluateSingleSourcePolicy` (`provider.go:464-492`), when `sp.PolicyType() != selfmetrics.PolicyType`, normalize any workload `SourcePolicy` pipeline keys (`logs/*`, `metrics/*`, `traces/*`) onto the canonical pipeline IDs (`logs/googlecontrolplane`, `metrics/googlecontrolplane`, `traces/googlecontrolplane`) before merging, and update `mockSourcePolicy` in `provider_test.go:170` accordingly, while preserving `logs/<self_metrics_name>` and `metrics/<self_metrics_name>` for `self_metrics` policies.
  4. Update golden test fixture `testdata/policysets/builtin/result.yaml`, unit tests in `provider_test.go`, and `testCollectorFactories()` in `integration_test/policy_test/policy_test.go` (adding `nopreceiver.NewFactory()` and `queuebatchprocessor.NewFactory()`) so both `googlecontrolplaneprovider` and `integration_test/policy_test` remain 100% green.
- End-to-end runtime verification with a multi-provider `confmap.Resolver` (`uris: ["googlecontrolplane:file://...", "file:/tmp/.../custom_config.yaml"]`), real OTel Collector `otelcol.NewCollector` / `service.New` graph construction, `googlepolicyprocessor` with an active `LogFilterPolicy` (`ACTION_DROP` on `DEBUG` severity logs), receiver-specific pre-processing via the `forward` connector pattern, and live OTLP/gRPC export to `telemetry.googleapis.com:443` in GCP project `dashpole-dev` verified via Cloud Logging (`gcloud logging read` / `entries:list`).
- Updating `tango_onepager_7_custom_config_coexistence.md` (`Upfront Trade-Offs` #2 and distribution spec notes) in Phase 3 to reflect that `nop/googlecontrolplane` must be emitted unconditionally on `{logs,metrics,traces}/googlecontrolplane` (since omitting a canonical pipeline when zero remote `SourcePolicy` instances exist causes a 1-line `config.yaml` `receivers: [sqlquery/sep]` attachment to fail `otelcol.Config.Validate()` with missing exporters).

### Intentional PoC Boundaries (Documented for Productionization Gap Ledger)
- **Distro Spec Wiring (`specs/google-built-opentelemetry-collector.yaml`)**: Inspecting `specs/google-built-opentelemetry-collector.yaml` at `v0.162.0` reveals that `nop` is currently listed only under `exporters:` (not `receivers:`), and `sqlquery` is listed in `specs/otelopscol.yaml` and `cmd/distrogen/registry.yaml` but not yet in `specs/google-built-opentelemetry-collector.yaml`. Rather than regenerating the entire `google-built-opentelemetry-collector/generated_collector` OCB directory in this prototype branch, our Milestone 2 collector harness registers the complete required factory set (`otlpreceiver`, `nopreceiver`, `otelconftelemetry`, `googlecontrolplaneextension`, `googleclientauthextension`, `googlepolicyprocessor`, `queuebatchprocessor`, `otlpexporter`, `forwardconnector`, passthrough `resourcedetection` and `transform` processors for `BuiltInSelfMetricsPolicy`, and a local custom `sqlquery` log receiver) with an ephemeral port override for `otlp/default_self_metrics` so sequential collector runs never collide on `:18888`. We record the required `specs/google-built-opentelemetry-collector.yaml` additions in `PROTOTYPE_LEARNINGS.md` and `tango_onepager_7_custom_config_coexistence.md`.

---

## 3. Symmetric Candidate Evaluation Rubric

Both **Approach A (`component://` URI Expansion)** and **Approach B (Canonical `*/googlecontrolplane` Pipelines + `confmap.enableMergeAppendOption`)** are evaluated against the same multi-provider `confmap.Resolver` + `otelcol.Config` unmarshaling + `service.New` pipeline graph harness:

| Dimension | Approach A (`component://` URI Expansion) | Approach B (Canonical `*/googlecontrolplane` Pipelines) |
| :--- | :--- | :--- |
| **Unmarshals with Multi-Processor Destination Chain (`PreProcessLogIDs() > 1`)** | Must fail (`[]component.ID` cannot unmarshal nested `[]any` slice inside `processors:`) | Must succeed (`processors:` is a flat slice on `logs/googlecontrolplane` untouched by `config.yaml`) |
| **Processor Instance Count (`googlepolicy`, `queue_batch`) with 1 Remote Source + 1 Local Receiver** | 2 instances of each (1 in `logs/remote_source`, 1 in `logs/custom_sql`) | 1 shared instance of each (in `logs/googlecontrolplane`) |
| **Valid Collector Startup with 0 Remote `SourcePolicy` Instances** | N/A | Must succeed (`nop/googlecontrolplane` satisfies `len(cfg.Receivers) > 0` on all 3 canonical pipelines) |
| **Self-Metrics Isolation (`logs/default_self_metrics`, `metrics/default_self_metrics`)** | Isolated | Isolated (never includes `googlepolicy`) |

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Empirical Proof of `component://` Failures + Canonical Workload Pipeline Implementation
- **Deliverables**:
  1. Baseline failure reproduction test (`TestBaselineComponentSchemeFailures` in `coexistence_test.go`) proving:
     - **Failure 1a**: At `v0.162.0`, `googlecontrolplane:component://global_policy_processor` returns the full root config map (`exporters`, `extensions`, `processors`, `receivers`, `service`), causing `otelcol.Config` unmarshaling of `service.pipelines.logs/custom_sql.processors[0]` to fail with a map-to-string type error.
     - **Failure 1b**: Even with a hypothetical `component://active_destination_log_preprocess` provider returning `[]any{"batch/gcp_bytes", "batch/gcp_items"}`, `confmap.Resolver` expands `processors: [googlepolicy, ${mockcomponent:active_destination_log_preprocess}]` into `["googlepolicy", ["batch/gcp_bytes", "batch/gcp_items"]]`, failing `otelcol.Config` unmarshaling.
     - **Failure 1c**: Separate per-source pipelines (`logs/policy_source` + `logs/custom_sql`) instantiate 2 separate `googlepolicy` processors and 2 separate `queue_batch` processors when building the collector pipeline graph.
  2. Implementation in `provider.go`, `provider_test.go`, `testdata/policysets/builtin/result.yaml`, and `integration_test/policy_test/policy_test.go`:
     - Remove `innerSchemeComponent` from `provider.go:75`, `180`, and `210` and verify `googlecontrolplane:component://...` returns `ErrURIInvalidInnerScheme`.
     - Synthesize `receivers.nop/googlecontrolplane` and canonical workload pipelines `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` in `evaluatePolicySet`.
     - Normalize non-`self_metrics` `SourcePolicy` pipeline outputs in `evaluateSingleSourcePolicy` onto `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` (and update `mockSourcePolicy` in `provider_test.go`).
     - Update `testCollectorFactories()` in `integration_test/policy_test/policy_test.go` to register `newStubReceiverFactory("nop")` and `WithLogs`/`WithMetrics`/`WithTraces` builders on `newStubProcessorFactory` (for `queue_batch`).
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -run "TestRetrieve|TestPolicySets|TestBaselineComponentSchemeFailures|TestCanonicalWorkloadPipelines" ./... && cd /tmp/prototypes/tab7_custom_config_coexistence/integration_test/policy_test && go test -v -run "TestPolicyIntegration/drop_by_body_equals" ./...
  ```
- **Baseline-vs-Change Proof**:
  - Before our `provider.go` change: `googlecontrolplane:component:my-config` succeeds by dumping the root config map, and `service::pipelines::logs/googlecontrolplane` does not exist in `testdata/policysets/builtin/result.yaml` (so merging a local `config.yaml` that sets only `service.pipelines.logs/googlecontrolplane.receivers: [sqlquery/sep]` fails `otelcol.Config.Validate()` with `pipeline "logs/googlecontrolplane" must have at least one exporter`).
  - After our `provider.go` change: `googlecontrolplane:component:my-config` fails with `ErrURIInvalidInnerScheme`, `testdata/policysets/builtin/result.yaml` contains `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` wired with `nop/googlecontrolplane`, `googlepolicy`, `queue_batch/default_gcp_destination_batch_*`, and `otlp_grpc/default_gcp_destination`, and merging a 1-line `receivers: [sqlquery/sep]` produces a valid `otelcol.Config`.

### Milestone 2: End-to-End Multi-Provider Coexistence Verification with `confmap.Resolver`, `googlepolicyprocessor`, `forward` Connector, and Live `telemetry.googleapis.com:443` in `dashpole-dev`
- **Deliverables**:
  1. End-to-end integration test (`TestEndToEndCustomConfigCoexistence_LiveGCP` and `TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline` in `coexistence_test.go`) executing a multi-provider `confmap.Resolver` (`uris: ["googlecontrolplane:file://...", "file:/tmp/.../custom_config.yaml"]`) with the full collector factory set (`otlpreceiver`, `nopreceiver`, `otelconftelemetry`, `googlecontrolplaneextension`, `googleclientauthextension`, `googlepolicyprocessor`, `queuebatchprocessor`, `otlpexporter`, `forwardconnector`, passthrough `resourcedetection`/`transform` processors, and ephemeral `otlp/default_self_metrics` port override) where:
     - `googlecontrolplane:file://...` loads a `LogFilterPolicy` (`drop-debug-logs`, dropping logs with `severity_text == "DEBUG"`) and an optional remote workload `SourcePolicy`.
     - `file:/tmp/.../custom_config.yaml` defines a custom SQL-poll log receiver (`sqlquery/sep` simulated via a local receiver factory emitting 2 SEP log records: 1 `DEBUG` watermark/heartbeat log and 1 `INFO` security alert log with a unique test run UUID) attached via `service.pipelines.logs/googlecontrolplane.receivers: [sqlquery/sep]`.
  2. Empirical verification of all four coexistence invariants:
     - **Invariant 1 (Merge & Deduplication)**: `confmap.Resolver.Resolve(ctx)` merges and deduplicates `logs/googlecontrolplane.receivers` (`[nop/googlecontrolplane, ..., sqlquery/sep]`) while preserving `processors: [googlepolicy, queue_batch/default_gcp_destination_batch_logs]` and `exporters: [otlp_grpc/default_gcp_destination]`.
     - **Invariant 2 (Single Shared Processor & Queue Instance)**: Counting `CreateLogs` calls on wrapped `googlepolicyprocessor` and `queuebatchprocessor` factories during collector startup confirms **exactly 1** `googlepolicy` logs processor instance and **exactly 1** workload `queue_batch/default_gcp_destination_batch_logs` instance (`2` total `queue_batch` logs processor instances: `1` in `logs/googlecontrolplane` + `1` in `logs/default_self_metrics`) are created (even when both a remote `SourcePolicy` receiver and a local `config.yaml` receiver are active).
     - **Invariant 3 (Receiver-Specific Pre-Processing via `forward` Connector)**: When a customer needs a custom processor to run *only* on `sqlquery/sep` before entering `logs/googlecontrolplane`, configuring `logs/sep_pre` $\rightarrow$ `forward/sep` $\rightarrow$ `logs/googlecontrolplane.receivers: [forward/sep]` mutates only `sqlquery/sep` logs while still routing them through the single shared `googlepolicy` processor.
     - **Invariant 4 (Live `telemetry.googleapis.com:443` Export in `dashpole-dev` + Negative Control)**:
       - With `LogFilterPolicy` active (`Change`): the collector exports `sqlquery/sep` logs to live `telemetry.googleapis.com:443` in `dashpole-dev`; querying Cloud Logging (`gcloud logging read` / `entries:list`) by the unique run UUID confirms the `INFO` SEP log arrived in `dashpole-dev` while the `DEBUG` SEP log was dropped (`count(INFO) == 1`, `count(DEBUG) == 0`).
       - Without `LogFilterPolicy` (`Baseline / Negative Control`): both `INFO` and `DEBUG` logs pass through and arrive in Cloud Logging in `dashpole-dev` (`count(INFO) == 1`, `count(DEBUG) == 1`).
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -run "TestEndToEndCustomConfigCoexistence" ./...
  ```

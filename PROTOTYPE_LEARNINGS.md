# Prototype Learnings: Integrating Custom Local `config.yaml` Receivers with Control-Plane Policies in GBOC (Tab 7)

**Author**: dashpole@google.com  
**Target Design Doc**: [`tango_onepager_7_custom_config_coexistence.md`](file:///usr/local/google/home/dashpole/.gemini/jetski/brain/1ff2e532-7fd7-4ef1-b7e4-6532e6080076/tango_onepager_7_custom_config_coexistence.md)  
**Prototype Branch**: `proto/tango-tab7-custom-config-coexistence` (pushed to `fork`: `git@github.com:dashpole/opentelemetry-operations-collector.git`, based on `v0.162.0`)  
**Evidence Artifacts**:
- [`milestone_1_evidence.md`](file:///tmp/prototypes/tab7_custom_config_coexistence/milestone_1_evidence.md)
- [`milestone_2_evidence.md`](file:///tmp/prototypes/tab7_custom_config_coexistence/milestone_2_evidence.md)

---

## 1. Executive Summary & Decision Verdict

**Verdict**: **Adopt Canonical Workload Pipelines (`{logs,metrics,traces}/googlecontrolplane` + `nop/googlecontrolplane` + `confmap.enableMergeAppendOption`) and remove `innerSchemeComponent` (`component://`) from `googlecontrolplaneprovider`.**

Our prototype empirically falsified the `${googlecontrolplane:component://...}` URI expansion escape-hatch design from `go/gboc-learns-to-tango` across three independent failure modes and validated the Canonical Workload Pipelines replacement end-to-end against a real OpenTelemetry Collector pipeline graph and live Cloud Logging (`telemetry.googleapis.com:443`) in GCP project `dashpole-dev`.

---

## 2. What Worked vs. What Surprised Us

### What Worked as Hypothesized
1. **Empirical Proof of All Three `component://` Architectural Blockers (`TestBaselineComponentSchemeFailures` in [`coexistence_test.go:374-587`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/coexistence_test.go#L374-L587))**:
   - **Failure 1a (`v0.162.0` Stub Behavior)**: In `v0.162.0`, `case innerSchemeComponent:` in `provider.go` delegated directly to `p.evaluateActivePolicySet(ctx)`, dumping the entire root collector configuration map into `service.pipelines[logs/custom_sql].processors[0]` and failing `otelcol.ConfigProvider.Get` unmarshaling with `'service.pipelines[logs/custom_sql].processors[0]' has invalid keys: exporters, extensions, processors, receivers, service`. After removing `innerSchemeComponent`, `googlecontrolplane:component://...` fails fast at `confmap.Resolver.Resolve` with `ErrURIInvalidInnerScheme` (`"component://global_policy_processor": inner scheme is unsupported: component`).
   - **Failure 1b (`confmap` Sequence-Splicing Blocker)**: When a `component://active_destination_log_preprocess` provider returns a multi-processor slice (`[]any{"batch/gcp_bytes", "batch/gcp_items"}`, as required by Two-Stage Batch Chaining in One-Pager #1 or `resourcedetection` + `queue_batch`), `confmap.Resolver` expands `- ${mockcomponent:component://active_destination_log_preprocess}` inside `processors:` into a nested slice `[]any{"googlepolicy", []any{"batch/gcp_bytes", "batch/gcp_items"}}`, failing `otelcol.ConfigProvider.Get` with `'service.pipelines[logs/custom_sql].processors[1]' expected a map or struct, got "slice"`.
   - **Failure 1c (Per-Pipeline Processor & Queue Duplication)**: Defining separate workload log pipelines (`logs/policy_source` + `logs/custom_sql`) instantiates **2** separate `googlepolicy` processors and **2** separate workload `queue_batch` processors in the OTel Collector runtime graph (`googlePolicyInstances == 2`, `queueBatchInstances == 2`).
2. **Canonical Workload Pipelines + `confmap.enableMergeAppendOption` Coexistence ([`provider.go:358-381`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L358-L381), [`537-617`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L537-L617))**:
   - Synthesizing `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` pre-wired with `receivers: [nop/googlecontrolplane]`, `processors: [googlepolicy, ...destPreProcessIDs]`, and `exporters: destPolicy.ExporterIDs()` allows a customer's `/etc/otelcol-google/config.yaml` to attach custom receivers (`sqlquery/sep`, `sqlquery/direct_app`) with a single line (`service.pipelines.logs/googlecontrolplane.receivers: [sqlquery/sep]`).
   - Wrapping `googlepolicyprocessor` and `queuebatchprocessor` factories in [`TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/coexistence_test.go#L1068-L1218) and [`TestEndToEndCustomConfigCoexistence_LiveGCP`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/coexistence_test.go#L1278-L1432) proved that **exactly 1** `googlepolicy` logs processor instance (`googlePolicyInstances.Load() == 1`) and **1** workload `queue_batch` logs processor instance (`queueBatchInstances.Load() == 2` total: `1` in `logs/googlecontrolplane` + `1` in `logs/default_self_metrics`) are created across remote `SourcePolicy` receivers, direct local receivers, and `forward`-connector pre-pipelines.
   - Live export to `telemetry.googleapis.com:443` in `dashpole-dev` confirmed that `googlepolicy` (`01-drop-debug.json`) dropped the `DEBUG` SEP watermark log while exporting the `INFO` SEP security-alert log (`changeRunID=a63c0d08-60e4-4a27-906f-0c8ca0622a48` $\rightarrow$ `insertId=1t8a1icf3kmm3x`, `count(INFO) == 1`, `count(DEBUG) == 0`), whereas the negative control without `LogFilterPolicy` exported both (`baselineRunID=7fc83f3e-64cb-4f2b-933c-28b62f7db54f` $\rightarrow$ `insertId=1t8a1icf3kmm5e` [`INFO`] and `insertId=1t8a1icf3kmm5d` [`DEBUG`], `count(INFO) == 1`, `count(DEBUG) == 1`).

### What Surprised Us
1. **Why `nop/googlecontrolplane` Must Be Unconditional (Falsifying the "Omit When 0 Remote Sources" Option)**:
   - Initially, `tango_onepager_7_custom_config_coexistence.md` (`Upfront Trade-Offs` #2) suggested that `googlecontrolplaneprovider` could *either* omit a canonical workload pipeline when no `SourcePolicy` produces that signal *or* attach a `nop` receiver.
   - Our baseline test (`TestBaselineComponentSchemeFailures/BaselineWithoutCanonicalPipelinesFailsValidationOnOneLineCustomReceiver` in [`coexistence_test.go:589-674`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/coexistence_test.go#L589-L674)) proved that if `googlecontrolplaneprovider` omits `logs/googlecontrolplane` when zero remote `SourcePolicy` entries exist, then a customer's 1-line `config.yaml` (`service.pipelines.logs/googlecontrolplane.receivers: [otlp/sep_sql]`) creates a pipeline with `receivers: [otlp/sep_sql]` and empty `exporters: []`, failing `confmap.Validate(cfg)` with `service::pipelines::logs/googlecontrolplane: must have at least one exporter`.
   - Therefore, `nop/googlecontrolplane` and `{logs,metrics,traces}/googlecontrolplane` **must be emitted unconditionally**.
2. **Distro Spec Gap in `specs/google-built-opentelemetry-collector.yaml`**:
   - At `v0.162.0`, `specs/google-built-opentelemetry-collector.yaml` includes `nop` only under `exporters:` (line 94), not under `receivers:` (even though `nopreceiver` is already registered in `cmd/distrogen/registry.yaml:166`). Similarly, `sqlquery` (`sqlqueryreceiver`) is in `cmd/distrogen/registry.yaml:247` and `specs/otelopscol.yaml:49`, but not yet listed under `receivers:` in `specs/google-built-opentelemetry-collector.yaml`.
   - In addition, `integration_test/policy_test/policy_test.go` (`testCollectorFactories`) needed `newStubReceiverFactory("nop")` and `WithLogs`/`WithMetrics`/`WithTraces` builders on `newStubProcessorFactory` (`queue_batch`) once `{logs,metrics,traces}/googlecontrolplane` were synthesized unconditionally.

---

## 3. Design-Doc Discrepancies & Spec Updates Applied

We updated [`tango_onepager_7_custom_config_coexistence.md`](file:///usr/local/google/home/dashpole/.gemini/jetski/brain/1ff2e532-7fd7-4ef1-b7e4-6532e6080076/tango_onepager_7_custom_config_coexistence.md) in place to reflect three prototype discoveries:
1. **`Proposed Design` Diagram & Bullet 1 (`lines 52-89`)**: Explicitly show `nop/googlecontrolplane` in `receivers:` on `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane`, synthesized via `synthesizeCanonicalWorkloadPipelines` and normalized via `canonicalizeWorkloadPipelineConf` for non-`self_metrics` `SourcePolicy` instances.
2. **`Proposed Design` Bullet 3 & Bullet 4 (`lines 108-115`)**:
   - Document all three `innerSchemeComponent` removal locations in `provider.go` (`75`, `180`, `210`).
   - Add **Bullet 4 (`Distribution Spec Prerequisites`)** specifying that `nop` and `sqlquery` must be added under `receivers:` in `specs/google-built-opentelemetry-collector.yaml`.
3. **`Upfront Trade-Offs` #2 (`line 122`)**: Replace the "either omit or attach `nop`" wording with the mandatory requirement to **unconditionally emit `nop/googlecontrolplane`** on `{logs,metrics,traces}/googlecontrolplane`, citing the `service::pipelines::logs/googlecontrolplane: must have at least one exporter` validation failure when zero remote `SourcePolicy` entries exist.

---

## 4. Productionization Gap Ledger

| Gap ID | Component / File | PoC Simplification | Productionization Requirement |
| :--- | :--- | :--- | :--- |
| **G1** | `specs/google-built-opentelemetry-collector.yaml` & `google-built-opentelemetry-collector/generated_collector/` | Registered `nop` receiver and custom `sqlquery` receiver directly in `coexistence_test.go`'s `buildCoexistenceFactories` rather than regenerating the OCB distribution in `google-built-opentelemetry-collector/generated_collector/`. | Add `nop` and `sqlquery` under `receivers:` in `specs/google-built-opentelemetry-collector.yaml` and run `make gen-google-built-otel` (`make update-google-built-otel`) to regenerate `google-built-opentelemetry-collector/generated_collector/`. |
| **G2** | Concrete `SourcePolicy` Drivers (`filelog`, `windowseventlog`, `hostmetrics`, etc.) | Added `canonicalizeWorkloadPipelineConf` in `provider.go:582-617` to normalize any non-`self_metrics` `SourcePolicy` pipeline key (`logs/*`, `metrics/*`, `traces/*`) onto `{logs,metrics,traces}/googlecontrolplane`. | As new concrete `SourcePolicy` drivers are added to `googlecontrolplaneprovider/policies/`, have their `LogsPipelines` / `MetricsPipelines` / `TracesPipelines` methods emit `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` directly (while keeping `canonicalizeWorkloadPipelineConf` as a defensive normalization layer). |
| **G3** | `go/gboc-learns-to-tango` Design Doc | Updated `tango_onepager_7_custom_config_coexistence.md` and `provider.go`. | Update the `Custom Config Escape Hatch` section of `go/gboc-learns-to-tango` to replace `${googlecontrolplane:component://...}` examples with canonical pipeline receiver merging (`service.pipelines.logs/googlecontrolplane.receivers: [sqlquery/sep]`) and the `forward` connector pre-pipeline pattern. |

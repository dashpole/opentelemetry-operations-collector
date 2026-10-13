# Demo & Walkthrough Plan: Integrating Custom Local `config.yaml` Receivers with Control-Plane Policies in GBOC (Tab 7)

**Author**: dashpole@google.com  
**Target Design Doc**: [`tango_onepager_7_custom_config_coexistence.md`](file:///usr/local/google/home/dashpole/.gemini/jetski/brain/1ff2e532-7fd7-4ef1-b7e4-6532e6080076/tango_onepager_7_custom_config_coexistence.md)  
**Companion Learnings Report**: [`PROTOTYPE_LEARNINGS.md`](file:///tmp/prototypes/tab7_custom_config_coexistence/PROTOTYPE_LEARNINGS.md)  
**Prototype Workspace & Branch**: `/tmp/prototypes/tab7_custom_config_coexistence` (`proto/tango-tab7-custom-config-coexistence`)

---

## Option A: Interactive Live / Terminal Walkthrough

This 3-step live terminal walkthrough reproduces the entire prototype story in under 45 seconds: (1) why the `component://` URI escape hatch in `go/gboc-learns-to-tango` fails, (2) how Canonical Workload Pipelines (`{logs,metrics,traces}/googlecontrolplane` + `nop/googlecontrolplane` + `confmap.enableMergeAppendOption`) solve custom receiver coexistence with a 1-line `receivers:` entry, and (3) live OTLP export to `telemetry.googleapis.com:443` in `dashpole-dev` with `LogFilterPolicy` enforcement.

### Step 1: Reproduce the 3 `component://` Failures & Verify `nop/googlecontrolplane` Requirement

Run `TestBaselineComponentSchemeFailures` and `TestCanonicalWorkloadPipelines`:

```bash
cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  go test -v -run "TestBaselineComponentSchemeFailures|TestCanonicalWorkloadPipelines" ./...
```

**What to Highlight in the Terminal Output**:
1. **`Failure1_ComponentSchemeInProviderAtV0162OrAfterRemoval`**:
   - Post-change `googlecontrolplane:component://global_policy_processor` is rejected immediately with `"component://global_policy_processor": inner scheme is unsupported: component` (`ErrURIInvalidInnerScheme`).
   - Simulating the `v0.162.0` stub (`mockcomponent:component://root_config_map` delegating to `evaluateActivePolicySet`) dumps the root config map into `processors[0]` and fails `ConfigProvider.Get` with `'service.pipelines[logs/custom_sql].processors[0]' has invalid keys: exporters, extensions, processors, receivers, service`.
2. **`Failure2_ConfmapCannotSpliceMultiProcessorSliceIntoSequence`**:
   - Expanding `component://active_destination_log_preprocess` when a destination policy returns 2 processors (`["batch/gcp_bytes", "batch/gcp_items"]`) produces a nested slice `[]interface {}{"googlepolicy", []interface {}{"batch/gcp_bytes", "batch/gcp_items"}}` inside `processors:`, failing `ConfigProvider.Get` with `'service.pipelines[logs/custom_sql].processors[1]' expected a map or struct, got "slice"`.
3. **`Failure3_PerPipelineProcessorAndQueueDuplication`**:
   - Separate pipelines (`logs/policy_source` + `logs/custom_sql`) instantiate `googlepolicy instances=2, queue_batch instances=2`.
4. **`BaselineWithoutCanonicalPipelinesFailsValidationOnOneLineCustomReceiver` & `TestCanonicalWorkloadPipelines`**:
   - Without `logs/googlecontrolplane` and `nop/googlecontrolplane`, a 1-line `config.yaml` (`service.pipelines.logs/googlecontrolplane.receivers: [otlp/sep_sql]`) fails `confmap.Validate(cfg)` with `service::pipelines::logs/googlecontrolplane: must have at least one exporter`.
   - With `synthesizeCanonicalWorkloadPipelines` and `canonicalizeWorkloadPipelineConf` in [`provider.go:537-617`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L537-L617), `confmap.Validate(cfg)` succeeds with `receivers=[nop/googlecontrolplane otlp/sep_sql]`, `processors=[googlepolicy queue_batch/default_gcp_destination_batch_logs]`, `exporters=[otlp_grpc/default_gcp_destination]`, and `googlePolicyInstances == 1`.

### Step 2: Verify Receiver-Specific Pre-Processing via `forward` Connector

Run `TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline`:

```bash
cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  go test -v -run "TestEndToEndCustomConfigCoexistence_ForwardConnectorPrePipeline" ./...
```

**What to Highlight**:
- `logs/googlecontrolplane.receivers` merges and deduplicates to `[nop/googlecontrolplane, otlp/remote_policy_source, sqlquery/direct_app, forward/sep]`.
- Only **1** `googlepolicy` logs processor instance (`googlePolicyInstances == 1`) and **1** workload `queue_batch` logs processor instance (`queueBatchInstances == 2` total across `logs/googlecontrolplane` and `logs/default_self_metrics`) are instantiated.
- Both `DEBUG` logs (from `sqlquery/sep` and `sqlquery/direct_app`) are dropped by `googlepolicy`, `sqlquery/sep`'s `INFO` log carries `sep.preprocessed == "true"` from `logs/sep_pre` $\rightarrow$ `forward/sep`, and `sqlquery/direct_app`'s `INFO` log is untouched by `transform/sep_only`.

### Step 3: Live `telemetry.googleapis.com:443` Export in `dashpole-dev` (+ Negative Control)

Run `TestEndToEndCustomConfigCoexistence_LiveGCP`:

```bash
cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && \
  go test -v -run "TestEndToEndCustomConfigCoexistence_LiveGCP" ./...
```

**What to Highlight**:
- **Change Run (with `LogFilterPolicy` `01-drop-debug.json`)**: Exports to live `telemetry.googleapis.com:443` in `dashpole-dev` (`changeRunID=a63c0d08-60e4-4a27-906f-0c8ca0622a48` in [`milestone_2_evidence.md`](file:///tmp/prototypes/tab7_custom_config_coexistence/milestone_2_evidence.md)); Cloud Logging returns **1** `INFO` security-alert log (`insertId=1t8a1icf3kmm3x`) and **0** `DEBUG` watermark logs.
- **Negative Control Run (without `LogFilterPolicy`)**: Exports to live `telemetry.googleapis.com:443` in `dashpole-dev` (`baselineRunID=7fc83f3e-64cb-4f2b-933c-28b62f7db54f`); Cloud Logging returns both **1** `INFO` log (`insertId=1t8a1icf3kmm5e`) and **1** `DEBUG` log (`insertId=1t8a1icf3kmm5d`).

---

## Option B: AI-Narrated Walkthrough Video Storyboard & Script

> [!NOTE]
> All spoken narration blocks below are strictly formatted for Text-to-Speech (TTS) rendering: zero backticks, zero raw code punctuation or symbols, and zero URLs.

### Scene 1: Problem & Motivation (0:00 – 0:30)
- **Visual Plan**:
  - Title card: *Integrating Custom Local Receivers with Control-Plane Policies in GBOC (Tab 7)*.
  - Split diagram showing Allianz Google SecOps findings Thirty-Two, Thirty-Three, Forty, Seventy-One, and Seventy-Two: SQL polling receivers for Symantec Endpoint Protection on Microsoft SQL Server and Oracle Unified Audit Trail in local configuration files alongside remote Telemetry Policies.
- **Spoken Narration**:
  Enterprise customers migrating from Bindplane to the Google-Built OpenTelemetry Collector need to poll logs from commercial databases such as Symantec Endpoint Protection on Microsoft SQL Server and Oracle Unified Audit Trail. Because remote Telemetry Policies intentionally never carry database credentials or custom SQL queries, customers define these SQL receivers in a local configuration file on the host. However, those locally collected logs still need to flow through the Control Plane's active filter policies, batch queues, and Google Cloud destination exporter.

### Scene 2: Why the Component URI Escape Hatch Fails (0:30 – 1:10)
- **Visual Plan**:
  - Side-by-side code and terminal view of `TestBaselineComponentSchemeFailures` in `coexistence_test.go`.
  - Highlight the three failures: (1) the unimplemented stub in version zero dot one six two dumping the root configuration map into the processors list, (2) OpenTelemetry `confmap` failing to splice a two-stage batch processor list into a YAML sequence, and (3) duplicate policy and batch queue processor instances when running separate pipelines.
- **Spoken Narration**:
  The original design proposed a component URI expansion syntax inside the local configuration file. Our prototype proved three architectural blockers with that approach. First, in version zero dot one six two, the component scheme was an unimplemented stub that dumped the entire root configuration map into the processor list. Second, even if implemented, OpenTelemetry configuration expansion cannot splice a multi-processor slice, such as our two-stage byte and item batch chain, into a YAML sequence without creating a nested list that fails unmarshaling. Third, OpenTelemetry instantiates processors once per pipeline, so creating separate pipelines for local receivers duplicates the policy engine and batch queue buffers in memory.

### Scene 3: Canonical Workload Pipelines & Unconditional No-Op Receiver (1:10 – 1:50)
- **Visual Plan**:
  - Architecture diagram of `synthesizeCanonicalWorkloadPipelines` and `canonicalizeWorkloadPipelineConf` in `provider.go`.
  - Show how `googlecontrolplaneprovider` unconditionally emits the `nop` receiver and pre-wires the canonical logs, metrics, and traces pipelines, while `confmap` merge-append combines remote policy receivers and a single-line local receiver entry.
- **Spoken Narration**:
  Instead of URI expansion, we leverage the configuration merge-append feature gate that the control plane provider already enables. The provider unconditionally synthesizes canonical logs, metrics, and traces workload pipelines pre-wired with a no-op receiver, the Google policy processor, the destination batch chain, and the default Google Cloud exporter. Our baseline test proved that the no-op receiver must be emitted unconditionally, because omitting the canonical pipeline when zero remote source policies exist causes a one-line local receiver attachment to fail validation for having no exporter. With canonical pipelines in place, a customer attaches a custom SQL receiver with a single line, and when receiver-specific pre-processing is needed, a local pre-pipeline routes into the canonical pipeline through a standard forward connector while still sharing a single policy processor instance.

### Scene 4: Live Cloud Logging Verification & Production Next Steps (1:50 – 2:30)
- **Visual Plan**:
  - Terminal capture of `TestEndToEndCustomConfigCoexistence_LiveGCP` in `milestone_2_evidence.md`.
  - Highlight the live export to Google Cloud Telemetry in the `dashpole-dev` project: the change run with the debug drop policy active ingests one info security alert log and zero debug watermark logs, while the negative control run without the policy ingests both the info and debug logs.
  - Closing checklist card showing the three productionization items (`G1` through `G3` from `PROTOTYPE_LEARNINGS.md`).
- **Spoken Narration**:
  We verified this end to end against live Google Cloud Logging in our development project. When our custom SQL receiver emitted both a debug watermark log and an info security alert log with a remote log filter policy active, the single shared policy processor dropped the debug record in memory and exported only the info alert over OTLP gRPC to Google Cloud. In our negative control run without the filter policy, both the info and debug records arrived in Cloud Logging. To productionize this change, we will add the no-op and SQL query receivers to the distribution specification, regenerate the collector build, and update the architecture documentation.

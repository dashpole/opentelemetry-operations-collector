# Milestone 1 Evidence: Canonical Workload Pipelines vs. `component://` Scheme in `googlecontrolplaneprovider`

## 1. Summary of Changes

1. **Removed `innerSchemeComponent` (`component://`) from `googlecontrolplaneprovider`**:
   - Removed `innerSchemeComponent = "component"` and its `case innerSchemeComponent:` branch from `provider.Retrieve` in [`provider.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L68-L77) (and lines [`182-214`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L182-L214)) so `googlecontrolplane:component:...` URIs fall into `default:` and return `ErrURIInvalidInnerScheme` (`inner scheme is unsupported: component`).
2. **Unconditional Canonical Workload Pipelines (`{logs,metrics,traces}/googlecontrolplane`)**:
   - Added `synthesizeCanonicalWorkloadPipelines` in [`provider.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L537-L580) (called from `evaluatePolicySet` at [`provider.go:358-381`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L358-L381)) to unconditionally emit `receivers.nop/googlecontrolplane: {}` and pre-wire `logs/googlecontrolplane`, `metrics/googlecontrolplane`, and `traces/googlecontrolplane` with:
     - `receivers: [nop/googlecontrolplane]`
     - `processors: [googlepolicy, <destPolicy.PreProcess{Log,Metric,Trace}IDs()...>]`
     - `exporters: [<destPolicy.ExporterIDs()...>]`
3. **Canonical Pipeline Routing for Non-`self_metrics` `SourcePolicy` Instances**:
   - Added `canonicalizeWorkloadPipelineConf` in [`provider.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L582-L617) (called from `evaluateSingleSourcePolicy` at [`provider.go:486-533`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go#L486-L533)) so any non-`self_metrics` `SourcePolicy` contributes its `receivers` into `{logs,metrics,traces}/googlecontrolplane` (deduplicated via `confmap.enableMergeAppendOption`), while `SelfMetricsPolicy` continues to build its isolated `{logs,metrics}/default_self_metrics` pipelines without `googlepolicy`.
4. **Updated Golden & Integration Tests**:
   - Updated [`testdata/policysets/builtin/result.yaml`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/builtin/result.yaml) and [`provider_test.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider_test.go) to assert the canonical workload pipelines and `component://` rejection (`TestRetrieve_ComponentSchemeRejected`).
   - Updated [`integration_test/policy_test/policy_test.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/integration_test/policy_test/policy_test.go) `testCollectorFactories()` to register `newStubReceiverFactory("nop")` and `WithLogs`/`WithMetrics`/`WithTraces` builders on `newStubProcessorFactory` (for `queue_batch`), since `googlecontrolplaneprovider` now unconditionally instantiates `{logs,metrics,traces}/googlecontrolplane` with `nop/googlecontrolplane` and `queue_batch/default_gcp_destination_batch_*`.

---

## 2. Deterministic Baseline-vs-Change Proof

Every test run of [`coexistence_test.go`](file:///tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/coexistence_test.go) deterministically executes both the `v0.162.0` baseline failure reproductions AND the post-change positive assertions (with zero dead `if/else` branches):

```bash
cd /tmp/prototypes/tab7_custom_config_coexistence/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -run "TestRetrieve|TestPolicySets|TestBaselineComponentSchemeFailures|TestCanonicalWorkloadPipelines" ./... && cd /tmp/prototypes/tab7_custom_config_coexistence/integration_test/policy_test && go test -v -run "TestPolicyIntegration/drop_by_body_equals" ./...
```

### Verification Output

```text
=== RUN   TestBaselineComponentSchemeFailures
=== RUN   TestBaselineComponentSchemeFailures/Failure1_ComponentSchemeInProviderAtV0162OrAfterRemoval
    coexistence_test.go:381: Resolve failed as expected with removed component:// scheme: "component://global_policy_processor": inner scheme is unsupported: component
    coexistence_test.go:420: Baseline v0.162.0 ConfigProvider.Get error: cannot unmarshal the configuration: decoding failed due to the following error(s):

        'service.pipelines[logs/custom_sql].processors[0]' has invalid keys: exporters, extensions, processors, receivers, service
=== RUN   TestBaselineComponentSchemeFailures/Failure2_ConfmapCannotSpliceMultiProcessorSliceIntoSequence
    coexistence_test.go:461: Resolved processors slice with multi-processor component:// expansion: []interface {}{"googlepolicy", []interface {}{"batch/gcp_bytes", "batch/gcp_items"}}
    coexistence_test.go:484: ConfigProvider.Get error on multi-processor component:// slice: cannot unmarshal the configuration: decoding failed due to the following error(s):

        'service.pipelines[logs/custom_sql].processors[1]' expected a map or struct, got "slice"
=== RUN   TestBaselineComponentSchemeFailures/Failure3_PerPipelineProcessorAndQueueDuplication
    coexistence_test.go:552: Separate pipelines created googlepolicy instances=2, queue_batch instances=2
=== RUN   TestBaselineComponentSchemeFailures/BaselineWithoutCanonicalPipelinesFailsValidationOnOneLineCustomReceiver
    coexistence_test.go:608: Baseline v0.162.0 (without canonical workload pipelines) confmap.Validate(cfg) error: service::pipelines::logs/googlecontrolplane: must have at least one exporter
    coexistence_test.go:636: With canonical workload pipelines implemented, confmap.Validate(cfg) succeeded; receivers=[nop/googlecontrolplane otlp/sep_sql], processors=[googlepolicy queue_batch/default_gcp_destination_batch_logs], exporters=[otlp_grpc/default_gcp_destination]
--- PASS: TestBaselineComponentSchemeFailures (0.14s)
    --- PASS: TestBaselineComponentSchemeFailures/Failure1_ComponentSchemeInProviderAtV0162OrAfterRemoval (0.05s)
    --- PASS: TestBaselineComponentSchemeFailures/Failure2_ConfmapCannotSpliceMultiProcessorSliceIntoSequence (0.01s)
    --- PASS: TestBaselineComponentSchemeFailures/Failure3_PerPipelineProcessorAndQueueDuplication (0.02s)
    --- PASS: TestBaselineComponentSchemeFailures/BaselineWithoutCanonicalPipelinesFailsValidationOnOneLineCustomReceiver (0.06s)
=== RUN   TestCanonicalWorkloadPipelines
=== RUN   TestCanonicalWorkloadPipelines/ZeroRemoteSourcesPlusLocalCustomReceiverMergesAndSharesSingleProcessorInstance
=== RUN   TestCanonicalWorkloadPipelines/MultipleRemoteSourcesAndLocalReceiverShareSingleWorkloadPipeline
--- PASS: TestCanonicalWorkloadPipelines (0.12s)
    --- PASS: TestCanonicalWorkloadPipelines/ZeroRemoteSourcesPlusLocalCustomReceiverMergesAndSharesSingleProcessorInstance (0.07s)
    --- PASS: TestCanonicalWorkloadPipelines/MultipleRemoteSourcesAndLocalReceiverShareSingleWorkloadPipeline (0.05s)
--- PASS: TestPolicySets (0.03s)
    --- PASS: TestPolicySets/builtin (0.03s)
--- PASS: TestRetrieve_ComponentSchemeRejected (0.00s)
--- PASS: TestRetrieve (0.04s)
--- PASS: TestRetrieve_NoFleetID (0.01s)
--- PASS: TestRetrieve_XDSServerUnreachable (5.05s)
--- PASS: TestRetrieve_XDSNumericProjectDoesNotClobberProjectID (5.06s)
--- PASS: TestRetrieve_MultipleDestinationPolicies (0.06s)
--- PASS: TestRetrieve_ActivePolicySetWithCustomDestination (0.03s)
--- PASS: TestRetrieve_ActivePolicySetWithCustomSelfMetrics (0.10s)
--- PASS: TestRetrieve_ActivePolicySetWithOtherSource (0.07s)
--- PASS: TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure (0.10s)
--- PASS: TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet (0.07s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	10.89s
=== RUN   TestPolicyIntegration
=== RUN   TestPolicyIntegration/drop_by_body_equals
--- PASS: TestPolicyIntegration (0.37s)
    --- PASS: TestPolicyIntegration/drop_by_body_equals (0.36s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/integration_test/policy_test	0.38s
```

# Milestone 1 Evidence: `WindowsEventLogSourcePolicy` High-Throughput Defaults in `googlecontrolplaneprovider`

## 1. Deliverables Implemented

1. [`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy.go):
   - Registers policy driver `"windows_event_log"` (`PolicyType = "windows_event_log"`) via `googlepolicy.RegisterPolicyDriver`.
   - Implements `WindowsEventLogSourcePolicy` (`googlepolicy.SourcePolicy`), accepting `id` (or `name`) and `channels` (`[]string`).
   - Validates non-empty policy identifier, non-empty `channels`, and rejects empty/whitespace-only channel names or channel names containing the confmap delimiter `"::"`.
   - Deduplicates channels case-insensitively both within a single policy (`DeduplicateChannels`) and across multiple `WindowsEventLogSourcePolicy` instances in the same `PolicySet` via `WithSeenChannels(ctx, seen)`, preserving the first-seen channel name casing for `channel: <Name>` while normalizing receiver component IDs to `windowseventlog/<lowercase_channel>`.
   - Synthesizes `windowseventlog/<lowercase_channel>` receiver blocks with GBOC's four high-throughput defaults:
     - `suppress_rendering_info: true`
     - `event_driven_scraping: true`
     - `max_reads: 500`
     - `ignore_channel_errors: true`
   - Wires all deduplicated receivers into `service::pipelines::logs/windows_event_log` with `googlepolicy` and destination batch processors (`queue_batch/default_gcp_destination_batch_logs`) and destination exporters (`otlp_grpc/default_gcp_destination`).
2. [`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy_test.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog/windowseventlog_policy_test.go):
   - Unit tests for single-policy and multi-policy case-insensitive channel deduplication, pipeline generation, validation edge cases, registry loading, and direct unmarshaling into upstream `*windowseventlogreceiver.WindowsLogConfig`.
3. [`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider.go) & [`provider_test.go`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/provider_test.go):
   - Imports `policies/windowseventlog`, initializes a per-`PolicySet` `windowseventlog.NewSeenChannels()` tracker in `evaluatePolicySet`, and passes `windowseventlog.WithSeenChannels(ctx, seenWinEventLogChannels)` when evaluating `*windowseventlog.WindowsEventLogSourcePolicy` instances in `evaluateSingleSourcePolicy`.
4. [`components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/windowseventlog/`](components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/testdata/policysets/windowseventlog/result.yaml):
   - Golden end-to-end `filePolicyManager` + `googlecontrolplaneprovider` test with two overlapping policies (`01_dc_events.json` with `["Security", "Application", "security"]` and `02_wec_events.json` with `["SECURITY", "System", "application"]`).

---

## 2. Baseline-vs-Change Proof

### A. Before Milestone 1 (Unmodified `v0.162.0` Baseline)

1. Attempting to load `"windows_event_log"` from `googlepolicy.LoadPolicy` before Milestone 1 fails because no `"windows_event_log"` driver is registered:
   ```text
   === RUN   TestBaselineBeforeMilestone1
       baseline_check_test.go:13: Baseline LoadPolicy(windows_event_log) failed as expected before Milestone 1: no driver found for policy type: windows_event_log
   --- FAIL: TestBaselineBeforeMilestone1 (0.00s)
   FAIL
   ```
2. Inspecting upstream `windowseventlogreceiver.NewFactory().CreateDefaultConfig().(*windowseventlogreceiver.WindowsLogConfig)` at `v0.162.0` confirms the low-throughput upstream defaults:
   - `InputConfig.SuppressRenderingInfo == false`
   - `InputConfig.EventDrivenScraping == false`
   - `InputConfig.MaxReads == 100`
   - `InputConfig.IgnoreChannelErrors == false`

### B. After Milestone 1 (With `WindowsEventLogSourcePolicy` Registered & Evaluated)

1. `googlepolicy.LoadPolicy("windows_event_log", ...)` succeeds, and `TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal` verifies that unmarshaling `receivers::windowseventlog/security` into `*windowseventlogreceiver.WindowsLogConfig` transitions all four fields from `(false, false, 100, false)` to `(true, true, 500, true)` with `Channel: "Security"`.
2. Evaluating overlapping policies `01_dc_events.json` (`["Security", "Application", "security"]`) and `02_wec_events.json` (`["SECURITY", "System", "application"]`) in `TestPolicySets/windowseventlog` produces the exact golden configuration in `testdata/policysets/windowseventlog/result.yaml`:
   ```yaml
   receivers:
       windowseventlog/application:
           channel: Application
           event_driven_scraping: true
           ignore_channel_errors: true
           max_reads: 500
           suppress_rendering_info: true
       windowseventlog/security:
           channel: Security
           event_driven_scraping: true
           ignore_channel_errors: true
           max_reads: 500
           suppress_rendering_info: true
       windowseventlog/system:
           channel: System
           event_driven_scraping: true
           ignore_channel_errors: true
           max_reads: 500
           suppress_rendering_info: true
   service:
       pipelines:
           logs/windows_event_log:
               exporters:
                   - otlp_grpc/default_gcp_destination
               processors:
                   - googlepolicy
                   - queue_batch/default_gcp_destination_batch_logs
               receivers:
                   - windowseventlog/security
                   - windowseventlog/application
                   - windowseventlog/system
   ```

---

## 3. Exact Verification Command & Raw Output

Command:
```bash
cd /tmp/prototypes/tab4_windowseventlog_defaults/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider && go test -v -race -count=1 ./...
```

Output:
```text
=== RUN   TestGenerateCollectorID_AlreadySet
--- PASS: TestGenerateCollectorID_AlreadySet (0.00s)
=== RUN   TestGenerateCollectorID_FromEnv
--- PASS: TestGenerateCollectorID_FromEnv (0.00s)
=== RUN   TestGenerateCollectorID_DefaultHostname
--- PASS: TestGenerateCollectorID_DefaultHostname (0.00s)
=== RUN   TestGenerateCollectorID_GetHostnameError
--- PASS: TestGenerateCollectorID_GetHostnameError (0.00s)
=== RUN   TestPolicySets
=== RUN   TestPolicySets/builtin
=== RUN   TestPolicySets/windowseventlog
--- PASS: TestPolicySets (0.31s)
    --- PASS: TestPolicySets/builtin (0.14s)
    --- PASS: TestPolicySets/windowseventlog (0.17s)
=== RUN   TestValidateProviderScheme
--- PASS: TestValidateProviderScheme (0.00s)
=== RUN   TestScheme
--- PASS: TestScheme (0.00s)
=== RUN   TestUnsupportedScheme
--- PASS: TestUnsupportedScheme (0.00s)
=== RUN   TestEmptyURI
--- PASS: TestEmptyURI (0.00s)
=== RUN   TestEmptyTarget
--- PASS: TestEmptyTarget (0.00s)
=== RUN   TestRetrieve
--- PASS: TestRetrieve (0.11s)
=== RUN   TestShutdown
--- PASS: TestShutdown (0.00s)
=== RUN   TestRetrieve_NoFleetID
--- PASS: TestRetrieve_NoFleetID (0.03s)
=== RUN   TestRetrieve_XDSServerUnreachable
--- PASS: TestRetrieve_XDSServerUnreachable (5.10s)
=== RUN   TestRetrieve_XDSNumericProjectDoesNotClobberProjectID
--- PASS: TestRetrieve_XDSNumericProjectDoesNotClobberProjectID (5.11s)
=== RUN   TestRetrieve_MultipleDestinationPolicies
--- PASS: TestRetrieve_MultipleDestinationPolicies (0.09s)
=== RUN   TestRetrieve_ActivePolicySetWithCustomDestination
--- PASS: TestRetrieve_ActivePolicySetWithCustomDestination (0.05s)
=== RUN   TestRetrieve_ActivePolicySetWithCustomSelfMetrics
--- PASS: TestRetrieve_ActivePolicySetWithCustomSelfMetrics (0.09s)
=== RUN   TestRetrieve_ActivePolicySetWithOtherSource
--- PASS: TestRetrieve_ActivePolicySetWithOtherSource (0.10s)
=== RUN   TestRetrieve_ActivePolicySetWithMultipleWindowsEventLogPolicies
--- PASS: TestRetrieve_ActivePolicySetWithMultipleWindowsEventLogPolicies (0.11s)
=== RUN   TestFilterPolicyDriversRegistered
=== RUN   TestFilterPolicyDriversRegistered/log_filter
=== RUN   TestFilterPolicyDriversRegistered/metric_filter
=== RUN   TestFilterPolicyDriversRegistered/trace_filter
=== RUN   TestFilterPolicyDriversRegistered/windows_event_log
--- PASS: TestFilterPolicyDriversRegistered (0.01s)
    --- PASS: TestFilterPolicyDriversRegistered/log_filter (0.00s)
    --- PASS: TestFilterPolicyDriversRegistered/metric_filter (0.00s)
    --- PASS: TestFilterPolicyDriversRegistered/trace_filter (0.00s)
    --- PASS: TestFilterPolicyDriversRegistered/windows_event_log (0.00s)
=== RUN   TestMergeAppendUnionsServiceExtensions
--- PASS: TestMergeAppendUnionsServiceExtensions (0.00s)
=== RUN   TestMergeAppendDeduplicatesServiceExtensions
--- PASS: TestMergeAppendDeduplicatesServiceExtensions (0.00s)
=== RUN   TestMergeAppendWithoutServiceExtensions
--- PASS: TestMergeAppendWithoutServiceExtensions (0.00s)
=== RUN   TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure
--- PASS: TestRetrieve_RecordsPolicyEvaluateErrorEventOnFailure (0.11s)
=== RUN   TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet
--- PASS: TestRetrieve_BuiltinPolicyErrorDoesNotRollbackActivePolicySet (0.03s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider	12.591s
?   	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/gcpdestination	[no test files]
=== RUN   TestSelfMetricsPolicy_Evaluate
--- PASS: TestSelfMetricsPolicy_Evaluate (0.06s)
=== RUN   TestSelfMetricsPolicy_Evaluate_ExampleYamlMatch
--- PASS: TestSelfMetricsPolicy_Evaluate_ExampleYamlMatch (0.04s)
=== RUN   TestSelfMetricsPolicy_Evaluate_MissingContext
--- PASS: TestSelfMetricsPolicy_Evaluate_MissingContext (0.00s)
=== RUN   TestSelfMetricsPolicy_Pipelines
--- PASS: TestSelfMetricsPolicy_Pipelines (0.00s)
=== RUN   TestSelfMetricsPolicy_Evaluate_WithProjectID
--- PASS: TestSelfMetricsPolicy_Evaluate_WithProjectID (0.04s)
=== RUN   TestSelfMetricsPolicy_Validate
=== RUN   TestSelfMetricsPolicy_Validate/valid
=== RUN   TestSelfMetricsPolicy_Validate/missing_name
=== RUN   TestSelfMetricsPolicy_Validate/zero_port
=== RUN   TestSelfMetricsPolicy_Validate/port_out_of_range
=== RUN   TestSelfMetricsPolicy_Validate/negative_port
--- PASS: TestSelfMetricsPolicy_Validate (0.00s)
    --- PASS: TestSelfMetricsPolicy_Validate/valid (0.00s)
    --- PASS: TestSelfMetricsPolicy_Validate/missing_name (0.00s)
    --- PASS: TestSelfMetricsPolicy_Validate/zero_port (0.00s)
    --- PASS: TestSelfMetricsPolicy_Validate/port_out_of_range (0.00s)
    --- PASS: TestSelfMetricsPolicy_Validate/negative_port (0.00s)
=== RUN   TestSelfMetricsPolicy_Validate_LoadableFromRegistry
--- PASS: TestSelfMetricsPolicy_Validate_LoadableFromRegistry (0.00s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/selfmetrics	1.617s
=== RUN   TestWindowsEventLogPolicy_Evaluate_SingleAndDeduplicated
--- PASS: TestWindowsEventLogPolicy_Evaluate_SingleAndDeduplicated (0.00s)
=== RUN   TestWindowsEventLogPolicy_Evaluate_CrossPolicyDeduplication
--- PASS: TestWindowsEventLogPolicy_Evaluate_CrossPolicyDeduplication (0.01s)
=== RUN   TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal
--- PASS: TestWindowsEventLogPolicy_BaselineVsChange_UpstreamConfigUnmarshal (0.00s)
=== RUN   TestWindowsEventLogPolicy_Pipelines
--- PASS: TestWindowsEventLogPolicy_Pipelines (0.00s)
=== RUN   TestWindowsEventLogPolicy_Validate
=== RUN   TestWindowsEventLogPolicy_Validate/valid_with_id
=== RUN   TestWindowsEventLogPolicy_Validate/valid_with_name
=== RUN   TestWindowsEventLogPolicy_Validate/valid_with_matching_id_and_name
=== RUN   TestWindowsEventLogPolicy_Validate/missing_id_and_name
=== RUN   TestWindowsEventLogPolicy_Validate/whitespace-only_id
=== RUN   TestWindowsEventLogPolicy_Validate/conflicting_id_and_name
=== RUN   TestWindowsEventLogPolicy_Validate/empty_channels_slice
=== RUN   TestWindowsEventLogPolicy_Validate/empty_channel_entry
=== RUN   TestWindowsEventLogPolicy_Validate/channel_with_confmap_delimiter
--- PASS: TestWindowsEventLogPolicy_Validate (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/valid_with_id (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/valid_with_name (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/valid_with_matching_id_and_name (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/missing_id_and_name (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/whitespace-only_id (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/conflicting_id_and_name (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/empty_channels_slice (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/empty_channel_entry (0.00s)
    --- PASS: TestWindowsEventLogPolicy_Validate/channel_with_confmap_delimiter (0.00s)
=== RUN   TestWindowsEventLogPolicy_LoadableFromRegistry
--- PASS: TestWindowsEventLogPolicy_LoadableFromRegistry (0.00s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/provider/googlecontrolplaneprovider/policies/windowseventlog	1.306s
```

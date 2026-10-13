# Prototype Plan: Tab 5 — Handling Pre-Rendered WEF `RenderingInfo` and Event ID Enrichment

**Branch**: `proto/tango-tab5-wef-renderinginfo-strip` (based on `v0.162.0`)  
**Companion Design Doc**: `tango_onepager_5_wef_rendering_info_stripping.md` (Project Tango — Bindplane Finding `#79`)  
**Target Components**:
- `proto/policy/v1alpha1/` & `gen/go/policy/v1alpha1/` (`LogFieldSelector.body_field`, `LogTransformPolicy`, `LogAddAction`, `LogRemoveAction`, `LogRenameAction`, `LogRedactAction`)
- `pkg/googlepolicy` (`LogPolicyTransformer` interface & `TransformStage` enum in `policy.go`), `pkg/googlepolicy/logfilter` (`body_field` path extraction), and `pkg/googlepolicy/logtransform` (policy compilation, optional `matches` where `len(matches) == 0` matches all records, fail-open field mutation, and `"log_transform"` driver registration)
- `components/google-built-opentelemetry-collector/processor/googlepolicyprocessor` (`factory.go` blank-import registration of `pkg/googlepolicy/logtransform`, plus `Evaluator` / `TransformLogs` implementing Double-Applied Filtering + deterministic 4-stage `rename -> add -> remove -> redact` execution with lexicographical Policy ID tie-breaking)

---

## 1. Core Question & Hypothesis

During the Allianz Google SecOps / Bindplane evaluation (**Finding `#79`**), setting `suppress_rendering_info: true` on `windowseventlogreceiver` failed to strip `<RenderingInfo>` from Windows Event Forwarding (`ForwardedEvents`) logs when Windows Event Collector (WEC) subscriptions used the default `ContentFormat = RenderedText`. Under `RenderedText`, each source workstation formats `<RenderingInfo Culture="de-DE"><Message>...` locally before forwarding the XML over WinRM, so `EvtRenderEventXML` on the WEC server returns XML that already has `<RenderingInfo>` baked in.

This prototype tests two falsifiable hypotheses:
1. **Engine Hypothesis (`LogTransformPolicy` in `pkg/googlepolicy` + `googlepolicyprocessor`)**:
   We can implement `LogTransformPolicy` (`add`, `remove`, `rename`, `redact`) and `LogFieldSelector.body_field` in `pkg/googlepolicy` and `googlepolicyprocessor` adhering to the `go/tango-preview-policies` execution contract:
   - Pass 1 Pre-Transform `LogFilterPolicy` evaluation -> Stage-ordered `LogTransformPolicy` (`rename -> add -> remove -> redact`, tie-broken lexicographically by `id`) -> Pass 2 Post-Transform `LogFilterPolicy` evaluation.
   - `O(1)` map key deletion (`pcommon.Map.Remove`) for `LogRemoveAction` targeting `body_field` paths (`["rendering_info"]`, `["message"]`, `["details"]`) when `LogRecord.Body()` is `ValueTypeMap` (`raw: false`).
   - Pre-compiled RE2 `regexp.Regexp` replacement for `LogRedactAction` targeting `record_field: LOG_RECORD_FIELD_BODY` (`regex: "<RenderingInfo[^>]*>[\\s\\S]*?</RenderingInfo>"`, `replacement: ""`) when `LogRecord.Body()` is `ValueTypeStr` (`raw: true`).
2. **WEF Empirical Payload & Live Export Hypothesis (`raw: false` and `raw: true`)**:
   When realistic pre-rendered German (`Culture='de-DE'`) WEF XML payloads (Windows Security Event `4624` Logon and Event `4688` Process Creation with `~2.5–3.2 KB` of `<RenderingInfo>`) are parsed through the verbatim upstream `pkg/stanza/operator/input/windows` XML unmarshaling and `pkg/stanza/adapter` `pcommon.Value` conversion pipeline (vendored into `internal/stanzawindows` in `googlepolicyprocessor` because `unmarshalEventXML`, `formattedBody`, `unmarshalRawEventXML`, `parseSecurity`, and `upsertToAttributeVal` are unexported in `pkg/stanza`, and `pcommon.Value.FromRaw` rejects `[]string` slices produced for `keywords` and `details["Additional Context"]`):
   - **Baseline (`suppress_rendering_info: true` only, no `LogTransformPolicy`)**: Both `raw: false` and `raw: true` retain the full German `RenderingInfo` prose (and in `raw: false`, `formattedBody` duplicates the German `<RenderingInfo><Message>` prose across `body["rendering_info"]["message"]` AND `body["message"]` + `body["details"]`, inflating serialized OTLP `LogRecord` size even further).
   - **With `LogTransformPolicy`**:
     * In structured mode (`raw: false`), `LogRemoveAction` on `body_field` paths `["rendering_info"]`, `["message"]`, and `["details"]` removes 100% of the multi-KB `<RenderingInfo><Message>` prose block in `O(1)` map deletes while preserving `event_id`, `provider`, `computer`, `channel`, `record_id`, `system_time`, `security`, `execution`, and `event_data` (note: top-level `body["level"]`, `body["task"]`, `body["opcode"]`, and `body["keywords"]` retain the short labels copied from `e.RenderingInfo` by `formattedBody` at `xml.go:88-102`). We also measure 2-action removal (`["rendering_info"]` + `["message"]`, as currently drawn in the Companion Design Doc's ASCII diagram) vs. 3-action removal (`["rendering_info"]` + `["message"]` + `["details"]`) to prove why `body_field(["details"])` is mandatory when `<Channel>Security</Channel>` triggers `parseSecurity` (`security.go:10-59`), which puts ~95%+ of the German `<Message>` prose into `body["details"]`.
     * In raw XML mode (`raw: true`), `LogRedactAction` strips 100% of `<RenderingInfo ...>...</RenderingInfo>` (`2,829 B` on Event `4624`, `2,278 B` on Event `4688`) while preserving `<System>` and `<EventData>` as valid XML.
     * Serialized OTLP protobuf `LogRecord` size drops by **78.1–79.0%** in structured mode (`1C` vs. `1A`, where `formattedBody` duplicates the German message across `rendering_info.message` and `details`) and by **57.6–58.7%** (`100%` of `<RenderingInfo>`) in raw XML mode (`2B` vs. `2A`), and the transformed OTLP `ExportLogsServiceRequest` batch is accepted (`gRPC OK`, `rejected_log_records: 0`) by live Google Cloud Telemetry API (`telemetry.mtls.googleapis.com:443` / `telemetry.googleapis.com:443`) in project `dashpole-dev`.

---

## 2. Scope & Intentional Simplifications

- **In Scope**:
  - Protobuf definitions and generated Go code in `proto/policy/v1alpha1/` and `gen/go/policy/v1alpha1/` for `LogFieldSelector.body_field` and `LogTransformPolicy` (`LogAddAction`, `LogRemoveAction`, `LogRenameAction`, `LogRedactAction`).
  - `LogPolicyTransformer` interface and `TransformStage` (`StageRename`, `StageAdd`, `StageRemove`, `StageRedact`) in `pkg/googlepolicy/policy.go`, `body_field` support in `pkg/googlepolicy/logfilter`, and full compilation, validation, optional-`matches` (`len(matches) == 0` matches all records), and fail-open mutation in `pkg/googlepolicy/logtransform`.
  - Blank-import registration of `pkg/googlepolicy/logtransform` in `googlepolicyprocessor/factory.go`, and deterministic stage ordering (`rename -> add -> remove -> redact`), lexicographical `id` tie-breaking, and Double-Applied Filtering (Pass 1 Pre-Transform Filter -> Transform -> Pass 2 Post-Transform Filter) in `googlepolicyprocessor`.
  - Verbatim upstream `pkg/stanza/operator/input/windows` (`xml.go`, `security.go`, `config_all.go`) and `pkg/stanza/adapter` (`upsertToAttributeVal`) XML-to-`plog.LogRecord` conversion in `googlepolicyprocessor/internal/stanzawindows` on realistic German (`de-DE`) WEF Event `4624` and `4688` XML payloads.
  - Live OTLP gRPC export of transformed WEF logs to `telemetry.googleapis.com:443` in `dashpole-dev`.
- **Out of Scope (Intentional PoC Boundaries)**:
  - `TraceTransformPolicy` and `MetricTransformPolicy` (this prototype focuses on `LogTransformPolicy` for Tab 5).
  - Modifying Google SecOps cloud-side `WINEVTLOG` / `WINDOWS_AD` CBN/UDM parsers (cloud-side parser `EventID -> English Event Name` normalization is an ingestion-service concern; here we verify that the OTLP payload delivered to Google Cloud retains the complete `(Provider.Name, EventID.ID, EventData)` tuple needed by the cloud parser).
  - Provisioning a live Windows Server WEC VM (since `suppress_rendering_info: true` invokes `EvtRenderEventXML` which returns the stored WEF XML verbatim, feeding the pre-rendered WEF XML bytes into the verbatim upstream `pkg/stanza/operator/input/windows` parser exercises the collector's exact XML-to-`plog.LogRecord` code path deterministically on Linux).

---

## 3. Candidate Approaches & Fair Comparison Rubric

We evaluate five configurations (`1A`, `1B`, `1C`, `2A`, and `2B`) on identical pre-rendered `de-DE` WEF XML inputs (Security Event `4624` and `4688`):

| Configuration | Mode | Active `LogTransformPolicy` Rules | Metrics Measured |
| :--- | :--- | :--- | :--- |
| **1A. Structured Baseline** | `raw: false` (`internal/stanzawindows`: `UnmarshalEventXML` + `FormattedBody` + `UpsertToAttributeVal`) | None (`suppress_rendering_info: true` only) | Serialized `plog.Logs` protobuf bytes, presence of `rendering_info`/`message`/`details`, preservation of `event_id`/`provider`/`event_data`, CPU ns/op |
| **1B. Structured Partial Strip (2-Action)** | `raw: false` | `remove(body_field(["rendering_info"]))`, `remove(body_field(["message"]))` | Quantifies residual `body["details"]` bloat left by `parseSecurity` when `<Channel>Security</Channel>` (demonstrating why the Companion Design Doc's 2-action diagram must be updated to 3 actions) |
| **1C. Structured Full Strip (3-Action Proposed)** | `raw: false` | `remove(body_field(["rendering_info"]))`, `remove(body_field(["message"]))`, `remove(body_field(["details"]))` | Serialized bytes, % reduction vs. 1A, field integrity, CPU ns/op |
| **2A. Raw XML Baseline** | `raw: true` (`internal/stanzawindows`: `UnmarshalRawEventXML`) | None (`suppress_rendering_info: true` only) | Serialized `plog.Logs` protobuf bytes, XML validity, CPU ns/op |
| **2B. Raw XML Redact (Proposed)** | `raw: true` | `redact(record_field(BODY), regex="<RenderingInfo[^>]*>[\\s\\S]*?</RenderingInfo>", replacement="")` | Serialized bytes, % reduction vs. 2A, XML validity (`<System>` + `<EventData>` intact), CPU ns/op |

---

## 4. Incremental Milestones & Falsifiable Exit Gates

### Milestone 1: Prototype `LogTransformPolicy` & `body_field` in `pkg/googlepolicy` and `googlepolicyprocessor`

- **Deliverable**:
  1. Extend `proto/policy/v1alpha1/log_filter_policy.proto` with `AttributePath body_field = 6` on `LogFieldSelector` and add `proto/policy/v1alpha1/log_transform_policy.proto` (`LogTransformPolicy`, `LogAddAction`, `LogRemoveAction`, `LogRenameAction`, `LogRedactAction`), regenerating `gen/go/policy/v1alpha1/*.pb.go`.
  2. Define `LogPolicyTransformer` and `TransformStage` (`StageRename`, `StageAdd`, `StageRemove`, `StageRedact`) in `pkg/googlepolicy/policy.go`; support `body_field` in `pkg/googlepolicy/logfilter`; and implement `pkg/googlepolicy/logtransform` (registering policy type `"log_transform"` via `ProtoDriver[*policyv1alpha1.LogTransformPolicy]`, allowing empty `matches` where `len(matches) == 0` matches all records, and enforcing fail-open mutation semantics).
  3. Blank-import `_ "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logtransform"` in `googlepolicyprocessor/factory.go`, and update `googlepolicyprocessor` (`Evaluator` and `TransformLogs`) to execute Pass 1 Pre-Transform `LogFilterPolicy` -> Stage-ordered `LogTransformPolicy` (`rename -> add -> remove -> redact`, sorted by `PolicyName()` within each stage) -> Pass 2 Post-Transform `LogFilterPolicy`.
  4. Unit tests in `pkg/googlepolicy/logtransform/policy_test.go`, `pkg/googlepolicy/logfilter/policy_test.go`, and `components/google-built-opentelemetry-collector/processor/googlepolicyprocessor/evaluator_test.go`.
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab5_wef_rendering_info/pkg/googlepolicy && go test -v ./...
  cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && go test -v ./...
  ```
- **Baseline-vs-Change Proof (Falsifiable Exit Gate)**:
  - **Negative Control (No-Op / Disabled Policy)**: With an empty `Evaluator` (or non-matching `matches` condition, e.g. `body_field(["channel"]) == "Application"` on a `"Security"` event), `TransformLogs` leaves `body["rendering_info"]`, `body["message"]`, `body["details"]`, and raw XML `<RenderingInfo>` completely untouched (`0 bytes` reduced).
  - **Positive Control (Enabled Policy)**: With matching (or unconditional empty-`matches`) `LogTransformPolicy` rules, `LogRemoveAction` deletes `body["rendering_info"]`, `body["message"]`, and `body["details"]` from structured map bodies (`raw: false`), `LogRedactAction` strips `<RenderingInfo[^>]*>[\s\S]*?</RenderingInfo>` from string bodies (`raw: true`), stage ordering (`rename -> add -> remove -> redact`) and lexicographical ID tie-breaking hold regardless of input policy slice order, and Pass 2 `LogFilterPolicy` can filter on attributes/fields mutated by `LogTransformPolicy`.

### Milestone 2: Empirical Baseline-vs-Change Proof on Pre-Rendered WEF Events + Live `telemetry.googleapis.com:443` Export in `dashpole-dev`

- **Deliverable**:
  1. Package `components/google-built-opentelemetry-collector/processor/googlepolicyprocessor/internal/stanzawindows` containing the verbatim cross-platform XML unmarshaling (`xml.go`, `security.go`, `config_all.go`) and `pcommon.Value` conversion (`upsertToAttributeVal` from `pkg/stanza/adapter/converter.go`, handling `[]string` slices in `keywords` and `details["Additional Context"]`) from upstream `pkg/stanza@v0.160.0/operator/input/windows`, plus a benchmark and empirical verification test suite (`wef_rendering_info_test.go`) parsing realistic pre-rendered German (`Culture="de-DE"`) WEF XML payloads for Windows Security Event `4624` (`4,869 B` XML with `2,829 B` `<RenderingInfo>`) and Event `4688` (`3,840 B` XML with `2,278 B` `<RenderingInfo>`).
  2. Quantitative comparison table measuring serialized OTLP `plog.ProtoMarshaler` byte sizes and CPU cost (`testing.B` benchmark, serial execution) across all five Configurations (`1A`, `1B`, `1C`, `2A`, and `2B`).
  3. Live end-to-end export test/harness sending transformed WEF OTLP `ExportLogsServiceRequest` batches (both structured `1C` and raw XML `2B`) over TLS/mTLS gRPC (`enterprise-certificate-proxy` on Cloudtop -> `telemetry.mtls.googleapis.com:443` / `telemetry.googleapis.com:443`) in GCP project `dashpole-dev` using `gcloud auth print-access-token`, verified via `gcloud logging read` in `dashpole-dev`.
- **Exact Verification Command**:
  ```bash
  cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && \
    go test -v -run TestWEFRenderingInfo -bench BenchmarkWEFRenderingInfo -benchmem ./...
  ```
- **Baseline-vs-Change Proof (Falsifiable Exit Gate)**:
  - **Baseline (`1A` and `2A`)**: Verify that parsing the pre-rendered WEF XML via `internal/stanzawindows` (`UnmarshalEventXML` + `FormattedBody` + `UpsertToAttributeVal` for `raw: false`) produces a serialized OTLP `LogRecord` of `6,919 B` (Event `4624`) and `5,470 B` (Event `4688`) due to `formattedBody` duplicating the German `<RenderingInfo><Message>` into both `body.rendering_info.message` and `body.message` + `body.details`, and `UnmarshalRawEventXML` (`raw: true`) produces `4,910 B` (Event `4624`) and `3,881 B` (Event `4688`) containing `<RenderingInfo Culture='de-DE'>`.
  - **Change (`1B`, `1C`, and `2B`)**: Verify that `1B` (2-action removal without `["details"]`) leaves `2,669 B` (Event `4624`) and `2,034 B` (Event `4688`) of parsed German prose in `body["details"]`, whereas `1C` (3-action removal with `["details"]`) reduces serialized OTLP `LogRecord` size by `>= 75%` (`-79.0%` down to `1,455 B` for Event `4624`; `-78.1%` down to `1,198 B` for Event `4688`), `2B` (`LogRedactAction` on raw XML) strips 100% of `<RenderingInfo>` (`>= 55%` total event reduction: `-57.6%` down to `2,081 B` for Event `4624`; `-58.7%` down to `1,603 B` for Event `4688`), both preserve 100% of `<System>` (`event_id.id == 4624/4688`, `provider.name == "Microsoft-Windows-Security-Auditing"`, `computer`, `channel`) and `<EventData>` keys, leave valid XML in `2B`, and succeed with gRPC `OK` (`rejected_log_records: 0`) against live Google Cloud Telemetry API in `dashpole-dev`.

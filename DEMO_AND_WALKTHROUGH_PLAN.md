# Demo & Walkthrough Plan: Tab 5 — Pre-Rendered WEF `RenderingInfo` Stripping & Event ID Enrichment

**Target Audience**: Project Tango / Google-Built OpenTelemetry Collector (`GBOC`) & Google SecOps Ingestion Engineers  
**Key Takeaway**: When Windows Event Collector (WEC) subscriptions forward `ForwardedEvents` with `ContentFormat = RenderedText`, `suppress_rendering_info: true` on `windowseventlogreceiver` cannot remove the localized `<RenderingInfo Culture='de-DE'>` already baked into the stored XML (and Stanza's `raw: false` parser actually duplicates the German `<Message>` across `body.rendering_info` and `body.details`, inflating Event `4624` to `6,919 B`); our `LogTransformPolicy` prototype in `pkg/googlepolicy` and `googlepolicyprocessor` strips 100% of that localized prose (`-79.0%` down to `1,455 B` in structured mode with `0` extra allocations, or `-57.6%` down to `2,081 B` in raw XML mode) while preserving 100% of `<System>` and `<EventData>` for Google Cloud / SecOps ingestion.

---

## Step-by-Step Live Terminal Walkthrough

### Step 1: Inspect the `LogFieldSelector.body_field` & `LogTransformPolicy` Protobuf Contract
**Talking Point**: Show how we extended `LogFieldSelector` with `AttributePath body_field = 6` (`log_filter_policy.proto`) so policies can address nested fields inside a structured `LogRecord.Body()` map (`raw: false`), and defined `LogTransformPolicy` (`add`, `remove`, `rename`, `redact`) in `log_transform_policy.proto`.

```bash
cd /tmp/prototypes/tab5_wef_rendering_info && \
  git diff v0.162.0 -- proto/policy/v1alpha1/
```

**Expected Output**:
- `proto/policy/v1alpha1/log_filter_policy.proto`: `AttributePath body_field = 6;` added to `LogFieldSelector.target`.
- `proto/policy/v1alpha1/log_transform_policy.proto`: `LogTransformPolicy`, `LogAddAction`, `LogRemoveAction`, `LogRenameAction`, and `LogRedactAction`.

---

### Step 2: Verify the Engine — 4-Stage Deterministic Ordering, Lexicographical Tie-Breaking, & Double-Applied Filtering
**Talking Point**: Demonstrate that `pkg/googlepolicy/logtransform` and `googlepolicyprocessor` enforce the `go/tango-preview-policies` execution contract: Pass 1 Pre-Transform `LogFilterPolicy` -> `rename -> add -> remove -> redact` (tie-broken lexicographically by Policy ID) -> Pass 2 Post-Transform `LogFilterPolicy`, plus fail-open semantics and destination-before-source safety in `LogRenameAction`.

```bash
cd /tmp/prototypes/tab5_wef_rendering_info/pkg/googlepolicy && \
  go test -v ./logfilter/... ./logtransform/... -run "TestBodyFieldMatcher|TestValidation|TestLogRemoveAction_BodyField|TestLogRedactAction_RawXMLBody|TestLogAddAndRenameActions|TestRegistryIntegration"
cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && \
  go test -v -run TestEvaluator_LogTransformStageOrderingAndDoubleFilter ./...
```

**Expected Output**:
```text
=== RUN   TestBodyFieldMatcher
--- PASS: TestBodyFieldMatcher (0.00s)
=== RUN   TestValidation
--- PASS: TestValidation (0.00s)
=== RUN   TestLogRemoveAction_BodyField
--- PASS: TestLogRemoveAction_BodyField (0.00s)
=== RUN   TestLogRedactAction_RawXMLBody
--- PASS: TestLogRedactAction_RawXMLBody (0.00s)
=== RUN   TestLogAddAndRenameActions
--- PASS: TestLogAddAndRenameActions (0.00s)
=== RUN   TestRegistryIntegration
--- PASS: TestRegistryIntegration (0.00s)
=== RUN   TestEvaluator_LogTransformStageOrderingAndDoubleFilter
--- PASS: TestEvaluator_LogTransformStageOrderingAndDoubleFilter (0.00s)
```

---

### Step 3: Run the Before-vs-After Empirical WEF Comparison (`1A`, `1B`, `1C`, `2A`, `2B`) & CPU Benchmarks
**Talking Point**: Run the verbatim upstream Stanza Windows XML parser (`internal/stanzawindows`) on realistic German (`Culture='de-DE'`) WEF Security Event `4624` (`4,869 B` XML) and Event `4688` (`3,840 B` XML) payloads across all 5 configurations:
- **Before (`1A` Structured Baseline)**: `6,919 B` (Event `4624`) and `5,470 B` (Event `4688`) — `+42%` larger than the raw XML because Stanza's `formattedBody` + `parseSecurity` duplicates the German `<RenderingInfo><Message>` into both `body["rendering_info"]` (`2,739 B`) and `body["details"]` (`2,669 B`).
- **Why 2 Actions Are Not Enough (`1B` Partial Strip)**: Removing only `rendering_info` and `message` leaves **`2,669 B`** (Event `4624`) and **`2,034 B`** (Event `4688`) of German prose inside `body["details"]`.
- **After — Structured 3-Action Strip (`1C`)**: Removing `rendering_info`, `message`, and `details` drops Event `4624` to **`1,455 B` (`-79.0%`)** and Event `4688` to **`1,198 B` (`-78.1%`)** in **`~1.1 µs` with `0 B/op` and `0 allocs/op`** incremental overhead, while preserving 100% of `<System>` and all **27** (`4624`) / **15** (`4688`) `event_data` keys.
- **After — Raw XML Redact (`2B`)**: `LogRedactAction` strips 100% of `<RenderingInfo>` (`4,910 B -> 2,081 B`, **`-57.6%`** on Event `4624`; `3,881 B -> 1,603 B`, **`-58.7%`** on Event `4688`), leaving valid XML with all `<System>` and `<EventData>` elements intact.

```bash
cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && \
  go test -v -run TestWEFRenderingInfo_EmpiricalComparison -bench BenchmarkWEFRenderingInfo -benchmem ./...
```

**Expected Output**:
```text
=== RUN   TestWEFRenderingInfo_EmpiricalComparison
=== RUN   TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4624_Logon_deDE
    wef_rendering_info_test.go:561: [Security_Event_4624_Logon_deDE] Raw XML input=4869 B | 1A(Structured Baseline)=6919 B | 1B(2-Action Partial)=4124 B (-40.4%, residual details=2669 B) | 1C(3-Action Full)=1455 B (-79.0%) | 2A(Raw Baseline)=4910 B | 2B(Raw Redact)=2081 B (-57.6%)
=== RUN   TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4688_ProcessCreate_deDE
    wef_rendering_info_test.go:561: [Security_Event_4688_ProcessCreate_deDE] Raw XML input=3840 B | 1A(Structured Baseline)=5470 B | 1B(2-Action Partial)=3232 B (-40.9%, residual details=2034 B) | 1C(3-Action Full)=1198 B (-78.1%) | 2A(Raw Baseline)=3881 B | 2B(Raw Redact)=1603 B (-58.7%)
--- PASS: TestWEFRenderingInfo_EmpiricalComparison (0.01s)
BenchmarkWEFRenderingInfo/1A_StructuredBaseline_NoOp-24         	   62293	     18868 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/1B_StructuredPartialStrip_2Action-24  	   62623	     19565 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/1C_StructuredFullStrip_3Action-24     	   60146	     20003 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/2A_RawXMLBaseline_NoOp-24             	  956107	      1088 ns/op	     440 B/op	       9 allocs/op
BenchmarkWEFRenderingInfo/2B_RawXMLRedact_Regex-24              	    9981	    111961 ns/op	    4597 B/op	      13 allocs/op
```

---

### Step 4: Live End-to-End OTLP Export to Google Cloud Telemetry API (`dashpole-dev`) & Cloud Logging Inspection
**Talking Point**: Execute the live OTLP gRPC export test (`TestWEFRenderingInfo_LiveCloudLoggingExport`) sending transformed structured (`1C` + `LogAddAction` Event ID enrichment `event.action = "user-login" | "process-created"`) and raw XML (`2B`) WEF logs to `telemetry.mtls.googleapis.com:443` in `dashpole-dev`, then query Cloud Logging to inspect the ingested records.

```bash
cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && \
  go test -v -run TestWEFRenderingInfo_LiveCloudLoggingExport ./... && \
  gcloud logging read 'labels."wef.mode" != ""' --project=dashpole-dev --limit=4 --format=json
```

**Expected Output**:
- `TestWEFRenderingInfo_LiveCloudLoggingExport` logs:
  `Successfully exported 4 transformed WEF log records (2 structured 1C + 2 raw XML 2B) to telemetry.mtls.googleapis.com:443 in project dashpole-dev (rejected_log_records=0, error_message="")`
- `gcloud logging read` returns the 4 ingested entries in `projects/dashpole-dev/logs/otlp`:
  * `labels["wef.mode"] = "1C_structured_full_strip"` with `labels["event.action"] = "user-login"` (`event_id.id = 4624`, all 27 `event_data` attributes) and `"process-created"` (`event_id.id = 4688`, all 15 `event_data` attributes), and zero `rendering_info`, `message`, or `details` fields in `jsonPayload`.
  * `labels["wef.mode"] = "2B_raw_xml_redact"` with clean `<Event><System>...</System><EventData>...</EventData></Event>` XML in `textPayload` and `<RenderingInfo>` completely stripped.

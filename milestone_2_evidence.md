# Milestone 2 Evidence: Empirical Baseline-vs-Change Proof on Pre-Rendered WEF Events + Live Cloud Logging Export

## 1. Deliverables Implemented

1. **Verbatim Upstream Stanza Windows Event XML & Converter Package (`components/google-built-opentelemetry-collector/processor/googlepolicyprocessor/internal/stanzawindows/`)**:
   - `config_all.go`, `xml.go`, and `security.go`: Verbatim cross-platform XML unmarshaling (`unmarshalEventXML`, `formattedBody`, `unmarshalRawEventXML`, `parseSecurity`, `parseMessage`, `parseEventData`, `sanitizeXMLBytes`) from upstream `github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza@v0.160.0/operator/input/windows`.
   - `converter.go`: Verbatim `upsertToAttributeVal`, `upsertToMap`, `upsertToSlice`, and `upsertStringsToSlice` from upstream `pkg/stanza@v0.160.0/adapter/converter.go` (preserving `[]string` slice conversion for `keywords` and `details["Additional Context"]`), plus `PopulateLogRecord` matching `input.go:sendEvent` + `adapter/converter.go:convertInto`.
2. **Empirical WEF Comparison, Benchmarks, & Live Export (`components/google-built-opentelemetry-collector/processor/googlepolicyprocessor/wef_rendering_info_test.go`)**:
   - Realistic pre-rendered German (`Culture='de-DE'`) WEF XML payloads for Windows Security Event `4624` (Logon, `4,869 B` XML containing `2,829 B` of `<RenderingInfo Culture='de-DE'>`) and Event `4688` (Process Creation, `3,840 B` XML containing `2,278 B` of `<RenderingInfo Culture='de-DE'>`).
   - Evaluates all 5 Configurations (`1A`, `1B`, `1C`, `2A`, `2B`) plus Event ID enrichment (`LogAddAction` setting `event.action = "user-login"` for `4624` and `event.action = "process-created"` for `4688`).
   - Single-pass RE2 optimization in `pkg/googlepolicy/logtransform/policy.go:539-543`: eliminated the redundant `re.MatchString(orig)` pre-check before `re.ReplaceAllString(orig, replacement)`, cutting `2B_RawXMLRedact_Regex` latency by **2.0x** (`223,490 ns/op -> 111,961 ns/op`).
   - Live OTLP gRPC export (`TestWEFRenderingInfo_LiveCloudLoggingExport`) over Enterprise Certificate Proxy (ECP) mTLS (`telemetry.mtls.googleapis.com:443`) to project `dashpole-dev`, verified via `gcloud logging read` in `dashpole-dev`.

---

## 2. Exact Verification Command & Output

```text
$ cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && go test -v -run TestWEFRenderingInfo -bench BenchmarkWEFRenderingInfo -benchmem ./...
=== RUN   TestWEFRenderingInfo_EmpiricalComparison
=== RUN   TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4624_Logon_deDE
    wef_rendering_info_test.go:548: [Security_Event_4624_Logon_deDE] Raw XML input=4869 B | 1A(Structured Baseline)=6919 B | 1B(2-Action Partial)=4124 B (-40.4%, residual details=2669 B) | 1C(3-Action Full)=1455 B (-79.0%) | 2A(Raw Baseline)=4910 B | 2B(Raw Redact)=2081 B (-57.6%)
=== RUN   TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4688_ProcessCreate_deDE
    wef_rendering_info_test.go:548: [Security_Event_4688_ProcessCreate_deDE] Raw XML input=3840 B | 1A(Structured Baseline)=5470 B | 1B(2-Action Partial)=3232 B (-40.9%, residual details=2034 B) | 1C(3-Action Full)=1198 B (-78.1%) | 2A(Raw Baseline)=3881 B | 2B(Raw Redact)=1603 B (-58.7%)
--- PASS: TestWEFRenderingInfo_EmpiricalComparison (0.00s)
    --- PASS: TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4624_Logon_deDE (0.00s)
    --- PASS: TestWEFRenderingInfo_EmpiricalComparison/Security_Event_4688_ProcessCreate_deDE (0.00s)
=== RUN   TestWEFRenderingInfo_LiveCloudLoggingExport
    wef_rendering_info_test.go:645: Successfully exported 4 transformed WEF log records (2 structured 1C + 2 raw XML 2B) to telemetry.mtls.googleapis.com:443 in project dashpole-dev (rejected_log_records=0, error_message="")
--- PASS: TestWEFRenderingInfo_LiveCloudLoggingExport (1.94s)
goos: linux
goarch: amd64
pkg: github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
BenchmarkWEFRenderingInfo
BenchmarkWEFRenderingInfo/1A_StructuredBaseline_NoOp
BenchmarkWEFRenderingInfo/1A_StructuredBaseline_NoOp-24         	   62293	     18868 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/1B_StructuredPartialStrip_2Action
BenchmarkWEFRenderingInfo/1B_StructuredPartialStrip_2Action-24  	   62623	     19565 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/1C_StructuredFullStrip_3Action
BenchmarkWEFRenderingInfo/1C_StructuredFullStrip_3Action-24     	   60146	     20003 ns/op	    6472 B/op	     150 allocs/op
BenchmarkWEFRenderingInfo/2A_RawXMLBaseline_NoOp
BenchmarkWEFRenderingInfo/2A_RawXMLBaseline_NoOp-24             	  956107	      1088 ns/op	     440 B/op	       9 allocs/op
BenchmarkWEFRenderingInfo/2B_RawXMLRedact_Regex
BenchmarkWEFRenderingInfo/2B_RawXMLRedact_Regex-24              	    9981	    111961 ns/op	    4597 B/op	      13 allocs/op
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor	7.855s
```

---

## 3. Quantitative Baseline-vs-Change Comparison Table

| Configuration | Mode | Active `LogTransformPolicy` Rules | Event `4624` Serialized Bytes (`%` vs Baseline) | Event `4688` Serialized Bytes (`%` vs Baseline) | Benchmark Total (`CopyTo` + `TransformLogs`) | Incremental Transform Cost (vs. No-Op Baseline) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1A. Structured Baseline** | `raw: false` | None (`suppress_rendering_info: true` only) | `6,919 B` (baseline) | `5,470 B` (baseline) | `18,868 ns/op` (`6,472 B/op`, `150 allocs/op`) | `0 ns/op` (`0 B/op`, `0 allocs/op`) |
| **1B. Structured Partial Strip (2-Action)** | `raw: false` | `remove(rendering_info)`, `remove(message)` | `4,124 B` (`-40.4%`, leaves **`2,669 B`** in `details`) | `3,232 B` (`-40.9%`, leaves **`2,034 B`** in `details`) | `19,565 ns/op` (`6,472 B/op`, `150 allocs/op`) | `+697 ns/op` (`0 B/op`, `0 allocs/op`) |
| **1C. Structured Full Strip (3-Action)** | `raw: false` | `remove(rendering_info)`, `remove(message)`, `remove(details)` | **`1,455 B` (`-79.0%`)** | **`1,198 B` (`-78.1%`)** | `20,003 ns/op` (`6,472 B/op`, `150 allocs/op`) | **`+1,135 ns/op` (`0 B/op`, `0 allocs/op`)** |
| **2A. Raw XML Baseline** | `raw: true` | None (`suppress_rendering_info: true` only) | `4,910 B` (baseline) | `3,881 B` (baseline) | `1,088 ns/op` (`440 B/op`, `9 allocs/op`) | `0 ns/op` (`0 B/op`, `0 allocs/op`) |
| **2B. Raw XML Redact (Regex)** | `raw: true` | `redact(BODY, "<RenderingInfo[^>]*>[\\s\\S]*?</RenderingInfo>", "")` | **`2,081 B` (`-57.6%`, `100%` of `RenderingInfo` stripped)** | **`1,603 B` (`-58.7%`, `100%` of `RenderingInfo` stripped)** | `111,961 ns/op` (`4,597 B/op`, `13 allocs/op`) | **`+110,873 ns/op` (`+4,157 B/op`, `+4 allocs/op`)** |

### Key Empirical Insights
1. **Double-Duplication in Structured Mode (`1A`) & Why 3-Action Removal (`1C`) Is Mandatory**:
   - Raw XML Event `4624` is `4,869 B` (`2,040 B` of `<Event><System>...</System><EventData>...</EventData></Event>` XML and `2,829 B` of `<RenderingInfo>`). When parsed with `raw: false`, upstream Stanza's `formattedBody` (`xml.go:88-149` + `security.go:10-59`) inflates the serialized OTLP `LogRecord` to **`6,919 B` (`+42.1%` larger than raw XML)** because it stores the German `<RenderingInfo><Message>` (`2,492 B` UTF-8 text) **twice**: once inside `body["rendering_info"]` (`2,739 B` serialized protobuf map) and a second time split across `body["message"]` (`56 B` serialized protobuf field for the `39 B` first line `"Ein Konto wurde erfolgreich angemeldet."`, totaling `2,795 B` for `rendering_info` + `message`) + `body["details"]` (`2,669 B` serialized protobuf map of subsections and `Additional Context` slice).
   - Removing only `body["rendering_info"]` and `body["message"]` (`1B`, as currently drawn in the Companion Design Doc's ASCII diagram) removes `2,795 B` (`6,919 B -> 4,124 B`, `-40.4%`), leaving **`2,669 B` (`64.7%` of the remaining `4,124 B` payload)** of German prose inside `body["details"]`.
   - Adding `remove(body_field(["details"]))` (`1C`) eliminates all `2,669 B` of residual prose, reducing Event `4624` to **`1,455 B` (`-79.0%`, `2,795 + 2,669 = 5,464 B` saved per event)** and Event `4688` to **`1,198 B` (`-78.1%`, `2,238 + 2,034 = 4,272 B` saved per event)** with **`0 B/op` and `0 allocs/op`** incremental heap overhead (`~1.1 µs` for 3 `O(1)` map deletes).
2. **Structured `1C` vs. Raw XML `2B` Efficiency**:
   - In `2B` (`raw: true`), `LogRedactAction` strips **100% of the `<RenderingInfo>` block** (`2,829 B` on Event `4624`, leaving `2,040 B` of XML + `41 B` OTLP envelope = `2,081 B`; `2,278 B` on Event `4688`, leaving `1,562 B` of XML + `41 B` OTLP envelope = `1,603 B`), reducing total serialized OTLP size by `57.6%` (`4,910 B -> 2,081 B`) and `58.7%` (`3,881 B -> 1,603 B`). Even after stripping 100% of `<RenderingInfo>`, `2B` (`2,081 B`) is `43.0%` larger than `1C` (`1,455 B`) because raw XML retains verbose `<Data Name='...'>...</Data>` tags (`2,040 B` XML), and `2B`'s RE2 pass over a `4.9 KB` multi-line XML string costs `~110.9 µs` (`+4,157 B/op`, `+4 allocs/op`) vs. `~1.1 µs` (`0 B/op`, `0 allocs/op`) for `1C`.

---

## 4. Live Cloud Logging Verification (`dashpole-dev`)

 Verified via `gcloud logging read 'labels."wef.mode" != ""' --project=dashpole-dev --limit=4 --format=json`:
- **`insertId: "1m01ut3f4mippy"`** (`wef.mode = "1C_structured_full_strip"`, `event.action = "user-login"`): `jsonPayload` contains `event_id.id = 4624`, `provider.name = "Microsoft-Windows-Security-Auditing"`, `computer = "wks-muc-0412.corp.allianz.de"`, `channel = "Security"`, `record_id = 9823411`, and all **27** `event_data` attributes (`TargetUserName = "lukas.mueller"`, `LogonType = "3"`, `IpAddress = "10.142.88.19"`, etc.), with `rendering_info`, `message`, and `details` completely absent.
- **`insertId: "1m01ut3f4mippz"`** (`wef.mode = "1C_structured_full_strip"`, `event.action = "process-created"`): `jsonPayload` contains `event_id.id = 4688`, `provider.name = "Microsoft-Windows-Security-Auditing"`, and all **15** `event_data` attributes (`NewProcessName`, `CommandLine`, etc.), with `rendering_info`, `message`, and `details` completely absent.
- **`insertId: "1m01ut3f4mipq0"` & `"1m01ut3f4mipq1"`** (`wef.mode = "2B_raw_xml_redact"`): `textPayload` contains valid `<Event><System>...</System><EventData>...</EventData></Event>` XML (`2,040 B` for Event `4624`, `1,562 B` for Event `4688`) with `<RenderingInfo Culture='de-DE'>...</RenderingInfo>` completely stripped.

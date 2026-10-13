# Milestone 1 Evidence: `LogTransformPolicy` & `body_field` in `pkg/googlepolicy` and `googlepolicyprocessor`

## 1. Deliverables Implemented

1. **Protobuf & Generated Code (`proto/policy/v1alpha1/` & `gen/go/policy/v1alpha1/`)**:
   - Extended `LogFieldSelector` in `proto/policy/v1alpha1/log_filter_policy.proto` with `AttributePath body_field = 6` to traverse structured (`ValueTypeMap`) `LogRecord.Body()` maps.
   - Added `proto/policy/v1alpha1/log_transform_policy.proto` defining `LogTransformPolicy` (`id`, optional `repeated LogMatcher matches`, and `oneof action`: `LogAddAction add`, `LogRemoveAction remove`, `LogRenameAction rename`, `LogRedactAction redact`).
   - Regenerated `gen/go/policy/v1alpha1/log_filter_policy.pb.go` and `gen/go/policy/v1alpha1/log_transform_policy.pb.go` via `buf generate`.

2. **`pkg/googlepolicy` & `pkg/googlepolicy/logtransform`**:
   - Defined `TransformStage` (`StageRename = 1`, `StageAdd = 2`, `StageRemove = 3`, `StageRedact = 4`) and `LogPolicyTransformer` (`Stage() TransformStage`, `TransformLog(ctx LogContext) bool`) in `pkg/googlepolicy/policy.go`.
   - Added `LogFieldSelector_BodyField` extraction and exported `CompiledMatcher` / `CompileMatcher` in `pkg/googlepolicy/logfilter/policy.go`.
   - Implemented `pkg/googlepolicy/logtransform/policy.go` registering policy type `"log_transform"` via `googlepolicy.ProtoDriver[*policyv1alpha1.LogTransformPolicy]`:
     * Optional `matches` (`len(matches) == 0` matches all log records; non-empty `matches` are ANDed).
     * `LogRemoveAction`: `O(1)` map key deletion (`m.Remove(key)`) for single-segment paths (`["rendering_info"]`, `["message"]`, `["details"]`), nested map traversal for multi-segment paths, and fail-open (`return false`) when target is absent or `LogRecord.Body()` is not a map.
     * `LogRedactAction`: pre-compiles RE2 `regexp.Regexp` at policy load time (`NewPolicyFromProto`), skips allocation when `!re.MatchString(orig)`, and applies `re.ReplaceAllString(orig, replacement)` in place on `record_field: LOG_RECORD_FIELD_BODY` (`ValueTypeStr`), `severity_text`, or string map attributes/`body_field`s.
     * `LogRenameAction` & `LogAddAction`: supports `upsert: false` (default) and `upsert: true` across `log_attribute`, `resource_attribute`, `scope_attribute`, `body_field`, and `record_field`. `LogRenameAction` guards self-rename (`proto.Equal(rn.GetFrom(), rn.GetTo())` is a safe no-op returning `false`) and writes `toTarget.set` before calling `fromTarget.remove` so a failed destination write (e.g., `body_field` on a string-bodied record) never destroys the source field.

3. **`googlepolicyprocessor` (`factory.go`, `evaluator.go`, `log_transformer.go`)**:
   - Blank-imported `_ "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logtransform"` in `factory.go`.
   - Updated `NewEvaluator` in `evaluator.go` to collect `googlepolicy.LogPolicyTransformer` policies and sort them deterministically by `(Stage(), PolicyName())` (`rename -> add -> remove -> redact`, with lexicographical Policy ID tie-breaking).
   - Updated `TransformLogs` and `EvalLog` in `log_transformer.go` to execute Double-Applied Filtering:
     * **Pass 1 (Pre-Transform Filter)**: Evaluates `LogFilterPolicy` rules first; dropped records are pruned immediately without running transforms.
     * **Transform Sequence**: Runs pre-sorted `logTransformPolicies` in place and tracks whether any policy mutated the record (`mutated`).
     * **Pass 2 (Post-Transform Filter)**: Re-evaluates `LogFilterPolicy` rules if `mutated && len(e.logPolicies) > 0`.

---

## 2. Verification Commands & Outputs

### Protobuf Generation & Tests
```text
$ cd /tmp/prototypes/tab5_wef_rendering_info && PATH="/tmp/prototypes/tab5_wef_rendering_info/.tools:$PATH" /tmp/prototypes/tab5_wef_rendering_info/.tools/buf generate && go test -v ./gen/go/...
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1	0.122s
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1	0.109s
```

### `pkg/googlepolicy` Unit Tests (`logfilter` & `logtransform`)
```text
$ cd /tmp/prototypes/tab5_wef_rendering_info/pkg/googlepolicy && go test -v ./logfilter/... ./logtransform/...
=== RUN   TestBodyFieldMatcher
--- PASS: TestBodyFieldMatcher (0.00s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logfilter	0.101s
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
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logtransform	0.103s
```

### `googlepolicyprocessor` Unit Tests
```text
$ cd /tmp/prototypes/tab5_wef_rendering_info/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor && go test -v ./...
=== RUN   TestEvaluator_LogTransformStageOrderingAndDoubleFilter
--- PASS: TestEvaluator_LogTransformStageOrderingAndDoubleFilter (0.00s)
PASS
ok  	github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/googlepolicyprocessor	0.155s
```

---

## 3. Baseline-vs-Change Proof (Non-Tautological Validation)

1. **Negative Control (No-Op / Non-Matching Policy)**:
   - In `TestEvaluator_NoPoliciesIsNoOp` (`evaluator_test.go:484-490`), an empty `Evaluator` leaves `plog.Logs` completely untouched.
   - In `TestLogRemoveAction_BodyField` (`pkg/googlepolicy/logtransform/policy_test.go:88-115`), a `LogTransformPolicy` with `matches: [body_field(["channel"]) == "Application"]` evaluated against a `"Security"` event returns `false` from `TransformLog(ctx)` and leaves `body["rendering_info"]`, `body["message"]`, and `body["details"]` intact (`exists == true`).
   - In `TestBodyFieldMatcher` (`pkg/googlepolicy/logfilter/policy_test.go:1734-1765`), evaluating `body_field(["event_id", "id"]) == 4624` against a string-bodied (`ValueTypeStr`) log record fails open (`EvalNoMatch`), whereas evaluating against a structured map body with `event_id.id = 4624` returns `EvalDrop`.
2. **Positive Control (Enabled Policy & Deterministic Ordering)**:
   - In `TestLogRemoveAction_BodyField` (`policy_test.go:118-150`), applying `LogRemoveAction` on `["rendering_info"]`, `["message"]`, and `["details"]` removes all three fields (`hasRI == false`, `hasMsg == false`, `hasDetails == false`) while preserving `body["event_id"]["id"] == 4624` and `body["channel"] == "Security"`. Calling `TransformLog(ctx)` a second time after removal returns `false` (proving `TransformLog` only reports `true` when an actual mutation occurs).
   - In `TestLogRedactAction_RawXMLBody` (`policy_test.go:152-190`), applying `LogRedactAction` with `regex: "<RenderingInfo[^>]*>[\\s\\S]*?</RenderingInfo>"` and `replacement: ""` strips the multi-line German `<RenderingInfo Culture='de-DE'>...</RenderingInfo>` block from the XML string body while preserving `<System>` and `<EventData>`.
   - In `TestLogAddAndRenameActions` (`policy_test.go:258-310`), self-rename (`from == to` with `upsert: true`) and failed destination write (`log_attribute` -> `body_field` when `Body()` is a string) both return `false` and preserve the source attribute intact.
   - In `TestEvaluator_LogTransformStageOrderingAndDoubleFilter` (`evaluator_test.go:1177-1325`), supplying policies in reverse stage and reverse lexicographical order (`redact`, `remove`, `b-add-tier`, `a-add-tier`, `add-user-id`, `rename`) deterministically executes `rename -> add (a-add-tier -> add-user-id -> b-add-tier) -> remove -> redact` (renaming `legacy_user_id` -> `user.id`, skipping `add-user-id` with `upsert: false`, setting `tier` via `a-add-tier` and overwriting it via `b-add-tier` with `upsert: true`, removing `body.rendering_info`, and redacting `secret-123` inside the newly renamed `user.id`), and Pass 2 `LogFilterPolicy` drops Record 2 based on its post-rename `user.id` attribute.

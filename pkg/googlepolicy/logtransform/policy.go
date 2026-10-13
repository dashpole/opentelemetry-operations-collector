// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package logtransform compiles and evaluates LogTransformPolicy configurations.
package logtransform

import (
	"errors"
	"fmt"
	"regexp"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/internal/matcher"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy/logfilter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/protobuf/proto"
)

// PolicyType is the registered policy type identifier for log transform policies.
const PolicyType = "log_transform"

var driver = googlepolicy.ProtoDriver[*policyv1alpha1.LogTransformPolicy]{
	New: func(pb *policyv1alpha1.LogTransformPolicy) (googlepolicy.Policy, error) {
		return NewPolicyFromProto(pb)
	},
}

func init() {
	if err := googlepolicy.RegisterPolicyDriver(PolicyType, driver); err != nil {
		panic(err)
	}
}

var (
	// ErrNilProto is returned when a nil LogTransformPolicy protobuf is provided.
	ErrNilProto = errors.New("log transform policy proto cannot be nil")
	// ErrMissingID is returned when the policy ID is empty.
	ErrMissingID = errors.New("log transform policy id cannot be empty")
	// ErrMissingAction is returned when the policy action oneof is unset.
	ErrMissingAction = errors.New("log transform policy must specify an action (add, remove, rename, or redact)")
	// ErrMissingTarget is returned when an action omits a required target field selector.
	ErrMissingTarget = errors.New("log transform action must specify a valid target field selector")
	// ErrUnsupportedTarget is returned when an action targets a read-only or unsupported field.
	ErrUnsupportedTarget = errors.New("log transform action targets an unsupported field selector")
	// ErrMissingValue is returned when LogAddAction omits its value.
	ErrMissingValue = errors.New("log add action must specify a valid value")
)

type transformOp func(ctx googlepolicy.LogContext) bool

// Policy represents a compiled, validated LogTransformPolicy ready for hot-loop execution.
type Policy struct {
	proto    *policyv1alpha1.LogTransformPolicy
	stage    googlepolicy.TransformStage
	matchers []logfilter.CompiledMatcher
	op       transformOp
}

var _ googlepolicy.TransformationPolicy = (*Policy)(nil)
var _ googlepolicy.LogPolicyTransformer = (*Policy)(nil)

// NewPolicyFromProto validates and compiles a LogTransformPolicy protobuf into a Policy.
func NewPolicyFromProto(pb *policyv1alpha1.LogTransformPolicy) (*Policy, error) {
	if pb == nil {
		return nil, ErrNilProto
	}
	if pb.GetId() == "" {
		return nil, ErrMissingID
	}

	matchers := make([]logfilter.CompiledMatcher, 0, len(pb.GetMatches()))
	for i, m := range pb.GetMatches() {
		cm, err := logfilter.CompileMatcher(m)
		if err != nil {
			return nil, fmt.Errorf("matcher[%d]: %w", i, err)
		}
		matchers = append(matchers, cm)
	}

	stage, op, err := compileAction(pb)
	if err != nil {
		return nil, err
	}

	return &Policy{
		proto:    pb,
		stage:    stage,
		matchers: matchers,
		op:       op,
	}, nil
}

// PolicyName returns the unique identifier of this policy instance.
func (p *Policy) PolicyName() string {
	return p.proto.GetId()
}

// PolicyType returns the type identifier of this policy ("log_transform").
func (p *Policy) PolicyType() string {
	return PolicyType
}

// PolicyClass returns PolicyClassTransformation for log transform policies.
func (p *Policy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassTransformation
}

// TargetSignals returns the telemetry signals targeted by this policy (SignalLogs).
func (p *Policy) TargetSignals() []googlepolicy.Signal {
	return []googlepolicy.Signal{googlepolicy.SignalLogs}
}

// Validate checks whether the compiled policy is valid.
func (p *Policy) Validate() error {
	if p.proto == nil {
		return ErrNilProto
	}
	return nil
}

// Stage returns the deterministic execution stage of this transform policy.
func (p *Policy) Stage() googlepolicy.TransformStage {
	return p.stage
}

// Proto returns the underlying protobuf message for this policy.
func (p *Policy) Proto() proto.Message {
	return p.proto
}

// TransformLog evaluates the policy's matchers (if any) and applies the
// compiled transformation action in place. It returns true if the log record
// was modified.
func (p *Policy) TransformLog(ctx googlepolicy.LogContext) bool {
	for i := range p.matchers {
		if !p.matchers[i].Eval(ctx) {
			return false
		}
	}
	return p.op(ctx)
}

func compileAction(pb *policyv1alpha1.LogTransformPolicy) (googlepolicy.TransformStage, transformOp, error) {
	if pb.GetAction() == nil {
		return 0, nil, ErrMissingAction
	}

	switch act := pb.GetAction().(type) {
	case *policyv1alpha1.LogTransformPolicy_Rename:
		op, err := compileRename(act.Rename)
		if err != nil {
			return 0, nil, err
		}
		return googlepolicy.StageRename, op, nil

	case *policyv1alpha1.LogTransformPolicy_Add:
		op, err := compileAdd(act.Add)
		if err != nil {
			return 0, nil, err
		}
		return googlepolicy.StageAdd, op, nil

	case *policyv1alpha1.LogTransformPolicy_Remove:
		op, err := compileRemove(act.Remove)
		if err != nil {
			return 0, nil, err
		}
		return googlepolicy.StageRemove, op, nil

	case *policyv1alpha1.LogTransformPolicy_Redact:
		op, err := compileRedact(act.Redact)
		if err != nil {
			return 0, nil, err
		}
		return googlepolicy.StageRedact, op, nil

	default:
		return 0, nil, ErrMissingAction
	}
}

// mapRootAccessor resolves the root pcommon.Map for attribute or body_field selectors.
type mapRootAccessor func(ctx googlepolicy.LogContext) (pcommon.Map, bool)

type compiledMapTarget struct {
	root  mapRootAccessor
	steps []matcher.PathStep
}

func compileMapTarget(sel *policyv1alpha1.LogFieldSelector) (*compiledMapTarget, error) {
	if sel == nil || sel.Target == nil {
		return nil, ErrMissingTarget
	}

	switch t := sel.Target.(type) {
	case *policyv1alpha1.LogFieldSelector_LogAttribute:
		steps, err := matcher.CompilePath(t.LogAttribute, "log")
		if err != nil {
			return nil, err
		}
		return &compiledMapTarget{
			root: func(ctx googlepolicy.LogContext) (pcommon.Map, bool) {
				if ctx.Record == (plog.LogRecord{}) {
					return pcommon.Map{}, false
				}
				return ctx.Record.Attributes(), true
			},
			steps: steps,
		}, nil

	case *policyv1alpha1.LogFieldSelector_ResourceAttribute:
		steps, err := matcher.CompilePath(t.ResourceAttribute, "resource")
		if err != nil {
			return nil, err
		}
		return &compiledMapTarget{
			root: func(ctx googlepolicy.LogContext) (pcommon.Map, bool) {
				if ctx.Resource == (pcommon.Resource{}) {
					return pcommon.Map{}, false
				}
				return ctx.Resource.Attributes(), true
			},
			steps: steps,
		}, nil

	case *policyv1alpha1.LogFieldSelector_ScopeAttribute:
		steps, err := matcher.CompilePath(t.ScopeAttribute, "scope")
		if err != nil {
			return nil, err
		}
		return &compiledMapTarget{
			root: func(ctx googlepolicy.LogContext) (pcommon.Map, bool) {
				if ctx.Scope == (pcommon.InstrumentationScope{}) {
					return pcommon.Map{}, false
				}
				return ctx.Scope.Attributes(), true
			},
			steps: steps,
		}, nil

	case *policyv1alpha1.LogFieldSelector_BodyField:
		steps, err := matcher.CompilePath(t.BodyField, "body")
		if err != nil {
			return nil, err
		}
		return &compiledMapTarget{
			root: func(ctx googlepolicy.LogContext) (pcommon.Map, bool) {
				if ctx.Record == (plog.LogRecord{}) || ctx.Record.Body().Type() != pcommon.ValueTypeMap {
					return pcommon.Map{}, false
				}
				return ctx.Record.Body().Map(), true
			},
			steps: steps,
		}, nil

	default:
		return nil, ErrUnsupportedTarget
	}
}

func (mt *compiledMapTarget) get(ctx googlepolicy.LogContext) (pcommon.Value, bool) {
	m, ok := mt.root(ctx)
	if !ok || len(mt.steps) == 0 {
		return pcommon.Value{}, false
	}
	val, ok := m.Get(mt.steps[0].Key)
	if !ok {
		return pcommon.Value{}, false
	}
	for _, step := range mt.steps[1:] {
		switch val.Type() {
		case pcommon.ValueTypeMap:
			child, found := val.Map().Get(step.Key)
			if !found {
				return pcommon.Value{}, false
			}
			val = child
		case pcommon.ValueTypeSlice:
			if !step.IsIndex || step.Index >= val.Slice().Len() {
				return pcommon.Value{}, false
			}
			val = val.Slice().At(step.Index)
		default:
			return pcommon.Value{}, false
		}
	}
	return val, true
}

func (mt *compiledMapTarget) remove(ctx googlepolicy.LogContext) bool {
	m, ok := mt.root(ctx)
	if !ok || len(mt.steps) == 0 {
		return false
	}
	if len(mt.steps) == 1 {
		return m.Remove(mt.steps[0].Key)
	}
	val, ok := m.Get(mt.steps[0].Key)
	if !ok {
		return false
	}
	for _, step := range mt.steps[1 : len(mt.steps)-1] {
		switch val.Type() {
		case pcommon.ValueTypeMap:
			child, found := val.Map().Get(step.Key)
			if !found {
				return false
			}
			val = child
		case pcommon.ValueTypeSlice:
			if !step.IsIndex || step.Index >= val.Slice().Len() {
				return false
			}
			val = val.Slice().At(step.Index)
		default:
			return false
		}
	}
	if val.Type() != pcommon.ValueTypeMap {
		return false
	}
	return val.Map().Remove(mt.steps[len(mt.steps)-1].Key)
}

func (mt *compiledMapTarget) set(ctx googlepolicy.LogContext, assign func(dest pcommon.Value), upsert bool) bool {
	m, ok := mt.root(ctx)
	if !ok || len(mt.steps) == 0 {
		return false
	}
	parent := m
	for _, step := range mt.steps[:len(mt.steps)-1] {
		child, found := parent.Get(step.Key)
		if !found {
			parent = parent.PutEmptyMap(step.Key)
			continue
		}
		if child.Type() != pcommon.ValueTypeMap {
			return false
		}
		parent = child.Map()
	}
	lastKey := mt.steps[len(mt.steps)-1].Key
	if !upsert {
		if _, exists := parent.Get(lastKey); exists {
			return false
		}
	}
	assign(parent.PutEmpty(lastKey))
	return true
}

func compileValueAssigner(v *policyv1alpha1.Value) (func(dest pcommon.Value), error) {
	if v == nil || v.GetValue() == nil {
		return nil, ErrMissingValue
	}
	switch val := v.GetValue().(type) {
	case *policyv1alpha1.Value_StringValue:
		s := val.StringValue
		return func(dest pcommon.Value) { dest.SetStr(s) }, nil
	case *policyv1alpha1.Value_IntValue:
		n := val.IntValue
		return func(dest pcommon.Value) { dest.SetInt(n) }, nil
	case *policyv1alpha1.Value_BoolValue:
		b := val.BoolValue
		return func(dest pcommon.Value) { dest.SetBool(b) }, nil
	case *policyv1alpha1.Value_DoubleValue:
		d := val.DoubleValue
		return func(dest pcommon.Value) { dest.SetDouble(d) }, nil
	case *policyv1alpha1.Value_BytesValue:
		b := val.BytesValue
		return func(dest pcommon.Value) { dest.SetEmptyBytes().FromRaw(b) }, nil
	default:
		return nil, ErrMissingValue
	}
}

func compileAdd(add *policyv1alpha1.LogAddAction) (transformOp, error) {
	if add == nil || add.GetTarget() == nil || add.GetTarget().Target == nil {
		return nil, ErrMissingTarget
	}
	assign, err := compileValueAssigner(add.GetValue())
	if err != nil {
		return nil, err
	}
	upsert := add.GetUpsert()

	if rf, ok := add.GetTarget().Target.(*policyv1alpha1.LogFieldSelector_RecordField); ok {
		switch rf.RecordField {
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY:
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) {
					return false
				}
				if !upsert && ctx.Record.Body().Type() != pcommon.ValueTypeEmpty {
					return false
				}
				assign(ctx.Record.Body())
				return true
			}, nil
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT:
			sv, ok := add.GetValue().GetValue().(*policyv1alpha1.Value_StringValue)
			if !ok {
				return nil, fmt.Errorf("severity_text add requires string_value")
			}
			text := sv.StringValue
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) {
					return false
				}
				if !upsert && ctx.Record.SeverityText() != "" {
					return false
				}
				ctx.Record.SetSeverityText(text)
				return true
			}, nil
		default:
			return nil, ErrUnsupportedTarget
		}
	}

	mt, err := compileMapTarget(add.GetTarget())
	if err != nil {
		return nil, err
	}
	return func(ctx googlepolicy.LogContext) bool {
		return mt.set(ctx, assign, upsert)
	}, nil
}

func compileRemove(rm *policyv1alpha1.LogRemoveAction) (transformOp, error) {
	if rm == nil || rm.GetTarget() == nil || rm.GetTarget().Target == nil {
		return nil, ErrMissingTarget
	}

	if rf, ok := rm.GetTarget().Target.(*policyv1alpha1.LogFieldSelector_RecordField); ok {
		switch rf.RecordField {
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY:
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) || ctx.Record.Body().Type() == pcommon.ValueTypeEmpty {
					return false
				}
				pcommon.NewValueEmpty().CopyTo(ctx.Record.Body())
				return true
			}, nil
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT:
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) || ctx.Record.SeverityText() == "" {
					return false
				}
				ctx.Record.SetSeverityText("")
				return true
			}, nil
		default:
			return nil, ErrUnsupportedTarget
		}
	}

	mt, err := compileMapTarget(rm.GetTarget())
	if err != nil {
		return nil, err
	}
	return func(ctx googlepolicy.LogContext) bool {
		return mt.remove(ctx)
	}, nil
}

func compileRename(rn *policyv1alpha1.LogRenameAction) (transformOp, error) {
	if rn == nil || rn.GetFrom() == nil || rn.GetTo() == nil {
		return nil, ErrMissingTarget
	}
	fromTarget, err := compileMapTarget(rn.GetFrom())
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}
	toTarget, err := compileMapTarget(rn.GetTo())
	if err != nil {
		return nil, fmt.Errorf("to: %w", err)
	}
	if proto.Equal(rn.GetFrom(), rn.GetTo()) {
		return func(googlepolicy.LogContext) bool { return false }, nil
	}
	upsert := rn.GetUpsert()

	return func(ctx googlepolicy.LogContext) bool {
		srcVal, ok := fromTarget.get(ctx)
		if !ok {
			return false
		}
		if !upsert {
			if _, dstExists := toTarget.get(ctx); dstExists {
				return false
			}
		}
		tmp := pcommon.NewValueEmpty()
		srcVal.CopyTo(tmp)
		if !toTarget.set(ctx, func(dest pcommon.Value) { tmp.CopyTo(dest) }, true) {
			return false
		}
		fromTarget.remove(ctx)
		return true
	}, nil
}

func compileRedact(rd *policyv1alpha1.LogRedactAction) (transformOp, error) {
	if rd == nil || rd.GetTarget() == nil || rd.GetTarget().Target == nil {
		return nil, ErrMissingTarget
	}

	var re *regexp.Regexp
	if expr := rd.GetRegex(); expr != "" {
		compiled, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid redact regex %q: %w", expr, err)
		}
		re = compiled
	}
	replacement := rd.GetReplacement()

	redactString := func(orig string) (string, bool) {
		if re == nil {
			if orig == replacement {
				return orig, false
			}
			return replacement, true
		}
		// regexp.ReplaceAllString returns orig without allocating when no match is found,
		// so calling it directly avoids a redundant second RE2 pass via re.MatchString.
		replaced := re.ReplaceAllString(orig, replacement)
		return replaced, replaced != orig
	}

	if rf, ok := rd.GetTarget().Target.(*policyv1alpha1.LogFieldSelector_RecordField); ok {
		switch rf.RecordField {
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_BODY:
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) || ctx.Record.Body().Type() != pcommon.ValueTypeStr {
					return false
				}
				replaced, changed := redactString(ctx.Record.Body().Str())
				if changed {
					ctx.Record.Body().SetStr(replaced)
				}
				return changed
			}, nil
		case policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT:
			return func(ctx googlepolicy.LogContext) bool {
				if ctx.Record == (plog.LogRecord{}) || ctx.Record.SeverityText() == "" {
					return false
				}
				replaced, changed := redactString(ctx.Record.SeverityText())
				if changed {
					ctx.Record.SetSeverityText(replaced)
				}
				return changed
			}, nil
		default:
			return nil, ErrUnsupportedTarget
		}
	}

	mt, err := compileMapTarget(rd.GetTarget())
	if err != nil {
		return nil, err
	}
	return func(ctx googlepolicy.LogContext) bool {
		val, ok := mt.get(ctx)
		if !ok || val.Type() != pcommon.ValueTypeStr {
			return false
		}
		replaced, changed := redactString(val.Str())
		if changed {
			val.SetStr(replaced)
		}
		return changed
	}, nil
}

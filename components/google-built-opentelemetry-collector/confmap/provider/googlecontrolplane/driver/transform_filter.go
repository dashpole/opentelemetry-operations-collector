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

package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	TypeURLLogFilterPolicy    = "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
	TypeURLMetricFilterPolicy = "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"
	TypeURLTraceFilterPolicy  = "type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy"

	TypeURLLogFilterPolicyAlias    = "log_filter_policy@v1"
	TypeURLMetricFilterPolicyAlias = "metric_filter_policy@v1"
	TypeURLTraceFilterPolicyAlias  = "trace_filter_policy@v1"
)

// TransformFilterDriver translates filter policies into policy/global configuration entries.
type TransformFilterDriver struct {
	typeURL string
}

// NewLogFilterDriver creates a driver for LogFilterPolicy.
func NewLogFilterDriver() *TransformFilterDriver {
	return &TransformFilterDriver{typeURL: TypeURLLogFilterPolicy}
}

// NewMetricFilterDriver creates a driver for MetricFilterPolicy.
func NewMetricFilterDriver() *TransformFilterDriver {
	return &TransformFilterDriver{typeURL: TypeURLMetricFilterPolicy}
}

// NewTraceFilterDriver creates a driver for TraceFilterPolicy.
func NewTraceFilterDriver() *TransformFilterDriver {
	return &TransformFilterDriver{typeURL: TypeURLTraceFilterPolicy}
}

func (d *TransformFilterDriver) TypeURL() string {
	return d.typeURL
}

func (d *TransformFilterDriver) Class() PolicyClass {
	return PolicyClassTransformation
}

func (d *TransformFilterDriver) Validate(msg proto.Message) error {
	resolvedMsg := msg
	if anyMsg, ok := msg.(*anypb.Any); ok {
		var target proto.Message
		switch d.typeURL {
		case TypeURLLogFilterPolicy, TypeURLLogFilterPolicyAlias:
			target = &policyv1alpha1.LogFilterPolicy{}
		case TypeURLMetricFilterPolicy, TypeURLMetricFilterPolicyAlias:
			target = &policyv1alpha1.MetricFilterPolicy{}
		case TypeURLTraceFilterPolicy, TypeURLTraceFilterPolicyAlias:
			target = &policyv1alpha1.TraceFilterPolicy{}
		default:
			return fmt.Errorf("unsupported filter policy type: %s", d.typeURL)
		}
		if err := anypb.UnmarshalTo(anyMsg, target, proto.UnmarshalOptions{DiscardUnknown: true}); err != nil {
			return fmt.Errorf("failed to unmarshal policy: %w", err)
		}
		resolvedMsg = target
	}

	switch p := resolvedMsg.(type) {
	case *policyv1alpha1.LogFilterPolicy:
		if p.GetAction() == policyv1alpha1.Action_ACTION_UNSPECIFIED {
			return errors.New("policy action must not be ACTION_UNSPECIFIED")
		}
		if len(p.GetMatches()) == 0 {
			return errors.New("log filter policy must contain at least one matcher")
		}
		for i, m := range p.GetMatches() {
			if m == nil {
				return fmt.Errorf("matcher[%d] must not be nil", i)
			}
			if m.GetTarget() == nil || m.GetTarget().GetTarget() == nil {
				return fmt.Errorf("matcher[%d] must specify a valid target", i)
			}
			if m.GetPredicate() == nil {
				return fmt.Errorf("matcher[%d] must specify a predicate", i)
			}
			if exists, ok := m.GetPredicate().(*policyv1alpha1.LogMatcher_Exists); ok && exists.Exists == nil {
				return fmt.Errorf("matcher[%d] exists predicate cannot be nil", i)
			}
			if regex := m.GetRegex(); regex != "" {
				if _, err := regexp.Compile(regex); err != nil {
					return fmt.Errorf("matcher[%d] has invalid regex %q: %w", i, regex, err)
				}
			}
		}

	case *policyv1alpha1.MetricFilterPolicy:
		if p.GetAction() == policyv1alpha1.Action_ACTION_UNSPECIFIED {
			return errors.New("policy action must not be ACTION_UNSPECIFIED")
		}
		if len(p.GetMatches()) == 0 {
			return errors.New("metric filter policy must contain at least one matcher")
		}
		for i, m := range p.GetMatches() {
			if m == nil {
				return fmt.Errorf("matcher[%d] must not be nil", i)
			}
			if m.GetTarget() == nil || m.GetTarget().GetTarget() == nil {
				return fmt.Errorf("matcher[%d] must specify a valid target", i)
			}
			if m.GetPredicate() == nil {
				return fmt.Errorf("matcher[%d] must specify a predicate", i)
			}
			if exists, ok := m.GetPredicate().(*policyv1alpha1.MetricMatcher_Exists); ok && exists.Exists == nil {
				return fmt.Errorf("matcher[%d] exists predicate cannot be nil", i)
			}
			if regex := m.GetRegex(); regex != "" {
				if _, err := regexp.Compile(regex); err != nil {
					return fmt.Errorf("matcher[%d] has invalid regex %q: %w", i, regex, err)
				}
			}
		}

	case *policyv1alpha1.TraceFilterPolicy:
		if p.GetAction() == policyv1alpha1.Action_ACTION_UNSPECIFIED {
			return errors.New("policy action must not be ACTION_UNSPECIFIED")
		}
		if len(p.GetMatches()) == 0 {
			return errors.New("trace filter policy must contain at least one matcher")
		}
		for i, m := range p.GetMatches() {
			if m == nil {
				return fmt.Errorf("matcher[%d] must not be nil", i)
			}
			if m.GetTarget() == nil || m.GetTarget().GetTarget() == nil {
				return fmt.Errorf("matcher[%d] must specify a valid target", i)
			}
			if m.GetPredicate() == nil {
				return fmt.Errorf("matcher[%d] must specify a predicate", i)
			}
			if exists, ok := m.GetPredicate().(*policyv1alpha1.TraceMatcher_Exists); ok && exists.Exists == nil {
				return fmt.Errorf("matcher[%d] exists predicate cannot be nil", i)
			}
			if regex := m.GetRegex(); regex != "" {
				if _, err := regexp.Compile(regex); err != nil {
					return fmt.Errorf("matcher[%d] has invalid regex %q: %w", i, regex, err)
				}
			}
		}

	default:
		return fmt.Errorf("unexpected message type %T for driver %s", resolvedMsg, d.typeURL)
	}

	return nil
}

func (d *TransformFilterDriver) GenerateConfig(msg proto.Message, ctx *CompilationContext) (*ConfigFragment, error) {
	resolvedMsg := msg
	if anyMsg, ok := msg.(*anypb.Any); ok {
		var target proto.Message
		switch d.typeURL {
		case TypeURLLogFilterPolicy, TypeURLLogFilterPolicyAlias:
			target = &policyv1alpha1.LogFilterPolicy{}
		case TypeURLMetricFilterPolicy, TypeURLMetricFilterPolicyAlias:
			target = &policyv1alpha1.MetricFilterPolicy{}
		case TypeURLTraceFilterPolicy, TypeURLTraceFilterPolicyAlias:
			target = &policyv1alpha1.TraceFilterPolicy{}
		default:
			return nil, fmt.Errorf("unsupported filter policy type: %s", d.typeURL)
		}
		if err := anypb.UnmarshalTo(anyMsg, target, proto.UnmarshalOptions{DiscardUnknown: true}); err != nil {
			return nil, fmt.Errorf("failed to unmarshal policy: %w", err)
		}
		resolvedMsg = target
	}

	marshaler := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	jsonBytes, err := marshaler.Marshal(resolvedMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal policy to JSON: %w", err)
	}

	var ruleMap map[string]any
	if err := json.Unmarshal(jsonBytes, &ruleMap); err != nil {
		return nil, fmt.Errorf("failed to parse policy JSON: %w", err)
	}

	policyID := ""
	if ctx != nil {
		policyID = ctx.PolicyID
	}

	frag := NewConfigFragment()
	frag.Processors["policy/global"] = map[string]any{
		"policies": []any{
			map[string]any{
				"id":       policyID,
				"type_url": d.typeURL,
				"rule":     ruleMap,
			},
		},
	}

	if ctx != nil && ctx.ResolvedTokens != nil {
		ctx.ResolvedTokens["global_policy_processor"] = "policy/global"
	}

	return frag, nil
}

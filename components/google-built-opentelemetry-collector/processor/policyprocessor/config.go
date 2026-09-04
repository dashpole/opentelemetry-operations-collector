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

package policyprocessor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
)

const (
	TypeURLLogFilterPolicy         = "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
	TypeURLMetricFilterPolicy      = "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"
	TypeURLTraceFilterPolicy       = "type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy"
	ShortTypeURLLogFilterPolicy    = "google.telemetry.policy.v1alpha1.LogFilterPolicy"
	ShortTypeURLMetricFilterPolicy = "google.telemetry.policy.v1alpha1.MetricFilterPolicy"
	ShortTypeURLTraceFilterPolicy  = "google.telemetry.policy.v1alpha1.TraceFilterPolicy"
)

// Config defines the configuration for policyprocessor.
type Config struct {
	InformerExtensions   []component.ID `mapstructure:"informer_extensions"`
	StartupTimeout       time.Duration  `mapstructure:"startup_timeout"`
	FailOnStartupTimeout bool           `mapstructure:"fail_on_startup_timeout"`

	Policies []PolicyConfig `mapstructure:"policies"`

	// Compiled stores parsed and validated protobuf messages bound with Policy IDs.
	Compiled CompiledPolicies `mapstructure:"-"`
}

// PolicyConfig represents a single raw policy configuration entry.
type PolicyConfig struct {
	ID      string         `mapstructure:"id"`
	TypeURL string         `mapstructure:"type_url"`
	Rule    map[string]any `mapstructure:"rule"`
}

// CompiledLogPolicy wraps a parsed LogFilterPolicy with its policy ID, pre-compiled matchers,
// and pre-allocated measurement options for zero-allocation metric reporting.
type CompiledLogPolicy struct {
	ID         string
	Policy     *policyv1alpha1.LogFilterPolicy
	Matchers   []CompiledLogMatcher
	KeepOption metric.MeasurementOption
	DropOption metric.MeasurementOption
}

// CompiledMetricPolicy wraps a parsed MetricFilterPolicy with its policy ID, pre-compiled matchers,
// whether it operates at the data point level, and pre-allocated measurement options.
type CompiledMetricPolicy struct {
	ID               string
	Policy           *policyv1alpha1.MetricFilterPolicy
	Matchers         []CompiledMetricMatcher
	IsDataPointLevel bool
	KeepOption       metric.MeasurementOption
	DropOption       metric.MeasurementOption
}

// CompiledTracePolicy wraps a parsed TraceFilterPolicy with its policy ID, pre-compiled matchers,
// and pre-allocated measurement options for zero-allocation metric reporting.
type CompiledTracePolicy struct {
	ID         string
	Policy     *policyv1alpha1.TraceFilterPolicy
	Matchers   []CompiledTraceMatcher
	KeepOption metric.MeasurementOption
	DropOption metric.MeasurementOption
}

// CompiledPolicies aggregates all compiled signal policies, including pre-partitioned metric policies.
type CompiledPolicies struct {
	LogPolicies              []CompiledLogPolicy
	MetricPolicies           []CompiledMetricPolicy
	MetricInstrumentPolicies []CompiledMetricPolicy
	MetricDataPointPolicies  []CompiledMetricPolicy
	TracePolicies            []CompiledTracePolicy
}

var _ confmap.Unmarshaler = (*Config)(nil)
var _ confmap.Validator = (*Config)(nil)

// Unmarshal implements confmap.Unmarshaler to parse policy configurations forward-compatibly.
func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	type rawConfig Config
	raw := rawConfig{
		StartupTimeout:       cfg.StartupTimeout,
		FailOnStartupTimeout: cfg.FailOnStartupTimeout,
	}
	if raw.StartupTimeout == 0 {
		raw.StartupTimeout = 5 * time.Second
	}
	if err := conf.Unmarshal(&raw); err != nil {
		return err
	}
	cfg.InformerExtensions = raw.InformerExtensions
	cfg.StartupTimeout = raw.StartupTimeout
	cfg.FailOnStartupTimeout = raw.FailOnStartupTimeout
	cfg.Policies = raw.Policies

	return cfg.Compile()
}

// Validate implements confmap.Validator.
func (cfg *Config) Validate() error {
	if cfg.StartupTimeout < 0 {
		return fmt.Errorf("startup_timeout must be non-negative, got %v", cfg.StartupTimeout)
	}
	seen := make(map[component.ID]struct{}, len(cfg.InformerExtensions))
	for _, id := range cfg.InformerExtensions {
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate informer extension %q specified in informer_extensions", id)
		}
		seen[id] = struct{}{}
	}
	return cfg.Compile()
}

// Compile compiles and strictly validates all policy configurations.
func (cfg *Config) Compile() error {
	compiled, err := CompilePolicies(cfg.Policies)
	if err != nil {
		return err
	}
	cfg.Compiled = *compiled
	return nil
}

// CompilePolicies compiles a list of PolicyConfig entries into CompiledPolicies.
func CompilePolicies(policies []PolicyConfig) (*CompiledPolicies, error) {
	compiled := &CompiledPolicies{}
	for i, p := range policies {
		if err := compilePolicyConfig(i, p, compiled); err != nil {
			return nil, err
		}
	}
	return compiled, nil
}

func compilePolicyConfig(i int, p PolicyConfig, compiled *CompiledPolicies) error {
	if p.Rule == nil {
		return fmt.Errorf("policy %q (index %d): rule must not be nil", p.ID, i)
	}

	ruleJSON, err := json.Marshal(p.Rule)
	if err != nil {
		return fmt.Errorf("policy %q (index %d): failed to marshal policy rule to JSON: %w", p.ID, i, err)
	}

	unmarshalOpts := protojson.UnmarshalOptions{
		DiscardUnknown: true, // Forward-compatible fail-open for unrecognized fields
	}

	switch p.TypeURL {
	case TypeURLLogFilterPolicy, ShortTypeURLLogFilterPolicy:
		var lp policyv1alpha1.LogFilterPolicy
		if err := unmarshalOpts.Unmarshal(ruleJSON, &lp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal log policy: %w", p.ID, err)
		}
		policyID := p.ID
		if policyID == "" {
			policyID = lp.GetId()
		}
		if policyID == "" {
			return fmt.Errorf("log policy at index %d: ID must be specified", i)
		}
		clp, err := compileLogPolicy(policyID, &lp)
		if err != nil {
			return err
		}
		compiled.LogPolicies = append(compiled.LogPolicies, *clp)

	case TypeURLMetricFilterPolicy, ShortTypeURLMetricFilterPolicy:
		var mp policyv1alpha1.MetricFilterPolicy
		if err := unmarshalOpts.Unmarshal(ruleJSON, &mp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal metric policy: %w", p.ID, err)
		}
		policyID := p.ID
		if policyID == "" {
			policyID = mp.GetId()
		}
		if policyID == "" {
			return fmt.Errorf("metric policy at index %d: ID must be specified", i)
		}
		cmp, err := compileMetricPolicy(policyID, &mp)
		if err != nil {
			return err
		}
		compiled.MetricPolicies = append(compiled.MetricPolicies, *cmp)
		if cmp.IsDataPointLevel {
			compiled.MetricDataPointPolicies = append(compiled.MetricDataPointPolicies, *cmp)
		} else {
			compiled.MetricInstrumentPolicies = append(compiled.MetricInstrumentPolicies, *cmp)
		}

	case TypeURLTraceFilterPolicy, ShortTypeURLTraceFilterPolicy:
		var tp policyv1alpha1.TraceFilterPolicy
		if err := unmarshalOpts.Unmarshal(ruleJSON, &tp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal trace policy: %w", p.ID, err)
		}
		policyID := p.ID
		if policyID == "" {
			policyID = tp.GetId()
		}
		if policyID == "" {
			return fmt.Errorf("trace policy at index %d: ID must be specified", i)
		}
		ctp, err := compileTracePolicy(policyID, &tp)
		if err != nil {
			return err
		}
		compiled.TracePolicies = append(compiled.TracePolicies, *ctp)

	default:
		return fmt.Errorf("unsupported policy type_url %q for policy %q", p.TypeURL, p.ID)
	}

	return nil
}

func compileLogPolicy(policyID string, lp *policyv1alpha1.LogFilterPolicy) (*CompiledLogPolicy, error) {
	if policyID == "" {
		return nil, fmt.Errorf("log policy ID must be specified")
	}
	if lp.Action == nil || (*lp.Action != policyv1alpha1.Action_ACTION_KEEP && *lp.Action != policyv1alpha1.Action_ACTION_DROP) {
		return nil, fmt.Errorf("log policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, lp.GetAction())
	}
	if len(lp.GetMatches()) == 0 {
		return nil, fmt.Errorf("log policy %q: at least one matcher is required", policyID)
	}

	compiledMatchers := make([]CompiledLogMatcher, 0, len(lp.GetMatches()))
	for mIdx, m := range lp.GetMatches() {
		if m == nil {
			return nil, fmt.Errorf("log policy %q: matcher at index %d is nil", policyID, mIdx)
		}
		if err := validateLogTarget(m.GetTarget()); err != nil {
			return nil, fmt.Errorf("log policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		pred, err := NewLogPredicate(m)
		if err != nil {
			return nil, fmt.Errorf("log policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		compiledMatchers = append(compiledMatchers, CompiledLogMatcher{
			Target:    m.GetTarget(),
			Predicate: pred,
		})
	}

	return &CompiledLogPolicy{
		ID:       policyID,
		Policy:   lp,
		Matchers: compiledMatchers,
		KeepOption: metric.WithAttributes(
			attribute.String("signal", "logs"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "keep"),
		),
		DropOption: metric.WithAttributes(
			attribute.String("signal", "logs"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "drop"),
		),
	}, nil
}

func compileMetricPolicy(policyID string, mp *policyv1alpha1.MetricFilterPolicy) (*CompiledMetricPolicy, error) {
	if policyID == "" {
		return nil, fmt.Errorf("metric policy ID must be specified")
	}
	if mp.Action == nil || (*mp.Action != policyv1alpha1.Action_ACTION_KEEP && *mp.Action != policyv1alpha1.Action_ACTION_DROP) {
		return nil, fmt.Errorf("metric policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, mp.GetAction())
	}
	if len(mp.GetMatches()) == 0 {
		return nil, fmt.Errorf("metric policy %q: at least one matcher is required", policyID)
	}

	var isDataPointLevel bool
	compiledMatchers := make([]CompiledMetricMatcher, 0, len(mp.GetMatches()))
	for mIdx, m := range mp.GetMatches() {
		if m == nil {
			return nil, fmt.Errorf("metric policy %q: matcher at index %d is nil", policyID, mIdx)
		}
		if err := validateMetricTarget(m.GetTarget()); err != nil {
			return nil, fmt.Errorf("metric policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		if _, ok := m.GetTarget().GetTarget().(*policyv1alpha1.MetricFieldSelector_DatapointAttribute); ok {
			isDataPointLevel = true
		}
		pred, err := NewMetricPredicate(m)
		if err != nil {
			return nil, fmt.Errorf("metric policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		compiledMatchers = append(compiledMatchers, CompiledMetricMatcher{
			Target:    m.GetTarget(),
			Predicate: pred,
		})
	}

	return &CompiledMetricPolicy{
		ID:               policyID,
		Policy:           mp,
		Matchers:         compiledMatchers,
		IsDataPointLevel: isDataPointLevel,
		KeepOption: metric.WithAttributes(
			attribute.String("signal", "metrics"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "keep"),
		),
		DropOption: metric.WithAttributes(
			attribute.String("signal", "metrics"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "drop"),
		),
	}, nil
}

func compileTracePolicy(policyID string, tp *policyv1alpha1.TraceFilterPolicy) (*CompiledTracePolicy, error) {
	if policyID == "" {
		return nil, fmt.Errorf("trace policy ID must be specified")
	}
	if tp.Action == nil || (*tp.Action != policyv1alpha1.Action_ACTION_KEEP && *tp.Action != policyv1alpha1.Action_ACTION_DROP) {
		return nil, fmt.Errorf("trace policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, tp.GetAction())
	}
	if len(tp.GetMatches()) == 0 {
		return nil, fmt.Errorf("trace policy %q: at least one matcher is required", policyID)
	}

	compiledMatchers := make([]CompiledTraceMatcher, 0, len(tp.GetMatches()))
	for mIdx, m := range tp.GetMatches() {
		if m == nil {
			return nil, fmt.Errorf("trace policy %q: matcher at index %d is nil", policyID, mIdx)
		}
		if err := validateTraceTarget(m.GetTarget()); err != nil {
			return nil, fmt.Errorf("trace policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		pred, err := NewTracePredicate(m)
		if err != nil {
			return nil, fmt.Errorf("trace policy %q: matcher at index %d: %w", policyID, mIdx, err)
		}
		compiledMatchers = append(compiledMatchers, CompiledTraceMatcher{
			Target:    m.GetTarget(),
			Predicate: pred,
		})
	}

	return &CompiledTracePolicy{
		ID:       policyID,
		Policy:   tp,
		Matchers: compiledMatchers,
		KeepOption: metric.WithAttributes(
			attribute.String("signal", "traces"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "keep"),
		),
		DropOption: metric.WithAttributes(
			attribute.String("signal", "traces"),
			attribute.String("policy_id", policyID),
			attribute.String("action", "drop"),
		),
	}, nil
}

func compileTypedExtensionConfig(pid string, typedCfg *v3.TypedExtensionConfig, compiled *CompiledPolicies) error {
	if typedCfg == nil || typedCfg.TypedConfig == nil {
		return fmt.Errorf("policy %q: nil typed config", pid)
	}
	typeURL := typedCfg.TypedConfig.TypeUrl
	if !isFilterPolicy(typeURL) {
		return nil
	}

	policyID := pid
	if policyID == "" {
		policyID = typedCfg.Name
	}

	switch {
	case strings.HasSuffix(typeURL, "LogFilterPolicy"):
		var lp policyv1alpha1.LogFilterPolicy
		if err := unmarshalPolicyProto(typedCfg.TypedConfig, &lp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal log policy: %w", policyID, err)
		}
		if policyID == "" {
			policyID = lp.GetId()
		}
		clp, err := compileLogPolicy(policyID, &lp)
		if err != nil {
			return err
		}
		compiled.LogPolicies = append(compiled.LogPolicies, *clp)

	case strings.HasSuffix(typeURL, "MetricFilterPolicy"):
		var mp policyv1alpha1.MetricFilterPolicy
		if err := unmarshalPolicyProto(typedCfg.TypedConfig, &mp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal metric policy: %w", policyID, err)
		}
		if policyID == "" {
			policyID = mp.GetId()
		}
		cmp, err := compileMetricPolicy(policyID, &mp)
		if err != nil {
			return err
		}
		compiled.MetricPolicies = append(compiled.MetricPolicies, *cmp)
		if cmp.IsDataPointLevel {
			compiled.MetricDataPointPolicies = append(compiled.MetricDataPointPolicies, *cmp)
		} else {
			compiled.MetricInstrumentPolicies = append(compiled.MetricInstrumentPolicies, *cmp)
		}

	case strings.HasSuffix(typeURL, "TraceFilterPolicy"):
		var tp policyv1alpha1.TraceFilterPolicy
		if err := unmarshalPolicyProto(typedCfg.TypedConfig, &tp); err != nil {
			return fmt.Errorf("policy %q: failed to unmarshal trace policy: %w", policyID, err)
		}
		if policyID == "" {
			policyID = tp.GetId()
		}
		ctp, err := compileTracePolicy(policyID, &tp)
		if err != nil {
			return err
		}
		compiled.TracePolicies = append(compiled.TracePolicies, *ctp)

	default:
		return nil
	}
	return nil
}

func isFilterPolicy(typeURL string) bool {
	return strings.HasSuffix(typeURL, "google.telemetry.policy.v1alpha1.LogFilterPolicy") ||
		strings.HasSuffix(typeURL, "google.telemetry.policy.v1alpha1.MetricFilterPolicy") ||
		strings.HasSuffix(typeURL, "google.telemetry.policy.v1alpha1.TraceFilterPolicy")
}

func unmarshalPolicyProto(typedConfig *anypb.Any, target proto.Message) error {
	if typedConfig == nil {
		return errors.New("typed config is nil")
	}
	data := typedConfig.Value
	if len(data) == 0 {
		return errors.New("typed config value is empty")
	}
	unmarshalOpts := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(trimmed, []byte("{")) || bytes.HasPrefix(trimmed, []byte("[")) {
		if err := unmarshalOpts.Unmarshal(trimmed, target); err == nil {
			return nil
		}
	}
	if err := proto.Unmarshal(data, target); err == nil {
		return nil
	}
	if err := unmarshalOpts.Unmarshal(trimmed, target); err == nil {
		return nil
	}
	return fmt.Errorf("failed to unmarshal policy from binary or json proto")
}

func validateLogTarget(target *policyv1alpha1.LogFieldSelector) error {
	if target == nil || target.Target == nil {
		return fmt.Errorf("target must be specified")
	}

	switch t := target.Target.(type) {
	case *policyv1alpha1.LogFieldSelector_RecordField:
		if t.RecordField == policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_UNSPECIFIED {
			return fmt.Errorf("record_field must not be LOG_RECORD_FIELD_UNSPECIFIED")
		}
	case *policyv1alpha1.LogFieldSelector_LogAttribute:
		if t.LogAttribute == "" {
			return fmt.Errorf("log_attribute key must not be empty")
		}
	case *policyv1alpha1.LogFieldSelector_ResourceAttribute:
		if t.ResourceAttribute == "" {
			return fmt.Errorf("resource_attribute key must not be empty")
		}
	case *policyv1alpha1.LogFieldSelector_ScopeAttribute:
		if t.ScopeAttribute == "" {
			return fmt.Errorf("scope_attribute key must not be empty")
		}
	case *policyv1alpha1.LogFieldSelector_ScopeField:
		if t.ScopeField == policyv1alpha1.ScopeField_SCOPE_FIELD_UNSPECIFIED {
			return fmt.Errorf("scope_field must not be SCOPE_FIELD_UNSPECIFIED")
		}
	default:
		return fmt.Errorf("unknown log target type %T", target.Target)
	}
	return nil
}

func validateMetricTarget(target *policyv1alpha1.MetricFieldSelector) error {
	if target == nil || target.Target == nil {
		return fmt.Errorf("target must be specified")
	}

	switch t := target.Target.(type) {
	case *policyv1alpha1.MetricFieldSelector_DescriptorField:
		if t.DescriptorField == policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_UNSPECIFIED {
			return fmt.Errorf("descriptor_field must not be METRIC_DESCRIPTOR_FIELD_UNSPECIFIED")
		}
	case *policyv1alpha1.MetricFieldSelector_DatapointAttribute:
		if t.DatapointAttribute == "" {
			return fmt.Errorf("datapoint_attribute key must not be empty")
		}
	case *policyv1alpha1.MetricFieldSelector_ResourceAttribute:
		if t.ResourceAttribute == "" {
			return fmt.Errorf("resource_attribute key must not be empty")
		}
	case *policyv1alpha1.MetricFieldSelector_ScopeAttribute:
		if t.ScopeAttribute == "" {
			return fmt.Errorf("scope_attribute key must not be empty")
		}
	case *policyv1alpha1.MetricFieldSelector_ScopeField:
		if t.ScopeField == policyv1alpha1.ScopeField_SCOPE_FIELD_UNSPECIFIED {
			return fmt.Errorf("scope_field must not be SCOPE_FIELD_UNSPECIFIED")
		}
	default:
		return fmt.Errorf("unknown metric target type %T", target.Target)
	}
	return nil
}

func validateTraceTarget(target *policyv1alpha1.TraceFieldSelector) error {
	if target == nil || target.Target == nil {
		return fmt.Errorf("target must be specified")
	}

	switch t := target.Target.(type) {
	case *policyv1alpha1.TraceFieldSelector_RecordField:
		if t.RecordField == policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_UNSPECIFIED {
			return fmt.Errorf("record_field must not be SPAN_RECORD_FIELD_UNSPECIFIED")
		}
	case *policyv1alpha1.TraceFieldSelector_SpanAttribute:
		if t.SpanAttribute == "" {
			return fmt.Errorf("span_attribute key must not be empty")
		}
	case *policyv1alpha1.TraceFieldSelector_ResourceAttribute:
		if t.ResourceAttribute == "" {
			return fmt.Errorf("resource_attribute key must not be empty")
		}
	case *policyv1alpha1.TraceFieldSelector_ScopeAttribute:
		if t.ScopeAttribute == "" {
			return fmt.Errorf("scope_attribute key must not be empty")
		}
	case *policyv1alpha1.TraceFieldSelector_ScopeField:
		if t.ScopeField == policyv1alpha1.ScopeField_SCOPE_FIELD_UNSPECIFIED {
			return fmt.Errorf("scope_field must not be SCOPE_FIELD_UNSPECIFIED")
		}
	default:
		return fmt.Errorf("unknown trace target type %T", target.Target)
	}
	return nil
}

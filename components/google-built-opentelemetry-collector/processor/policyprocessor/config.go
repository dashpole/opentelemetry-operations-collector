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
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/encoding/protojson"

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
	var raw rawConfig
	if err := conf.Unmarshal(&raw); err != nil {
		return err
	}
	cfg.Policies = raw.Policies

	return cfg.Compile()
}

// Validate implements confmap.Validator.
func (cfg *Config) Validate() error {
	return cfg.Compile()
}

// Compile compiles and strictly validates all policy configurations.
func (cfg *Config) Compile() error {
	cfg.Compiled = CompiledPolicies{}

	unmarshalOpts := protojson.UnmarshalOptions{
		DiscardUnknown: true, // Forward-compatible fail-open for unrecognized fields
	}

	for i, p := range cfg.Policies {
		if p.Rule == nil {
			return fmt.Errorf("policy %q (index %d): rule must not be nil", p.ID, i)
		}

		ruleJSON, err := json.Marshal(p.Rule)
		if err != nil {
			return fmt.Errorf("policy %q (index %d): failed to marshal policy rule to JSON: %w", p.ID, i, err)
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
			if lp.Action == nil || (*lp.Action != policyv1alpha1.Action_ACTION_KEEP && *lp.Action != policyv1alpha1.Action_ACTION_DROP) {
				return fmt.Errorf("log policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, lp.GetAction())
			}
			if len(lp.GetMatches()) == 0 {
				return fmt.Errorf("log policy %q: at least one matcher is required", policyID)
			}

			compiledMatchers := make([]CompiledLogMatcher, 0, len(lp.GetMatches()))
			for mIdx, m := range lp.GetMatches() {
				if m == nil {
					return fmt.Errorf("log policy %q: matcher at index %d is nil", policyID, mIdx)
				}
				if err := validateLogTarget(m.GetTarget()); err != nil {
					return fmt.Errorf("log policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				pred, err := NewLogPredicate(m)
				if err != nil {
					return fmt.Errorf("log policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				compiledMatchers = append(compiledMatchers, CompiledLogMatcher{
					Target:    m.GetTarget(),
					Predicate: pred,
				})
			}

			cfg.Compiled.LogPolicies = append(cfg.Compiled.LogPolicies, CompiledLogPolicy{
				ID:       policyID,
				Policy:   &lp,
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
			})

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
			if mp.Action == nil || (*mp.Action != policyv1alpha1.Action_ACTION_KEEP && *mp.Action != policyv1alpha1.Action_ACTION_DROP) {
				return fmt.Errorf("metric policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, mp.GetAction())
			}
			if len(mp.GetMatches()) == 0 {
				return fmt.Errorf("metric policy %q: at least one matcher is required", policyID)
			}

			var isDataPointLevel bool
			compiledMatchers := make([]CompiledMetricMatcher, 0, len(mp.GetMatches()))
			for mIdx, m := range mp.GetMatches() {
				if m == nil {
					return fmt.Errorf("metric policy %q: matcher at index %d is nil", policyID, mIdx)
				}
				if err := validateMetricTarget(m.GetTarget()); err != nil {
					return fmt.Errorf("metric policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				if _, ok := m.GetTarget().GetTarget().(*policyv1alpha1.MetricFieldSelector_DatapointAttribute); ok {
					isDataPointLevel = true
				}
				pred, err := NewMetricPredicate(m)
				if err != nil {
					return fmt.Errorf("metric policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				compiledMatchers = append(compiledMatchers, CompiledMetricMatcher{
					Target:    m.GetTarget(),
					Predicate: pred,
				})
			}

			compiledMetric := CompiledMetricPolicy{
				ID:               policyID,
				Policy:           &mp,
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
			}

			cfg.Compiled.MetricPolicies = append(cfg.Compiled.MetricPolicies, compiledMetric)
			if isDataPointLevel {
				cfg.Compiled.MetricDataPointPolicies = append(cfg.Compiled.MetricDataPointPolicies, compiledMetric)
			} else {
				cfg.Compiled.MetricInstrumentPolicies = append(cfg.Compiled.MetricInstrumentPolicies, compiledMetric)
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
			if tp.Action == nil || (*tp.Action != policyv1alpha1.Action_ACTION_KEEP && *tp.Action != policyv1alpha1.Action_ACTION_DROP) {
				return fmt.Errorf("trace policy %q: action must be ACTION_KEEP or ACTION_DROP, got %v", policyID, tp.GetAction())
			}
			if len(tp.GetMatches()) == 0 {
				return fmt.Errorf("trace policy %q: at least one matcher is required", policyID)
			}

			compiledMatchers := make([]CompiledTraceMatcher, 0, len(tp.GetMatches()))
			for mIdx, m := range tp.GetMatches() {
				if m == nil {
					return fmt.Errorf("trace policy %q: matcher at index %d is nil", policyID, mIdx)
				}
				if err := validateTraceTarget(m.GetTarget()); err != nil {
					return fmt.Errorf("trace policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				pred, err := NewTracePredicate(m)
				if err != nil {
					return fmt.Errorf("trace policy %q: matcher at index %d: %w", policyID, mIdx, err)
				}
				compiledMatchers = append(compiledMatchers, CompiledTraceMatcher{
					Target:    m.GetTarget(),
					Predicate: pred,
				})
			}

			cfg.Compiled.TracePolicies = append(cfg.Compiled.TracePolicies, CompiledTracePolicy{
				ID:       policyID,
				Policy:   &tp,
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
			})

		default:
			return fmt.Errorf("unsupported policy type_url %q for policy %q", p.TypeURL, p.ID)
		}
	}

	return nil
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

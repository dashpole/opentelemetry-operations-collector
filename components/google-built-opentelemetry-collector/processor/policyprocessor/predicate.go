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
	"fmt"
	"regexp"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
)

// PredicateEvaluator evaluates exists, exact string, and RE2 regex matching
// with support for negation. Evaluates against both string and stack-allocated
// []byte buffers for zero-heap-allocation ID matching.
type PredicateEvaluator struct {
	isExists   bool
	isExact    bool
	exactStr   string
	exactBytes []byte
	regex      *regexp.Regexp
	negate     bool
}

// NewLogPredicate creates a PredicateEvaluator from a LogMatcher.
func NewLogPredicate(m *policyv1alpha1.LogMatcher) (PredicateEvaluator, error) {
	if m == nil || m.Predicate == nil {
		return PredicateEvaluator{}, fmt.Errorf("predicate must be specified")
	}

	switch pred := m.Predicate.(type) {
	case *policyv1alpha1.LogMatcher_Exists:
		if pred.Exists == nil {
			return PredicateEvaluator{}, fmt.Errorf("exists predicate must not be nil")
		}
		return PredicateEvaluator{
			isExists: true,
			negate:   m.Negate,
		}, nil
	case *policyv1alpha1.LogMatcher_Exact:
		return PredicateEvaluator{
			isExact:    true,
			exactStr:   pred.Exact,
			exactBytes: []byte(pred.Exact),
			negate:     m.Negate,
		}, nil
	case *policyv1alpha1.LogMatcher_Regex:
		r, err := regexp.Compile(pred.Regex)
		if err != nil {
			return PredicateEvaluator{}, fmt.Errorf("malformed RE2 regex %q: %w", pred.Regex, err)
		}
		return PredicateEvaluator{
			regex:  r,
			negate: m.Negate,
		}, nil
	default:
		return PredicateEvaluator{}, fmt.Errorf("unsupported log predicate type %T", m.Predicate)
	}
}

// NewMetricPredicate creates a PredicateEvaluator from a MetricMatcher.
func NewMetricPredicate(m *policyv1alpha1.MetricMatcher) (PredicateEvaluator, error) {
	if m == nil || m.Predicate == nil {
		return PredicateEvaluator{}, fmt.Errorf("predicate must be specified")
	}

	switch pred := m.Predicate.(type) {
	case *policyv1alpha1.MetricMatcher_Exists:
		if pred.Exists == nil {
			return PredicateEvaluator{}, fmt.Errorf("exists predicate must not be nil")
		}
		return PredicateEvaluator{
			isExists: true,
			negate:   m.Negate,
		}, nil
	case *policyv1alpha1.MetricMatcher_Exact:
		return PredicateEvaluator{
			isExact:    true,
			exactStr:   pred.Exact,
			exactBytes: []byte(pred.Exact),
			negate:     m.Negate,
		}, nil
	case *policyv1alpha1.MetricMatcher_Regex:
		r, err := regexp.Compile(pred.Regex)
		if err != nil {
			return PredicateEvaluator{}, fmt.Errorf("malformed RE2 regex %q: %w", pred.Regex, err)
		}
		return PredicateEvaluator{
			regex:  r,
			negate: m.Negate,
		}, nil
	default:
		return PredicateEvaluator{}, fmt.Errorf("unsupported metric predicate type %T", m.Predicate)
	}
}

// NewTracePredicate creates a PredicateEvaluator from a TraceMatcher.
func NewTracePredicate(m *policyv1alpha1.TraceMatcher) (PredicateEvaluator, error) {
	if m == nil || m.Predicate == nil {
		return PredicateEvaluator{}, fmt.Errorf("predicate must be specified")
	}

	switch pred := m.Predicate.(type) {
	case *policyv1alpha1.TraceMatcher_Exists:
		if pred.Exists == nil {
			return PredicateEvaluator{}, fmt.Errorf("exists predicate must not be nil")
		}
		return PredicateEvaluator{
			isExists: true,
			negate:   m.Negate,
		}, nil
	case *policyv1alpha1.TraceMatcher_Exact:
		return PredicateEvaluator{
			isExact:    true,
			exactStr:   pred.Exact,
			exactBytes: []byte(pred.Exact),
			negate:     m.Negate,
		}, nil
	case *policyv1alpha1.TraceMatcher_Regex:
		r, err := regexp.Compile(pred.Regex)
		if err != nil {
			return PredicateEvaluator{}, fmt.Errorf("malformed RE2 regex %q: %w", pred.Regex, err)
		}
		return PredicateEvaluator{
			regex:  r,
			negate: m.Negate,
		}, nil
	default:
		return PredicateEvaluator{}, fmt.Errorf("unsupported trace predicate type %T", m.Predicate)
	}
}

// Eval evaluates the predicate against a string value.
func (pe PredicateEvaluator) Eval(val string, exists bool) bool {
	var match bool
	if pe.isExists {
		match = exists
	} else if !exists {
		match = false
	} else if pe.isExact {
		match = (val == pe.exactStr)
	} else if pe.regex != nil {
		match = pe.regex.MatchString(val)
	}

	if pe.negate {
		return !match
	}
	return match
}

// EvalExactBytes evaluates exact string matches directly against a byte slice.
// Does NOT reference regexp.Match, guaranteeing that val does not escape to heap.
func (pe PredicateEvaluator) EvalExactBytes(val []byte, exists bool) bool {
	var match bool
	if pe.isExists {
		match = exists
	} else if !exists {
		match = false
	} else if pe.isExact {
		match = bytes.Equal(val, pe.exactBytes)
	}

	if pe.negate {
		return !match
	}
	return match
}

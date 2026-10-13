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

package googlepolicyprocessor

import (
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"go.opentelemetry.io/collector/pdata/plog"
)

// TransformLogs applies active transformation policies in-place across the Resource -> Scope -> Record hierarchy.
// Dropped records are pruned, and empty scopes/resources are removed.
func (e *Evaluator) TransformLogs(ld plog.Logs) {
	if len(e.logPolicies) == 0 && len(e.logTransformPolicies) == 0 {
		return
	}

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		resource := rl.Resource()
		resourceSchemaURL := rl.SchemaUrl()

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			scope := sl.Scope()
			scopeSchemaURL := sl.SchemaUrl()

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				ctx := LogContext{
					Record:            lr,
					Resource:          resource,
					Scope:             scope,
					ResourceSchemaURL: resourceSchemaURL,
					ScopeSchemaURL:    scopeSchemaURL,
				}
				return e.EvalLog(ctx)
			})

			return sl.LogRecords().Len() == 0
		})

		return rl.ScopeLogs().Len() == 0
	})
}

// EvalLog returns true if the log record should be DROPPED, false if KEPT.
// It executes Double-Applied Filtering around the deterministic Transform stage
// sequence (Pass 1 Pre-Transform Filter -> rename -> add -> remove -> redact ->
// Pass 2 Post-Transform Filter).
func (e *Evaluator) EvalLog(ctx LogContext) bool {
	if len(e.logPolicies) > 0 && e.evalLogFilters(ctx) {
		return true
	}

	if len(e.logTransformPolicies) == 0 {
		return false
	}

	var mutated bool
	for _, tp := range e.logTransformPolicies {
		if tp.TransformLog(ctx) {
			mutated = true
		}
	}

	if mutated && len(e.logPolicies) > 0 {
		return e.evalLogFilters(ctx)
	}
	return false
}

func (e *Evaluator) evalLogFilters(ctx LogContext) bool {
	var hasDrop bool
	for _, p := range e.logPolicies {
		switch p.EvaluateLog(ctx) {
		case googlepolicy.EvalKeep:
			return false
		case googlepolicy.EvalDrop:
			hasDrop = true
		}
	}
	return hasDrop
}

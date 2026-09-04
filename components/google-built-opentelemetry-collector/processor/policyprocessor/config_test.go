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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestPolicyProcessor_ConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		expectErr   bool
		errContains string
	}{
		{
			name: "valid log policy",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "log-keep-info",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "log-keep-info",
							"action": "ACTION_KEEP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT",
									},
									"exact": "INFO",
								},
							},
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "valid metric policy with datapoint attribute",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "metric-drop-dev",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "metric-drop-dev",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"datapoint_attribute": "env",
									},
									"exact": "dev",
								},
							},
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "valid trace policy with regex",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "trace-keep-http",
						TypeURL: TypeURLTraceFilterPolicy,
						Rule: map[string]any{
							"id":     "trace-keep-http",
							"action": "ACTION_KEEP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "SPAN_RECORD_FIELD_NAME",
									},
									"regex": "^HTTP.*",
								},
							},
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "forward-compatibility ignores unknown fields",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "forward-compat-policy",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":             "forward-compat-policy",
							"action":         "ACTION_KEEP",
							"unknown_future": "something_new",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_BODY",
									},
									"exact": "test",
								},
							},
						},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "reject ACTION_UNSPECIFIED",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "bad-action",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "bad-action",
							"action": "ACTION_UNSPECIFIED",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_BODY",
									},
									"exact": "foo",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "got ACTION_UNSPECIFIED",
		},
		{
			name: "reject empty matchers",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "no-matchers",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":      "no-matchers",
							"action":  "ACTION_DROP",
							"matches": []any{},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "at least one matcher is required",
		},
		{
			name: "reject malformed RE2 regex",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "bad-regex",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "bad-regex",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_BODY",
									},
									"regex": "[a-z(",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "malformed RE2 regex",
		},
		{
			name: "reject missing target",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "missing-target",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "missing-target",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"exact": "foo",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "target must be specified",
		},
		{
			name: "reject unspecified record field",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unspecified-field",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "unspecified-field",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_UNSPECIFIED",
									},
									"exact": "foo",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "LOG_RECORD_FIELD_UNSPECIFIED",
		},
		{
			name: "reject empty log attribute key",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "empty-attr",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "empty-attr",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"log_attribute": "",
									},
									"exact": "foo",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "log_attribute key must not be empty",
		},
		{
			name: "reject missing predicate",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "missing-pred",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "missing-pred",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{
										"record_field": "LOG_RECORD_FIELD_BODY",
									},
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "predicate must be specified",
		},
		{
			name: "reject unsupported type_url",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unsupported-type",
						TypeURL: "type.googleapis.com/unknown.PolicyType",
						Rule: map[string]any{
							"id": "unsupported-type",
						},
					},
				},
			},
			expectErr:   true,
			errContains: "unsupported policy type_url",
		},
		{
			name: "reject nil rule",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "nil-rule",
						TypeURL: TypeURLLogFilterPolicy,
						Rule:    nil,
					},
				},
			},
			expectErr:   true,
			errContains: "rule must not be nil",
		},
		{
			name: "reject out-of-range action integer",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "bad-action-int",
						TypeURL: TypeURLLogFilterPolicy,
						Rule: map[string]any{
							"id":     "bad-action-int",
							"action": 99,
							"matches": []any{
								map[string]any{
									"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
									"exact":  "foo",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "action must be ACTION_KEEP or ACTION_DROP",
		},
		{
			name: "reject metric empty datapoint_attribute key",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "empty-dp-attr",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "empty-dp-attr",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"datapoint_attribute": ""},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "datapoint_attribute key must not be empty",
		},
		{
			name: "reject metric METRIC_DESCRIPTOR_FIELD_UNSPECIFIED",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unspecified-desc-field",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "unspecified-desc-field",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"descriptor_field": "METRIC_DESCRIPTOR_FIELD_UNSPECIFIED"},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "METRIC_DESCRIPTOR_FIELD_UNSPECIFIED",
		},
		{
			name: "reject metric SCOPE_FIELD_UNSPECIFIED",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unspecified-metric-scope-field",
						TypeURL: TypeURLMetricFilterPolicy,
						Rule: map[string]any{
							"id":     "unspecified-metric-scope-field",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"scope_field": "SCOPE_FIELD_UNSPECIFIED"},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "SCOPE_FIELD_UNSPECIFIED",
		},
		{
			name: "reject trace empty span_attribute key",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "empty-span-attr",
						TypeURL: TypeURLTraceFilterPolicy,
						Rule: map[string]any{
							"id":     "empty-span-attr",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"span_attribute": ""},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "span_attribute key must not be empty",
		},
		{
			name: "reject trace SPAN_RECORD_FIELD_UNSPECIFIED",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unspecified-span-field",
						TypeURL: TypeURLTraceFilterPolicy,
						Rule: map[string]any{
							"id":     "unspecified-span-field",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"record_field": "SPAN_RECORD_FIELD_UNSPECIFIED"},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "SPAN_RECORD_FIELD_UNSPECIFIED",
		},
		{
			name: "reject trace SCOPE_FIELD_UNSPECIFIED",
			cfg: &Config{
				Policies: []PolicyConfig{
					{
						ID:      "unspecified-trace-scope-field",
						TypeURL: TypeURLTraceFilterPolicy,
						Rule: map[string]any{
							"id":     "unspecified-trace-scope-field",
							"action": "ACTION_DROP",
							"matches": []any{
								map[string]any{
									"target": map[string]any{"scope_field": "SCOPE_FIELD_UNSPECIFIED"},
									"exact":  "val",
								},
							},
						},
					},
				},
			},
			expectErr:   true,
			errContains: "SCOPE_FIELD_UNSPECIFIED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPolicyProcessor_UnmarshalConf(t *testing.T) {
	rawMap := map[string]any{
		"policies": []any{
			map[string]any{
				"id":       "p1",
				"type_url": TypeURLLogFilterPolicy,
				"rule": map[string]any{
					"id":     "p1",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{
								"record_field": "LOG_RECORD_FIELD_BODY",
							},
							"exact": "keep-me",
						},
					},
				},
			},
		},
	}

	conf := confmap.NewFromStringMap(rawMap)
	cfg := &Config{}
	err := cfg.Unmarshal(conf)
	require.NoError(t, err)
	assert.Len(t, cfg.Compiled.LogPolicies, 1)
	assert.Equal(t, "p1", cfg.Compiled.LogPolicies[0].ID)
}

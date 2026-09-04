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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestFactory(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, component.MustNewType("policy"), f.Type())

	defaultCfg := f.CreateDefaultConfig()
	assert.Equal(t, &Config{StartupTimeout: 5 * time.Second}, defaultCfg)

	validCfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "test-log-policy",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "test-log-policy",
					"action": "ACTION_KEEP",
					"matches": []any{
						map[string]any{
							"target": map[string]any{"record_field": "LOG_RECORD_FIELD_BODY"},
							"exact":  "hello",
						},
					},
				},
			},
		},
	}

	invalidCfg := &Config{
		Policies: []PolicyConfig{
			{
				ID:      "invalid-policy",
				TypeURL: TypeURLLogFilterPolicy,
				Rule: map[string]any{
					"id":     "invalid-policy",
					"action": "ACTION_UNSPECIFIED",
				},
			},
		},
	}

	ctx := context.Background()
	set := processortest.NewNopSettings(f.Type())

	t.Run("create logs processor", func(t *testing.T) {
		lp, err := f.CreateLogs(ctx, set, validCfg, consumertest.NewNop())
		require.NoError(t, err)
		assert.NotNil(t, lp)

		_, err = f.CreateLogs(ctx, set, invalidCfg, consumertest.NewNop())
		require.Error(t, err)
	})

	t.Run("create metrics processor", func(t *testing.T) {
		mp, err := f.CreateMetrics(ctx, set, validCfg, consumertest.NewNop())
		require.NoError(t, err)
		assert.NotNil(t, mp)

		_, err = f.CreateMetrics(ctx, set, invalidCfg, consumertest.NewNop())
		require.Error(t, err)
	})

	t.Run("create traces processor", func(t *testing.T) {
		tp, err := f.CreateTraces(ctx, set, validCfg, consumertest.NewNop())
		require.NoError(t, err)
		assert.NotNil(t, tp)

		_, err = f.CreateTraces(ctx, set, invalidCfg, consumertest.NewNop())
		require.Error(t, err)
	})
}

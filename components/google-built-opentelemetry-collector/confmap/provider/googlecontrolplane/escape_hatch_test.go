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

package googlecontrolplane

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscapeHatchResolver(t *testing.T) {
	resolver := NewEscapeHatchResolver()
	tokens := map[string]string{
		"global_policy_processor":              "policy/global",
		"active_destination_exporter":          "otlp/gcp_destination",
		"active_destination_metric_preprocess": "batch/gcp_destination_metrics",
	}
	resolver.UpdateTokens(tokens)

	t.Run("ResolveToken for known tokens", func(t *testing.T) {
		for token, expected := range tokens {
			retrieved, err := resolver.ResolveToken(token)
			require.NoError(t, err)
			require.NotNil(t, retrieved)

			asStr, err := retrieved.AsString()
			require.NoError(t, err)
			assert.Equal(t, expected, asStr)
		}
	})

	t.Run("ResolveURI variations for component// and component://", func(t *testing.T) {
		testCases := []struct {
			uri         string
			expectedStr string
		}{
			{
				uri:         "component//global_policy_processor",
				expectedStr: "policy/global",
			},
			{
				uri:         "component://global_policy_processor",
				expectedStr: "policy/global",
			},
			{
				uri:         "googlecontrolplane:component//active_destination_exporter",
				expectedStr: "otlp/gcp_destination",
			},
			{
				uri:         "googlecontrolplane://component//active_destination_exporter",
				expectedStr: "otlp/gcp_destination",
			},
			{
				uri:         "googlecontrolplane:component://active_destination_metric_preprocess",
				expectedStr: "batch/gcp_destination_metrics",
			},
			{
				uri:         "googlecontrolplane://component://active_destination_metric_preprocess",
				expectedStr: "batch/gcp_destination_metrics",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.uri, func(t *testing.T) {
				retrieved, isToken, err := resolver.ResolveURI(tc.uri)
				require.True(t, isToken)
				require.NoError(t, err)
				require.NotNil(t, retrieved)

				asStr, err := retrieved.AsString()
				require.NoError(t, err)
				assert.Equal(t, tc.expectedStr, asStr)
			})
		}
	})

	t.Run("Non-token URI", func(t *testing.T) {
		retrieved, isToken, err := resolver.ResolveURI("googlecontrolplane:file:///etc/otelcol/policies.json")
		assert.False(t, isToken)
		assert.Nil(t, retrieved)
		assert.NoError(t, err)
	})

	t.Run("Descriptive error on unregistered token", func(t *testing.T) {
		retrieved, err := resolver.ResolveToken("nonexistent_component")
		require.Error(t, err)
		assert.Nil(t, retrieved)
		assert.Contains(t, err.Error(), "unknown component token: \"nonexistent_component\"")

		retrieved2, isToken, err2 := resolver.ResolveURI("component//unknown_token")
		assert.True(t, isToken)
		require.Error(t, err2)
		assert.Nil(t, retrieved2)
		assert.Contains(t, err2.Error(), "unknown component token: \"unknown_token\"")
	})
}

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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name          string
		rawURI        string
		expected      *ParsedURI
		expectedError string
	}{
		{
			name:   "Valid xDS with all query parameters",
			rawURI: "googlecontrolplane:xds://telemetrydirector.googleapis.com:443?gcp.fleet_id=1234&project=my-project&base_config=file:///etc/otelcol/base.yaml&startup_timeout=50ms",
			expected: &ParsedURI{
				Raw:            "googlecontrolplane:xds://telemetrydirector.googleapis.com:443?gcp.fleet_id=1234&project=my-project&base_config=file:///etc/otelcol/base.yaml&startup_timeout=50ms",
				Transport:      TransportXDS,
				Endpoint:       "telemetrydirector.googleapis.com:443",
				FleetID:        "1234",
				Project:        "my-project",
				BaseConfigURI:  "file:///etc/otelcol/base.yaml",
				StartupTimeout: 50 * time.Millisecond,
			},
		},
		{
			name:   "Valid xDS with double slash prefix",
			rawURI: "googlecontrolplane://xds://telemetrydirector.googleapis.com:443",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane://xds://telemetrydirector.googleapis.com:443",
				Transport: TransportXDS,
				Endpoint:  "telemetrydirector.googleapis.com:443",
			},
		},
		{
			name:   "Valid file with single colon",
			rawURI: "googlecontrolplane:file:///path/to/policies/",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane:file:///path/to/policies/",
				Transport: TransportFile,
				Path:      "/path/to/policies/",
			},
		},
		{
			name:   "Valid file with single colon and RFC 8089 single slash",
			rawURI: "googlecontrolplane:file:/etc/otelcol/policies.json",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane:file:/etc/otelcol/policies.json",
				Transport: TransportFile,
				Path:      "/etc/otelcol/policies.json",
			},
		},
		{
			name:   "Valid file with double slash prefix and RFC 8089 single slash",
			rawURI: "googlecontrolplane://file:/etc/otelcol/policies.json",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane://file:/etc/otelcol/policies.json",
				Transport: TransportFile,
				Path:      "/etc/otelcol/policies.json",
			},
		},
		{
			name:   "Valid file with double slash and startup_timeout",
			rawURI: "googlecontrolplane://file:///path/to/policies.json?startup_timeout=5s",
			expected: &ParsedURI{
				Raw:            "googlecontrolplane://file:///path/to/policies.json?startup_timeout=5s",
				Transport:      TransportFile,
				Path:           "/path/to/policies.json",
				StartupTimeout: 5 * time.Second,
			},
		},
		{
			name:   "Implicit xDS via host:port",
			rawURI: "googlecontrolplane://localhost:50051?gcp.fleet_id=test-fleet",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane://localhost:50051?gcp.fleet_id=test-fleet",
				Transport: TransportXDS,
				Endpoint:  "localhost:50051",
				FleetID:   "test-fleet",
			},
		},
		{
			name:   "Implicit file via triple slash",
			rawURI: "googlecontrolplane:///etc/otelcol/policies",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane:///etc/otelcol/policies",
				Transport: TransportFile,
				Path:      "/etc/otelcol/policies",
			},
		},
		{
			name:   "Implicit file via single slash",
			rawURI: "googlecontrolplane:/etc/otelcol/policies.json",
			expected: &ParsedURI{
				Raw:       "googlecontrolplane:/etc/otelcol/policies.json",
				Transport: TransportFile,
				Path:      "/etc/otelcol/policies.json",
			},
		},
		{
			name:          "Invalid scheme",
			rawURI:        "http://localhost:8080",
			expectedError: "invalid scheme",
		},
		{
			name:          "Unsupported transport sub-scheme",
			rawURI:        "googlecontrolplane:ftp://example.com/policies",
			expectedError: "unsupported transport scheme",
		},
		{
			name:          "Empty xDS endpoint",
			rawURI:        "googlecontrolplane:xds://",
			expectedError: "xds endpoint cannot be empty",
		},
		{
			name:          "Empty file path",
			rawURI:        "googlecontrolplane:file://",
			expectedError: "file path cannot be empty",
		},
		{
			name:          "Invalid startup_timeout format",
			rawURI:        "googlecontrolplane:xds://localhost:443?startup_timeout=invalid",
			expectedError: "invalid startup_timeout duration",
		},
		{
			name:          "Negative startup_timeout",
			rawURI:        "googlecontrolplane:xds://localhost:443?startup_timeout=-5s",
			expectedError: "startup_timeout duration must be non-negative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseURI(tc.rawURI)
			if tc.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				assert.Nil(t, parsed)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, parsed)
			assert.Equal(t, tc.expected.Raw, parsed.Raw)
			assert.Equal(t, tc.expected.Transport, parsed.Transport)
			assert.Equal(t, tc.expected.Endpoint, parsed.Endpoint)
			assert.Equal(t, tc.expected.Path, parsed.Path)
			assert.Equal(t, tc.expected.FleetID, parsed.FleetID)
			assert.Equal(t, tc.expected.Project, parsed.Project)
			assert.Equal(t, tc.expected.BaseConfigURI, parsed.BaseConfigURI)
			assert.Equal(t, tc.expected.StartupTimeout, parsed.StartupTimeout)
		})
	}
}

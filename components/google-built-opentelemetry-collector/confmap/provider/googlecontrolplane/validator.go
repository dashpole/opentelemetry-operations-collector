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
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.uber.org/zap"
)

// Diagnostic records structured diagnostic metadata for a rejected or unsupported policy.
type Diagnostic struct {
	PolicyID string
	TypeURL  string
	Reason   string
	Event    string // EventPolicyUnsupported or EventPolicyRejected
}

// ValidationResult contains the outcome of Layer 1 fail-open validation.
type ValidationResult struct {
	ValidPolicies      []*v3.TypedExtensionConfig
	PolicyStatuses     map[string]PolicyStatusEntry
	Diagnostics        []Diagnostic
	HasSkippedPolicies bool
}

// SkippedSummary provides a concise textual summary of all skipped policies for xDS NACK error details.
func (vr *ValidationResult) SkippedSummary() string {
	if len(vr.Diagnostics) == 0 {
		return ""
	}
	parts := make([]string, len(vr.Diagnostics))
	for i, d := range vr.Diagnostics {
		parts[i] = fmt.Sprintf("[%s] %s (%s): %s", d.Event, d.PolicyID, d.TypeURL, d.Reason)
	}
	return strings.Join(parts, "; ")
}

// PolicyValidator implements Layer 1 Fail-Open policy validation.
type PolicyValidator struct {
	registry *driver.PolicyDriverRegistry
	logger   *zap.Logger
}

// NewPolicyValidator creates a new PolicyValidator.
func NewPolicyValidator(registry *driver.PolicyDriverRegistry, logger *zap.Logger) *PolicyValidator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PolicyValidator{
		registry: registry,
		logger:   logger,
	}
}

// ValidatePolicies executes Layer 1 validation directly on a slice of TypedExtensionConfig policies.
func (v *PolicyValidator) ValidatePolicies(policies []*v3.TypedExtensionConfig) *ValidationResult {
	return v.Validate(&xdsv1alpha1.TelemetryCollector{Policies: policies})
}

// Validate executes Layer 1 Fail-Open policy validation on incoming TelemetryCollector policies.
// Unsupported TypeURLs and invalid policies are skipped with diagnostics logged/recorded, while
// valid policies are retained for configuration compilation.
func (v *PolicyValidator) Validate(collector *xdsv1alpha1.TelemetryCollector) *ValidationResult {
	res := &ValidationResult{
		ValidPolicies:  make([]*v3.TypedExtensionConfig, 0),
		PolicyStatuses: make(map[string]PolicyStatusEntry),
		Diagnostics:    make([]Diagnostic, 0),
	}

	if collector == nil || len(collector.Policies) == 0 {
		return res
	}

	seenIDs := make(map[string]bool)
	var activeDestinationID string

	for _, p := range collector.Policies {
		policyID := p.GetName()

		// 1. Validate ID presence
		if policyID == "" {
			diag := Diagnostic{
				PolicyID: "<empty>",
				TypeURL:  p.GetTypedConfig().GetTypeUrl(),
				Reason:   "policy name/id must not be empty",
				Event:    EventPolicyRejected,
			}
			res.Diagnostics = append(res.Diagnostics, diag)
			res.HasSkippedPolicies = true
			v.logger.Warn("Policy rejected",
				zap.String("event.name", EventPolicyRejected),
				zap.String("policy_id", "<empty>"),
				zap.String("reason", diag.Reason),
			)
			continue
		}

		typeURL := ""
		if p.TypedConfig != nil {
			typeURL = p.TypedConfig.TypeUrl
		}

		// 2. Check for duplicate policy IDs
		if seenIDs[policyID] {
			diag := Diagnostic{
				PolicyID: policyID,
				TypeURL:  typeURL,
				Reason:   fmt.Sprintf("duplicate policy ID %q detected in policy set", policyID),
				Event:    EventPolicyRejected,
			}
			res.Diagnostics = append(res.Diagnostics, diag)
			res.PolicyStatuses[policyID] = PolicyStatusEntry{
				TypeURL: typeURL,
				Status:  "rejected",
			}
			res.HasSkippedPolicies = true
			v.logger.Warn("Policy rejected",
				zap.String("event.name", EventPolicyRejected),
				zap.String("policy_id", policyID),
				zap.String("type_url", typeURL),
				zap.String("reason", diag.Reason),
			)
			continue
		}
		seenIDs[policyID] = true

		// 3. Check for typed_config presence
		if p.TypedConfig == nil || typeURL == "" {
			diag := Diagnostic{
				PolicyID: policyID,
				TypeURL:  "",
				Reason:   "policy typed_config or type_url is missing",
				Event:    EventPolicyRejected,
			}
			res.Diagnostics = append(res.Diagnostics, diag)
			res.PolicyStatuses[policyID] = PolicyStatusEntry{
				TypeURL: "",
				Status:  "rejected",
			}
			res.HasSkippedPolicies = true
			v.logger.Warn("Policy rejected",
				zap.String("event.name", EventPolicyRejected),
				zap.String("policy_id", policyID),
				zap.String("reason", diag.Reason),
			)
			continue
		}

		// 4. Lookup driver for TypeURL
		drv := v.registry.GetDriver(typeURL)
		if drv == nil {
			diag := Diagnostic{
				PolicyID: policyID,
				TypeURL:  typeURL,
				Reason:   fmt.Sprintf("unsupported policy type_url: %q", typeURL),
				Event:    EventPolicyUnsupported,
			}
			res.Diagnostics = append(res.Diagnostics, diag)
			res.PolicyStatuses[policyID] = PolicyStatusEntry{
				TypeURL: typeURL,
				Status:  "unsupported",
			}
			res.HasSkippedPolicies = true
			v.logger.Warn("Policy unsupported",
				zap.String("event.name", EventPolicyUnsupported),
				zap.String("policy_id", policyID),
				zap.String("type_url", typeURL),
				zap.String("reason", diag.Reason),
			)
			continue
		}

		// 5. Enforce single active destination constraint
		if drv.Class() == driver.PolicyClassDestination {
			if activeDestinationID != "" {
				diag := Diagnostic{
					PolicyID: policyID,
					TypeURL:  typeURL,
					Reason:   fmt.Sprintf("multiple destination policies detected: policy %q conflicts with already active destination %q", policyID, activeDestinationID),
					Event:    EventPolicyRejected,
				}
				res.Diagnostics = append(res.Diagnostics, diag)
				res.PolicyStatuses[policyID] = PolicyStatusEntry{
					TypeURL: typeURL,
					Status:  "rejected",
				}
				res.HasSkippedPolicies = true
				v.logger.Warn("Policy rejected",
					zap.String("event.name", EventPolicyRejected),
					zap.String("policy_id", policyID),
					zap.String("type_url", typeURL),
					zap.String("reason", diag.Reason),
				)
				continue
			}
			activeDestinationID = policyID
		}

		// 6. Execute driver validation (e.g. malformed regex, invalid action)
		if err := drv.Validate(p.TypedConfig); err != nil {
			diag := Diagnostic{
				PolicyID: policyID,
				TypeURL:  typeURL,
				Reason:   err.Error(),
				Event:    EventPolicyRejected,
			}
			res.Diagnostics = append(res.Diagnostics, diag)
			res.PolicyStatuses[policyID] = PolicyStatusEntry{
				TypeURL: typeURL,
				Status:  "rejected",
			}
			res.HasSkippedPolicies = true
			v.logger.Warn("Policy rejected",
				zap.String("event.name", EventPolicyRejected),
				zap.String("policy_id", policyID),
				zap.String("type_url", typeURL),
				zap.String("reason", diag.Reason),
			)
			continue
		}

		// Policy passed Layer 1 validation
		res.PolicyStatuses[policyID] = PolicyStatusEntry{
			TypeURL: typeURL,
			Status:  "accepted",
		}
		res.ValidPolicies = append(res.ValidPolicies, p)
	}

	return res
}

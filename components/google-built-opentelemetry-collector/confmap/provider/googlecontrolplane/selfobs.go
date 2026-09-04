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

const (
	// EventPolicyUnsupported indicates that a policy has an unsupported TypeURL.
	EventPolicyUnsupported = "telemetry.policy.unsupported"
	// EventPolicyRejected indicates that a policy failed structural or semantic validation.
	EventPolicyRejected = "telemetry.policy.rejected"
	// EventPolicyCompilationFailed indicates that the synthesized configuration failed PreValidator checks.
	EventPolicyCompilationFailed = "telemetry.policy.compilation_failed"
)

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

package driver

import (
	"google.golang.org/protobuf/proto"
)

const (
	TypeURLSelfObservability      = "type.googleapis.com/google.telemetry.policy.v1alpha1.SelfObservabilityPolicy"
	TypeURLSelfObservabilityAlias = "self_observability@v1"
)

// SelfObservabilityDriver generates configuration for the internal self-observability loopback receiver.
type SelfObservabilityDriver struct{}

// NewSelfObservabilityDriver creates a new SelfObservabilityDriver.
func NewSelfObservabilityDriver() *SelfObservabilityDriver {
	return &SelfObservabilityDriver{}
}

func (d *SelfObservabilityDriver) TypeURL() string {
	return TypeURLSelfObservability
}

func (d *SelfObservabilityDriver) Class() PolicyClass {
	return PolicyClassSource
}

func (d *SelfObservabilityDriver) Validate(policy proto.Message) error {
	return nil
}

func (d *SelfObservabilityDriver) GenerateConfig(policy proto.Message, ctx *CompilationContext) (*ConfigFragment, error) {
	frag := NewConfigFragment()

	receiverName := "otlp/selfobs_internal"
	frag.Receivers[receiverName] = map[string]any{
		"protocols": map[string]any{
			"grpc": map[string]any{
				"endpoint": "127.0.0.1:4320",
			},
		},
	}

	frag.Pipelines["metrics"] = PipelineConfig{
		Receivers: []string{receiverName},
	}

	return frag, nil
}

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
	TypeURLOtlpSource      = "type.googleapis.com/google.telemetry.policy.v1alpha1.OtlpSourcePolicy"
	TypeURLOtlpSourceAlias = "otlp_source@v1"
)

// OtlpSourceDriver generates collector configuration for receiving telemetry over OTLP (gRPC and HTTP).
type OtlpSourceDriver struct{}

// NewOtlpSourceDriver creates a new OtlpSourceDriver.
func NewOtlpSourceDriver() *OtlpSourceDriver {
	return &OtlpSourceDriver{}
}

func (d *OtlpSourceDriver) TypeURL() string {
	return TypeURLOtlpSource
}

func (d *OtlpSourceDriver) Class() PolicyClass {
	return PolicyClassSource
}

func (d *OtlpSourceDriver) Validate(policy proto.Message) error {
	return nil
}

func (d *OtlpSourceDriver) GenerateConfig(policy proto.Message, ctx *CompilationContext) (*ConfigFragment, error) {
	frag := NewConfigFragment()

	frag.Receivers["otlp"] = map[string]any{
		"protocols": map[string]any{
			"grpc": map[string]any{
				"endpoint": "0.0.0.0:4317",
			},
			"http": map[string]any{
				"endpoint": "0.0.0.0:4318",
			},
		},
	}

	frag.Pipelines["logs"] = PipelineConfig{
		Receivers: []string{"otlp"},
	}
	frag.Pipelines["metrics"] = PipelineConfig{
		Receivers: []string{"otlp"},
	}
	frag.Pipelines["traces"] = PipelineConfig{
		Receivers: []string{"otlp"},
	}

	return frag, nil
}

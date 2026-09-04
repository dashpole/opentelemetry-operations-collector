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
	TypeURLFilelogSource      = "type.googleapis.com/google.telemetry.policy.v1alpha1.FilelogSourcePolicy"
	TypeURLFilelogSourceAlias = "filelog_source@v1"
)

// FilelogSourceDriver generates collector configuration for tailing log files via filelog receiver.
type FilelogSourceDriver struct{}

// NewFilelogSourceDriver creates a new FilelogSourceDriver.
func NewFilelogSourceDriver() *FilelogSourceDriver {
	return &FilelogSourceDriver{}
}

func (d *FilelogSourceDriver) TypeURL() string {
	return TypeURLFilelogSource
}

func (d *FilelogSourceDriver) Class() PolicyClass {
	return PolicyClassSource
}

func (d *FilelogSourceDriver) Validate(policy proto.Message) error {
	return nil
}

func (d *FilelogSourceDriver) GenerateConfig(policy proto.Message, ctx *CompilationContext) (*ConfigFragment, error) {
	frag := NewConfigFragment()

	frag.Receivers["filelog"] = map[string]any{
		"include": []any{
			"/var/log/**/*.log",
		},
	}

	frag.Pipelines["logs"] = PipelineConfig{
		Receivers: []string{"filelog"},
	}

	return frag, nil
}

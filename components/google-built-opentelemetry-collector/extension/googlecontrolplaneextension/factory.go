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

package googlecontrolplaneextension

import (
	"context"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	// Type is the component type for googlecontrolplaneextension.
	Type = "googlecontrolplaneextension"
)

var (
	typeStr = component.MustNewType(Type)
)

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newExtension(set, cfg.(*Config)), nil
}

// NewFactory creates a factory for googlecontrolplaneextension.
func NewFactory() extension.Factory {
	return NewFactoryWithStatusRegistry(nil)
}

// NewFactoryWithStatusRegistry creates a factory for googlecontrolplaneextension with a custom StatusRegistry.
func NewFactoryWithStatusRegistry(sr *googlecontrolplane.StatusRegistry) extension.Factory {
	return extension.NewFactory(
		typeStr,
		createDefaultConfig,
		func(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			ext := newExtension(set, cfg.(*Config))
			if sr != nil {
				ext = ext.WithStatusRegistry(sr)
			}
			return ext, nil
		},
		component.StabilityLevelAlpha,
	)
}

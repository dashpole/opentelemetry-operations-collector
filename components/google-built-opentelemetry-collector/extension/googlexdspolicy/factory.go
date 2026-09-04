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

package googlexdspolicy

import (
	"context"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	// Type is the component type string for googlexdspolicy.
	Type = "googlexdspolicy"
)

var (
	typeStr = component.MustNewType(Type)
)

func createDefaultConfig() component.Config {
	return &Config{
		StartupTimeout: DefaultStartupTimeout,
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newExtension(set, cfg.(*Config)), nil
}

// NewFactory creates a factory for googlexdspolicy extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		typeStr,
		createDefaultConfig,
		createExtension,
		component.StabilityLevelAlpha,
	)
}

// NewFactoryWithRegistry creates a factory for googlexdspolicy extension using a custom InformerRegistry.
func NewFactoryWithRegistry(reg *controlplane.InformerRegistry) extension.Factory {
	return extension.NewFactory(
		typeStr,
		createDefaultConfig,
		func(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			return newExtension(set, cfg.(*Config)).WithRegistry(reg), nil
		},
		component.StabilityLevelAlpha,
	)
}

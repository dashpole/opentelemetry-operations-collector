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
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// PreValidator implements the Layer 2 crash-proof runtime gate.
// It verifies structural integrity, cross-references, and component configurations
// before a configuration can be loaded or reloaded by the collector.
type PreValidator struct {
	registry *driver.PolicyDriverRegistry
	logger   *zap.Logger
}

// NewPreValidator creates a new PreValidator.
func NewPreValidator(registry *driver.PolicyDriverRegistry, logger *zap.Logger) *PreValidator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PreValidator{
		registry: registry,
		logger:   logger,
	}
}

// Validate verifies the full topology and component configurations in conf.
func (pv *PreValidator) Validate(conf map[string]any) error {
	if conf == nil {
		err := fmt.Errorf("configuration must not be nil")
		pv.emitFailure("root", err)
		return err
	}

	receivers, _ := conf["receivers"].(map[string]any)
	processors, _ := conf["processors"].(map[string]any)
	exporters, _ := conf["exporters"].(map[string]any)
	extensions, _ := conf["extensions"].(map[string]any)
	service, _ := conf["service"].(map[string]any)

	// 1. Validate service.extensions cross-references
	if service != nil {
		serviceExts := toStringSlice(service["extensions"])
		for _, extName := range serviceExts {
			if _, exists := extensions[extName]; !exists {
				err := fmt.Errorf("service references undeclared extension: %s", extName)
				pv.emitFailure(extName, err)
				return err
			}
		}
	}

	// 2. Validate service.pipelines structural integrity & cross-references when configured.
	if service == nil {
		err := fmt.Errorf("service configuration is invalid or missing")
		pv.emitFailure("service", err)
		return err
	}

	// In composable configuration models (e.g. filter-only payloads or external base configs),
	// synthesizing zero pipelines is valid under Zero Empty Pipeline Synthesis.
	if pipelines, ok := service["pipelines"].(map[string]any); ok && len(pipelines) > 0 {
		for pipeName, pipeData := range pipelines {
		pMap, ok := pipeData.(map[string]any)
		if !ok {
			err := fmt.Errorf("pipeline %s configuration is invalid", pipeName)
			pv.emitFailure(pipeName, err)
			return err
		}

		// Validate receivers: must be non-empty and declared
		recvs := toStringSlice(pMap["receivers"])
		if len(recvs) == 0 {
			err := fmt.Errorf("pipeline %s must have at least one receiver", pipeName)
			pv.emitFailure(pipeName, err)
			return err
		}
		for _, r := range recvs {
			if _, exists := receivers[r]; !exists {
				err := fmt.Errorf("pipeline %s references undeclared receiver: %s", pipeName, r)
				pv.emitFailure(pipeName, err)
				return err
			}
		}

		// Validate processors: must be declared, and no duplicates allowed
		procs := toStringSlice(pMap["processors"])
		procSet := make(map[string]struct{}, len(procs))
		for _, pStr := range procs {
			if _, duplicate := procSet[pStr]; duplicate {
				err := fmt.Errorf("pipeline %s references duplicate processor %q", pipeName, pStr)
				pv.emitFailure(pipeName, err)
				return err
			}
			procSet[pStr] = struct{}{}
			if _, exists := processors[pStr]; !exists {
				err := fmt.Errorf("pipeline %s references undeclared processor: %s", pipeName, pStr)
				pv.emitFailure(pipeName, err)
				return err
			}
		}

		// Validate exporters: must be non-empty and declared
		exps := toStringSlice(pMap["exporters"])
		if len(exps) == 0 {
			err := fmt.Errorf("pipeline %s must have at least one exporter", pipeName)
			pv.emitFailure(pipeName, err)
			return err
		}
		for _, e := range exps {
			if _, exists := exporters[e]; !exists {
				err := fmt.Errorf("pipeline %s references undeclared exporter: %s", pipeName, e)
				pv.emitFailure(pipeName, err)
				return err
			}
		}
	}
	}

	// 3. In-Tree Component Config Validation across ALL component categories
	if err := pv.validateComponentMap("receiver", receivers, func(compType string) any {
		return pv.registry.GetReceiverFactory(compType)
	}); err != nil {
		return err
	}

	if err := pv.validateComponentMap("processor", processors, func(compType string) any {
		return pv.registry.GetProcessorFactory(compType)
	}); err != nil {
		return err
	}

	if err := pv.validateComponentMap("exporter", exporters, func(compType string) any {
		return pv.registry.GetExporterFactory(compType)
	}); err != nil {
		return err
	}

	if err := pv.validateComponentMap("extension", extensions, func(compType string) any {
		return pv.registry.GetExtensionFactory(compType)
	}); err != nil {
		return err
	}

	return nil
}

type factoryLookupFunc func(componentType string) any

func (pv *PreValidator) validateComponentMap(category string, components map[string]any, getFactory factoryLookupFunc) error {
	for compID, compCfg := range components {
		var cfgMap map[string]any
		if compCfg != nil {
			var ok bool
			cfgMap, ok = compCfg.(map[string]any)
			if !ok {
				err := fmt.Errorf("%s %q configuration is not a valid map", category, compID)
				pv.emitFailure(compID, err)
				return err
			}
		}
		// Nil-map safety: if compCfg is nil or empty, treat as empty map
		if cfgMap == nil {
			cfgMap = make(map[string]any)
		}

		parts := strings.SplitN(compID, "/", 2)
		compType := parts[0]

		factory := getFactory(compType)
		if factory == nil {
			err := fmt.Errorf("unknown %s type %q for component %q", category, compType, compID)
			pv.emitFailure(compID, err)
			return err
		}

		var cfg any
		switch f := factory.(type) {
		case receiver.Factory:
			cfg = f.CreateDefaultConfig()
		case processor.Factory:
			cfg = f.CreateDefaultConfig()
		case exporter.Factory:
			cfg = f.CreateDefaultConfig()
		case extension.Factory:
			cfg = f.CreateDefaultConfig()
		}

		if cfg != nil {
			if err := confmap.NewFromStringMap(cfgMap).Unmarshal(cfg); err != nil {
				err = fmt.Errorf("failed to unmarshal config for %s %q: %w", category, compID, err)
				pv.emitFailure(compID, err)
				return err
			}
			if v, ok := cfg.(interface{ Validate() error }); ok {
				if err := v.Validate(); err != nil {
					err = fmt.Errorf("invalid config for %s %q: %w", category, compID, err)
					pv.emitFailure(compID, err)
					return err
				}
			}
		}
	}
	return nil
}

func (pv *PreValidator) emitFailure(componentID string, err error) {
	pv.logger.Error("PreValidator rejected synthesized configuration; reload aborted to prevent process crash",
		zap.String("event.name", EventPolicyCompilationFailed),
		zap.String("component_id", componentID),
		zap.Error(err),
	)
}

func toStringSlice(val any) []string {
	switch v := val.(type) {
	case []string:
		return v
	case []any:
		res := make([]string, len(v))
		for i, item := range v {
			res[i] = fmt.Sprintf("%v", item)
		}
		return res
	default:
		return nil
	}
}

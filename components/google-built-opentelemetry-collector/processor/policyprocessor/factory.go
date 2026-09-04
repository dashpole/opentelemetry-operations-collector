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

package policyprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var componentType = component.MustNewType("policy")

// NewFactory creates a factory for the policyprocessor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		componentType,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, component.StabilityLevelAlpha),
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
		processor.WithTraces(createTracesProcessor, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		StartupTimeout: 5 * time.Second,
	}
}

func createLogsProcessor(
	ctx context.Context,
	params processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %T", cfg)
	}
	if err := pCfg.Validate(); err != nil {
		return nil, err
	}
	proc, err := newPolicyProcessor(params, pCfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx,
		params,
		cfg,
		nextConsumer,
		proc.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	params processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %T", cfg)
	}
	if err := pCfg.Validate(); err != nil {
		return nil, err
	}
	proc, err := newPolicyProcessor(params, pCfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(
		ctx,
		params,
		cfg,
		nextConsumer,
		proc.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
	)
}

func createTracesProcessor(
	ctx context.Context,
	params processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %T", cfg)
	}
	if err := pCfg.Validate(); err != nil {
		return nil, err
	}
	proc, err := newPolicyProcessor(params, pCfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(
		ctx,
		params,
		cfg,
		nextConsumer,
		proc.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
	)
}

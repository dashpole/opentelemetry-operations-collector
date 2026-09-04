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

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type evaluationRecorderFunc func(ctx context.Context, opt metric.MeasurementOption)

type policyProcessor struct {
	cfg                *Config
	logger             *zap.Logger
	evaluationsCounter metric.Int64Counter
}

func newPolicyProcessor(set processor.Settings, cfg *Config) (*policyProcessor, error) {
	mp := set.TelemetrySettings.MeterProvider
	if mp == nil {
		mp = noop.NewMeterProvider()
	}

	meter := mp.Meter("processor/policyprocessor")
	counter, err := meter.Int64Counter(
		"telemetry_policy_evaluations_total",
		metric.WithDescription("Total number of telemetry policy evaluations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return &policyProcessor{
		cfg:                cfg,
		logger:             set.Logger,
		evaluationsCounter: counter,
	}, nil
}

func (p *policyProcessor) recordEvaluation(ctx context.Context, opt metric.MeasurementOption) {
	if p.evaluationsCounter != nil && opt != nil {
		p.evaluationsCounter.Add(ctx, 1, opt)
	}
}

func (p *policyProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	pruneLogs(ctx, ld, p.cfg.Compiled.LogPolicies, p.recordEvaluation)
	return ld, nil
}

func (p *policyProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	pruneMetrics(
		ctx,
		md,
		p.cfg.Compiled.MetricInstrumentPolicies,
		p.cfg.Compiled.MetricDataPointPolicies,
		p.recordEvaluation,
	)
	return md, nil
}

func (p *policyProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	pruneTraces(ctx, td, p.cfg.Compiled.TracePolicies, p.recordEvaluation)
	return td, nil
}

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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
)

type evaluationRecorderFunc func(ctx context.Context, opt metric.MeasurementOption)

type policyStore struct {
	mu      sync.Mutex
	sources map[string]map[string]*v3.TypedExtensionConfig // informerID -> policyID -> Policy
}

type policyProcessor struct {
	cfg                  *Config
	logger               *zap.Logger
	evaluationsCounter   metric.Int64Counter
	compiled             atomic.Pointer[CompiledPolicies]
	store                policyStore
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	started              atomic.Bool
	informerExtensions   []component.ID
	startupTimeout       time.Duration
	failOnStartupTimeout bool
	staticPolicies       []PolicyConfig
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

	ctx, cancel := context.WithCancel(context.Background())

	startupTimeout := cfg.StartupTimeout
	if startupTimeout <= 0 {
		startupTimeout = 5 * time.Second
	}

	p := &policyProcessor{
		cfg:                  cfg,
		logger:               set.Logger,
		evaluationsCounter:   counter,
		informerExtensions:   cfg.InformerExtensions,
		startupTimeout:       startupTimeout,
		failOnStartupTimeout: cfg.FailOnStartupTimeout,
		staticPolicies:       cfg.Policies,
		ctx:                  ctx,
		cancel:               cancel,
	}

	p.store.sources = make(map[string]map[string]*v3.TypedExtensionConfig)

	initialCompiled := cfg.Compiled
	p.compiled.Store(&initialCompiled)

	return p, nil
}

// Start discovers configured informer extensions, validates readiness, and launches background watch loops.
func (p *policyProcessor) Start(ctx context.Context, host component.Host) error {
	if !p.started.CompareAndSwap(false, true) {
		return nil
	}

	if len(p.informerExtensions) == 0 {
		return nil
	}

	informers := make(map[string]controlplane.PolicyInformer, len(p.informerExtensions))
	hostExts := host.GetExtensions()
	for _, id := range p.informerExtensions {
		ext, ok := hostExts[id]
		if !ok {
			return fmt.Errorf("informer extension %q not found in host; ensure it is listed under service.extensions", id)
		}
		informer, ok := ext.(controlplane.PolicyInformer)
		if !ok {
			return fmt.Errorf("extension %q does not implement controlplane.PolicyInformer", id)
		}
		informers[id.String()] = informer
	}

	// Wait up to startupTimeout across informers on informer.Ready()
	startupTimeout := p.startupTimeout
	if startupTimeout <= 0 {
		startupTimeout = 5 * time.Second
	}
	startupTimer := time.NewTimer(startupTimeout)
	defer startupTimer.Stop()

	for _, id := range p.informerExtensions {
		informer := informers[id.String()]
		select {
		case <-informer.Ready():
		case <-startupTimer.C:
			if p.failOnStartupTimeout {
				return fmt.Errorf("policy informer %q did not become ready within %v", id, p.startupTimeout)
			}
			p.logger.Warn("Policy informer did not become ready within timeout; starting in fail-open degraded mode",
				zap.String("informer", id.String()),
				zap.Duration("timeout", p.startupTimeout),
			)
		case <-ctx.Done():
			return ctx.Err()
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}

	// Launch self-healing watch loops
	for _, id := range p.informerExtensions {
		informerID := id.String()
		informer := informers[informerID]
		p.wg.Add(1)
		go func(infID string, inf controlplane.PolicyInformer) {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				default:
				}

				ch, err := inf.Watch(p.ctx, "")
				if err != nil {
					p.logger.Error("Failed to subscribe policy watch; backing off before retry",
						zap.String("informer", infID), zap.Error(err))
					select {
					case <-p.ctx.Done():
						return
					case <-time.After(1 * time.Second):
						continue
					}
				}

			watchLoop:
				for {
					select {
					case <-p.ctx.Done():
						return
					case event, ok := <-ch:
						if !ok {
							if p.ctx.Err() == nil {
								p.logger.Warn("Policy watch channel closed unexpectedly; backing off before resubscribing",
									zap.String("informer", infID))
								select {
								case <-p.ctx.Done():
									return
								case <-time.After(100 * time.Millisecond):
								}
							}
							break watchLoop
						}
						if event.Type == controlplane.EventUnknown {
							continue
						}
						p.handleWatchEvent(infID, inf, event)
					}
				}
			}
		}(informerID, informer)
	}

	return nil
}

// Shutdown terminates all background watch loops.
func (p *policyProcessor) Shutdown(ctx context.Context) error {
	p.cancel()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *policyProcessor) handleWatchEvent(informerID string, informer controlplane.PolicyInformer, event controlplane.PolicyWatchEvent) {
	switch event.Type {
	case controlplane.EventResync:
		snapshot, err := informer.List("")
		if err != nil {
			p.logger.Error("Failed to list policies on EventResync",
				zap.String("informer", informerID),
				zap.Error(err),
			)
			return
		}

		p.store.mu.Lock()
		defer p.store.mu.Unlock()

		if p.store.sources == nil {
			p.store.sources = make(map[string]map[string]*v3.TypedExtensionConfig)
		}
		prevSources := p.store.sources[informerID]
		newSources := make(map[string]*v3.TypedExtensionConfig)
		for _, policy := range snapshot {
			if policy == nil || policy.TypedConfig == nil {
				continue
			}
			if isFilterPolicy(policy.TypedConfig.TypeUrl) {
				newSources[policy.Name] = policy
			}
		}
		p.store.sources[informerID] = newSources

		newCompiled, err := p.compileLocked()
		if err != nil {
			p.store.sources[informerID] = prevSources
			p.logger.Error("Failed to compile resynced policies; rolling back store and retaining active rules",
				zap.String("informer", informerID),
				zap.Error(err),
			)
			return
		}
		p.compiled.Store(newCompiled)

	case controlplane.EventAdded, controlplane.EventModified:
		typeURL := event.TypeURL
		if typeURL == "" && event.Policy != nil && event.Policy.TypedConfig != nil {
			typeURL = event.Policy.TypedConfig.TypeUrl
		}
		if !isFilterPolicy(typeURL) {
			return
		}

		p.store.mu.Lock()
		defer p.store.mu.Unlock()

		if p.store.sources == nil {
			p.store.sources = make(map[string]map[string]*v3.TypedExtensionConfig)
		}
		if p.store.sources[informerID] == nil {
			p.store.sources[informerID] = make(map[string]*v3.TypedExtensionConfig)
		}
		prevPolicy := p.store.sources[informerID][event.PolicyID]
		p.store.sources[informerID][event.PolicyID] = event.Policy

		newCompiled, err := p.compileLocked()
		if err != nil {
			if prevPolicy == nil {
				delete(p.store.sources[informerID], event.PolicyID)
			} else {
				p.store.sources[informerID][event.PolicyID] = prevPolicy
			}
			p.logger.Error("Failed to compile updated policy; rolling back store and retaining active rules",
				zap.String("informer", informerID),
				zap.String("policy_id", event.PolicyID),
				zap.Error(err),
			)
			return
		}
		p.compiled.Store(newCompiled)

	case controlplane.EventDeleted:
		if event.TypeURL != "" && !isFilterPolicy(event.TypeURL) {
			return
		}

		p.store.mu.Lock()
		defer p.store.mu.Unlock()

		if p.store.sources == nil || p.store.sources[informerID] == nil {
			return
		}
		prevPolicy, exists := p.store.sources[informerID][event.PolicyID]
		if !exists {
			return
		}
		delete(p.store.sources[informerID], event.PolicyID)

		newCompiled, err := p.compileLocked()
		if err != nil {
			if prevPolicy != nil {
				p.store.sources[informerID][event.PolicyID] = prevPolicy
			}
			p.logger.Error("Failed to compile after deleting policy; rolling back store and retaining active rules",
				zap.String("informer", informerID),
				zap.String("policy_id", event.PolicyID),
				zap.Error(err),
			)
			return
		}
		p.compiled.Store(newCompiled)
	}
}

func (p *policyProcessor) compileLocked() (*CompiledPolicies, error) {
	newCompiled := &CompiledPolicies{}
	seenPolicyIDs := make(map[string]bool)

	// 1. Iterate over informerExtensions in configuration slice order (first-match-wins)
	for _, extID := range p.informerExtensions {
		infID := extID.String()
		srcMap := p.store.sources[infID]
		if len(srcMap) == 0 {
			continue
		}

		policyIDs := make([]string, 0, len(srcMap))
		for pid := range srcMap {
			policyIDs = append(policyIDs, pid)
		}
		sort.Strings(policyIDs)

		for _, pid := range policyIDs {
			if seenPolicyIDs[pid] {
				continue // Earlier informer has higher precedence
			}
			seenPolicyIDs[pid] = true

			typedCfg := srcMap[pid]
			if typedCfg == nil {
				continue
			}
			if err := compileTypedExtensionConfig(pid, typedCfg, newCompiled); err != nil {
				return nil, err
			}
		}
	}

	// 2. Append any static policies whose IDs haven't been seen
	for i, staticPol := range p.staticPolicies {
		pid := staticPol.ID
		if pid == "" && staticPol.Rule != nil {
			if rid, ok := staticPol.Rule["id"].(string); ok {
				pid = rid
			}
		}
		if pid != "" && seenPolicyIDs[pid] {
			continue
		}
		if pid != "" {
			seenPolicyIDs[pid] = true
		}
		if err := compilePolicyConfig(i, staticPol, newCompiled); err != nil {
			return nil, err
		}
	}

	return newCompiled, nil
}

func (p *policyProcessor) recordEvaluation(ctx context.Context, opt metric.MeasurementOption) {
	if p.evaluationsCounter != nil && opt != nil {
		p.evaluationsCounter.Add(ctx, 1, opt)
	}
}

func (p *policyProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	compiled := p.compiled.Load()
	if compiled != nil && len(compiled.LogPolicies) > 0 {
		pruneLogs(ctx, ld, compiled.LogPolicies, p.recordEvaluation)
	}
	return ld, nil
}

func (p *policyProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	compiled := p.compiled.Load()
	if compiled != nil && (len(compiled.MetricInstrumentPolicies) > 0 || len(compiled.MetricDataPointPolicies) > 0) {
		pruneMetrics(
			ctx,
			md,
			compiled.MetricInstrumentPolicies,
			compiled.MetricDataPointPolicies,
			p.recordEvaluation,
		)
	}
	return md, nil
}

func (p *policyProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	compiled := p.compiled.Load()
	if compiled != nil && len(compiled.TracePolicies) > 0 {
		pruneTraces(ctx, td, compiled.TracePolicies, p.recordEvaluation)
	}
	return td, nil
}

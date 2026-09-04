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
	"errors"
	"fmt"
	"sync"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane/xds"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var defaultSupportedTypeURLs = []string{
	"type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.OtlpSourcePolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.FilelogSourcePolicy",
	"type.googleapis.com/google.telemetry.policy.v1alpha1.SelfObservabilityPolicy",
}

type googleXdsPolicyExtension struct {
	cfg       *Config
	logger    *zap.Logger
	set       extension.Settings
	registry  *controlplane.InformerRegistry
	dialOpts  []grpc.DialOption
	handle    *controlplane.ClientHandle
	metricReg metric.Registration
	readyCh   chan struct{}
	readyOnce sync.Once
	mu        sync.Mutex
}

var _ extension.Extension = (*googleXdsPolicyExtension)(nil)
var _ controlplane.PolicyInformer = (*googleXdsPolicyExtension)(nil)

func newExtension(set extension.Settings, cfg *Config) *googleXdsPolicyExtension {
	return &googleXdsPolicyExtension{
		cfg:      cfg,
		logger:   set.Logger,
		set:      set,
		registry: controlplane.DefaultRegistry,
		readyCh:  make(chan struct{}),
	}
}

// WithRegistry overrides the InformerRegistry (useful for isolated unit testing).
func (e *googleXdsPolicyExtension) WithRegistry(reg *controlplane.InformerRegistry) *googleXdsPolicyExtension {
	e.registry = reg
	return e
}

// WithDialOptions appends custom gRPC dial options (useful for testing with in-process servers).
func (e *googleXdsPolicyExtension) WithDialOptions(opts ...grpc.DialOption) *googleXdsPolicyExtension {
	e.dialOpts = append(e.dialOpts, opts...)
	return e
}

// Start acquires the pooled xDS transport client and registers self-observability metric gauges.
func (e *googleXdsPolicyExtension) Start(_ context.Context, _ component.Host) error {
	e.mu.Lock()
	if e.handle != nil {
		e.mu.Unlock()
		return errors.New("extension already started")
	}
	e.mu.Unlock()

	canonicalKey := controlplane.CanonicalXdsKey(
		e.cfg.Endpoint,
		e.cfg.FleetID,
		e.cfg.ProjectID,
		e.cfg.ServerAuthority,
		e.cfg.CACertPath,
		e.cfg.ClientCertPath,
		e.cfg.ClientKeyPath,
		e.cfg.Insecure,
	)

	reg := e.registry
	if reg == nil {
		reg = controlplane.DefaultRegistry
	}

	factory := func() (controlplane.InformerClient, error) {
		collectorID := e.cfg.CollectorID
		if collectorID == "" {
			collectorID = e.cfg.ProjectID
		}
		if collectorID == "" {
			collectorID = "gboc-collector"
		}

		supportedURLs := e.cfg.SupportedTypeURLs
		if len(supportedURLs) == 0 {
			supportedURLs = defaultSupportedTypeURLs
		}

		xdsCfg := xds.Config{
			Endpoint:          e.cfg.Endpoint,
			CollectorID:       collectorID,
			FleetID:           e.cfg.FleetID,
			Project:           e.cfg.ProjectID,
			ServerAuthority:   e.cfg.ServerAuthority,
			CACertPath:        e.cfg.CACertPath,
			ClientCertPath:    e.cfg.ClientCertPath,
			ClientKeyPath:     e.cfg.ClientKeyPath,
			Insecure:          e.cfg.Insecure,
			SupportedTypeURLs: supportedURLs,
			DialOptions:       e.dialOpts,
			Logger:            e.logger,
		}
		return xds.NewClient(xdsCfg)
	}

	handle, err := reg.Acquire(canonicalKey, factory)
	if err != nil {
		e.readyOnce.Do(func() { close(e.readyCh) })
		return fmt.Errorf("failed to acquire xDS client from registry: %w", err)
	}

	e.mu.Lock()
	e.handle = handle
	e.mu.Unlock()

	// Asynchronously forward readiness from underlying Informer
	go func() {
		<-handle.Informer.Ready()
		e.readyOnce.Do(func() { close(e.readyCh) })
	}()

	// Register Self-Observability Observable Gauges using unified RegisterCallback to prevent observation tearing
	mp := e.set.TelemetrySettings.MeterProvider
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("extension/googlexdspolicy")

	revGauge, err := meter.Int64ObservableGauge(
		"telemetry_policy_set_revision",
		metric.WithDescription("Current active revision of telemetry policies"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to register telemetry_policy_set_revision gauge: %w", err)
	}

	statusGauge, err := meter.Int64ObservableGauge(
		"telemetry_policy_status",
		metric.WithDescription("Status of individual telemetry policies"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to register telemetry_policy_status gauge: %w", err)
	}

	metricReg, err := meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		e.mu.Lock()
		h := e.handle
		e.mu.Unlock()
		if h != nil && h.Status != nil {
			rev, snapshot := h.Status.Snapshot()
			obs.ObserveInt64(revGauge, rev)
			for id, entry := range snapshot {
				obs.ObserveInt64(statusGauge, 1, metric.WithAttributes(
					attribute.String("policy_id", id),
					attribute.String("policy_type", entry.TypeURL),
					attribute.String("status", entry.Status.String()),
				))
			}
		}
		return nil
	}, revGauge, statusGauge)
	if err != nil {
		return fmt.Errorf("failed to register unified gauge callback: %w", err)
	}

	e.mu.Lock()
	e.metricReg = metricReg
	e.mu.Unlock()

	return nil
}

// Shutdown releases the reference on the pooled client handle and unregisters metric callbacks.
func (e *googleXdsPolicyExtension) Shutdown(_ context.Context) error {
	e.readyOnce.Do(func() { close(e.readyCh) })

	e.mu.Lock()
	handle := e.handle
	e.handle = nil
	metricReg := e.metricReg
	e.metricReg = nil
	e.mu.Unlock()

	if metricReg != nil {
		_ = metricReg.Unregister()
	}

	if handle != nil {
		return handle.Release()
	}
	return nil
}

// Ready returns a channel that is closed when initial policy retrieval is complete.
func (e *googleXdsPolicyExtension) Ready() <-chan struct{} {
	return e.readyCh
}

// List returns active policies matching typeURL.
func (e *googleXdsPolicyExtension) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	e.mu.Lock()
	handle := e.handle
	e.mu.Unlock()
	if handle == nil || handle.Informer == nil {
		return nil, errors.New("extension not running")
	}
	return handle.Informer.List(typeURL)
}

// Watch registers a listener for policy events.
func (e *googleXdsPolicyExtension) Watch(ctx context.Context, typeURL string) (<-chan controlplane.PolicyWatchEvent, error) {
	e.mu.Lock()
	handle := e.handle
	e.mu.Unlock()
	if handle == nil || handle.Informer == nil {
		return nil, errors.New("extension not running")
	}
	return handle.Informer.Watch(ctx, typeURL)
}

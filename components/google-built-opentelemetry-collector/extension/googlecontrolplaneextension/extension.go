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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type googleControlPlaneExtension struct {
	cfg            *Config
	logger         *zap.Logger
	set            extension.Settings
	statusRegistry *googlecontrolplane.StatusRegistry
	revGauge       metric.Int64ObservableGauge
	statusGauge    metric.Int64ObservableGauge
}

func newExtension(set extension.Settings, cfg *Config) *googleControlPlaneExtension {
	return &googleControlPlaneExtension{
		cfg:            cfg,
		logger:         set.Logger,
		set:            set,
		statusRegistry: googlecontrolplane.DefaultStatusRegistry,
	}
}

// WithStatusRegistry sets a custom StatusRegistry for the extension (e.g. for testing).
func (e *googleControlPlaneExtension) WithStatusRegistry(sr *googlecontrolplane.StatusRegistry) *googleControlPlaneExtension {
	e.statusRegistry = sr
	return e
}

func (e *googleControlPlaneExtension) Start(_ context.Context, _ component.Host) error {
	mp := e.set.TelemetrySettings.MeterProvider
	if mp == nil {
		mp = noop.NewMeterProvider()
	}

	meter := mp.Meter("extension/googlecontrolplaneextension")

	registry := e.statusRegistry
	if registry == nil {
		registry = googlecontrolplane.DefaultStatusRegistry
	}

	revGauge, err := meter.Int64ObservableGauge(
		"telemetry_policy_set_revision",
		metric.WithDescription("Current active revision of telemetry policies"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			rev, _ := registry.Snapshot()
			obs.Observe(rev)
			return nil
		}),
	)
	if err != nil {
		return err
	}
	e.revGauge = revGauge

	statusGauge, err := meter.Int64ObservableGauge(
		"telemetry_policy_status",
		metric.WithDescription("Status of individual telemetry policies"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
			_, snapshot := registry.Snapshot()
			for id, entry := range snapshot {
				obs.Observe(1, metric.WithAttributes(
					attribute.String("policy_id", id),
					attribute.String("policy_type", entry.TypeURL),
					attribute.String("status", entry.Status),
				))
			}
			return nil
		}),
	)
	if err != nil {
		return err
	}
	e.statusGauge = statusGauge

	return nil
}

func (e *googleControlPlaneExtension) Shutdown(context.Context) error {
	return nil
}

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
	"context"
	"sync"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/extension/filepolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/extension/googlexdspolicy"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/policyprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"google.golang.org/grpc/credentials"
)

// PolicyDriverRegistry manages supported policy drivers and component factories for validation and synthesis.
type PolicyDriverRegistry struct {
	mu         sync.RWMutex
	drivers    map[string]PolicyDriver
	receivers  map[string]receiver.Factory
	processors map[string]processor.Factory
	exporters  map[string]exporter.Factory
	extensions map[string]extension.Factory
}

// NewRegistry creates a new empty PolicyDriverRegistry.
func NewRegistry() *PolicyDriverRegistry {
	return &PolicyDriverRegistry{
		drivers:    make(map[string]PolicyDriver),
		receivers:  make(map[string]receiver.Factory),
		processors: make(map[string]processor.Factory),
		exporters:  make(map[string]exporter.Factory),
		extensions: make(map[string]extension.Factory),
	}
}

// NewDefaultRegistry creates a PolicyDriverRegistry initialized with all core drivers and component factories.
func NewDefaultRegistry() *PolicyDriverRegistry {
	reg := NewRegistry()

	// 1. Register Default Policy Drivers
	gcpDest := NewGcpDestinationDriver()
	reg.RegisterDriver(gcpDest)
	reg.RegisterDriverAlias(TypeURLGcpDestinationAlias, gcpDest)

	otlpSrc := NewOtlpSourceDriver()
	reg.RegisterDriver(otlpSrc)
	reg.RegisterDriverAlias(TypeURLOtlpSourceAlias, otlpSrc)

	filelogSrc := NewFilelogSourceDriver()
	reg.RegisterDriver(filelogSrc)
	reg.RegisterDriverAlias(TypeURLFilelogSourceAlias, filelogSrc)

	selfObs := NewSelfObservabilityDriver()
	reg.RegisterDriver(selfObs)
	reg.RegisterDriverAlias(TypeURLSelfObservabilityAlias, selfObs)

	logFilter := NewLogFilterDriver()
	reg.RegisterDriver(logFilter)
	reg.RegisterDriverAlias(TypeURLLogFilterPolicyAlias, logFilter)

	metricFilter := NewMetricFilterDriver()
	reg.RegisterDriver(metricFilter)
	reg.RegisterDriverAlias(TypeURLMetricFilterPolicyAlias, metricFilter)

	traceFilter := NewTraceFilterDriver()
	reg.RegisterDriver(traceFilter)
	reg.RegisterDriverAlias(TypeURLTraceFilterPolicyAlias, traceFilter)

	// 2. Register Default Component Factories
	otlpRecv := otlpreceiver.NewFactory()
	reg.RegisterReceiverFactory(otlpRecv)

	filelogRecv := filelogreceiver.NewFactory()
	reg.RegisterReceiverFactory(filelogRecv)
	reg.RegisterReceiverFactoryAlias("filelog", filelogRecv)

	reg.RegisterProcessorFactory(batchprocessor.NewFactory())
	reg.RegisterProcessorFactory(policyprocessor.NewFactory())

	otlpExp := otlpexporter.NewFactory()
	reg.RegisterExporterFactory(otlpExp)
	reg.RegisterExporterFactoryAlias("otlp", otlpExp)
	reg.RegisterExporterFactoryAlias("otlp_grpc", otlpExp)

	reg.RegisterExtensionFactory(NewGoogleClientAuthExtensionFactory())
	reg.RegisterExtensionFactory(NewGoogleControlPlaneExtensionFactory())
	reg.RegisterExtensionFactory(googlexdspolicy.NewFactory())
	reg.RegisterExtensionFactory(filepolicy.NewFactory())

	return reg
}

// RegisterDriver registers a PolicyDriver using its primary TypeURL.
func (r *PolicyDriverRegistry) RegisterDriver(driver PolicyDriver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[driver.TypeURL()] = driver
}

// RegisterDriverAlias registers an alias TypeURL for an existing driver.
func (r *PolicyDriverRegistry) RegisterDriverAlias(alias string, driver PolicyDriver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[alias] = driver
}

// GetDriver returns the PolicyDriver registered for the specified TypeURL, or nil if not found.
func (r *PolicyDriverRegistry) GetDriver(typeURL string) PolicyDriver {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.drivers[typeURL]
}

// ListDrivers returns all registered drivers (de-duplicated).
func (r *PolicyDriverRegistry) ListDrivers() []PolicyDriver {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[PolicyDriver]struct{}, len(r.drivers))
	list := make([]PolicyDriver, 0, len(r.drivers))
	for _, d := range r.drivers {
		if _, exists := seen[d]; !exists {
			seen[d] = struct{}{}
			list = append(list, d)
		}
	}
	return list
}

// SupportedTypeURLs returns all registered TypeURLs and aliases.
func (r *PolicyDriverRegistry) SupportedTypeURLs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	urls := make([]string, 0, len(r.drivers))
	for u := range r.drivers {
		urls = append(urls, u)
	}
	return urls
}

// Factory Registration & Retrieval

func (r *PolicyDriverRegistry) RegisterReceiverFactory(f receiver.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receivers[f.Type().String()] = f
}

func (r *PolicyDriverRegistry) RegisterReceiverFactoryAlias(alias string, f receiver.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receivers[alias] = f
}

func (r *PolicyDriverRegistry) GetReceiverFactory(compType string) receiver.Factory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if f, ok := r.receivers[compType]; ok {
		return f
	}
	if compType == "filelog" {
		return r.receivers["file_log"]
	} else if compType == "file_log" {
		return r.receivers["filelog"]
	}
	return nil
}

func (r *PolicyDriverRegistry) RegisterProcessorFactory(f processor.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processors[f.Type().String()] = f
}

func (r *PolicyDriverRegistry) RegisterProcessorFactoryAlias(alias string, f processor.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processors[alias] = f
}

func (r *PolicyDriverRegistry) GetProcessorFactory(compType string) processor.Factory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.processors[compType]
}

func (r *PolicyDriverRegistry) RegisterExporterFactory(f exporter.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exporters[f.Type().String()] = f
}

func (r *PolicyDriverRegistry) RegisterExporterFactoryAlias(alias string, f exporter.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exporters[alias] = f
}

func (r *PolicyDriverRegistry) GetExporterFactory(compType string) exporter.Factory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if f, ok := r.exporters[compType]; ok {
		return f
	}
	if compType == "otlp" {
		return r.exporters["otlp_grpc"]
	} else if compType == "otlp_grpc" {
		return r.exporters["otlp"]
	}
	return nil
}

func (r *PolicyDriverRegistry) RegisterExtensionFactory(f extension.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extensions[f.Type().String()] = f
}

func (r *PolicyDriverRegistry) RegisterExtensionFactoryAlias(alias string, f extension.Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extensions[alias] = f
}

func (r *PolicyDriverRegistry) GetExtensionFactory(compType string) extension.Factory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.extensions[compType]
}

// Stub extension factories for googleclientauth and googlecontrolplaneextension

type GoogleClientAuthConfig struct {
	Project string `mapstructure:"project"`
}

func (c *GoogleClientAuthConfig) Validate() error {
	return nil
}

type noopExtension struct{}

func (n *noopExtension) Start(context.Context, component.Host) error { return nil }
func (n *noopExtension) Shutdown(context.Context) error              { return nil }

type noopPerRPCCredentials struct{}

func (n *noopPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return nil, nil
}

func (n *noopPerRPCCredentials) RequireTransportSecurity() bool {
	return false
}

func (n *noopExtension) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	return &noopPerRPCCredentials{}, nil
}

var _ extensionauth.GRPCClient = (*noopExtension)(nil)

func NewGoogleClientAuthExtensionFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType("googleclientauth"),
		func() component.Config { return &GoogleClientAuthConfig{} },
		func(ctx context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			return &noopExtension{}, nil
		},
		component.StabilityLevelAlpha,
	)
}

type GoogleControlPlaneExtensionConfig struct{}

func (c *GoogleControlPlaneExtensionConfig) Validate() error {
	return nil
}

func NewGoogleControlPlaneExtensionFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType("googlecontrolplaneextension"),
		func() component.Config { return &GoogleControlPlaneExtensionConfig{} },
		func(ctx context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			return &noopExtension{}, nil
		},
		component.StabilityLevelAlpha,
	)
}

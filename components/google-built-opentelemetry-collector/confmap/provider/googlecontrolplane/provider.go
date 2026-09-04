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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/ingestor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane/file"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane/xds"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

// DefaultStartupTimeout is the maximum time the provider waits for initial policy receipt from
// the control plane before falling back to a base configuration.
const DefaultStartupTimeout = 15 * time.Second

// Provider implements confmap.Provider for the googlecontrolplane scheme.
// It manages policy ingestion via InformerRegistry (xDS or File transports), validates incoming
// policies, compiles them into runnable collector configurations, and triggers asynchronous
// in-process dynamic reloads.
type Provider struct {
	mu                       sync.RWMutex
	logger                   *zap.Logger
	driverRegistry           *driver.PolicyDriverRegistry
	registry                 *controlplane.InformerRegistry
	clientHandle             *controlplane.ClientHandle
	compiler                 *ConfigCompiler
	validator                *PolicyValidator
	prevalidator             *PreValidator
	escapeHatch              *EscapeHatchResolver
	currentConf              map[string]any
	resolvedTokens           map[string]string
	activeStructuralPolicies []*v3.TypedExtensionConfig
	watcher                  confmap.WatcherFunc
	watcherWg                sync.WaitGroup
	startupTimeout           time.Duration
	failOnTimeout            bool
	parsedURI                *ParsedURI
	initStreamOnce           sync.Once

	customCompiler           bool
	// Custom client factory hook for testing
	customClientFactory func() (controlplane.InformerClient, error)
	// Backward-compatible hook for testing
	ingestorFactory func(*ParsedURI) (ingestor.PolicyIngestor, error)
}

// ProviderOption configures a Provider instance.
type ProviderOption func(*Provider)

// WithProviderDriverRegistry sets the driver registry.
func WithProviderDriverRegistry(reg *driver.PolicyDriverRegistry) ProviderOption {
	return func(p *Provider) {
		p.driverRegistry = reg
		p.validator = NewPolicyValidator(reg, p.logger)
		p.compiler = NewConfigCompiler(reg, p.logger)
		p.prevalidator = NewPreValidator(reg, p.logger)
	}
}

// WithProviderInformerRegistry sets the control plane InformerRegistry.
func WithProviderInformerRegistry(reg *controlplane.InformerRegistry) ProviderOption {
	return func(p *Provider) {
		p.registry = reg
	}
}

// WithProviderCompiler sets a custom ConfigCompiler.
func WithProviderCompiler(c *ConfigCompiler) ProviderOption {
	return func(p *Provider) {
		p.compiler = c
		p.customCompiler = true
	}
}

// WithProviderClientFactory injects a custom factory for creating InformerClients (e.g. for testing).
func WithProviderClientFactory(factory func() (controlplane.InformerClient, error)) ProviderOption {
	return func(p *Provider) {
		p.customClientFactory = factory
	}
}

// WithProviderStartupTimeout sets the default startup timeout.
func WithProviderStartupTimeout(timeout time.Duration) ProviderOption {
	return func(p *Provider) {
		p.startupTimeout = timeout
	}
}

// WithProviderFailOnTimeout sets whether timeout during boot causes an error instead of fallback.
func WithProviderFailOnTimeout(fail bool) ProviderOption {
	return func(p *Provider) {
		p.failOnTimeout = fail
	}
}

// WithProviderStatusRegistry is retained for backwards compatibility.
func WithProviderStatusRegistry(sr *StatusRegistry) ProviderOption {
	return func(p *Provider) {}
}

// WithProviderIngestorFactory injects a custom factory for creating policy ingestors (e.g. for testing).
func WithProviderIngestorFactory(factory func(*ParsedURI) (ingestor.PolicyIngestor, error)) ProviderOption {
	return func(p *Provider) {
		p.ingestorFactory = factory
	}
}

// NewFactory creates a new confmap.ProviderFactory for the googlecontrolplane scheme.
func NewFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(newProvider)
}

// NewFactoryWithOptions creates a new confmap.ProviderFactory configured with custom ProviderOptions.
func NewFactoryWithOptions(opts ...ProviderOption) confmap.ProviderFactory {
	return confmap.NewProviderFactory(func(settings confmap.ProviderSettings) confmap.Provider {
		return newProviderWithOptions(settings, opts...)
	})
}

func newProvider(settings confmap.ProviderSettings) confmap.Provider {
	return newProviderWithOptions(settings)
}

func newProviderWithOptions(settings confmap.ProviderSettings, opts ...ProviderOption) *Provider {
	reg := driver.NewDefaultRegistry()
	logger := settings.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	p := &Provider{
		logger:         logger,
		driverRegistry: reg,
		validator:      NewPolicyValidator(reg, logger),
		compiler:       NewConfigCompiler(reg, logger),
		prevalidator:   NewPreValidator(reg, logger),
		escapeHatch:    NewEscapeHatchResolver(),
		resolvedTokens: make(map[string]string),
		startupTimeout: DefaultStartupTimeout,
		registry:       controlplane.DefaultRegistry,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// Scheme returns the scheme supported by this provider ("googlecontrolplane").
func (p *Provider) Scheme() string {
	return Scheme
}

// Retrieve resolves configuration for the specified URI.
// It handles component token queries (e.g. component//global_policy_processor) and root
// control plane URIs (xds:// or file://).
func (p *Provider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	clean := strings.TrimPrefix(uri, "googlecontrolplane:")
	clean = strings.TrimPrefix(clean, "//")
	if strings.HasPrefix(clean, "component//") || strings.HasPrefix(clean, "component:/") || strings.HasPrefix(clean, "component/") {
		p.mu.RLock()
		clientHandle := p.clientHandle
		p.mu.RUnlock()
		if clientHandle == nil {
			return nil, fmt.Errorf("component anchor %q cannot be resolved before root provider URI is retrieved; ensure googlecontrolplane root config precedes base config in --config arguments", uri)
		}

		token := p.extractComponentToken(uri)
		resolvedID, err := p.resolveComponentAnchor(ctx, token)
		if err != nil {
			return nil, err
		}
		return confmap.NewRetrieved(resolvedID)
	}

	return p.retrieveRootConfiguration(ctx, uri, watcher)
}

func (p *Provider) extractComponentToken(uri string) string {
	cleanURI := strings.TrimPrefix(uri, "googlecontrolplane:")
	cleanURI = strings.TrimPrefix(cleanURI, "//")
	if strings.HasPrefix(cleanURI, "component//") {
		return strings.TrimPrefix(cleanURI, "component//")
	}
	if strings.HasPrefix(cleanURI, "component:/") {
		return strings.TrimPrefix(cleanURI, "component:/")
	}
	if strings.HasPrefix(cleanURI, "component/") {
		return strings.TrimPrefix(cleanURI, "component/")
	}
	return strings.TrimPrefix(cleanURI, "/")
}

func (p *Provider) resolveComponentAnchor(ctx context.Context, token string) (string, error) {
	p.mu.RLock()
	handle := p.clientHandle
	hasConf := p.currentConf != nil
	startupTimeout := p.startupTimeout
	if startupTimeout <= 0 {
		startupTimeout = DefaultStartupTimeout
	}
	p.mu.RUnlock()

	if handle == nil {
		return "", fmt.Errorf("component anchor %q cannot be resolved before root provider URI is retrieved; ensure googlecontrolplane root config precedes base config in --config arguments", token)
	}

	if !hasConf {
		// Synchronize on handle.Informer.Ready() if root config has not yet finished bootstrapping
		select {
		case <-handle.Informer.Ready():
		case <-time.After(startupTimeout):
			return "", fmt.Errorf("timed out waiting for informer readiness while resolving component anchor %q", token)
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	switch token {
	case "global_policy_processor":
		return "policy/global", nil
	case "active_destination_exporter":
		if exp, ok := p.resolvedTokens["active_destination_exporter"]; ok && exp != "" {
			return exp, nil
		}
		if p.currentConf != nil {
			if exps, ok := p.currentConf["exporters"].(map[string]any); ok {
				for expName := range exps {
					if strings.Contains(expName, "gcp") || strings.Contains(expName, "googlecloud") {
						return expName, nil
					}
				}
			}
		}
		return "", fmt.Errorf("component anchor %q cannot be resolved: no active GCP destination exporter defined in control plane policy", token)
	default:
		if val, ok := p.resolvedTokens[token]; ok && val != "" {
			return val, nil
		}
		return "", fmt.Errorf("unknown component token: %q", token)
	}
}

func (p *Provider) retrieveRootConfiguration(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if watcher != nil {
		p.mu.Lock()
		p.watcher = watcher
		p.mu.Unlock()
	}

	p.mu.RLock()
	conf := p.currentConf
	p.mu.RUnlock()

	// 1. Dynamic Reload Path:
	// If already bootstrapped with a valid configuration, return current configuration under read lock.
	if conf != nil {
		return confmap.NewRetrieved(conf)
	}

	// 2. Initial Boot Path:
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid googlecontrolplane URI: %w", err)
	}

	var startErr error
	p.initStreamOnce.Do(func() {
		p.mu.Lock()
		p.parsedURI = parsed
		timeout := parsed.StartupTimeout
		if timeout <= 0 {
			timeout = p.startupTimeout
		}
		if timeout <= 0 {
			timeout = DefaultStartupTimeout
		}
		p.startupTimeout = timeout
		if parsed.FailOnStartupTimeout {
			p.failOnTimeout = true
		}

		insecure := parsed.Insecure || ((strings.HasPrefix(parsed.Endpoint, "127.0.0.1:") || strings.HasPrefix(parsed.Endpoint, "localhost:")) && parsed.CACertPath == "" && parsed.ClientCertPath == "")

		if !p.customCompiler {
			identity := NewIdentityResolver(parsed.FleetID)
			collectorID, _ := identity.ResolveID()
			opts := []CompilerOption{
				WithCompilerTransport(string(parsed.Transport)),
				WithCompilerFleetID(parsed.FleetID),
				WithCompilerCollectorID(collectorID),
				WithCompilerProjectID(parsed.Project),
				WithCompilerEndpoint(parsed.Endpoint),
				WithCompilerPath(parsed.Path),
				WithCompilerInsecure(insecure),
				WithCompilerServerAuthority(parsed.ServerAuthority),
				WithCompilerCACertPath(parsed.CACertPath),
				WithCompilerClientCertPath(parsed.ClientCertPath),
				WithCompilerClientKeyPath(parsed.ClientKeyPath),
			}
			p.compiler = NewConfigCompiler(p.driverRegistry, p.logger, opts...)
		}

		if parsed.BaseConfigURI != "" {
			if baseConf, err := p.compiler.LoadBaseConfig(parsed.BaseConfigURI); err == nil {
				p.compiler.SetBaseConfig(baseConf)
			} else {
				p.logger.Warn("Failed to load base configuration for compiler", zap.Error(err))
			}
		}

		reg := p.registry
		if reg == nil {
			reg = controlplane.DefaultRegistry
			p.registry = reg
		}

		var key string
		var factory func() (controlplane.InformerClient, error)
		if p.customClientFactory != nil {
			key = uri
			factory = p.customClientFactory
		} else if parsed.Transport == TransportFile {
			key, err = controlplane.CanonicalFileKey(parsed.Path)
			if err != nil {
				startErr = fmt.Errorf("invalid file path: %w", err)
				p.mu.Unlock()
				return
			}
			factory = func() (controlplane.InformerClient, error) {
				return file.NewClient(file.Config{
					Path:   parsed.Path,
					Logger: p.logger,
				})
			}
		} else {
			key = controlplane.CanonicalXdsKey(
				parsed.Endpoint,
				parsed.FleetID,
				parsed.Project,
				parsed.ServerAuthority,
				parsed.CACertPath,
				parsed.ClientCertPath,
				parsed.ClientKeyPath,
				insecure,
			)
			identity := NewIdentityResolver(parsed.FleetID)
			collectorID, _ := identity.ResolveID()
			supportedURLs := p.driverRegistry.SupportedTypeURLs()
			factory = func() (controlplane.InformerClient, error) {
				return xds.NewClient(xds.Config{
					Endpoint:          parsed.Endpoint,
					CollectorID:       collectorID,
					FleetID:           parsed.FleetID,
					Project:           parsed.Project,
					ServerAuthority:   parsed.ServerAuthority,
					CACertPath:        parsed.CACertPath,
					ClientCertPath:    parsed.ClientCertPath,
					ClientKeyPath:     parsed.ClientKeyPath,
					Insecure:          insecure,
					SupportedTypeURLs: supportedURLs,
					Logger:            p.logger,
				})
			}
		}

		handle, err := reg.Acquire(key, factory)
		if err != nil {
			startErr = fmt.Errorf("failed to acquire informer client: %w", err)
			p.mu.Unlock()
			return
		}
		p.clientHandle = handle
		handle.RegisterStructuralHandler(p.handleStructuralUpdate)
		p.mu.Unlock()
	})

	if startErr != nil {
		p.logger.Warn("Failed to acquire informer client, falling back to bootstrap configuration", zap.Error(startErr))
		return p.fallbackToBootstrap(parsed.BaseConfigURI)
	}

	p.mu.RLock()
	handle := p.clientHandle
	timeout := p.startupTimeout
	failOnTimeout := p.failOnTimeout
	p.mu.RUnlock()

	if handle == nil {
		return p.fallbackToBootstrap(parsed.BaseConfigURI)
	}

	select {
	case <-handle.Informer.Ready():
		p.mu.RLock()
		conf := p.currentConf
		p.mu.RUnlock()
		if conf != nil {
			return confmap.NewRetrieved(conf)
		}

		p.mu.Lock()
		if p.currentConf == nil {
			bootstrapConf, tokens, _ := p.compiler.Compile(nil)
			p.currentConf = bootstrapConf
			p.resolvedTokens = tokens
			if p.escapeHatch != nil {
				p.escapeHatch.UpdateTokens(tokens)
			}
		}
		conf = p.currentConf
		p.mu.Unlock()
		return confmap.NewRetrieved(conf)

	case <-time.After(timeout):
		if failOnTimeout {
			return nil, fmt.Errorf("timed out waiting for initial policy set from control plane")
		}
		p.logger.Warn("Provider timed out waiting for initial policy set from control plane; falling back to bootstrap configuration",
			zap.Duration("timeout", timeout))
		return p.fallbackToBootstrap(parsed.BaseConfigURI)

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Provider) fallbackToBootstrap(baseURI string) (*confmap.Retrieved, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentConf != nil {
		return confmap.NewRetrieved(p.currentConf)
	}

	if baseURI != "" && p.compiler != nil {
		if baseConf, err := p.compiler.LoadBaseConfig(baseURI); err == nil {
			p.currentConf = baseConf
			p.resolvedTokens = map[string]string{
				"global_policy_processor":     "policy/global",
				"active_destination_exporter": "otlp/gcp_destination",
			}
			if p.escapeHatch != nil {
				p.escapeHatch.UpdateTokens(p.resolvedTokens)
			}
			return confmap.NewRetrieved(baseConf)
		}
	}

	bootstrapConf, tokens, err := p.compiler.Compile(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to compile fallback bootstrap configuration: %w", err)
	}
	p.currentConf = bootstrapConf
	p.resolvedTokens = tokens
	if p.escapeHatch != nil {
		p.escapeHatch.UpdateTokens(tokens)
	}
	return confmap.NewRetrieved(bootstrapConf)
}

func (p *Provider) handleStructuralUpdate(ctx context.Context, update controlplane.PolicySnapshotUpdate) error {
	p.mu.RLock()
	isColdStartup := p.currentConf == nil
	activeStructural := p.activeStructuralPolicies
	p.mu.RUnlock()

	// Dynamic reload path: check if structural policies differ from active
	if !isColdStartup {
		newStructural := extractStructuralPolicies(update.Policies)
		if !hasStructuralDifferences(activeStructural, newStructural) {
			// No structural changes exist (e.g. filter-only updates or identical topology);
			// validate to populate statuses, then return nil so
			// filter policies will be diffed and streamed to informers directly.
			if update.Statuses != nil {
				validationResult := p.validator.ValidatePolicies(update.Policies)
				for k, v := range validationResult.PolicyStatuses {
					update.Statuses[k] = controlplane.PolicyStatusRecord{
						PolicyID: k,
						TypeURL:  v.TypeURL,
						Status:   toControlPlaneStatus(v.Status),
					}
				}
			}
			return nil
		}
	}

	// Structural changes exist or cold startup:
	// Layer 1: Policy Ingestion Validation (Fail-Open)
	validationResult := p.validator.ValidatePolicies(update.Policies)
	if update.Statuses != nil {
		for k, v := range validationResult.PolicyStatuses {
			update.Statuses[k] = controlplane.PolicyStatusRecord{
				PolicyID: k,
				TypeURL:  v.TypeURL,
				Status:   toControlPlaneStatus(v.Status),
			}
		}
	}
	for _, diag := range validationResult.Diagnostics {
		p.logger.Warn("Policy diagnostic emitted",
			zap.String("policy_id", diag.PolicyID),
			zap.String("type_url", diag.TypeURL),
			zap.String("reason", diag.Reason))
	}

	// Compile candidate configuration
	newConf, newTokens, err := p.compiler.Compile(validationResult.ValidPolicies)
	if err != nil {
		p.logger.Error("Failed to compile valid policies", zap.Error(err))
		p.recordStatusFailed(validationResult, err)
		return err
	}

	// Layer 2: PreValidator Runtime Gate (Crash-Proof)
	if err := p.prevalidator.Validate(newConf); err != nil {
		p.logger.Error("PreValidator rejected synthesized configuration; reload aborted to prevent process crash", zap.Error(err))
		p.recordStatusFailed(validationResult, err)
		return err
	}

	// Commit new configuration under lock
	p.mu.Lock()
	p.currentConf = newConf
	p.resolvedTokens = newTokens
	p.activeStructuralPolicies = extractStructuralPolicies(validationResult.ValidPolicies)
	if p.escapeHatch != nil {
		p.escapeHatch.UpdateTokens(newTokens)
	}
	watcher := p.watcher
	p.mu.Unlock()

	// Dynamic reload: trigger in-process collector reload asynchronously
	if !isColdStartup && watcher != nil {
		p.logger.Info("Triggering in-process collector reload with updated configuration")
		p.watcherWg.Add(1)
		go func() {
			defer p.watcherWg.Done()
			watcher(&confmap.ChangeEvent{})
		}()
	}

	return nil
}

func (p *Provider) recordStatusFailed(validationResult *ValidationResult, err error) {
	p.mu.RLock()
	handle := p.clientHandle
	p.mu.RUnlock()
	if handle != nil && handle.Status != nil {
		for _, pol := range validationResult.ValidPolicies {
			handle.Status.RecordStatus(pol.GetName(), pol.GetTypedConfig().GetTypeUrl(), controlplane.PolicyStatusFailed, err)
		}
	}
}

func extractStructuralPolicies(policies []*v3.TypedExtensionConfig) []*v3.TypedExtensionConfig {
	var res []*v3.TypedExtensionConfig
	for _, pol := range policies {
		if pol != nil && pol.TypedConfig != nil && controlplane.IsStructuralPolicy(pol.TypedConfig.TypeUrl) {
			res = append(res, pol)
		}
	}
	return res
}

func hasStructuralDifferences(oldPolicies, newPolicies []*v3.TypedExtensionConfig) bool {
	if len(oldPolicies) != len(newPolicies) {
		return true
	}
	oldMap := make(map[string]*v3.TypedExtensionConfig, len(oldPolicies))
	for _, pol := range oldPolicies {
		if pol != nil {
			oldMap[pol.GetName()] = pol
		}
	}
	for _, pol := range newPolicies {
		if pol == nil {
			continue
		}
		oldP, exists := oldMap[pol.GetName()]
		if !exists {
			return true
		}
		if oldP.TypedConfig == nil || pol.TypedConfig == nil {
			if oldP.TypedConfig != pol.TypedConfig {
				return true
			}
			continue
		}
		if oldP.TypedConfig.TypeUrl != pol.TypedConfig.TypeUrl || !bytes.Equal(oldP.TypedConfig.Value, pol.TypedConfig.Value) {
			return true
		}
	}
	return false
}

// Shutdown stops the policy ingestor and releases background streaming resources.
func (p *Provider) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.clientHandle != nil {
		_ = p.clientHandle.Release()
		p.clientHandle = nil
	}
	reg := p.registry
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.watcherWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if reg != nil {
		return reg.Shutdown(ctx)
	}
	return controlplane.DefaultRegistry.Shutdown(ctx)
}

func toControlPlaneStatus(status string) controlplane.PolicyStatus {
	switch status {
	case "accepted":
		return controlplane.PolicyStatusApplied
	case "rejected", "unsupported":
		return controlplane.PolicyStatusFailed
	default:
		return controlplane.PolicyStatusPending
	}
}

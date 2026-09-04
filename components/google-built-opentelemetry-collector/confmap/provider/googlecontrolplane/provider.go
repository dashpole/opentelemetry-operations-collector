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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/ingestor"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
	status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
)

// DefaultStartupTimeout is the maximum time the provider waits for initial policy receipt from
// the control plane before falling back to a base configuration.
const DefaultStartupTimeout = 15 * time.Second

// Provider implements confmap.Provider for the googlecontrolplane scheme.
// It manages policy ingestion via file or xDS, validates incoming policies, compiles them into
// runnable collector configurations, and triggers asynchronous in-process dynamic reloads.
type Provider struct {
	mu             sync.RWMutex
	logger         *zap.Logger
	driverRegistry *driver.PolicyDriverRegistry
	ingestor       ingestor.PolicyIngestor
	compiler       *ConfigCompiler
	validator      *PolicyValidator
	prevalidator   *PreValidator
	statusRegistry *StatusRegistry
	escapeHatch    *EscapeHatchResolver
	currentConf    map[string]any
	resolvedTokens map[string]string
	watcher        confmap.WatcherFunc
	bootstrapped   bool
	initialReady   chan struct{}
	initialOnce    sync.Once
	initStreamOnce sync.Once
	cancel         context.CancelFunc

	// Custom ingestor factory hook for testing
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

// WithProviderStatusRegistry sets the status registry.
func WithProviderStatusRegistry(sr *StatusRegistry) ProviderOption {
	return func(p *Provider) {
		p.statusRegistry = sr
	}
}

// WithProviderCompiler sets a custom ConfigCompiler.
func WithProviderCompiler(c *ConfigCompiler) ProviderOption {
	return func(p *Provider) {
		p.compiler = c
	}
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
		statusRegistry: DefaultStatusRegistry,
		escapeHatch:    NewEscapeHatchResolver(),
		resolvedTokens: make(map[string]string),
		initialReady:   make(chan struct{}),
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
	if watcher != nil {
		p.mu.Lock()
		p.watcher = watcher
		p.mu.Unlock()
	}

	// Delegate component token resolution to EscapeHatchResolver
	if p.escapeHatch != nil {
		if retrieved, isToken, err := p.escapeHatch.ResolveURI(uri); isToken {
			return retrieved, err
		}
	}

	return p.retrieveRootConfiguration(ctx, uri)
}

func (p *Provider) resolveComponentToken(token string) (*confmap.Retrieved, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	token = strings.TrimPrefix(token, "/")
	name, ok := p.resolvedTokens[token]
	if !ok {
		return nil, fmt.Errorf("unknown component token: %q", token)
	}
	return confmap.NewRetrieved(name)
}

func (p *Provider) retrieveRootConfiguration(ctx context.Context, uri string) (*confmap.Retrieved, error) {
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
		streamCtx, cancel := context.WithCancel(context.Background())

		ing, err := p.createIngestor(parsed)
		if err != nil {
			startErr = fmt.Errorf("failed to create policy ingestor: %w", err)
			cancel()
			return
		}

		if parsed.BaseConfigURI != "" && p.compiler != nil {
			if baseConf, err := p.compiler.LoadBaseConfig(parsed.BaseConfigURI); err == nil {
				p.compiler.SetBaseConfig(baseConf)
			} else {
				p.logger.Warn("Failed to load base configuration for compiler", zap.Error(err))
			}
		}

		p.mu.Lock()
		p.cancel = cancel
		p.ingestor = ing
		p.mu.Unlock()

		// Start background ingestion
		if err := ing.Start(streamCtx, p.handlePolicyUpdate); err != nil {
			startErr = fmt.Errorf("failed to start policy ingestor: %w", err)
			cancel()
			p.mu.Lock()
			p.cancel = nil
			p.ingestor = nil
			p.mu.Unlock()
			_ = ing.Stop(context.Background())
		}
	})

	if startErr != nil {
		p.logger.Warn("Failed to start policy ingestor, falling back to base configuration", zap.Error(startErr))
		return p.fallbackToBaseConfiguration(parsed.BaseConfigURI)
	}

	timeout := parsed.StartupTimeout
	if timeout <= 0 {
		timeout = DefaultStartupTimeout
	}

	select {
	case <-p.initialReady:
		p.mu.Lock()
		p.bootstrapped = true
		conf := p.currentConf
		p.mu.Unlock()
		return confmap.NewRetrieved(conf)

	case <-time.After(timeout):
		p.logger.Warn("Timed out waiting for initial policy set from control plane; falling back to base configuration",
			zap.Duration("timeout", timeout))
		return p.fallbackToBaseConfiguration(parsed.BaseConfigURI)

	case <-ctx.Done():
		// Caller context canceled (e.g. shutdown, SIGTERM during boot)
		p.mu.Lock()
		cancel := p.cancel
		p.cancel = nil
		ing := p.ingestor
		p.ingestor = nil
		p.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if ing != nil {
			_ = ing.Stop(context.Background())
		}
		return nil, ctx.Err()
	}
}

func (p *Provider) createIngestor(parsed *ParsedURI) (ingestor.PolicyIngestor, error) {
	if p.ingestorFactory != nil {
		return p.ingestorFactory(parsed)
	}

	switch parsed.Transport {
	case TransportFile:
		return ingestor.NewFileIngestor(parsed.Path, p.logger), nil

	case TransportXDS:
		identity := NewIdentityResolver(parsed.FleetID)
		collectorID, _ := identity.ResolveID()

		// Configure compiler context
		p.compiler = NewConfigCompiler(p.driverRegistry, p.logger,
			WithCompilerFleetID(parsed.FleetID),
			WithCompilerCollectorID(collectorID))

		supportedURLs := p.driverRegistry.SupportedTypeURLs()
		return ingestor.NewXdsIngestor(ingestor.XdsIngestorConfig{
			Endpoint:          parsed.Endpoint,
			CollectorID:       collectorID,
			FleetID:           parsed.FleetID,
			Project:           parsed.Project,
			SupportedTypeURLs: supportedURLs,
			Logger:            p.logger,
		}), nil

	default:
		return nil, fmt.Errorf("unsupported transport: %s", parsed.Transport)
	}
}

func (p *Provider) fallbackToBaseConfiguration(baseURI string) (*confmap.Retrieved, error) {
	baseConf, err := p.compiler.LoadBaseConfig(baseURI)
	if err != nil {
		return nil, fmt.Errorf("failed to load fallback base configuration: %w", err)
	}

	p.mu.Lock()
	p.currentConf = baseConf
	p.resolvedTokens = map[string]string{
		"global_policy_processor":     "policy/global",
		"active_destination_exporter": "otlp/gcp_destination",
	}
	if p.escapeHatch != nil {
		p.escapeHatch.UpdateTokens(p.resolvedTokens)
	}
	p.bootstrapped = true
	p.mu.Unlock()

	p.initialOnce.Do(func() {
		close(p.initialReady)
	})

	return confmap.NewRetrieved(baseConf)
}

func (p *Provider) handlePolicyUpdate(update ingestor.PolicyUpdate) error {
	// Layer 1: Policy Ingestion Validation (Fail-Open)
	validationResult := p.validator.Validate(update.Collector)
	for _, diag := range validationResult.Diagnostics {
		p.logger.Warn("Policy diagnostic emitted",
			zap.String("policy_id", diag.PolicyID),
			zap.String("type_url", diag.TypeURL),
			zap.String("reason", diag.Reason))
	}

	// Compile remaining valid policies
	newConf, newTokens, err := p.compiler.Compile(validationResult.ValidPolicies)
	if err != nil {
		p.logger.Error("Failed to compile valid policies", zap.Error(err))
		p.mu.RLock()
		ing := p.ingestor
		p.mu.RUnlock()
		if ing != nil {
			_ = ing.Acknowledge(context.Background(), ingestor.PolicyAck{
				Revision: update.Revision,
				Nonce:    update.Nonce,
				ErrorDetail: &status.Status{
					Code:    int32(codes.InvalidArgument),
					Message: fmt.Sprintf("Compilation failed: %v", err),
				},
			})
		}
		return err
	}

	// Layer 2: PreValidator Runtime Gate (Crash-Proof)
	if err := p.prevalidator.Validate(newConf); err != nil {
		p.logger.Error("PreValidator rejected synthesized configuration; reload aborted to prevent process crash", zap.Error(err))
		p.mu.RLock()
		ing := p.ingestor
		p.mu.RUnlock()
		if ing != nil {
			_ = ing.Acknowledge(context.Background(), ingestor.PolicyAck{
				Revision: update.Revision,
				Nonce:    update.Nonce,
				ErrorDetail: &status.Status{
					Code:    int32(codes.InvalidArgument),
					Message: fmt.Sprintf("Structural validation failed: %v", err),
				},
			})
		}
		return err
	}

	// Commit new configuration under lock
	p.mu.Lock()
	p.currentConf = newConf
	p.resolvedTokens = newTokens
	if p.escapeHatch != nil {
		p.escapeHatch.UpdateTokens(newTokens)
	}
	isBootstrapped := p.bootstrapped
	watcher := p.watcher
	ing := p.ingestor
	p.mu.Unlock()

	// Unblock initial boot if waiting
	p.initialOnce.Do(func() {
		close(p.initialReady)
	})

	// Update atomic StatusRegistry
	if p.statusRegistry != nil {
		p.statusRegistry.SetRevisionAndStatuses(update.RevisionNumber, validationResult.PolicyStatuses)
	}

	// Trigger dynamic reload ONLY if already bootstrapped.
	// Dispatched asynchronously so slow reload cycles never block transport ingestors.
	if isBootstrapped && watcher != nil {
		p.logger.Info("Triggering in-process collector reload with updated configuration")
		go watcher(&confmap.ChangeEvent{})
	}

	// Send acknowledgment to control plane
	if ing != nil {
		if validationResult.HasSkippedPolicies {
			_ = ing.Acknowledge(context.Background(), ingestor.PolicyAck{
				Revision: update.Revision,
				Nonce:    update.Nonce,
				ErrorDetail: &status.Status{
					Code:    int32(codes.InvalidArgument),
					Message: fmt.Sprintf("Enforced valid policies with skipped invalid policies: %s", validationResult.SkippedSummary()),
				},
			})
		} else {
			_ = ing.Acknowledge(context.Background(), ingestor.PolicyAck{
				Revision: update.Revision,
				Nonce:    update.Nonce,
				ErrorDetail: nil, // ACK
			})
		}
	}

	return nil
}

// Shutdown stops the policy ingestor and releases background streaming resources.
func (p *Provider) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	cancel := p.cancel
	p.cancel = nil
	ing := p.ingestor
	p.ingestor = nil
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ing != nil {
		return ing.Stop(ctx)
	}
	return nil
}

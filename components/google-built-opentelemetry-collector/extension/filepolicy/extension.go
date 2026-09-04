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

package filepolicy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane/file"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type filePolicyExtension struct {
	cfg       *Config
	logger    *zap.Logger
	set       extension.Settings
	registry  *controlplane.InformerRegistry
	handle    *controlplane.ClientHandle
	readyCh   chan struct{}
	readyOnce sync.Once
	mu        sync.Mutex
}

var _ extension.Extension = (*filePolicyExtension)(nil)
var _ controlplane.PolicyInformer = (*filePolicyExtension)(nil)

func newExtension(set extension.Settings, cfg *Config) *filePolicyExtension {
	return &filePolicyExtension{
		cfg:      cfg,
		logger:   set.Logger,
		set:      set,
		registry: controlplane.DefaultRegistry,
		readyCh:  make(chan struct{}),
	}
}

// WithRegistry overrides the InformerRegistry (useful for isolated unit testing).
func (e *filePolicyExtension) WithRegistry(reg *controlplane.InformerRegistry) *filePolicyExtension {
	e.registry = reg
	return e
}

// Start acquires the pooled file transport client from InformerRegistry.
func (e *filePolicyExtension) Start(_ context.Context, _ component.Host) error {
	e.mu.Lock()
	if e.handle != nil {
		e.mu.Unlock()
		return errors.New("extension already started")
	}
	e.mu.Unlock()

	canonicalKey, err := controlplane.CanonicalFileKey(e.cfg.Path)
	if err != nil {
		e.readyOnce.Do(func() { close(e.readyCh) })
		return fmt.Errorf("failed to derive canonical file key for path %s: %w", e.cfg.Path, err)
	}

	reg := e.registry
	if reg == nil {
		reg = controlplane.DefaultRegistry
	}

	factory := func() (controlplane.InformerClient, error) {
		fileCfg := file.Config{
			Path:             e.cfg.Path,
			DebounceInterval: e.cfg.DebounceInterval,
			Logger:           e.logger,
		}
		return file.NewClient(fileCfg)
	}

	handle, err := reg.Acquire(canonicalKey, factory)
	if err != nil {
		e.readyOnce.Do(func() { close(e.readyCh) })
		return fmt.Errorf("failed to acquire file policy client from registry: %w", err)
	}

	e.mu.Lock()
	e.handle = handle
	e.mu.Unlock()

	// Asynchronously forward readiness from underlying Informer
	go func() {
		<-handle.Informer.Ready()
		e.readyOnce.Do(func() { close(e.readyCh) })
	}()

	return nil
}

// Shutdown releases the handle reference in the registry.
func (e *filePolicyExtension) Shutdown(_ context.Context) error {
	e.readyOnce.Do(func() { close(e.readyCh) })

	e.mu.Lock()
	handle := e.handle
	e.handle = nil
	e.mu.Unlock()

	if handle != nil {
		return handle.Release()
	}
	return nil
}

// Ready returns a channel that is closed when initial policy load is complete.
func (e *filePolicyExtension) Ready() <-chan struct{} {
	return e.readyCh
}

// List returns active policies matching typeURL.
func (e *filePolicyExtension) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	e.mu.Lock()
	handle := e.handle
	e.mu.Unlock()
	if handle == nil || handle.Informer == nil {
		return nil, errors.New("extension not running")
	}
	return handle.Informer.List(typeURL)
}

// Watch registers a listener for policy events.
func (e *filePolicyExtension) Watch(ctx context.Context, typeURL string) (<-chan controlplane.PolicyWatchEvent, error) {
	e.mu.Lock()
	handle := e.handle
	e.mu.Unlock()
	if handle == nil || handle.Informer == nil {
		return nil, errors.New("extension not running")
	}
	return handle.Informer.Watch(ctx, typeURL)
}

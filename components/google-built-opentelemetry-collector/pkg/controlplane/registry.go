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

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// InformerClient is the unified interface implemented by transport clients (xDS, File).
type InformerClient interface {
	PolicyInformer
	Close() error
	Status() *StatusRegistry
	RegisterStructuralHandler(handler StructuralUpdateHandler)
}

// ClientHandle is an acquired reference to a pooled InformerClient.
// Consumers interact with Informer and Status and must invoke Release() when done.
type ClientHandle struct {
	Informer                  PolicyInformer
	Status                    *StatusRegistry
	release                   func() error
	registerStructuralHandler func(handler StructuralUpdateHandler)
	handlerRegistered         atomic.Bool
	released                  atomic.Bool
}

// RegisterStructuralHandler registers a callback for structural policy changes.
func (h *ClientHandle) RegisterStructuralHandler(handler StructuralUpdateHandler) {
	if h != nil && h.registerStructuralHandler != nil {
		h.handlerRegistered.Store(handler != nil)
		h.registerStructuralHandler(handler)
	}
}

// Release releases the handle's reference count. Calling Release multiple times is idempotent.
func (h *ClientHandle) Release() error {
	if h == nil {
		return nil
	}
	if h.released.CompareAndSwap(false, true) {
		if h.handlerRegistered.Load() && h.registerStructuralHandler != nil {
			h.registerStructuralHandler(nil)
			h.handlerRegistered.Store(false)
		}
		if h.release != nil {
			return h.release()
		}
	}
	return nil
}

// CanonicalXdsKey computes a deterministic connection key for xDS transports.
func CanonicalXdsKey(endpoint, fleetID, projectID, serverAuthority, caCertPath, clientCertPath, clientKeyPath string, insecure bool) string {
	return fmt.Sprintf("xds://%s?fleet_id=%s&project_id=%s&authority=%s&ca_cert=%s&client_cert=%s&client_key=%s&insecure=%t",
		strings.ToLower(strings.TrimSpace(endpoint)),
		strings.TrimSpace(fleetID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(serverAuthority),
		strings.TrimSpace(caCertPath),
		strings.TrimSpace(clientCertPath),
		strings.TrimSpace(clientKeyPath),
		insecure,
	)
}

// CanonicalFileKey computes a normalized canonical key for local file transports.
// Uses filepath.Abs and filepath.Clean without EvalSymlinks to preserve atomic symlink swap tracking.
func CanonicalFileKey(path string) (string, error) {
	absPath, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("file://%s", filepath.Clean(absPath)), nil
}

type registryEntry struct {
	refCount    int
	client      InformerClient
	lingerTimer *time.Timer
}

// InformerRegistry coordinates connection pooling and reference-counting with grace periods.
type InformerRegistry struct {
	mu          sync.Mutex
	entries     map[string]*registryEntry
	flightGroup singleflight.Group
	lingerDur   time.Duration
}

// NewInformerRegistry creates a new InformerRegistry with the given linger duration.
// If lingerDur < 0, a 5-second default is applied. A lingerDur of 0 disables lingering (immediate cleanup).
func NewInformerRegistry(lingerDur time.Duration) *InformerRegistry {
	if lingerDur < 0 {
		lingerDur = 5 * time.Second
	}
	return &InformerRegistry{
		entries:   make(map[string]*registryEntry),
		lingerDur: lingerDur,
	}
}

// DefaultRegistry is the process-wide InformerRegistry singleton with a 5s linger window.
var DefaultRegistry = NewInformerRegistry(5 * time.Second)

// Acquire retrieves an existing client or constructs a new one via factory using singleflight.
// Any active linger timer for the key is canceled on acquisition.
func (r *InformerRegistry) Acquire(key string, factory func() (InformerClient, error)) (*ClientHandle, error) {
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[string]*registryEntry)
	}
	entry, exists := r.entries[key]
	if exists {
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
			entry.lingerTimer = nil
		}
		entry.refCount++
		r.mu.Unlock()
		return r.makeHandle(key, entry), nil
	}
	r.mu.Unlock()

	// Execute factory outside lock via singleflight to prevent blocking the registry
	v, err, _ := r.flightGroup.Do(key, func() (any, error) {
		return factory()
	})
	if err != nil {
		return nil, err
	}
	newClient := v.(InformerClient)

	var toClose InformerClient
	r.mu.Lock()

	// Re-check entry under lock in case another goroutine acquired it concurrently
	entry, exists = r.entries[key]
	if exists {
		// Guard against closing newClient if it is the identical instance registered in entry
		if entry.client != newClient {
			toClose = newClient
		}
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
			entry.lingerTimer = nil
		}
		entry.refCount++
		r.mu.Unlock()
		if toClose != nil {
			_ = toClose.Close() // Executed strictly outside r.mu.Lock()
		}
		return r.makeHandle(key, entry), nil
	}

	entry = &registryEntry{
		refCount: 1,
		client:   newClient,
	}
	r.entries[key] = entry
	r.mu.Unlock()
	return r.makeHandle(key, entry), nil
}

func (r *InformerRegistry) makeHandle(key string, entry *registryEntry) *ClientHandle {
	handle := &ClientHandle{
		Informer: entry.client,
		Status:   entry.client.Status(),
		registerStructuralHandler: func(handler StructuralUpdateHandler) {
			entry.client.RegisterStructuralHandler(handler)
		},
	}
	handle.release = func() error {
		return r.releaseEntry(key, entry)
	}
	return handle
}

func (r *InformerRegistry) releaseEntry(key string, expected *registryEntry) error {
	r.mu.Lock()
	entry, exists := r.entries[key]
	if !exists || entry != expected {
		r.mu.Unlock()
		return nil
	}

	entry.refCount--
	if entry.refCount <= 0 {
		entry.refCount = 0
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
			entry.lingerTimer = nil
		}

		// Immediate synchronous closure when lingerDur == 0 (e.g. in tests)
		if r.lingerDur == 0 {
			var clientToClose InformerClient
			if e, ok := r.entries[key]; ok && e.refCount == 0 {
				clientToClose = e.client
				delete(r.entries, key)
			}
			r.mu.Unlock()
			if clientToClose != nil {
				return clientToClose.Close() // Executed strictly outside r.mu.Lock()
			}
			return nil
		}

		// Start linger timer to absorb transient reload drop windows
		var timer *time.Timer
		timer = time.AfterFunc(r.lingerDur, func() {
			var clientToClose InformerClient
			r.mu.Lock()
			// Verify entry is still at refCount 0 AND this callback matches the current active timer
			if e, ok := r.entries[key]; ok && e.refCount == 0 && e.lingerTimer == timer {
				clientToClose = e.client
				delete(r.entries, key)
			}
			r.mu.Unlock()

			if clientToClose != nil {
				_ = clientToClose.Close() // Executed strictly outside r.mu.Lock()
			}
		})
		entry.lingerTimer = timer
	}
	r.mu.Unlock()
	return nil
}

// Shutdown cancels all active linger timers, closes all live clients outside the lock,
// clears the registry, and returns any aggregated closure errors.
func (r *InformerRegistry) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	clientsToClose := make([]InformerClient, 0, len(r.entries))
	for key, entry := range r.entries {
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
			entry.lingerTimer = nil
		}
		if entry.client != nil {
			clientsToClose = append(clientsToClose, entry.client)
		}
		delete(r.entries, key)
	}
	r.mu.Unlock()

	var errs []error
	for _, client := range clientsToClose {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// RefCount returns the active reference count for the specified key (useful for tests).
func (r *InformerRegistry) RefCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, exists := r.entries[key]; exists {
		return entry.refCount
	}
	return 0
}

// HasEntry checks if the given key is currently present in the registry.
func (r *InformerRegistry) HasEntry(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.entries[key]
	return exists
}

// EntryCount returns the number of active entries in the registry.
func (r *InformerRegistry) EntryCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

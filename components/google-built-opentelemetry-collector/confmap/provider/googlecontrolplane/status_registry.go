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
	"sync"
	"sync/atomic"
)

// DefaultStatusRegistry is the shared status registry singleton.
var DefaultStatusRegistry = NewStatusRegistry()

// PolicyStatusEntry represents the status and TypeURL of a single policy.
type PolicyStatusEntry struct {
	TypeURL string
	Status  string // "accepted", "rejected", "unsupported"
}

// StatusRegistry maintains the active policy revision and per-policy statuses in a thread-safe manner.
type StatusRegistry struct {
	mu             sync.RWMutex
	revision       atomic.Int64
	policyStatuses map[string]PolicyStatusEntry
}

// NewStatusRegistry creates a new StatusRegistry.
func NewStatusRegistry() *StatusRegistry {
	return &StatusRegistry{
		policyStatuses: make(map[string]PolicyStatusEntry),
	}
}

// SetRevisionAndStatuses atomically updates the revision number and replaces all policy statuses.
// Replacing the map wholesale ensures that deleted policies in revision N+1 are immediately pruned,
// preventing metric leaks.
func (r *StatusRegistry) SetRevisionAndStatuses(rev int64, statuses map[string]PolicyStatusEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.revision.Store(rev)
	cloned := make(map[string]PolicyStatusEntry, len(statuses))
	for k, v := range statuses {
		cloned[k] = v
	}
	r.policyStatuses = cloned
}

// Snapshot returns the current revision number and a copy of the policy statuses map.
func (r *StatusRegistry) Snapshot() (int64, map[string]PolicyStatusEntry) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	clone := make(map[string]PolicyStatusEntry, len(r.policyStatuses))
	for k, v := range r.policyStatuses {
		clone[k] = v
	}
	return r.revision.Load(), clone
}

// Revision returns the current active policy revision number.
func (r *StatusRegistry) Revision() int64 {
	return r.revision.Load()
}

// GetStatus returns the status of a specific policy ID if present.
func (r *StatusRegistry) GetStatus(policyID string) (PolicyStatusEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.policyStatuses[policyID]
	return entry, ok
}

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
	"sync"
)

// PolicyStatus represents the current application state of a policy.
type PolicyStatus int

const (
	PolicyStatusPending PolicyStatus = iota
	PolicyStatusApplied
	PolicyStatusFailed
)

func (s PolicyStatus) String() string {
	switch s {
	case PolicyStatusPending:
		return "pending"
	case PolicyStatusApplied:
		return "applied"
	case PolicyStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// PolicyStatusRecord stores metadata and evaluation status for a single policy.
type PolicyStatusRecord struct {
	PolicyID string
	TypeURL  string
	Status   PolicyStatus
	Error    string
}

// PolicyStatusEntry is an alias for PolicyStatusRecord for compatibility.
type PolicyStatusEntry = PolicyStatusRecord

// StatusSnapshot is a snapshot of the current revision and policy status map.
type StatusSnapshot struct {
	Revision int64
	Statuses map[string]PolicyStatusRecord
}

// StatusRegistry maintains the active policy revision and per-policy statuses in a thread-safe manner.
type StatusRegistry struct {
	mu       sync.RWMutex
	revision int64
	statuses map[string]PolicyStatusRecord
}

// NewStatusRegistry creates an initialized StatusRegistry.
func NewStatusRegistry() *StatusRegistry {
	return &StatusRegistry{
		statuses: make(map[string]PolicyStatusRecord),
	}
}

// DefaultStatusRegistry is the package-level shared StatusRegistry instance.
var DefaultStatusRegistry = NewStatusRegistry()

// RecordStatus records or updates the status of an individual policy.
func (s *StatusRegistry) RecordStatus(policyID, typeURL string, status PolicyStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	if s.statuses == nil {
		s.statuses = make(map[string]PolicyStatusRecord)
	}
	s.statuses[policyID] = PolicyStatusRecord{
		PolicyID: policyID,
		TypeURL:  typeURL,
		Status:   status,
		Error:    errMsg,
	}
}

// SetRevisionAndStatuses atomically updates the revision number and replaces all policy statuses.
// Replacing the status map wholesale ensures that deleted policies in revision N+1 are immediately
// pruned, preventing metric/telemetry leaks.
func (s *StatusRegistry) SetRevisionAndStatuses(revision int64, statuses map[string]PolicyStatusRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.revision = revision
	s.statuses = make(map[string]PolicyStatusRecord, len(statuses))
	for k, v := range statuses {
		s.statuses[k] = v
	}
}

// DeleteStatus removes the status record for the specified policyID.
func (s *StatusRegistry) DeleteStatus(policyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.statuses, policyID)
}

// Snapshot returns the current revision and a copy of the policy statuses map.
func (s *StatusRegistry) Snapshot() (int64, map[string]PolicyStatusRecord) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cpy := make(map[string]PolicyStatusRecord, len(s.statuses))
	for k, v := range s.statuses {
		cpy[k] = v
	}
	return s.revision, cpy
}

// GetSnapshot returns a StatusSnapshot struct containing the current revision and copied statuses.
func (s *StatusRegistry) GetSnapshot() StatusSnapshot {
	rev, snap := s.Snapshot()
	return StatusSnapshot{
		Revision: rev,
		Statuses: snap,
	}
}

// Revision returns the current active policy revision number.
func (s *StatusRegistry) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// GetStatus returns the status of a specific policy ID if present.
func (s *StatusRegistry) GetStatus(policyID string) (PolicyStatusRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.statuses[policyID]
	return entry, ok
}

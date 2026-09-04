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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusRegistry_AtomicSnapshot(t *testing.T) {
	sr := NewStatusRegistry()

	const concurrency = 30
	const iterations = 500
	var wg sync.WaitGroup

	// Concurrently invoke RecordStatus, SetRevisionAndStatuses, DeleteStatus, and Snapshot
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				switch iter % 4 {
				case 0:
					sr.RecordStatus(
						fmt.Sprintf("policy-%d", workerID),
						"type.googleapis.com/test.Policy",
						PolicyStatusApplied,
						nil,
					)
				case 1:
					batch := map[string]PolicyStatusRecord{
						fmt.Sprintf("policy-rev-%d", workerID): {
							PolicyID: fmt.Sprintf("policy-rev-%d", workerID),
							TypeURL:  "type.googleapis.com/test.Policy",
							Status:   PolicyStatusApplied,
						},
					}
					sr.SetRevisionAndStatuses(int64(iter), batch)
				case 2:
					sr.DeleteStatus(fmt.Sprintf("policy-%d", workerID))
				case 3:
					rev, snap := sr.Snapshot()
					assert.True(t, rev >= 0)
					assert.NotNil(t, snap)
				}
			}
		}(i)
	}

	wg.Wait()

	// Final snapshot check
	rev, finalSnap := sr.Snapshot()
	assert.True(t, rev >= 0)
	assert.NotNil(t, finalSnap)
}

func TestStatusRegistry_CRUD(t *testing.T) {
	sr := NewStatusRegistry()

	sr.RecordStatus("p1", "type.A", PolicyStatusApplied, nil)
	sr.RecordStatus("p2", "type.B", PolicyStatusFailed, errors.New("invalid rule"))

	p1, ok := sr.GetStatus("p1")
	require.True(t, ok)
	assert.Equal(t, PolicyStatusApplied, p1.Status)
	assert.Equal(t, "applied", p1.Status.String())

	p2, ok := sr.GetStatus("p2")
	require.True(t, ok)
	assert.Equal(t, PolicyStatusFailed, p2.Status)
	assert.Equal(t, "invalid rule", p2.Error)

	// SetRevisionAndStatuses atomic purge
	sr.SetRevisionAndStatuses(5, map[string]PolicyStatusRecord{
		"p3": {
			PolicyID: "p3",
			TypeURL:  "type.C",
			Status:   PolicyStatusApplied,
		},
	})

	assert.Equal(t, int64(5), sr.Revision())
	_, ok1 := sr.GetStatus("p1")
	assert.False(t, ok1, "p1 must be purged after SetRevisionAndStatuses")
	_, ok2 := sr.GetStatus("p2")
	assert.False(t, ok2, "p2 must be purged after SetRevisionAndStatuses")
	p3, ok3 := sr.GetStatus("p3")
	assert.True(t, ok3)
	assert.Equal(t, "p3", p3.PolicyID)

	// DeleteStatus
	sr.DeleteStatus("p3")
	_, ok3After := sr.GetStatus("p3")
	assert.False(t, ok3After)
}

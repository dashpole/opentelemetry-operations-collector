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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusRegistry(t *testing.T) {
	t.Run("Initial state", func(t *testing.T) {
		reg := NewStatusRegistry()
		rev, snapshot := reg.Snapshot()
		assert.Equal(t, int64(0), rev)
		assert.Empty(t, snapshot)
		assert.Equal(t, int64(0), reg.Revision())
	})

	t.Run("SetRevisionAndStatuses and Snapshot", func(t *testing.T) {
		reg := NewStatusRegistry()
		statuses := map[string]PolicyStatusEntry{
			"policy-1": {TypeURL: "type.googleapis.com/test.Policy1", Status: "accepted"},
			"policy-2": {TypeURL: "type.googleapis.com/test.Policy2", Status: "rejected"},
		}

		reg.SetRevisionAndStatuses(1, statuses)

		rev, snapshot := reg.Snapshot()
		assert.Equal(t, int64(1), rev)
		assert.Len(t, snapshot, 2)
		assert.Equal(t, "accepted", snapshot["policy-1"].Status)
		assert.Equal(t, "rejected", snapshot["policy-2"].Status)

		entry, ok := reg.GetStatus("policy-1")
		require.True(t, ok)
		assert.Equal(t, "accepted", entry.Status)

		_, ok = reg.GetStatus("nonexistent")
		assert.False(t, ok)
	})

	t.Run("Pruning deleted policies in revision N+1", func(t *testing.T) {
		reg := NewStatusRegistry()

		// Revision 1 with policy-1 and policy-2
		reg.SetRevisionAndStatuses(1, map[string]PolicyStatusEntry{
			"policy-1": {TypeURL: "type.googleapis.com/test.Policy1", Status: "accepted"},
			"policy-2": {TypeURL: "type.googleapis.com/test.Policy2", Status: "rejected"},
		})

		rev1, snap1 := reg.Snapshot()
		assert.Equal(t, int64(1), rev1)
		assert.Len(t, snap1, 2)

		// Revision 2: policy-2 removed, policy-3 added
		reg.SetRevisionAndStatuses(2, map[string]PolicyStatusEntry{
			"policy-1": {TypeURL: "type.googleapis.com/test.Policy1", Status: "accepted"},
			"policy-3": {TypeURL: "type.googleapis.com/test.Policy3", Status: "accepted"},
		})

		rev2, snap2 := reg.Snapshot()
		assert.Equal(t, int64(2), rev2)
		assert.Len(t, snap2, 2)
		assert.Contains(t, snap2, "policy-1")
		assert.Contains(t, snap2, "policy-3")
		assert.NotContains(t, snap2, "policy-2", "Deleted policy-2 must be pruned to prevent metric leaks")

		_, ok := reg.GetStatus("policy-2")
		assert.False(t, ok)
	})

	t.Run("Thread-safe concurrent reads and writes", func(t *testing.T) {
		reg := NewStatusRegistry()
		var wg sync.WaitGroup

		// Concurrent writers
		for w := 0; w < 10; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					statuses := map[string]PolicyStatusEntry{
						fmt.Sprintf("policy-%d-%d", workerID, i): {
							TypeURL: "type.googleapis.com/test.Policy",
							Status:  "accepted",
						},
					}
					reg.SetRevisionAndStatuses(int64(i+1), statuses)
				}
			}(w)
		}

		// Concurrent readers
		for r := 0; r < 10; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_ = reg.Revision()
					_, _ = reg.Snapshot()
					_, _ = reg.GetStatus("policy-0-0")
				}
			}()
		}

		wg.Wait()
		rev, snap := reg.Snapshot()
		assert.Greater(t, rev, int64(0))
		assert.NotEmpty(t, snap)
	})
}

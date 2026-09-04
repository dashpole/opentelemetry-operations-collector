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

package file

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestFileClient_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()
	policyFile := filepath.Join(tmpDir, "policy.json")

	initialJSON := `{
		"policies": [
			{
				"name": "rule-1",
				"typed_config": {
					"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
				}
			}
		]
	}`
	require.NoError(t, os.WriteFile(policyFile, []byte(initialJSON), 0644))

	logger := zaptest.NewLogger(t)
	cfg := Config{
		Path:             policyFile,
		DebounceInterval: 20 * time.Millisecond,
		Logger:           logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh, err := client.Watch(ctx, "")
	require.NoError(t, err)

	// Consume initial EventResync
	initEv := <-watchCh
	assert.Equal(t, controlplane.EventResync, initEv.Type)

	// List check
	list, err := client.List("")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "rule-1", list[0].Name)

	// Status check
	rev, statuses := client.Status().Snapshot()
	assert.Equal(t, int64(1), rev)
	assert.Equal(t, controlplane.PolicyStatusApplied, statuses["rule-1"].Status)

	// Update file with new policy
	updatedJSON := `{
		"policies": [
			{
				"name": "rule-1",
				"typed_config": {
					"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
				}
			},
			{
				"name": "rule-2",
				"typed_config": {
					"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"
				}
			}
		]
	}`
	require.NoError(t, os.WriteFile(policyFile, []byte(updatedJSON), 0644))

	// Wait for event on watch channel
	select {
	case ev := <-watchCh:
		assert.Equal(t, controlplane.EventAdded, ev.Type)
		assert.Equal(t, "rule-2", ev.PolicyID)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for file modification watch event")
	}

	// Verify updated StatusRegistry
	revAfter, statusesAfter := client.Status().Snapshot()
	assert.Equal(t, int64(2), revAfter)
	assert.Contains(t, statusesAfter, "rule-1")
	assert.Contains(t, statusesAfter, "rule-2")

	// Update file with empty policies: verify deletion diffing works cleanly (Finding 2)
	emptyJSON := `{"policies": []}`
	require.NoError(t, os.WriteFile(policyFile, []byte(emptyJSON), 0644))

	// Should receive EventDeleted for both rules in deterministic sorted order
	deletedRules := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case ev := <-watchCh:
			assert.Equal(t, controlplane.EventDeleted, ev.Type)
			assert.Nil(t, ev.Policy)
			deletedRules[ev.PolicyID] = true
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for policy deletion events")
		}
	}
	assert.True(t, deletedRules["rule-1"])
	assert.True(t, deletedRules["rule-2"])

	revEmpty, statusesEmpty := client.Status().Snapshot()
	assert.Equal(t, int64(3), revEmpty)
	assert.Empty(t, statusesEmpty, "StatusRegistry must be purged of deleted policies")
}

func TestFileClient_DirectoryWatch(t *testing.T) {
	tmpDir := t.TempDir()
	policy1 := filepath.Join(tmpDir, "policy1.json")
	require.NoError(t, os.WriteFile(policy1, []byte(`{
		"name": "p1",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
		}
	}`), 0644))

	logger := zaptest.NewLogger(t)
	cfg := Config{
		Path:             tmpDir,
		DebounceInterval: 20 * time.Millisecond,
		Logger:           logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh, err := client.Watch(ctx, "")
	require.NoError(t, err)
	<-watchCh // Resync

	list, err := client.List("")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "p1", list[0].Name)

	// Add policy2 in the directory
	policy2 := filepath.Join(tmpDir, "policy2.json")
	require.NoError(t, os.WriteFile(policy2, []byte(`{
		"name": "p2",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"
		}
	}`), 0644))

	select {
	case ev := <-watchCh:
		assert.Equal(t, controlplane.EventAdded, ev.Type)
		assert.Equal(t, "p2", ev.PolicyID)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for directory addition event")
	}

	// Delete policy1 from directory
	require.NoError(t, os.Remove(policy1))

	select {
	case ev := <-watchCh:
		assert.Equal(t, controlplane.EventDeleted, ev.Type)
		assert.Equal(t, "p1", ev.PolicyID)
		assert.Nil(t, ev.Policy)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for directory deletion event")
	}
}

func TestFileClient_SymlinkSwap(t *testing.T) {
	// Simulates Kubernetes ConfigMap atomic ..data symlink swap
	tmpDir := t.TempDir()

	dataDir1 := filepath.Join(tmpDir, "..data_1")
	require.NoError(t, os.Mkdir(dataDir1, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir1, "rules.json"), []byte(`{
		"name": "initial-rule",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
		}
	}`), 0644))

	dataSymlink := filepath.Join(tmpDir, "..data")
	require.NoError(t, os.Symlink(dataDir1, dataSymlink))

	fileSymlink := filepath.Join(tmpDir, "rules.json")
	require.NoError(t, os.Symlink(filepath.Join(dataSymlink, "rules.json"), fileSymlink))

	logger := zaptest.NewLogger(t)
	cfg := Config{
		Path:             tmpDir,
		DebounceInterval: 20 * time.Millisecond,
		Logger:           logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh, err := client.Watch(ctx, "")
	require.NoError(t, err)
	<-watchCh // Resync

	list, err := client.List("")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "initial-rule", list[0].Name)

	// Step 2: Prepare ..data_2 with swapped rule
	dataDir2 := filepath.Join(tmpDir, "..data_2")
	require.NoError(t, os.Mkdir(dataDir2, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir2, "rules.json"), []byte(`{
		"name": "swapped-rule",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
		}
	}`), 0644))

	// Atomically swap ..data symlink to ..data_2
	tmpSymlink := filepath.Join(tmpDir, "..data_tmp")
	require.NoError(t, os.Symlink(dataDir2, tmpSymlink))
	require.NoError(t, os.Rename(tmpSymlink, dataSymlink))

	// Verify watcher detects symlink swap and delivers EventAdded for swapped-rule and EventDeleted for initial-rule
	events := make(map[string]controlplane.PolicyWatchEvent)
	timeout := time.After(3 * time.Second)
	for len(events) < 2 {
		select {
		case ev := <-watchCh:
			events[ev.PolicyID] = ev
		case <-timeout:
			t.Fatalf("timed out waiting for symlink swap events (received %d)", len(events))
		}
	}

	assert.Equal(t, controlplane.EventAdded, events["swapped-rule"].Type)
	assert.Equal(t, controlplane.EventDeleted, events["initial-rule"].Type)
}

func TestFileClient_StructuralHandler(t *testing.T) {
	tmpDir := t.TempDir()
	policyFile := filepath.Join(tmpDir, "policy.json")
	require.NoError(t, os.WriteFile(policyFile, []byte(`{
		"name": "rule-1",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy"
		}
	}`), 0644))

	logger := zaptest.NewLogger(t)
	cfg := Config{
		Path:             policyFile,
		DebounceInterval: 20 * time.Millisecond,
		Logger:           logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	var handlerCalls atomic.Int64
	client.RegisterStructuralHandler(func(ctx context.Context, update controlplane.PolicySnapshotUpdate) error {
		handlerCalls.Add(1)
		return nil
	})

	// Trigger update
	require.NoError(t, os.WriteFile(policyFile, []byte(`{
		"name": "rule-2",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy"
		}
	}`), 0644))

	require.Eventually(t, func() bool {
		return handlerCalls.Load() >= 1
	}, 3*time.Second, 20*time.Millisecond, "StructuralUpdateHandler must be invoked on file update")
}

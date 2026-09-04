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

package ingestor

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func createSamplePolicy(id string) *v3.TypedExtensionConfig {
	filter := &policyv1alpha1.LogFilterPolicy{
		Id:     id,
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
			},
		},
	}
	anyFilter, _ := anypb.New(filter)
	return &v3.TypedExtensionConfig{
		Name:        id,
		TypedConfig: anyFilter,
	}
}

func createSampleCollector(policyIDs ...string) *xdsv1alpha1.TelemetryCollector {
	col := &xdsv1alpha1.TelemetryCollector{
		Policies: make([]*v3.TypedExtensionConfig, 0, len(policyIDs)),
	}
	for _, id := range policyIDs {
		filter := &policyv1alpha1.LogFilterPolicy{
			Id:     id,
			Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
			Matches: []*policyv1alpha1.LogMatcher{
				{
					Target: &policyv1alpha1.LogFieldSelector{
						Target: &policyv1alpha1.LogFieldSelector_RecordField{
							RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
						},
					},
					Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
				},
			},
		}
		anyFilter, _ := anypb.New(filter)
		col.Policies = append(col.Policies, &v3.TypedExtensionConfig{
			Name:        id,
			TypedConfig: anyFilter,
		})
	}
	return col
}

func TestFileIngestor(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("Single JSON policy file and modification", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "policies.json")

		initialCol := createSampleCollector("policy-1")
		data, err := protojson.Marshal(initialCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, data, 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(filePath, logger, WithFileDebounce(20*time.Millisecond))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		// Verify initial load
		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			assert.Equal(t, int64(1), u.RevisionNumber)
			require.Len(t, u.Collector.Policies, 1)
			assert.Equal(t, "policy-1", u.Collector.Policies[0].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial policy update")
		}

		// Acknowledge update
		require.NoError(t, ing.Acknowledge(ctx, PolicyAck{Revision: "1", Nonce: "1"}))
		assert.Equal(t, "1", ing.LastAck().Revision)

		// Modify file on disk
		updatedCol := createSampleCollector("policy-1", "policy-2")
		updatedData, err := protojson.Marshal(updatedCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, updatedData, 0o600))

		// Verify update event
		select {
		case u := <-updateCh:
			assert.Equal(t, "2", u.Revision)
			assert.Equal(t, int64(2), u.RevisionNumber)
			require.Len(t, u.Collector.Policies, 2)
			assert.Equal(t, "policy-1", u.Collector.Policies[0].Name)
			assert.Equal(t, "policy-2", u.Collector.Policies[1].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for modified policy update")
		}
	})

	t.Run("Single binary protobuf file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "policies.pb")

		initialCol := createSampleCollector("policy-bin-1")
		data, err := proto.Marshal(initialCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, data, 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(filePath, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			require.Len(t, u.Collector.Policies, 1)
			assert.Equal(t, "policy-bin-1", u.Collector.Policies[0].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for binary protobuf policy update")
		}
	})

	t.Run("Directory with multiple policy files", func(t *testing.T) {
		tmpDir := t.TempDir()
		p1Path := filepath.Join(tmpDir, "10_p1.json")
		p2Path := filepath.Join(tmpDir, "20_p2.json")

		p1Col := createSampleCollector("policy-dir-1")
		d1, _ := protojson.Marshal(p1Col)
		require.NoError(t, os.WriteFile(p1Path, d1, 0o600))

		p2Col := createSampleCollector("policy-dir-2")
		d2, _ := protojson.Marshal(p2Col)
		require.NoError(t, os.WriteFile(p2Path, d2, 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(tmpDir, logger, WithFileDebounce(20*time.Millisecond))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		// Verify combined policies
		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			require.Len(t, u.Collector.Policies, 2)
			assert.Equal(t, "policy-dir-1", u.Collector.Policies[0].Name)
			assert.Equal(t, "policy-dir-2", u.Collector.Policies[1].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for directory policy update")
		}

		// Modify one file
		p1Updated := createSampleCollector("policy-dir-1-modified")
		d1Updated, _ := protojson.Marshal(p1Updated)
		require.NoError(t, os.WriteFile(p1Path, d1Updated, 0o600))

		select {
		case u := <-updateCh:
			assert.Equal(t, "2", u.Revision)
			require.Len(t, u.Collector.Policies, 2)
			assert.Equal(t, "policy-dir-1-modified", u.Collector.Policies[0].Name)
			assert.Equal(t, "policy-dir-2", u.Collector.Policies[1].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for directory update after modification")
		}
	})

	t.Run("Single TypedExtensionConfig in JSON file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "single_policy.json")

		policy := createSamplePolicy("single-json-policy")
		data, err := protojson.Marshal(policy)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, data, 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(filePath, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			require.Len(t, u.Collector.Policies, 1)
			assert.Equal(t, "single-json-policy", u.Collector.Policies[0].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for single TypedExtensionConfig JSON update")
		}
	})

	t.Run("Single TypedExtensionConfig in binary protobuf file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "single_policy.pb")

		policy := createSamplePolicy("single-proto-policy")
		data, err := proto.Marshal(policy)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, data, 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(filePath, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			require.Len(t, u.Collector.Policies, 1)
			assert.Equal(t, "single-proto-policy", u.Collector.Policies[0].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for single TypedExtensionConfig binary protobuf update")
		}
	})

	t.Run("Initial file invalid, subsequent edit triggers dynamic reload", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "policy.json")

		// Write invalid content initially
		require.NoError(t, os.WriteFile(filePath, []byte("{ invalid: json"), 0o600))

		updateCh := make(chan PolicyUpdate, 10)
		ing := NewFileIngestor(filePath, logger, WithFileDebounce(20*time.Millisecond))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := ing.Start(ctx, func(u PolicyUpdate) error {
			updateCh <- u
			return nil
		})
		require.NoError(t, err)
		defer func() { _ = ing.Stop(context.Background()) }()

		// Verify no update was dispatched for invalid file
		select {
		case u := <-updateCh:
			t.Fatalf("unexpected update received for invalid file: %+v", u)
		case <-time.After(100 * time.Millisecond):
			// Expected no update
		}

		// Now write valid policy to disk
		validCol := createSampleCollector("fixed-policy-1")
		validData, err := protojson.Marshal(validCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, validData, 0o600))

		// Verify dynamic reload is triggered
		select {
		case u := <-updateCh:
			assert.Equal(t, "1", u.Revision)
			require.Len(t, u.Collector.Policies, 1)
			assert.Equal(t, "fixed-policy-1", u.Collector.Policies[0].Name)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for dynamic reload update after fixing file")
		}
	})
}

func TestFileIngestor_AtomicRename(t *testing.T) {
	logger := zaptest.NewLogger(t)
	baseDir := t.TempDir()

	// Simulate Kubernetes ConfigMap layout:
	// baseDir/
	//   version1/
	//     policies.json
	//   ..data -> version1
	//   policies.json -> ..data/policies.json
	v1Dir := filepath.Join(baseDir, "version1")
	require.NoError(t, os.MkdirAll(v1Dir, 0o755))

	col1 := createSampleCollector("k8s-policy-v1")
	d1, err := protojson.Marshal(col1)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(v1Dir, "policies.json"), d1, 0o600))

	dotData := filepath.Join(baseDir, "..data")
	require.NoError(t, os.Symlink(v1Dir, dotData))

	symlinkFile := filepath.Join(baseDir, "policies.json")
	require.NoError(t, os.Symlink(filepath.Join("..data", "policies.json"), symlinkFile))

	var mu sync.Mutex
	updates := make([]PolicyUpdate, 0)
	updateCh := make(chan PolicyUpdate, 10)

	ing := NewFileIngestor(baseDir, logger, WithFileDebounce(20*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = ing.Start(ctx, func(u PolicyUpdate) error {
		mu.Lock()
		updates = append(updates, u)
		mu.Unlock()
		updateCh <- u
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = ing.Stop(context.Background()) }()

	// Verify v1 initial load
	select {
	case u := <-updateCh:
		require.Len(t, u.Collector.Policies, 1)
		assert.Equal(t, "k8s-policy-v1", u.Collector.Policies[0].Name)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial k8s configmap update")
	}

	// Now simulate atomic ConfigMap update:
	// 1. Create version2 directory
	v2Dir := filepath.Join(baseDir, "version2")
	require.NoError(t, os.MkdirAll(v2Dir, 0o755))

	col2 := createSampleCollector("k8s-policy-v2")
	d2, err := protojson.Marshal(col2)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(v2Dir, "policies.json"), d2, 0o600))

	// 2. Create ..data_tmp symlink
	dotDataTmp := filepath.Join(baseDir, "..data_tmp")
	require.NoError(t, os.Symlink(v2Dir, dotDataTmp))

	// 3. Atomic rename ..data_tmp -> ..data
	require.NoError(t, os.Rename(dotDataTmp, dotData))

	// Verify v2 update event triggered by atomic rename
	select {
	case u := <-updateCh:
		require.Len(t, u.Collector.Policies, 1)
		assert.Equal(t, "k8s-policy-v2", u.Collector.Policies[0].Name)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for atomic rename policy update")
	}
}

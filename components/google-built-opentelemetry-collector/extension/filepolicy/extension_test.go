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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestFilePolicyExtension(t *testing.T) {
	ctx := context.Background()

	// 1. Test Factory and Config
	factory := NewFactory()
	assert.Equal(t, component.MustNewType("filepolicy"), factory.Type())

	defaultCfg := factory.CreateDefaultConfig().(*Config)
	assert.Equal(t, DefaultDebounceInterval, defaultCfg.DebounceInterval)

	invalidCfg := &Config{Path: ""}
	assert.Error(t, invalidCfg.Validate())

	// 2. Setup directory of policies
	tmpDir := t.TempDir()
	policy1Path := filepath.Join(tmpDir, "policy1.json")
	require.NoError(t, os.WriteFile(policy1Path, []byte(`{
		"name": "rule-1",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"
		}
	}`), 0644))

	// Setup observer logger to catch warnings on malformed files
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	set := extensiontest.NewNopSettings(typeStr)
	set.Logger = logger

	cfg := &Config{
		Path:             tmpDir,
		DebounceInterval: 20 * time.Millisecond,
	}
	require.NoError(t, cfg.Validate())

	isolatedReg := controlplane.NewInformerRegistry(0)
	defer func() { _ = isolatedReg.Shutdown(ctx) }()

	extAny, err := factory.Create(ctx, set, cfg)
	require.NoError(t, err)
	ext := extAny.(*filePolicyExtension).WithRegistry(isolatedReg)

	// 3. Start extension
	require.NoError(t, ext.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, ext.Shutdown(ctx))
	}()

	// 4. Verify Ready()
	select {
	case <-ext.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Informer.Ready()")
	}

	// 5. Verify initial List() delivers rule-1
	list, err := ext.List("")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "rule-1", list[0].Name)

	// 6. Subscribe to Watch()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watchCh, err := ext.Watch(watchCtx, "")
	require.NoError(t, err)

	// Consume initial EventResync
	initEv := <-watchCh
	assert.Equal(t, controlplane.EventResync, initEv.Type)

	// 7. Add policy2.json to directory
	policy2Path := filepath.Join(tmpDir, "policy2.json")
	require.NoError(t, os.WriteFile(policy2Path, []byte(`{
		"name": "rule-2",
		"typed_config": {
			"@type": "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"
		}
	}`), 0644))

	select {
	case ev := <-watchCh:
		assert.Equal(t, controlplane.EventAdded, ev.Type)
		assert.Equal(t, "rule-2", ev.PolicyID)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event on policy addition")
	}

	listAfter, err := ext.List("")
	require.NoError(t, err)
	assert.Len(t, listAfter, 2)

	// 8. Ingest malformed file: verify error is logged without extension panic
	malformedPath := filepath.Join(tmpDir, "bad_policy.json")
	require.NoError(t, os.WriteFile(malformedPath, []byte(`{ not valid json @#!`), 0644))

	require.Eventually(t, func() bool {
		warns := logs.FilterMessageSnippet("Failed to parse policy file in directory")
		return warns.Len() > 0
	}, 3*time.Second, 20*time.Millisecond, "expected warning log on malformed policy file")

	// Verify extension is still operational and retains valid policies
	listAfterMalformed, err := ext.List("")
	require.NoError(t, err)
	assert.Len(t, listAfterMalformed, 2)

	// Cleanup malformed file
	require.NoError(t, os.Remove(malformedPath))
}

func TestFilePolicyExtension_SymlinkSwap(t *testing.T) {
	ctx := context.Background()

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

	factory := NewFactory()
	cfg := &Config{
		Path:             tmpDir,
		DebounceInterval: 20 * time.Millisecond,
	}

	isolatedReg := controlplane.NewInformerRegistry(0)
	defer func() { _ = isolatedReg.Shutdown(ctx) }()

	set := extensiontest.NewNopSettings(typeStr)
	extAny, err := factory.Create(ctx, set, cfg)
	require.NoError(t, err)
	ext := extAny.(*filePolicyExtension).WithRegistry(isolatedReg)

	require.NoError(t, ext.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, ext.Shutdown(ctx))
	}()

	<-ext.Ready()

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watchCh, err := ext.Watch(watchCtx, "")
	require.NoError(t, err)
	<-watchCh // Resync

	initialList, err := ext.List("")
	require.NoError(t, err)
	require.Len(t, initialList, 1)
	assert.Equal(t, "initial-rule", initialList[0].Name)

	// Prepare ..data_2 with swapped rule
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

	swappedList, err := ext.List("")
	require.NoError(t, err)
	require.Len(t, swappedList, 1)
	assert.Equal(t, "swapped-rule", swappedList[0].Name)
}

func TestFilePolicy_LifecycleAndValidation(t *testing.T) {
	// 1. Config validation
	cfgBadDebounce := &Config{
		Path:             "/some/path",
		DebounceInterval: -10 * time.Millisecond,
	}
	assert.Error(t, cfgBadDebounce.Validate(), "must reject negative debounce_interval")

	cfgEmptyPath := &Config{
		Path: "",
	}
	assert.Error(t, cfgEmptyPath.Validate(), "must reject empty path")

	// 2. Pre-start Ready() and double Start() tests
	tmpDir := t.TempDir()
	policyPath := filepath.Join(tmpDir, "rules.json")
	require.NoError(t, os.WriteFile(policyPath, []byte(`{"policies": []}`), 0644))

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Path = policyPath

	set := extensiontest.NewNopSettings(component.MustNewType("filepolicy"))
	ext, err := factory.Create(context.Background(), set, cfg)
	require.NoError(t, err)

	fileExt := ext.(*filePolicyExtension)
	isolatedReg := controlplane.NewInformerRegistry(0)
	fileExt = fileExt.WithRegistry(isolatedReg)

	// Call Ready() BEFORE Start()
	readyCh := fileExt.Ready()
	select {
	case <-readyCh:
		t.Fatal("Ready() must not be closed before Start()")
	default:
	}

	// Start extension
	err = fileExt.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Pre-start Ready() should now be closed
	select {
	case <-readyCh:
		// Passed
	case <-time.After(3 * time.Second):
		t.Fatal("pre-start Ready() was not unblocked after Start()")
	}

	// Double start should return error
	errDouble := fileExt.Start(context.Background(), componenttest.NewNopHost())
	assert.Error(t, errDouble)
	assert.Contains(t, errDouble.Error(), "extension already started")

	// Shutdown
	err = fileExt.Shutdown(context.Background())
	require.NoError(t, err)

	// Post-shutdown List and Watch should return "extension not running"
	_, errList := fileExt.List("")
	assert.Error(t, errList)
	assert.Contains(t, errList.Error(), "extension not running")

	_, errWatch := fileExt.Watch(context.Background(), "")
	assert.Error(t, errWatch)
	assert.Contains(t, errWatch.Error(), "extension not running")
}

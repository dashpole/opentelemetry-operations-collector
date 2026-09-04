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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockInformerClient struct {
	mu                sync.Mutex
	status            *controlplane.StatusRegistry
	structuralHandler controlplane.StructuralUpdateHandler
	readyCh           chan struct{}
	readyOnce         sync.Once
	closed            bool
	policies          []*v3.TypedExtensionConfig
}

func newMockInformerClient() *mockInformerClient {
	return &mockInformerClient{
		status:  controlplane.NewStatusRegistry(),
		readyCh: make(chan struct{}),
	}
}

func (m *mockInformerClient) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policies, nil
}

func (m *mockInformerClient) Watch(ctx context.Context, typeURL string) (<-chan controlplane.PolicyWatchEvent, error) {
	ch := make(chan controlplane.PolicyWatchEvent, 10)
	return ch, nil
}

func (m *mockInformerClient) Ready() <-chan struct{} {
	return m.readyCh
}

func (m *mockInformerClient) MarkReady() {
	m.readyOnce.Do(func() {
		close(m.readyCh)
	})
}

func (m *mockInformerClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.MarkReady()
	return nil
}

func (m *mockInformerClient) Status() *controlplane.StatusRegistry {
	return m.status
}

func (m *mockInformerClient) RegisterStructuralHandler(handler controlplane.StructuralUpdateHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.structuralHandler = handler
}

func (m *mockInformerClient) TriggerStructural(ctx context.Context, policies []*v3.TypedExtensionConfig, revision string) error {
	m.mu.Lock()
	m.policies = policies
	handler := m.structuralHandler
	m.mu.Unlock()
	if handler == nil {
		for i := 0; i < 50; i++ {
			time.Sleep(10 * time.Millisecond)
			m.mu.Lock()
			handler = m.structuralHandler
			m.mu.Unlock()
			if handler != nil {
				break
			}
		}
	}
	if handler != nil {
		err := handler(ctx, controlplane.PolicySnapshotUpdate{
			Policies: policies,
			Revision: revision,
		})
		if err == nil {
			m.MarkReady()
		}
		return err
	}
	m.MarkReady()
	return nil
}

func (m *mockInformerClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func TestProviderLifecycle(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("Initial boot does not call watcher; subsequent updates trigger watcher", func(t *testing.T) {
		mockClient := newMockInformerClient()
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderClientFactory(func() (controlplane.InformerClient, error) {
				return mockClient, nil
			}))

		watcherCh := make(chan *confmap.ChangeEvent, 10)
		watcher := func(e *confmap.ChangeEvent) {
			watcherCh <- e
		}

		ctx := context.Background()

		// Deliver initial structural policies shortly after Acquire
		go func() {
			time.Sleep(20 * time.Millisecond)
			initialPolicies := []*v3.TypedExtensionConfig{
				{
					Name:        "gcp-dest",
					TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLGcpDestination},
				},
				{
					Name:        "otlp-src",
					TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLOtlpSource},
				},
			}
			_ = mockClient.TriggerStructural(ctx, initialPolicies, "1")
		}()

		retrieved, err := p.Retrieve(ctx, "googlecontrolplane:file:///var/policies?startup_timeout=2s", watcher)
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		// 1. Verify watcher was NOT called during initial boot
		select {
		case <-watcherCh:
			t.Fatal("watcher must not be called during initial bootstrapping")
		case <-time.After(100 * time.Millisecond):
		}

		// Verify initial configuration is loaded
		conf, err := retrieved.AsConf()
		require.NoError(t, err)
		require.NotNil(t, conf)

		// Verify component token was resolved
		tokenRetrieved, err := p.Retrieve(ctx, "googlecontrolplane:component//global_policy_processor", nil)
		require.NoError(t, err)
		require.NotNil(t, tokenRetrieved)
		tokenStr, err := tokenRetrieved.AsString()
		require.NoError(t, err)
		assert.Equal(t, "policy/global", tokenStr)

		// 2. Deliver revision 2 with structural change (new source policy): MUST trigger watcher asynchronously
		updatedPolicies := []*v3.TypedExtensionConfig{
			{
				Name:        "gcp-dest",
				TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLGcpDestination},
			},
			{
				Name:        "otlp-src",
				TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLOtlpSource},
			},
			{
				Name:        "filelog-src",
				TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLFilelogSource},
			},
		}
		err = mockClient.TriggerStructural(ctx, updatedPolicies, "2")
		require.NoError(t, err)

		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for watcher invocation on dynamic reload")
		}

		// Dynamic reload path returns currentConf immediately
		reloadRetrieved, err := p.Retrieve(ctx, "googlecontrolplane:file:///var/policies", watcher)
		require.NoError(t, err)
		require.NotNil(t, reloadRetrieved)

		require.NoError(t, p.Shutdown(ctx))
		assert.True(t, mockClient.IsClosed())
	})

	t.Run("Context cancellation during boot tears down client without leaks", func(t *testing.T) {
		mockClient := newMockInformerClient()
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderClientFactory(func() (controlplane.InformerClient, error) {
				return mockClient, nil
			}))

		cancelCtx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()

		// Retrieval blocks waiting for Ready(), then context cancels
		retrieved, err := p.Retrieve(cancelCtx, "googlecontrolplane:file:///var/policies?startup_timeout=10s", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, retrieved)

		require.NoError(t, p.Shutdown(context.Background()))
		assert.True(t, mockClient.IsClosed(), "Client must be cleanly closed when provider shuts down")
	})

	t.Run("Fast startup timeout fallback to base configuration", func(t *testing.T) {
		mockClient := newMockInformerClient()
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderClientFactory(func() (controlplane.InformerClient, error) {
				return mockClient, nil
			}))

		watcherCh := make(chan *confmap.ChangeEvent, 10)
		watcher := func(e *confmap.ChangeEvent) {
			watcherCh <- e
		}

		// Inject short timeout of 50ms without delivering policies from mockClient
		start := time.Now()
		retrieved, err := p.Retrieve(context.Background(), "googlecontrolplane:xds://127.0.0.1:443?startup_timeout=50ms", watcher)
		duration := time.Since(start)

		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.True(t, duration >= 40*time.Millisecond && duration < 1*time.Second, "Expected fast fallback around 50ms, took %v", duration)

		// Verify escape hatch token resolution after timeout fallback
		tokenProc, err := p.Retrieve(context.Background(), "googlecontrolplane:component//global_policy_processor", nil)
		require.NoError(t, err)
		require.NotNil(t, tokenProc)
		asStrProc, err := tokenProc.AsString()
		require.NoError(t, err)
		assert.Equal(t, "policy/global", asStrProc)

		tokenExp, err := p.Retrieve(context.Background(), "googlecontrolplane:component//active_destination_exporter", nil)
		require.NoError(t, err)
		require.NotNil(t, tokenExp)
		asStrExp, err := tokenExp.AsString()
		require.NoError(t, err)
		assert.Equal(t, "otlp/gcp_destination", asStrExp)

		// Watcher must not be called during fallback
		select {
		case <-watcherCh:
			t.Fatal("watcher must not be called on fallback to base configuration")
		default:
		}

		// Subsequent policy arrival after fallback triggers watcher
		updatedPolicies := []*v3.TypedExtensionConfig{
			{
				Name:        "gcp-dest",
				TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLGcpDestination},
			},
			{
				Name:        "otlp-src",
				TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLOtlpSource},
			},
		}
		err = mockClient.TriggerStructural(context.Background(), updatedPolicies, "1")
		require.NoError(t, err)

		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for dynamic reload after base config fallback")
		}

		require.NoError(t, p.Shutdown(context.Background()))
	})

	t.Run("End-to-end integration with real FileClient", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "policy.json")

		initialCol := map[string]any{
			"policies": []any{
				map[string]any{
					"name": "gcp-dest",
					"typed_config": map[string]any{
						"@type": driver.TypeURLGcpDestination,
					},
				},
				map[string]any{
					"name": "otlp-src",
					"typed_config": map[string]any{
						"@type": driver.TypeURLOtlpSource,
					},
				},
			},
		}
		initialBytes, err := json.Marshal(initialCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, initialBytes, 0o600))

		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger})

		watcherCh := make(chan *confmap.ChangeEvent, 10)
		watcher := func(e *confmap.ChangeEvent) {
			watcherCh <- e
		}

		uri := "googlecontrolplane:file://" + filePath + "?startup_timeout=2s"
		retrieved, err := p.Retrieve(context.Background(), uri, watcher)
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		// Verify no initial watcher event
		select {
		case <-watcherCh:
			t.Fatal("unexpected watcher call on boot")
		case <-time.After(50 * time.Millisecond):
		}

		// Modify file on disk with a new structural policy
		updatedCol := map[string]any{
			"policies": []any{
				map[string]any{
					"name": "gcp-dest",
					"typed_config": map[string]any{
						"@type": driver.TypeURLGcpDestination,
					},
				},
				map[string]any{
					"name": "otlp-src",
					"typed_config": map[string]any{
						"@type": driver.TypeURLOtlpSource,
					},
				},
				map[string]any{
					"name": "filelog-src",
					"typed_config": map[string]any{
						"@type": driver.TypeURLFilelogSource,
					},
				},
			},
		}
		updatedBytes, err := json.Marshal(updatedCol)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filePath, updatedBytes, 0o600))

		// Expect watcher event from file watcher
		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for watcher invocation on file change")
		}

		require.NoError(t, p.Shutdown(context.Background()))
	})

	t.Run("Factory creation", func(t *testing.T) {
		factory := NewFactory()
		require.NotNil(t, factory)
		assert.Equal(t, Scheme, factory.Create(confmap.ProviderSettings{Logger: logger}).Scheme())
	})
}

func TestProvider_HandlePolicyUpdate_ValidationAndNACK(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("Fail-open: valid policy accepted with skipped unsupported policy", func(t *testing.T) {
		mockClient := newMockInformerClient()
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderClientFactory(func() (controlplane.InformerClient, error) {
				return mockClient, nil
			}))

		// Bootstrap provider
		go func() {
			time.Sleep(10 * time.Millisecond)
			initialPolicies := []*v3.TypedExtensionConfig{
				{
					Name:        "gcp-dest",
					TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLGcpDestination},
				},
				{
					Name:        "otlp-src",
					TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLOtlpSource},
				},
			}
			_ = mockClient.TriggerStructural(context.Background(), initialPolicies, "1")
		}()

		_, err := p.Retrieve(context.Background(), "googlecontrolplane:xds://127.0.0.1:443", nil)
		require.NoError(t, err)

		// Send update with a valid structural policy AND an unsupported policy
		unsupportedPolicy := &v3.TypedExtensionConfig{
			Name: "unknown-policy-id",
			TypedConfig: &anypb.Any{
				TypeUrl: "type.googleapis.com/unsupported.Policy",
				Value:   []byte("data"),
			},
		}
		validNewPolicy := &v3.TypedExtensionConfig{
			Name:        "filelog-src",
			TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLFilelogSource},
		}

		policies := []*v3.TypedExtensionConfig{
			{Name: "gcp-dest", TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLGcpDestination}},
			{Name: "otlp-src", TypedConfig: &anypb.Any{TypeUrl: driver.TypeURLOtlpSource}},
			validNewPolicy,
			unsupportedPolicy,
		}

		// Validation allows valid policies (fail-open) and proceeds with compilation
		err = mockClient.TriggerStructural(context.Background(), policies, "2")
		require.NoError(t, err)
	})
}


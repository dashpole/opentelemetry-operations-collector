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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/ingestor"
	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockPolicyIngestor struct {
	mu       sync.Mutex
	onUpdate func(ingestor.PolicyUpdate) error
	startErr error
	stopErr  error
	started  bool
	stopped  bool
	acks     []ingestor.PolicyAck
}

func (m *mockPolicyIngestor) Start(ctx context.Context, onUpdate func(ingestor.PolicyUpdate) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startErr != nil {
		return m.startErr
	}
	m.started = true
	m.onUpdate = onUpdate
	return nil
}

func (m *mockPolicyIngestor) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	return m.stopErr
}

func (m *mockPolicyIngestor) Acknowledge(ctx context.Context, ack ingestor.PolicyAck) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, ack)
	return nil
}

func (m *mockPolicyIngestor) Trigger(update ingestor.PolicyUpdate) error {
	m.mu.Lock()
	fn := m.onUpdate
	m.mu.Unlock()
	if fn != nil {
		return fn(update)
	}
	return nil
}

func (m *mockPolicyIngestor) Acks() []ingestor.PolicyAck {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]ingestor.PolicyAck, len(m.acks))
	copy(res, m.acks)
	return res
}

func (m *mockPolicyIngestor) IsStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

func createTestCollectorWithPolicies(policyIDs ...string) *xdsv1alpha1.TelemetryCollector {
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

func TestProviderLifecycle(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("Initial boot does not call watcher; subsequent updates trigger watcher", func(t *testing.T) {
		mockIng := &mockPolicyIngestor{}
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderIngestorFactory(func(*ParsedURI) (ingestor.PolicyIngestor, error) {
				return mockIng, nil
			}))

		watcherCh := make(chan *confmap.ChangeEvent, 10)
		watcher := func(e *confmap.ChangeEvent) {
			watcherCh <- e
		}

		ctx := context.Background()

		// Deliver initial policy set shortly after Start() is called
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = mockIng.Trigger(ingestor.PolicyUpdate{
				Revision:       "1",
				RevisionNumber: 1,
				Nonce:          "nonce-1",
				Collector:      createTestCollectorWithPolicies("p1"),
			})
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

		// 2. Deliver revision 2: MUST trigger watcher asynchronously
		err = mockIng.Trigger(ingestor.PolicyUpdate{
			Revision:       "2",
			RevisionNumber: 2,
			Nonce:          "nonce-2",
			Collector:      createTestCollectorWithPolicies("p1", "p2"),
		})
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

		// Verify ACKs
		acks := mockIng.Acks()
		require.Len(t, acks, 2)
		assert.Equal(t, "1", acks[0].Revision)
		assert.Nil(t, acks[0].ErrorDetail)
		assert.Equal(t, "2", acks[1].Revision)
		assert.Nil(t, acks[1].ErrorDetail)

		// 3. Shutdown cleanly
		require.NoError(t, p.Shutdown(ctx))
		assert.True(t, mockIng.IsStopped())
	})

	t.Run("Context cancellation during boot tears down ingestor without leaks", func(t *testing.T) {
		mockIng := &mockPolicyIngestor{}
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderIngestorFactory(func(*ParsedURI) (ingestor.PolicyIngestor, error) {
				return mockIng, nil
			}))

		cancelCtx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()

		// Retrieval blocks waiting for policies, then context cancels
		retrieved, err := p.Retrieve(cancelCtx, "googlecontrolplane:file:///var/policies?startup_timeout=10s", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, retrieved)

		// Ingestor must be stopped on context cancellation
		assert.True(t, mockIng.IsStopped(), "Ingestor must be cleanly stopped when caller context is canceled")
	})

	t.Run("Fast startup timeout fallback to base configuration", func(t *testing.T) {
		mockIng := &mockPolicyIngestor{}
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderIngestorFactory(func(*ParsedURI) (ingestor.PolicyIngestor, error) {
				return mockIng, nil
			}))

		watcherCh := make(chan *confmap.ChangeEvent, 10)
		watcher := func(e *confmap.ChangeEvent) {
			watcherCh <- e
		}

		// Inject short timeout of 50ms without delivering policies from mockIng
		start := time.Now()
		retrieved, err := p.Retrieve(context.Background(), "googlecontrolplane:xds://telemetrydirector.googleapis.com:443?startup_timeout=50ms", watcher)
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
		err = mockIng.Trigger(ingestor.PolicyUpdate{
			Revision:       "1",
			RevisionNumber: 1,
			Nonce:          "n1",
			Collector:      createTestCollectorWithPolicies("fallback-p1"),
		})
		require.NoError(t, err)

		select {
		case ev := <-watcherCh:
			require.NotNil(t, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for dynamic reload after base config fallback")
		}

		require.NoError(t, p.Shutdown(context.Background()))
	})

	t.Run("End-to-end integration with real FileIngestor", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "policy.json")

		initialCol := createTestCollectorWithPolicies("e2e-p1")
		initialBytes, err := protojson.Marshal(initialCol)
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

		// Modify file on disk
		updatedCol := createTestCollectorWithPolicies("e2e-p1", "e2e-p2")
		updatedBytes, err := protojson.Marshal(updatedCol)
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

	t.Run("Fail-open: valid policy accepted with NACK for skipped unsupported policy", func(t *testing.T) {
		mockIng := &mockPolicyIngestor{}
		p := newProviderWithOptions(confmap.ProviderSettings{Logger: logger},
			WithProviderIngestorFactory(func(*ParsedURI) (ingestor.PolicyIngestor, error) {
				return mockIng, nil
			}))

		// Bootstrap provider
		go func() {
			time.Sleep(10 * time.Millisecond)
			_ = mockIng.Trigger(ingestor.PolicyUpdate{
				Revision:       "1",
				RevisionNumber: 1,
				Nonce:          "n1",
				Collector:      createTestCollectorWithPolicies("p1"),
			})
		}()

		_, err := p.Retrieve(context.Background(), "googlecontrolplane:file:///test", nil)
		require.NoError(t, err)

		// Send update with a valid policy AND an unsupported policy
		unsupportedPolicy := &v3.TypedExtensionConfig{
			Name: "unknown-policy-id",
			TypedConfig: &anypb.Any{
				TypeUrl: "type.googleapis.com/unsupported.Policy",
				Value:   []byte("data"),
			},
		}
		colWithUnsupported := createTestCollectorWithPolicies("p1-valid")
		colWithUnsupported.Policies = append(colWithUnsupported.Policies, unsupportedPolicy)

		err = mockIng.Trigger(ingestor.PolicyUpdate{
			Revision:       "2",
			RevisionNumber: 2,
			Nonce:          "n2",
			Collector:      colWithUnsupported,
		})
		require.NoError(t, err)

		acks := mockIng.Acks()
		require.Len(t, acks, 2)
		lastAck := acks[1]
		assert.Equal(t, "2", lastAck.Revision)
		assert.Equal(t, "n2", lastAck.Nonce)
		require.NotNil(t, lastAck.ErrorDetail, "Expected NACK with ErrorDetail for skipped policy")
		assert.Contains(t, lastAck.ErrorDetail.Message, "Enforced valid policies with skipped invalid policies")
		assert.Contains(t, lastAck.ErrorDetail.Message, "unknown-policy-id")
	})
}


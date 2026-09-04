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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockInformerClient struct {
	registry               *InformerRegistry
	closeCalled            atomic.Bool
	closeCount             atomic.Int64
	closeErr               error
	closeCalledOutsideLock atomic.Bool

	broadcaster    *PolicyBroadcaster
	statusRegistry *StatusRegistry
	handler        StructuralUpdateHandler
}

func newMockInformerClient(r *InformerRegistry, closeErr error) *mockInformerClient {
	return &mockInformerClient{
		registry:       r,
		closeErr:       closeErr,
		broadcaster:    NewPolicyBroadcaster(),
		statusRegistry: NewStatusRegistry(),
	}
}

func (m *mockInformerClient) Ready() <-chan struct{} {
	return m.broadcaster.Ready()
}

func (m *mockInformerClient) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	return m.broadcaster.List(typeURL)
}

func (m *mockInformerClient) Watch(ctx context.Context, typeURL string) (<-chan PolicyWatchEvent, error) {
	return m.broadcaster.Watch(ctx, typeURL)
}

func (m *mockInformerClient) Status() *StatusRegistry {
	return m.statusRegistry
}

func (m *mockInformerClient) RegisterStructuralHandler(handler StructuralUpdateHandler) {
	m.handler = handler
}

func (m *mockInformerClient) Close() error {
	m.closeCalled.Store(true)
	m.closeCount.Add(1)

	// Verify that registry lock is NOT held when Close() is invoked
	if m.registry != nil {
		if m.registry.mu.TryLock() {
			m.closeCalledOutsideLock.Store(true)
			m.registry.mu.Unlock()
		} else {
			m.closeCalledOutsideLock.Store(false)
		}
	} else {
		m.closeCalledOutsideLock.Store(true)
	}

	m.broadcaster.Close()
	return m.closeErr
}

func TestInformerRegistry_RefCounting(t *testing.T) {
	linger := 100 * time.Millisecond
	r := NewInformerRegistry(linger)

	var factoryCalls atomic.Int64
	var mockClient *mockInformerClient

	factory := func() (InformerClient, error) {
		factoryCalls.Add(1)
		mockClient = newMockInformerClient(r, nil)
		return mockClient, nil
	}

	key := CanonicalXdsKey("localhost:8080", "fleet-1", "my-project", "authority", "ca.pem", "cert.pem", "key.pem", true)

	// 1. Acquire instance A sets refCount = 1
	h1, err := r.Acquire(key, factory)
	require.NoError(t, err)
	require.NotNil(t, h1)
	assert.Equal(t, int64(1), factoryCalls.Load())
	assert.Equal(t, 1, r.RefCount(key))

	// 2. Concurrent Acquire calls return the same client without deadlocks or races
	const concurrentAcquires = 20
	var wg sync.WaitGroup
	handles := make([]*ClientHandle, concurrentAcquires)

	for i := 0; i < concurrentAcquires; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			h, err := r.Acquire(key, factory)
			assert.NoError(t, err)
			assert.Same(t, mockClient, h.Informer)
			handles[idx] = h
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(1), factoryCalls.Load(), "factory must be called exactly once")
	assert.Equal(t, 1+concurrentAcquires, r.RefCount(key))

	// Release concurrent handles
	for _, h := range handles {
		require.NoError(t, h.Release())
	}
	assert.Equal(t, 1, r.RefCount(key))

	// 3. Verify handle.Release() is idempotent (calling multiple times does not underflow)
	require.NoError(t, h1.Release())
	assert.Equal(t, 0, r.RefCount(key))
	require.NoError(t, h1.Release())
	require.NoError(t, h1.Release())
	assert.Equal(t, 0, r.RefCount(key), "multiple Release() calls must not cause underflow")

	// 4. Verify linger timer absorbs transient drops (re-acquiring within 50ms does not close connection)
	time.Sleep(30 * time.Millisecond)
	assert.False(t, mockClient.closeCalled.Load(), "client must not be closed while lingering")

	h2, err := r.Acquire(key, factory)
	require.NoError(t, err)
	assert.Equal(t, int64(1), factoryCalls.Load(), "re-acquire during linger must reuse client without factory call")
	assert.Equal(t, 1, r.RefCount(key))
	assert.False(t, mockClient.closeCalled.Load())

	// Now release and let linger window elapse
	require.NoError(t, h2.Release())
	assert.Equal(t, 0, r.RefCount(key))

	time.Sleep(150 * time.Millisecond)
	assert.True(t, mockClient.closeCalled.Load(), "client must be closed after linger duration elapses")
	assert.False(t, r.HasEntry(key), "entry must be removed after linger timer fires")

	// 5. Verify Close() was called strictly outside r.mu.Lock()
	assert.True(t, mockClient.closeCalledOutsideLock.Load(), "Close() must be invoked outside registry lock")
}

func TestInformerRegistry_FactoryFailure(t *testing.T) {
	r := NewInformerRegistry(0)
	key := "failure-key"

	expectedErr := errors.New("connection refused by remote host")
	failFactory := func() (InformerClient, error) {
		return nil, expectedErr
	}

	h, err := r.Acquire(key, failFactory)
	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, h)
	assert.False(t, r.HasEntry(key), "failed acquisition must not insert entry into registry")

	// Subsequent Acquire succeeds when factory succeeds
	successMock := newMockInformerClient(r, nil)
	successFactory := func() (InformerClient, error) {
		return successMock, nil
	}

	h2, err := r.Acquire(key, successFactory)
	require.NoError(t, err)
	require.NotNil(t, h2)
	assert.Equal(t, 1, r.RefCount(key))

	require.NoError(t, h2.Release())
	assert.True(t, successMock.closeCalled.Load(), "immediate closure with lingerDur == 0")
	assert.False(t, r.HasEntry(key))
}

func TestInformerRegistry_Shutdown(t *testing.T) {
	r := NewInformerRegistry(10 * time.Second)

	client1 := newMockInformerClient(r, nil)
	client2 := newMockInformerClient(r, nil)
	errClient := errors.New("error on client 3 close")
	client3 := newMockInformerClient(r, errClient)

	h1, err := r.Acquire("key1", func() (InformerClient, error) { return client1, nil })
	require.NoError(t, err)
	_ = h1

	// Acquire and release key2 so it has an active linger timer
	h2, err := r.Acquire("key2", func() (InformerClient, error) { return client2, nil })
	require.NoError(t, err)
	require.NoError(t, h2.Release())
	assert.Equal(t, 0, r.RefCount("key2"))

	h3, err := r.Acquire("key3", func() (InformerClient, error) { return client3, nil })
	require.NoError(t, err)
	_ = h3

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	shutdownErr := r.Shutdown(ctx)
	require.Error(t, shutdownErr)
	assert.True(t, errors.Is(shutdownErr, errClient), "shutdown must return aggregated client closure errors")

	assert.True(t, client1.closeCalled.Load(), "client1 must be closed")
	assert.True(t, client2.closeCalled.Load(), "client2 linger timer must be aborted and client closed")
	assert.True(t, client3.closeCalled.Load(), "client3 must be closed")
	assert.Equal(t, 0, r.EntryCount(), "all entries must be flushed on shutdown")
}

func TestCanonicalKeys(t *testing.T) {
	k1 := CanonicalXdsKey("  Endpoint.com:443  ", "fleet1", "proj-a", "auth", "ca.pem", "cert.pem", "key.pem", false)
	k2 := CanonicalXdsKey("endpoint.com:443", "fleet1", "proj-a", "auth", "ca.pem", "cert.pem", "key.pem", false)
	assert.Equal(t, k1, k2, "CanonicalXdsKey must normalize whitespace and case")

	kDiffProject := CanonicalXdsKey("endpoint.com:443", "fleet1", "proj-b", "auth", "ca.pem", "cert.pem", "key.pem", false)
	assert.NotEqual(t, k1, kDiffProject, "different project IDs must produce distinct canonical keys")

	fKey, err := CanonicalFileKey("  /etc/otelcol/../otelcol/policies  ")
	require.NoError(t, err)
	assert.Equal(t, "file:///etc/otelcol/policies", fKey)
}

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
	"fmt"
	"testing"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPolicyInformer_Backpressure(t *testing.T) {
	broadcaster := NewPolicyBroadcaster()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a slow subscriber that does not read from channel
	ch, err := broadcaster.Watch(ctx, "")
	require.NoError(t, err)

	// Read the initial EventResync event emitted upon subscription
	initialEvent := <-ch
	assert.Equal(t, EventResync, initialEvent.Type)

	// 2. Flood the informer with 200 events
	const floodCount = 200
	doneCh := make(chan struct{})
	go func() {
		for i := 0; i < floodCount; i++ {
			broadcaster.Broadcast(PolicyWatchEvent{
				Type:     EventAdded,
				PolicyID: fmt.Sprintf("flood-policy-%d", i),
				TypeURL:  "type.googleapis.com/test.Policy",
			})
		}
		close(doneCh)
	}()

	// 3. Verify broadcaster does not hang
	select {
	case <-doneCh:
		// Succeeded without deadlock or blocking
	case <-time.After(3 * time.Second):
		t.Fatal("broadcaster hung while broadcasting to full subscriber channel")
	}

	// 4. Verify the subscriber receives EventResync due to buffer overflow
	var receivedResync bool
	drainTimeout := time.After(2 * time.Second)

readLoop:
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				break readLoop
			}
			if ev.Type == EventResync {
				receivedResync = true
			}
		case <-drainTimeout:
			break readLoop
		default:
			break readLoop
		}
	}

	assert.True(t, receivedResync, "slow subscriber must receive EventResync when buffer overflows")
}

func TestPolicyInformer_WatchContextCancellation(t *testing.T) {
	broadcaster := NewPolicyBroadcaster()

	subCtx, cancel := context.WithCancel(context.Background())
	ch, err := broadcaster.Watch(subCtx, "")
	require.NoError(t, err)

	assert.Equal(t, 1, broadcaster.SubscriberCount())

	// Read initial resync
	ev := <-ch
	assert.Equal(t, EventResync, ev.Type)

	// Broadcast an event
	broadcaster.Broadcast(PolicyWatchEvent{
		Type:     EventAdded,
		PolicyID: "p1",
	})

	// Cancel subscriber context
	cancel()

	// Verify channel yields buffered event and closes promptly without goroutine leak or consumer data loss
	channelClosed := make(chan struct{})
	var received []PolicyWatchEvent
	go func() {
		for e := range ch {
			received = append(received, e)
		}
		close(channelClosed)
	}()

	select {
	case <-channelClosed:
		// channel closed cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber channel was not closed after context cancellation")
	}

	assert.Len(t, received, 1, "buffered event must NOT be stolen or discarded by cancellation cleanup")
	assert.Equal(t, "p1", received[0].PolicyID)

	// Verify subscriber is unregistered
	require.Eventually(t, func() bool {
		return broadcaster.SubscriberCount() == 0
	}, 1*time.Second, 10*time.Millisecond, "subscriber must be unregistered upon cancellation")
}

func TestPolicyInformer_CloseUnblocksReady(t *testing.T) {
	broadcaster := NewPolicyBroadcaster()
	ready := broadcaster.Ready()

	select {
	case <-ready:
		t.Fatal("Ready() should not be closed initially")
	default:
	}

	broadcaster.Close()

	select {
	case <-ready:
		// Passed: Close() must unblock Ready() waiters
	case <-time.After(1 * time.Second):
		t.Fatal("Close() did not unblock Ready() waiter")
	}
}

func TestPolicyInformer_ListAndDiff(t *testing.T) {
	broadcaster := NewPolicyBroadcaster()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := broadcaster.Watch(ctx, "")
	require.NoError(t, err)

	initEv := <-ch
	assert.Equal(t, EventResync, initEv.Type)

	dummyConfig, err := anypb.New(&v3.TypedExtensionConfig{Name: "inner"})
	require.NoError(t, err)

	p1 := &v3.TypedExtensionConfig{
		Name:        "p1",
		TypedConfig: dummyConfig,
	}
	p2 := &v3.TypedExtensionConfig{
		Name:        "p2",
		TypedConfig: dummyConfig,
	}

	// Initial policies: [p1, p2]
	added, modified, deleted := broadcaster.UpdatePolicies([]*v3.TypedExtensionConfig{p1, p2})
	assert.Len(t, added, 2)
	assert.Empty(t, modified)
	assert.Empty(t, deleted)

	// List check
	list, err := broadcaster.List("")
	require.NoError(t, err)
	assert.Len(t, list, 2)

	// Ready check
	select {
	case <-broadcaster.Ready():
		// Ready channel should be closed
	default:
		t.Fatal("broadcaster should be ready after UpdatePolicies")
	}
}

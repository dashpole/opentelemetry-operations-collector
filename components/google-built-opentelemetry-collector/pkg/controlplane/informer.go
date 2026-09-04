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
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/protobuf/proto"
)

// SubscriberBufferSize is the bounded ring buffer size for each subscriber watch channel.
const SubscriberBufferSize = 100

// PolicyEventType represents the lifecycle transition of a policy.
type PolicyEventType int

const (
	EventUnknown PolicyEventType = iota
	EventAdded
	EventModified
	EventDeleted
	EventResync // Emitted upon subscription, reconnection, or buffer overflow to force List() reconciliation
)

// EventType is an alias for PolicyEventType for compatibility across the codebase.
type EventType = PolicyEventType

func (t PolicyEventType) String() string {
	switch t {
	case EventAdded:
		return "EventAdded"
	case EventModified:
		return "EventModified"
	case EventDeleted:
		return "EventDeleted"
	case EventResync:
		return "EventResync"
	default:
		return "EventUnknown"
	}
}

// PolicyWatchEvent represents a single event delivered to an Informer subscriber.
type PolicyWatchEvent struct {
	Type     PolicyEventType
	PolicyID string
	TypeURL  string
	// Policy is populated for EventAdded and EventModified.
	// For EventDeleted, Policy is nil and consumers rely on PolicyID and TypeURL.
	// For EventResync, Policy is nil, PolicyID is empty, and consumers MUST call Informer.List() to reconcile state.
	Policy *v3.TypedExtensionConfig
}

// PolicySnapshotUpdate encapsulates a complete revision snapshot delivered by the control plane.
type PolicySnapshotUpdate struct {
	Revision       string
	RevisionNumber int64
	Nonce          string
	Policies       []*v3.TypedExtensionConfig
}

// StructuralUpdateHandler is invoked by transport engines when full policy snapshots arrive,
// allowing confmap.Provider to evaluate topology changes and coordinate ACK/NACK responses.
type StructuralUpdateHandler func(ctx context.Context, update PolicySnapshotUpdate) error

// PolicyInformer provides read access and change notifications for active policies.
type PolicyInformer interface {
	// Ready returns a channel that is closed when the informer has completed
	// its initial connection and policy retrieval.
	Ready() <-chan struct{}

	// List returns a snapshot of all active policies matching the given typeURL.
	// If typeURL is empty, all active policies are returned.
	List(typeURL string) ([]*v3.TypedExtensionConfig, error)

	// Watch registers a listener for policy additions, modifications, and deletions.
	// Upon subscription or reconnection, Watch emits an initial EventResync event,
	// prompting the subscriber to populate its state via List() in O(N) time before live delta updates flow.
	// The channel is closed when ctx is canceled or the subscriber is unregistered.
	Watch(ctx context.Context, typeURL string) (<-chan PolicyWatchEvent, error)
}

type subscriber struct {
	ch           chan PolicyWatchEvent
	typeURL      string
	cancel       context.CancelFunc
	slow         atomic.Bool
	resyncQueued atomic.Bool
	closeOnce    sync.Once
}

// PolicyBroadcaster is a thread-safe broadcaster supporting bounded subscriber channels,
// backpressure with resync on overflow, and background context cancellation cleanup.
type PolicyBroadcaster struct {
	subscribersMu sync.RWMutex
	subscribers   map[*subscriber]struct{}

	policiesMu sync.RWMutex
	policies   map[string]*v3.TypedExtensionConfig // policyID -> Policy

	readyCh   chan struct{}
	readyOnce sync.Once
}

// NewPolicyBroadcaster creates an initialized PolicyBroadcaster.
func NewPolicyBroadcaster() *PolicyBroadcaster {
	return &PolicyBroadcaster{
		subscribers: make(map[*subscriber]struct{}),
		policies:    make(map[string]*v3.TypedExtensionConfig),
		readyCh:     make(chan struct{}),
	}
}

// Ready returns a channel that is closed when initial policy retrieval is complete.
func (b *PolicyBroadcaster) Ready() <-chan struct{} {
	return b.readyCh
}

// MarkReady marks the broadcaster as ready by closing readyCh.
func (b *PolicyBroadcaster) MarkReady() {
	b.readyOnce.Do(func() {
		close(b.readyCh)
	})
}

// IsReady reports whether the broadcaster has completed initial retrieval.
func (b *PolicyBroadcaster) IsReady() bool {
	select {
	case <-b.readyCh:
		return true
	default:
		return false
	}
}

// List returns a snapshot copy of all active policies matching typeURL.
func (b *PolicyBroadcaster) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	b.policiesMu.RLock()
	defer b.policiesMu.RUnlock()

	out := make([]*v3.TypedExtensionConfig, 0, len(b.policies))
	for _, p := range b.policies {
		if p == nil {
			continue
		}
		pTypeURL := ""
		if p.TypedConfig != nil {
			pTypeURL = p.TypedConfig.TypeUrl
		}
		if typeURL == "" || pTypeURL == typeURL {
			out = append(out, proto.Clone(p).(*v3.TypedExtensionConfig))
		}
	}
	return out, nil
}

// Watch registers a listener for policy changes.
// It immediately emits an initial EventResync event.
func (b *PolicyBroadcaster) Watch(ctx context.Context, typeURL string) (<-chan PolicyWatchEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	subCtx, subCancel := context.WithCancel(ctx)
	sub := &subscriber{
		ch:      make(chan PolicyWatchEvent, SubscriberBufferSize),
		typeURL: typeURL,
		cancel:  subCancel,
	}

	// Enqueue initial EventResync event
	sub.ch <- PolicyWatchEvent{Type: EventResync}

	b.subscribersMu.Lock()
	if b.subscribers == nil {
		b.subscribers = make(map[*subscriber]struct{})
	}
	b.subscribers[sub] = struct{}{}
	b.subscribersMu.Unlock()

	// Dedicated cleanup goroutine waits on subCtx.Done()
	go func() {
		<-subCtx.Done()

		b.subscribersMu.Lock()
		delete(b.subscribers, sub)
		b.subscribersMu.Unlock()

		sub.closeOnce.Do(func() {
			close(sub.ch)
		})
	}()

	return sub.ch, nil
}

// Broadcast dispatches an event to all interested subscribers.
// If a subscriber's buffer is full, the oldest event is dropped and EventResync is enqueued.
// If the channel remains blocked, the subscriber is flagged slow and canceled.
func (b *PolicyBroadcaster) Broadcast(event PolicyWatchEvent) {
	b.subscribersMu.RLock()
	defer b.subscribersMu.RUnlock()

	for sub := range b.subscribers {
		// Filter by TypeURL if subscriber specified a filter, unless event is EventResync
		if event.Type != EventResync && sub.typeURL != "" && event.TypeURL != "" && sub.typeURL != event.TypeURL {
			continue
		}

		select {
		case sub.ch <- event:
			if sub.resyncQueued.Load() {
				sub.resyncQueued.Store(false)
			}
		default:
			// Channel buffer is full: drop oldest event to make room for EventResync
			select {
			case <-sub.ch:
			default:
			}
			if sub.resyncQueued.CompareAndSwap(false, true) {
				select {
				case sub.ch <- PolicyWatchEvent{Type: EventResync}:
				default:
					sub.resyncQueued.Store(false)
					sub.slow.Store(true)
					sub.cancel() // Signals the cleanup goroutine immediately
				}
			}
		}
	}
}

// UpdatePolicies updates the internal policy map, marks the broadcaster ready,
// diffs against the existing policies, and broadcasts EventAdded, EventModified, and EventDeleted events.
func (b *PolicyBroadcaster) UpdatePolicies(newPolicies []*v3.TypedExtensionConfig) (added, modified, deleted []PolicyWatchEvent) {
	b.policiesMu.Lock()
	if b.policies == nil {
		b.policies = make(map[string]*v3.TypedExtensionConfig)
	}

	newMap := make(map[string]*v3.TypedExtensionConfig, len(newPolicies))
	for _, p := range newPolicies {
		if p != nil && p.Name != "" {
			newMap[p.Name] = p
		}
	}

	// Detect added and modified in deterministic sorted order
	names := make([]string, 0, len(newMap))
	for name := range newMap {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		p := newMap[name]
		oldP, exists := b.policies[name]
		typeURL := ""
		if p.TypedConfig != nil {
			typeURL = p.TypedConfig.TypeUrl
		}
		if !exists {
			ev := PolicyWatchEvent{
				Type:     EventAdded,
				PolicyID: name,
				TypeURL:  typeURL,
				Policy:   p,
			}
			added = append(added, ev)
		} else if !proto.Equal(p, oldP) {
			ev := PolicyWatchEvent{
				Type:     EventModified,
				PolicyID: name,
				TypeURL:  typeURL,
				Policy:   p,
			}
			modified = append(modified, ev)
		}
	}

	// Detect deleted in deterministic sorted order
	oldNames := make([]string, 0, len(b.policies))
	for name := range b.policies {
		oldNames = append(oldNames, name)
	}
	sort.Strings(oldNames)

	for _, name := range oldNames {
		oldP := b.policies[name]
		if _, exists := newMap[name]; !exists {
			typeURL := ""
			if oldP != nil && oldP.TypedConfig != nil {
				typeURL = oldP.TypedConfig.TypeUrl
			}
			ev := PolicyWatchEvent{
				Type:     EventDeleted,
				PolicyID: name,
				TypeURL:  typeURL,
				Policy:   nil,
			}
			deleted = append(deleted, ev)
		}
	}

	b.policies = newMap
	b.policiesMu.Unlock()

	b.MarkReady()

	for _, ev := range added {
		b.Broadcast(ev)
	}
	for _, ev := range modified {
		b.Broadcast(ev)
	}
	for _, ev := range deleted {
		b.Broadcast(ev)
	}

	return added, modified, deleted
}

// SubscriberCount returns the number of active subscribers (useful for testing).
func (b *PolicyBroadcaster) SubscriberCount() int {
	b.subscribersMu.RLock()
	defer b.subscribersMu.RUnlock()
	return len(b.subscribers)
}

// Close closes all active subscriber channels, unregisters them, and unblocks pending Ready() waiters.
func (b *PolicyBroadcaster) Close() {
	b.MarkReady() // Unblock any pending waiters on <-b.Ready()

	b.subscribersMu.Lock()
	subs := make([]*subscriber, 0, len(b.subscribers))
	for s := range b.subscribers {
		subs = append(subs, s)
	}
	b.subscribers = make(map[*subscriber]struct{})
	b.subscribersMu.Unlock()

	for _, sub := range subs {
		sub.cancel()
		sub.closeOnce.Do(func() {
			close(sub.ch)
		})
	}
}

// IsStructuralPolicy returns true if the typeURL corresponds to a structural pipeline policy
// (e.g. destination exporter or source receiver) that requires collector reload.
func IsStructuralPolicy(typeURL string) bool {
	return strings.Contains(typeURL, "GcpDestinationPolicy") ||
		strings.Contains(typeURL, "OtlpSourcePolicy")
}

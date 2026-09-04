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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	_ "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// DefaultDebounceInterval is the cooldown duration for debouncing filesystem events.
const DefaultDebounceInterval = 50 * time.Millisecond

// Config defines configuration options for the filesystem policy watcher client.
type Config struct {
	Path             string
	DebounceInterval time.Duration
	Logger           *zap.Logger
}

// Client implements controlplane.InformerClient by watching a local file or directory.
type Client struct {
	cfg            Config
	cleanPath      string
	logger         *zap.Logger
	broadcaster    *controlplane.PolicyBroadcaster
	statusRegistry *controlplane.StatusRegistry

	mu                sync.RWMutex
	structuralHandler controlplane.StructuralUpdateHandler
	watcher           *fsnotify.Watcher
	currentRev        int64
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	closed            bool
}

var _ controlplane.InformerClient = (*Client)(nil)

// NewClient creates and starts a new file InformerClient.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.DebounceInterval <= 0 {
		cfg.DebounceInterval = DefaultDebounceInterval
	}

	absPath, err := filepath.Abs(strings.TrimSpace(cfg.Path))
	if err != nil {
		return nil, fmt.Errorf("failed to determine absolute path for %s: %w", cfg.Path, err)
	}
	cleanPath := filepath.Clean(absPath)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	c := &Client{
		cfg:            cfg,
		cleanPath:      cleanPath,
		logger:         cfg.Logger,
		broadcaster:    controlplane.NewPolicyBroadcaster(),
		statusRegistry: controlplane.NewStatusRegistry(),
		watcher:        watcher,
	}

	// Register path with watcher
	if err := watcher.Add(cleanPath); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("failed to add watch on path %s: %w", cleanPath, err)
	}
	if fi, err := os.Stat(cleanPath); err == nil && !fi.IsDir() {
		_ = watcher.Add(filepath.Dir(cleanPath))
	}

	// Initial policy load
	policies, err := c.loadPolicies()
	if err != nil {
		c.logger.Warn("Failed initial policy load from file/dir; watcher will monitor for updates",
			zap.String("path", cleanPath),
			zap.Error(err),
		)
		c.broadcaster.MarkReady()
	} else {
		c.currentRev = 1
		c.broadcaster.UpdatePolicies(policies)
		c.updateStatusRegistry(1, policies)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchLoop(ctx)
	}()

	return c, nil
}

// Ready returns a channel that is closed when initial policy retrieval is complete.
func (c *Client) Ready() <-chan struct{} {
	return c.broadcaster.Ready()
}

// List returns active policies matching typeURL.
func (c *Client) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	return c.broadcaster.List(typeURL)
}

// Watch registers a subscriber for policy events.
func (c *Client) Watch(ctx context.Context, typeURL string) (<-chan controlplane.PolicyWatchEvent, error) {
	return c.broadcaster.Watch(ctx, typeURL)
}

// Status returns the client's StatusRegistry.
func (c *Client) Status() *controlplane.StatusRegistry {
	return c.statusRegistry
}

// RegisterStructuralHandler registers a callback for structural policy changes.
func (c *Client) RegisterStructuralHandler(handler controlplane.StructuralUpdateHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.structuralHandler = handler
}

// Close stops the file watcher and releases resources.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	watcher := c.watcher
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if watcher != nil {
		_ = watcher.Close()
	}

	c.wg.Wait()
	c.broadcaster.Close()
	return nil
}

func (c *Client) watchLoop(ctx context.Context) {
	var debounceTimer *time.Timer
	var debounceCh <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case event, ok := <-c.watcher.Events:
			if !ok {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			}

			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove|fsnotify.Chmod) != 0 {
				if event.Op&(fsnotify.Rename|fsnotify.Remove|fsnotify.Create) != 0 {
					_ = c.watcher.Add(c.cleanPath)
					if fi, err := os.Stat(c.cleanPath); err == nil && !fi.IsDir() {
						_ = c.watcher.Add(filepath.Dir(c.cleanPath))
					}
				}

				if debounceTimer == nil {
					debounceTimer = time.NewTimer(c.cfg.DebounceInterval)
					debounceCh = debounceTimer.C
				} else {
					if !debounceTimer.Stop() {
						select {
						case <-debounceTimer.C:
						default:
						}
					}
					debounceTimer.Reset(c.cfg.DebounceInterval)
				}
			}

		case err, ok := <-c.watcher.Errors:
			if !ok {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			}
			c.logger.Warn("fsnotify watcher encountered error", zap.Error(err))

		case <-debounceCh:
			debounceTimer = nil
			debounceCh = nil

			_ = c.watcher.Add(c.cleanPath)
			if fi, err := os.Stat(c.cleanPath); err == nil && !fi.IsDir() {
				_ = c.watcher.Add(filepath.Dir(c.cleanPath))
			}

			c.triggerUpdate(ctx)
		}
	}
}

func (c *Client) triggerUpdate(ctx context.Context) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.currentRev++
	revNum := c.currentRev
	handler := c.structuralHandler
	c.mu.Unlock()

	policies, err := c.loadPolicies()
	if err != nil {
		c.logger.Error("Failed to reload policy files during dynamic update", zap.Error(err))
		return
	}

	revStr := strconv.FormatInt(revNum, 10)
	if handler != nil {
		update := controlplane.PolicySnapshotUpdate{
			Revision:       revStr,
			RevisionNumber: revNum,
			Nonce:          revStr,
			Policies:       policies,
		}
		if err := handler(ctx, update); err != nil {
			c.logger.Warn("StructuralUpdateHandler returned error on file policy update; aborting", zap.Error(err))
			return
		}
	} else {
		// Standalone mode: warn if structural policies are present without a handler
		for _, p := range policies {
			typeURL := ""
			if p != nil && p.TypedConfig != nil {
				typeURL = p.TypedConfig.TypeUrl
			}
			if controlplane.IsStructuralPolicy(typeURL) {
				c.logger.Warn(fmt.Sprintf("Structural policy %s received but ignored; pipeline reconfiguration requires confmap.Provider", p.Name),
					zap.String("policy_id", p.Name),
					zap.String("type_url", typeURL),
				)
			}
		}
	}

	c.broadcaster.UpdatePolicies(policies)
	c.updateStatusRegistry(revNum, policies)
}

func (c *Client) updateStatusRegistry(revNum int64, policies []*v3.TypedExtensionConfig) {
	statuses := make(map[string]controlplane.PolicyStatusRecord, len(policies))
	for _, p := range policies {
		if p == nil {
			continue
		}
		typeURL := ""
		if p.TypedConfig != nil {
			typeURL = p.TypedConfig.TypeUrl
		}
		statuses[p.Name] = controlplane.PolicyStatusRecord{
			PolicyID: p.Name,
			TypeURL:  typeURL,
			Status:   controlplane.PolicyStatusApplied,
		}
	}
	c.statusRegistry.SetRevisionAndStatuses(revNum, statuses)
}

func (c *Client) loadPolicies() ([]*v3.TypedExtensionConfig, error) {
	fi, err := os.Stat(c.cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to access %s: %w", c.cleanPath, err)
	}

	if !fi.IsDir() {
		return c.loadFromFile(c.cleanPath)
	}

	entries, err := os.ReadDir(c.cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", c.cleanPath, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	var combined []*v3.TypedExtensionConfig
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		childPath := filepath.Join(c.cleanPath, entry.Name())
		childInfo, err := os.Stat(childPath)
		if err != nil {
			c.logger.Warn("Failed to stat directory entry", zap.String("path", childPath), zap.Error(err))
			continue
		}
		if childInfo.IsDir() {
			continue
		}

		pols, err := c.loadFromFile(childPath)
		if err != nil {
			c.logger.Warn("Failed to parse policy file in directory", zap.String("path", childPath), zap.Error(err))
			continue
		}
		combined = append(combined, pols...)
	}

	return combined, nil
}

func (c *Client) loadFromFile(filePath string) ([]*v3.TypedExtensionConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	// 1. Try protojson as TelemetryCollector
	var col xdsv1alpha1.TelemetryCollector
	unmarshalOpts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := unmarshalOpts.Unmarshal(trimmed, &col); err == nil && len(col.Policies) > 0 {
		return col.Policies, nil
	}

	// 2. Try binary protobuf as TelemetryCollector
	var colProto xdsv1alpha1.TelemetryCollector
	if err := proto.Unmarshal(data, &colProto); err == nil && len(colProto.Policies) > 0 {
		return colProto.Policies, nil
	}

	// 3. Try protojson as single TypedExtensionConfig
	var singleExt v3.TypedExtensionConfig
	if err := unmarshalOpts.Unmarshal(trimmed, &singleExt); err == nil && singleExt.TypedConfig != nil {
		return []*v3.TypedExtensionConfig{&singleExt}, nil
	}

	// 4. Try binary protobuf as single TypedExtensionConfig
	var singleProtoExt v3.TypedExtensionConfig
	if err := proto.Unmarshal(data, &singleProtoExt); err == nil && singleProtoExt.TypedConfig != nil {
		return []*v3.TypedExtensionConfig{&singleProtoExt}, nil
	}

	// 5. Fallback: parse as generic JSON if protojson didn't resolve
	var genericDoc struct {
		Policies []struct {
			Name        string          `json:"name"`
			TypedConfig json.RawMessage `json:"typed_config"`
		} `json:"policies"`
		Name        string          `json:"name"`
		TypedConfig json.RawMessage `json:"typed_config"`
	}
	if err := json.Unmarshal(trimmed, &genericDoc); err == nil {
		if genericDoc.Policies != nil {
			pols := make([]*v3.TypedExtensionConfig, 0, len(genericDoc.Policies))
			for _, p := range genericDoc.Policies {
				var tc struct {
					TypeURL string `json:"@type"`
				}
				_ = json.Unmarshal(p.TypedConfig, &tc)
				pols = append(pols, &v3.TypedExtensionConfig{
					Name: p.Name,
					TypedConfig: &anypb.Any{
						TypeUrl: tc.TypeURL,
						Value:   p.TypedConfig,
					},
				})
			}
			return pols, nil
		}
		if genericDoc.Name != "" && len(genericDoc.TypedConfig) > 0 {
			var tc struct {
				TypeURL string `json:"@type"`
			}
			_ = json.Unmarshal(genericDoc.TypedConfig, &tc)
			return []*v3.TypedExtensionConfig{
				{
					Name: genericDoc.Name,
					TypedConfig: &anypb.Any{
						TypeUrl: tc.TypeURL,
						Value:   genericDoc.TypedConfig,
					},
				},
			}, nil
		}
	}

	return nil, fmt.Errorf("failed to parse %s as TelemetryCollector or TypedExtensionConfig", filePath)
}

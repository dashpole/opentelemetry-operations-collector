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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DefaultDebounceDuration is the default cooldown window for debouncing file system events.
const DefaultDebounceDuration = 50 * time.Millisecond

// FileIngestor implements PolicyIngestor by reading policy definitions from local files or
// directories and watching for modifications using fsnotify with debouncing.
type FileIngestor struct {
	path             string
	logger           *zap.Logger
	debounceDuration time.Duration

	mu         sync.Mutex
	onUpdate   func(PolicyUpdate) error
	watcher    *fsnotify.Watcher
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	currentRev int64
	lastAck    PolicyAck
	closed     bool
}

// FileIngestorOption configures a FileIngestor.
type FileIngestorOption func(*FileIngestor)

// WithFileDebounce configures a custom debounce duration for file modifications.
func WithFileDebounce(d time.Duration) FileIngestorOption {
	return func(f *FileIngestor) {
		if d > 0 {
			f.debounceDuration = d
		}
	}
}

// NewFileIngestor creates a new FileIngestor for the specified file or directory path.
func NewFileIngestor(path string, logger *zap.Logger, opts ...FileIngestorOption) *FileIngestor {
	if logger == nil {
		logger = zap.NewNop()
	}
	f := &FileIngestor{
		path:             path,
		logger:           logger,
		debounceDuration: DefaultDebounceDuration,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Start reads initial policy configurations and launches a background fsnotify watcher.
func (f *FileIngestor) Start(ctx context.Context, onUpdate func(PolicyUpdate) error) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return fmt.Errorf("file ingestor is closed")
	}
	f.onUpdate = onUpdate
	f.mu.Unlock()

	// 1. Setup watcher and start watchLoop first (so subsequent edits trigger reload even if initial file is invalid)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	if err := watcher.Add(f.path); err != nil {
		_ = watcher.Close()
		return fmt.Errorf("failed to add watch on path %s: %w", f.path, err)
	}

	// If f.path is a file, also watch parent directory to capture atomic replacements/renames
	if fi, err := os.Stat(f.path); err == nil && !fi.IsDir() {
		dir := filepath.Dir(f.path)
		_ = watcher.Add(dir)
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		_ = watcher.Close()
		return fmt.Errorf("file ingestor is closed")
	}
	f.watcher = watcher
	watchCtx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(1)
	f.mu.Unlock()

	go func() {
		defer f.wg.Done()
		f.watchLoop(watchCtx)
	}()

	// 2. Initial read
	col, err := f.loadCollector()
	if err != nil {
		f.logger.Warn("Failed initial policy read; watcher will monitor for subsequent updates",
			zap.String("path", f.path),
			zap.Error(err))
		return nil
	}

	f.mu.Lock()
	f.currentRev = 1
	revStr := strconv.FormatInt(f.currentRev, 10)
	f.mu.Unlock()

	update := PolicyUpdate{
		Revision:       revStr,
		RevisionNumber: 1,
		Nonce:          revStr,
		Collector:      col,
	}

	if err := onUpdate(update); err != nil {
		f.logger.Warn("Initial policy update handler failed; watcher will monitor for subsequent updates",
			zap.String("path", f.path),
			zap.Error(err))
		return nil
	}

	return nil
}

// watchLoop listens for fsnotify events and debounces them before invoking the update handler.
func (f *FileIngestor) watchLoop(ctx context.Context) {
	var debounceTimer *time.Timer
	var debounceCh <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case event, ok := <-f.watcher.Events:
			if !ok {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			}

			// Relevant events: write, create, rename, remove, chmod
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove|fsnotify.Chmod) != 0 {
				// If a file or symlink was created, renamed, or removed, re-add watch on path
				if event.Op&(fsnotify.Rename|fsnotify.Remove|fsnotify.Create) != 0 {
					_ = f.watcher.Add(f.path)
					if fi, err := os.Stat(f.path); err == nil && !fi.IsDir() {
						_ = f.watcher.Add(filepath.Dir(f.path))
					}
				}

				if debounceTimer == nil {
					debounceTimer = time.NewTimer(f.debounceDuration)
					debounceCh = debounceTimer.C
				} else {
					if !debounceTimer.Stop() {
						select {
						case <-debounceTimer.C:
						default:
						}
					}
					debounceTimer.Reset(f.debounceDuration)
				}
			}

		case err, ok := <-f.watcher.Errors:
			if !ok {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			}
			f.logger.Warn("fsnotify watcher encountered error", zap.Error(err))

		case <-debounceCh:
			debounceTimer = nil
			debounceCh = nil
			// Re-assert watch on path before trigger to ensure newly replaced files/inodes are tracked
			f.mu.Lock()
			w := f.watcher
			f.mu.Unlock()
			if w != nil {
				_ = w.Add(f.path)
				if fi, err := os.Stat(f.path); err == nil && !fi.IsDir() {
					_ = w.Add(filepath.Dir(f.path))
				}
			}
			f.triggerUpdate()
		}
	}
}

func (f *FileIngestor) triggerUpdate() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.currentRev++
	revNum := f.currentRev
	revStr := strconv.FormatInt(revNum, 10)
	onUpdate := f.onUpdate
	f.mu.Unlock()

	col, err := f.loadCollector()
	if err != nil {
		f.logger.Error("Failed to reload policy files during dynamic reload", zap.Error(err))
		return
	}

	if onUpdate != nil {
		if err := onUpdate(PolicyUpdate{
			Revision:       revStr,
			RevisionNumber: revNum,
			Nonce:          revStr,
			Collector:      col,
		}); err != nil {
			f.logger.Warn("Policy update handler returned error during reload", zap.Error(err))
		}
	}
}

// Stop closes the file watcher and waits for background goroutines to complete.
func (f *FileIngestor) Stop(ctx context.Context) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	cancel := f.cancel
	watcher := f.watcher
	f.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if watcher != nil {
		_ = watcher.Close()
	}

	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Acknowledge stores the acknowledgment status from the caller.
func (f *FileIngestor) Acknowledge(ctx context.Context, ack PolicyAck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAck = ack
	return nil
}

// LastAck returns the most recently acknowledged PolicyAck (useful for testing).
func (f *FileIngestor) LastAck() PolicyAck {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAck
}

// CurrentRevision returns the current revision counter (useful for testing).
func (f *FileIngestor) CurrentRevision() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentRev
}

// loadCollector reads policy files from f.path (file or directory) and builds a unified TelemetryCollector.
func (f *FileIngestor) loadCollector() (*xdsv1alpha1.TelemetryCollector, error) {
	fi, err := os.Stat(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to access %s: %w", f.path, err)
	}

	if !fi.IsDir() {
		return f.loadFromFile(f.path)
	}

	entries, err := os.ReadDir(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", f.path, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	combined := &xdsv1alpha1.TelemetryCollector{
		Policies: make([]*v3.TypedExtensionConfig, 0),
	}

	for _, entry := range entries {
		// Skip hidden files and special directories (e.g. Kubernetes ..data symlinks)
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		childPath := filepath.Join(f.path, entry.Name())
		childInfo, err := os.Stat(childPath)
		if err != nil {
			f.logger.Warn("Failed to stat directory entry", zap.String("path", childPath), zap.Error(err))
			continue
		}
		if childInfo.IsDir() {
			continue
		}

		col, err := f.loadFromFile(childPath)
		if err != nil {
			f.logger.Warn("Failed to parse policy file in directory", zap.String("path", childPath), zap.Error(err))
			continue
		}
		if col != nil && len(col.Policies) > 0 {
			combined.Policies = append(combined.Policies, col.Policies...)
		}
	}

	return combined, nil
}

// loadFromFile parses a file as a TelemetryCollector (or a single TypedExtensionConfig) in JSON or protobuf format.
func (f *FileIngestor) loadFromFile(filePath string) (*xdsv1alpha1.TelemetryCollector, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		return &xdsv1alpha1.TelemetryCollector{}, nil
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return &xdsv1alpha1.TelemetryCollector{}, nil
	}

	// 1. Try protojson as TelemetryCollector with non-empty policies
	col := &xdsv1alpha1.TelemetryCollector{}
	unmarshalOpts := protojson.UnmarshalOptions{DiscardUnknown: true}
	errJSONCol := unmarshalOpts.Unmarshal(trimmed, col)
	if errJSONCol == nil && len(col.Policies) > 0 {
		return col, nil
	}

	// 2. Try binary protobuf as TelemetryCollector on original untrimmed data
	colProto := &xdsv1alpha1.TelemetryCollector{}
	if err := proto.Unmarshal(data, colProto); err == nil && len(colProto.Policies) > 0 {
		return colProto, nil
	}

	// 3. Try protojson as a single TypedExtensionConfig
	var singleExt v3.TypedExtensionConfig
	if err := unmarshalOpts.Unmarshal(trimmed, &singleExt); err == nil && singleExt.TypedConfig != nil {
		return &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{&singleExt},
		}, nil
	}

	// 4. Try binary protobuf as a single TypedExtensionConfig on original untrimmed data
	var singleProtoExt v3.TypedExtensionConfig
	if err := proto.Unmarshal(data, &singleProtoExt); err == nil && singleProtoExt.TypedConfig != nil {
		return &xdsv1alpha1.TelemetryCollector{
			Policies: []*v3.TypedExtensionConfig{&singleProtoExt},
		}, nil
	}

	// 5. If it was a valid JSON TelemetryCollector with 0 policies (e.g. {"policies": []}), return empty collector
	if errJSONCol == nil {
		return col, nil
	}

	return nil, fmt.Errorf("failed to parse %s as TelemetryCollector or TypedExtensionConfig", filePath)
}

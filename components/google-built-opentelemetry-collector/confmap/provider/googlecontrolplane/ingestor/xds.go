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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Canonical TypeURL for TelemetryCollector xDS resources.
const TelemetryCollectorTypeURL = "type.googleapis.com/google.telemetry.xds.v1alpha1.TelemetryCollector"

// Default backoff settings for xDS stream reconnection.
const (
	DefaultInitialBackoff = 50 * time.Millisecond
	DefaultMaxBackoff     = 5 * time.Second
)

// XdsIngestorConfig defines the configuration options for XdsIngestor.
type XdsIngestorConfig struct {
	Endpoint          string
	CollectorID       string
	FleetID           string
	Project           string
	SupportedTypeURLs []string
	DialOptions       []grpc.DialOption
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	Logger            *zap.Logger
}

// XdsIngestor implements PolicyIngestor for xDS v3 (TelemetryDiscoveryService) streaming.
type XdsIngestor struct {
	cfg      XdsIngestorConfig
	logger   *zap.Logger
	node     *envoycorev3.Node
	onUpdate func(PolicyUpdate) error

	mu               sync.Mutex
	conn             *grpc.ClientConn
	stream           grpc.BidiStreamingClient[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]
	lastAckedVersion string
	lastNonce        string
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	closed           bool
}

// NewXdsIngestor creates an XdsIngestor with populated Node capability negotiation.
func NewXdsIngestor(cfg XdsIngestorConfig) *XdsIngestor {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = DefaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}

	// De-duplicate and sort supported TypeURLs for deterministic capability advertisement
	urls := make([]string, 0, len(cfg.SupportedTypeURLs))
	seen := make(map[string]struct{}, len(cfg.SupportedTypeURLs))
	for _, u := range cfg.SupportedTypeURLs {
		if _, exists := seen[u]; !exists && u != "" {
			seen[u] = struct{}{}
			urls = append(urls, u)
		}
	}
	sort.Strings(urls)

	extensions := make([]*envoycorev3.Extension, 0, len(urls))
	for _, u := range urls {
		extensions = append(extensions, &envoycorev3.Extension{
			Name:     u,
			Category: "telemetry.policy",
			TypeUrls: []string{u},
		})
	}

	node := &envoycorev3.Node{
		Id:         cfg.CollectorID,
		Cluster:    cfg.FleetID,
		Extensions: extensions,
	}

	return &XdsIngestor{
		cfg:    cfg,
		logger: cfg.Logger,
		node:   node,
	}
}

// Start initiates the background streaming connection to the xDS management server.
func (x *XdsIngestor) Start(ctx context.Context, onUpdate func(PolicyUpdate) error) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return fmt.Errorf("xds ingestor is closed")
	}

	x.onUpdate = onUpdate

	runCtx, cancel := context.WithCancel(ctx)
	x.cancel = cancel

	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		x.streamLoop(runCtx)
	}()

	return nil
}

// streamLoop runs the reconnect loop with exponential backoff.
func (x *XdsIngestor) streamLoop(ctx context.Context) {
	backoff := x.cfg.InitialBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := x.connectAndStream(ctx, func() {
			backoff = x.cfg.InitialBackoff
		})
		if ctx.Err() != nil {
			return
		}

		x.logger.Warn("xDS stream disconnected; backing off before reconnect",
			zap.Duration("backoff", backoff),
			zap.Error(err))

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > x.cfg.MaxBackoff {
				backoff = x.cfg.MaxBackoff
			}
		case <-ctx.Done():
			return
		}
	}
}

// connectAndStream establishes a single gRPC streaming session and processes incoming responses.
func (x *XdsIngestor) connectAndStream(ctx context.Context, onConnected func()) error {
	dialOpts := x.cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conn, err := grpc.NewClient(x.cfg.Endpoint, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial xDS endpoint %s: %w", x.cfg.Endpoint, err)
	}
	defer conn.Close()

	client := xdsv1alpha1.NewTelemetryDiscoveryServiceClient(conn)
	stream, err := client.StreamTelemetryCollectors(ctx)
	if err != nil {
		return fmt.Errorf("failed to open TelemetryCollector stream: %w", err)
	}

	x.mu.Lock()
	x.conn = conn
	x.stream = stream
	lastAckedVersion := x.lastAckedVersion
	lastNonce := x.lastNonce
	x.mu.Unlock()

	defer func() {
		x.mu.Lock()
		x.stream = nil
		x.conn = nil
		x.mu.Unlock()
	}()

	// Send initial DiscoveryRequest advertising capabilities and preserved version/nonce
	initReq := &discoveryv3.DiscoveryRequest{
		Node:          x.node,
		TypeUrl:       TelemetryCollectorTypeURL,
		VersionInfo:   lastAckedVersion,
		ResponseNonce: lastNonce,
	}

	x.mu.Lock()
	err = stream.Send(initReq)
	x.mu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send initial xDS discovery request: %w", err)
	}

	// Read loop
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("xDS stream receive error: %w", err)
		}

		if onConnected != nil {
			onConnected()
			onConnected = nil
		}

		col := &xdsv1alpha1.TelemetryCollector{
			Policies: make([]*envoycorev3.TypedExtensionConfig, 0),
		}

		for _, res := range resp.Resources {
			var item xdsv1alpha1.TelemetryCollector
			if err := anypb.UnmarshalTo(res, &item, proto.UnmarshalOptions{DiscardUnknown: true}); err == nil {
				col.Policies = append(col.Policies, item.Policies...)
			} else if res.TypeUrl == TelemetryCollectorTypeURL || strings.HasSuffix(res.TypeUrl, "TelemetryCollector") {
				if err := proto.Unmarshal(res.Value, &item); err == nil {
					col.Policies = append(col.Policies, item.Policies...)
				}
			}
		}

		revNum, _ := strconv.ParseInt(resp.VersionInfo, 10, 64)
		update := PolicyUpdate{
			Revision:       resp.VersionInfo,
			RevisionNumber: revNum,
			Nonce:          resp.Nonce,
			Collector:      col,
		}

		x.mu.Lock()
		onUpdate := x.onUpdate
		x.mu.Unlock()

		if onUpdate != nil {
			if err := onUpdate(update); err != nil {
				x.logger.Warn("Policy update callback returned error", zap.Error(err))
			}
		}
	}
}

// Acknowledge sends an ACK or NACK DiscoveryRequest over the active xDS stream.
func (x *XdsIngestor) Acknowledge(ctx context.Context, ack PolicyAck) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return fmt.Errorf("xds ingestor is closed")
	}

	var versionInfo string
	if ack.ErrorDetail == nil {
		// ACK: advance last acknowledged version
		x.lastAckedVersion = ack.Revision
		x.lastNonce = ack.Nonce
		versionInfo = ack.Revision
	} else {
		// NACK: retain previous version_info, update response_nonce to rejected response nonce
		x.lastNonce = ack.Nonce
		versionInfo = x.lastAckedVersion
	}

	req := &discoveryv3.DiscoveryRequest{
		Node:          x.node,
		TypeUrl:       TelemetryCollectorTypeURL,
		VersionInfo:   versionInfo,
		ResponseNonce: ack.Nonce,
		ErrorDetail:   ack.ErrorDetail,
	}

	if x.stream != nil {
		if err := x.stream.Send(req); err != nil {
			x.logger.Warn("Failed to send acknowledgment over xDS stream", zap.Error(err))
			return err
		}
	}
	return nil
}

// Stop closes the client connection and cancels background streaming goroutines.
func (x *XdsIngestor) Stop(ctx context.Context) error {
	x.mu.Lock()
	if x.closed {
		x.mu.Unlock()
		return nil
	}
	x.closed = true
	cancel := x.cancel
	conn := x.conn
	x.stream = nil
	x.conn = nil
	x.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}

	done := make(chan struct{})
	go func() {
		x.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LastAckedVersion returns the last acknowledged version string (useful for verification).
func (x *XdsIngestor) LastAckedVersion() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.lastAckedVersion
}

// LastNonce returns the last recorded nonce (useful for verification).
func (x *XdsIngestor) LastNonce() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.lastNonce
}

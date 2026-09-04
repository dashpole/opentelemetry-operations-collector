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

package xds

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// TelemetryCollectorTypeURL is the canonical TypeURL for TelemetryCollector resources.
const TelemetryCollectorTypeURL = "type.googleapis.com/google.telemetry.xds.v1alpha1.TelemetryCollector"

// Default backoff settings for xDS stream reconnection.
const (
	DefaultInitialBackoff = 50 * time.Millisecond
	DefaultMaxBackoff     = 5 * time.Second
)

// Config defines connection and capability options for the xDS Client.
type Config struct {
	Endpoint          string
	CollectorID       string
	FleetID           string
	Project           string
	ServerAuthority   string
	CACertPath        string
	ClientCertPath    string
	ClientKeyPath     string
	Insecure          bool
	SupportedTypeURLs []string
	DialOptions       []grpc.DialOption
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	Logger            *zap.Logger
}

// Client implements controlplane.InformerClient for xDS v3 (TelemetryDiscoveryService) streaming.
type Client struct {
	cfg            Config
	logger         *zap.Logger
	node           *envoycorev3.Node
	broadcaster    *controlplane.PolicyBroadcaster
	statusRegistry *controlplane.StatusRegistry

	mu                sync.RWMutex
	structuralHandler controlplane.StructuralUpdateHandler
	lastAckedVersion  string
	lastNonce         string
	lastNackError     error

	streamMu sync.Mutex
	conn     *grpc.ClientConn
	stream   grpc.BidiStreamingClient[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]

	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

var _ controlplane.InformerClient = (*Client)(nil)

// NewClient creates and starts a new xDS InformerClient.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = DefaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}

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

	client := &Client{
		cfg:            cfg,
		logger:         cfg.Logger,
		node:           node,
		broadcaster:    controlplane.NewPolicyBroadcaster(),
		statusRegistry: controlplane.NewStatusRegistry(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client.cancel = cancel

	client.wg.Add(1)
	go func() {
		defer client.wg.Done()
		client.streamLoop(ctx)
	}()

	return client, nil
}

// Ready returns a channel that is closed when initial policy retrieval is complete.
func (c *Client) Ready() <-chan struct{} {
	return c.broadcaster.Ready()
}

// List returns active policies matching typeURL.
func (c *Client) List(typeURL string) ([]*envoycorev3.TypedExtensionConfig, error) {
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

// Close terminates the xDS streaming client, releases gRPC resources, and closes informers.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	c.streamMu.Lock()
	conn := c.conn
	c.conn = nil
	c.stream = nil
	c.streamMu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	c.wg.Wait()
	c.broadcaster.Close()
	return nil
}

func (c *Client) streamLoop(ctx context.Context) {
	backoff := c.cfg.InitialBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := c.connectAndStream(ctx, func() {
			backoff = c.cfg.InitialBackoff
		})
		if ctx.Err() != nil {
			return
		}

		c.logger.Warn("xDS stream disconnected; backing off before reconnect",
			zap.Duration("backoff", backoff),
			zap.Error(err))

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > c.cfg.MaxBackoff {
				backoff = c.cfg.MaxBackoff
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) buildDialOptions() ([]grpc.DialOption, error) {
	if len(c.cfg.DialOptions) > 0 {
		return c.cfg.DialOptions, nil
	}

	if c.cfg.Insecure {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	tlsConfig := &tls.Config{}
	if c.cfg.ServerAuthority != "" {
		tlsConfig.ServerName = c.cfg.ServerAuthority
	}

	if c.cfg.CACertPath != "" {
		caCert, err := os.ReadFile(c.cfg.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate from %s: %w", c.cfg.CACertPath, err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", c.cfg.CACertPath)
		}
		tlsConfig.RootCAs = caPool
	}

	if c.cfg.ClientCertPath != "" && c.cfg.ClientKeyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(c.cfg.ClientCertPath, c.cfg.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client keypair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	if c.cfg.ServerAuthority != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(c.cfg.ServerAuthority))
	}
	return dialOpts, nil
}

func (c *Client) connectAndStream(ctx context.Context, onConnected func()) error {
	dialOpts, err := c.buildDialOptions()
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(c.cfg.Endpoint, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial xDS endpoint %s: %w", c.cfg.Endpoint, err)
	}
	defer conn.Close()

	client := xdsv1alpha1.NewTelemetryDiscoveryServiceClient(conn)
	stream, err := client.StreamTelemetryCollectors(ctx)
	if err != nil {
		return fmt.Errorf("failed to open TelemetryCollector stream: %w", err)
	}

	c.streamMu.Lock()
	c.conn = conn
	c.stream = stream
	c.streamMu.Unlock()

	defer func() {
		c.streamMu.Lock()
		c.stream = nil
		c.conn = nil
		c.streamMu.Unlock()
	}()

	initReq := &discoveryv3.DiscoveryRequest{
		Node:          c.node,
		TypeUrl:       TelemetryCollectorTypeURL,
		VersionInfo:   c.LastAckedVersion(),
		ResponseNonce: c.LastNonce(),
	}

	if err := c.sendRequest(initReq); err != nil {
		return fmt.Errorf("failed to send initial discovery request: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("xDS stream receive error: %w", err)
		}

		if onConnected != nil {
			onConnected()
			onConnected = nil
		}

		if err := c.ProcessResponse(ctx, resp); err != nil {
			c.logger.Warn("Failed to process xDS DiscoveryResponse", zap.Error(err))
		}
	}
}

// ProcessResponse processes an incoming DiscoveryResponse, enforces the structural pre-validation
// gate, dispatches ACK or NACK, diffs against active policies, and updates the status registry.
func (c *Client) ProcessResponse(ctx context.Context, resp *discoveryv3.DiscoveryResponse) error {
	var allPolicies []*envoycorev3.TypedExtensionConfig
	for _, res := range resp.Resources {
		var col xdsv1alpha1.TelemetryCollector
		if err := anypb.UnmarshalTo(res, &col, proto.UnmarshalOptions{DiscardUnknown: true}); err == nil {
			allPolicies = append(allPolicies, col.Policies...)
		} else if res.TypeUrl == TelemetryCollectorTypeURL || strings.HasSuffix(res.TypeUrl, "TelemetryCollector") {
			if err := proto.Unmarshal(res.Value, &col); err == nil {
				allPolicies = append(allPolicies, col.Policies...)
			}
		} else {
			var single envoycorev3.TypedExtensionConfig
			if err := anypb.UnmarshalTo(res, &single, proto.UnmarshalOptions{DiscardUnknown: true}); err == nil {
				allPolicies = append(allPolicies, &single)
			}
		}
	}

	revNum, _ := strconv.ParseInt(resp.VersionInfo, 10, 64)
	update := controlplane.PolicySnapshotUpdate{
		Revision:       resp.VersionInfo,
		RevisionNumber: revNum,
		Nonce:          resp.Nonce,
		Policies:       allPolicies,
		Statuses:       make(map[string]controlplane.PolicyStatusRecord, len(allPolicies)),
	}

	c.mu.RLock()
	handler := c.structuralHandler
	c.mu.RUnlock()

	if handler != nil {
		if err := handler(ctx, update); err != nil {
			c.logger.Warn("Structural pre-validation failed for xDS revision; sending NACK",
				zap.String("revision", resp.VersionInfo),
				zap.String("nonce", resp.Nonce),
				zap.Error(err),
			)

			c.mu.Lock()
			c.lastNonce = resp.Nonce
			c.lastNackError = err
			c.mu.Unlock()

			nackReq := &discoveryv3.DiscoveryRequest{
				Node:          c.node,
				TypeUrl:       TelemetryCollectorTypeURL,
				VersionInfo:   c.LastAckedVersion(),
				ResponseNonce: resp.Nonce,
				ErrorDetail: &status.Status{
					Code:    int32(codes.InvalidArgument),
					Message: err.Error(),
				},
			}
			_ = c.sendRequest(nackReq)
			// Abort revision without updating R_old, without updating StatusRegistry, without emitting watch events
			return err
		}
	} else {
		// Standalone mode: warn if structural policies are present without a handler
		for _, p := range allPolicies {
			typeURL := ""
			if p != nil && p.TypedConfig != nil {
				typeURL = p.TypedConfig.TypeUrl
			}
			if isStructuralPolicy(typeURL) {
				c.logger.Warn(fmt.Sprintf("Structural policy %s received but ignored; pipeline reconfiguration requires confmap.Provider", p.Name),
					zap.String("policy_id", p.Name),
					zap.String("type_url", typeURL),
				)
			}
		}
	}

	// Structural pre-validation passed: send ACK
	c.mu.Lock()
	c.lastAckedVersion = resp.VersionInfo
	c.lastNonce = resp.Nonce
	c.lastNackError = nil
	c.mu.Unlock()

	ackReq := &discoveryv3.DiscoveryRequest{
		Node:          c.node,
		TypeUrl:       TelemetryCollectorTypeURL,
		VersionInfo:   resp.VersionInfo,
		ResponseNonce: resp.Nonce,
	}
	_ = c.sendRequest(ackReq)

	// Commit & Diff against R_old: broadcasts EventAdded, EventModified, EventDeleted
	c.broadcaster.UpdatePolicies(allPolicies)

	// Atomically update StatusRegistry
	statuses := update.Statuses
	if len(statuses) == 0 {
		statuses = make(map[string]controlplane.PolicyStatusRecord, len(allPolicies))
		for _, p := range allPolicies {
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
	}
	c.statusRegistry.SetRevisionAndStatuses(revNum, statuses)

	return nil
}

func (c *Client) sendRequest(req *discoveryv3.DiscoveryRequest) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if c.stream != nil {
		return c.stream.Send(req)
	}
	return nil
}

func isStructuralPolicy(typeURL string) bool {
	return controlplane.IsStructuralPolicy(typeURL)
}

// LastAckedVersion returns the last acknowledged version info string.
func (c *Client) LastAckedVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAckedVersion
}

// LastNonce returns the last received nonce string.
func (c *Client) LastNonce() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastNonce
}

// LastNackError returns the last error from a rejected revision (useful for tests).
func (c *Client) LastNackError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastNackError
}

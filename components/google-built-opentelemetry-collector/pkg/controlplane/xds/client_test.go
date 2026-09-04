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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockDiscoveryServer struct {
	xdsv1alpha1.UnimplementedTelemetryDiscoveryServiceServer

	reqCh       chan *discoveryv3.DiscoveryRequest
	respCh      chan *discoveryv3.DiscoveryResponse
	streamErrCh chan error
}

func newMockDiscoveryServer() *mockDiscoveryServer {
	return &mockDiscoveryServer{
		reqCh:       make(chan *discoveryv3.DiscoveryRequest, 20),
		respCh:      make(chan *discoveryv3.DiscoveryResponse, 20),
		streamErrCh: make(chan error, 1),
	}
}

func (m *mockDiscoveryServer) StreamTelemetryCollectors(stream grpc.BidiStreamingServer[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]) error {
	errCh := make(chan error, 2)

	// Sender goroutine
	go func() {
		for {
			select {
			case resp, ok := <-m.respCh:
				if !ok {
					errCh <- nil
					return
				}
				if err := stream.Send(resp); err != nil {
					errCh <- err
					return
				}
			case err := <-m.streamErrCh:
				if err != nil {
					errCh <- err
					return
				}
			case <-stream.Context().Done():
				errCh <- stream.Context().Err()
				return
			}
		}
	}()

	// Receiver goroutine
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			m.reqCh <- req
		}
	}()

	return <-errCh
}

func startMockServer(t *testing.T, srv *mockDiscoveryServer) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	xdsv1alpha1.RegisterTelemetryDiscoveryServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return lis.Addr().String(), cleanup
}

func TestXdsClient_SotWDiffing(t *testing.T) {
	logger := zaptest.NewLogger(t)
	srv := newMockDiscoveryServer()
	endpoint, cleanup := startMockServer(t, srv)
	defer cleanup()

	supportedURLs := []string{
		"type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
		"type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
		"type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy",
	}

	cfg := Config{
		Endpoint:          endpoint,
		CollectorID:       "test-collector-id",
		FleetID:           "test-fleet-id",
		Project:           "test-project-id",
		SupportedTypeURLs: supportedURLs,
		DialOptions:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		Logger:            logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	// Verify initial DiscoveryRequest advertises capabilities
	select {
	case initReq := <-srv.reqCh:
		assert.Equal(t, TelemetryCollectorTypeURL, initReq.TypeUrl)
		assert.Equal(t, "test-collector-id", initReq.Node.Id)
		assert.Equal(t, "test-fleet-id", initReq.Node.Cluster)
		assert.Len(t, initReq.Node.Extensions, 3)
		for _, ext := range initReq.Node.Extensions {
			assert.Equal(t, "telemetry.policy", ext.Category)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh, err := client.Watch(ctx, "")
	require.NoError(t, err)

	// Consume initial EventResync
	initEvent := <-watchCh
	assert.Equal(t, controlplane.EventResync, initEvent.Type)

	// Step 1: Push revision 1 with policies [P1, P2]
	p1 := &envoycorev3.TypedExtensionConfig{
		Name: "P1",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Value:   []byte("rule-v1"),
		},
	}
	p2 := &envoycorev3.TypedExtensionConfig{
		Name: "P2",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
			Value:   []byte("rule-v2"),
		},
	}

	col1 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{p1, p2},
	}
	col1Bytes, err := proto.Marshal(col1)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Nonce:       "nonce-1",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources: []*anypb.Any{
			{
				TypeUrl: TelemetryCollectorTypeURL,
				Value:   col1Bytes,
			},
		},
	}

	// Verify client sends ACK for revision 1
	select {
	case ack1 := <-srv.reqCh:
		assert.Equal(t, "1", ack1.VersionInfo)
		assert.Equal(t, "nonce-1", ack1.ResponseNonce)
		assert.Nil(t, ack1.ErrorDetail)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ACK for revision 1")
	}

	// Verify watch received EventAdded for both P1 and P2
	eventsRev1 := make(map[string]controlplane.PolicyWatchEvent)
	for i := 0; i < 2; i++ {
		select {
		case ev := <-watchCh:
			eventsRev1[ev.PolicyID] = ev
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for revision 1 events (got %d)", len(eventsRev1))
		}
	}
	assert.Equal(t, controlplane.EventAdded, eventsRev1["P1"].Type)
	assert.Equal(t, "P1", eventsRev1["P1"].PolicyID)
	assert.Equal(t, p1.TypedConfig.TypeUrl, eventsRev1["P1"].TypeURL)
	assert.NotNil(t, eventsRev1["P1"].Policy)

	assert.Equal(t, controlplane.EventAdded, eventsRev1["P2"].Type)
	assert.Equal(t, "P2", eventsRev1["P2"].PolicyID)
	assert.Equal(t, p2.TypedConfig.TypeUrl, eventsRev1["P2"].TypeURL)
	assert.NotNil(t, eventsRev1["P2"].Policy)

	// Verify StatusRegistry has revision 1 and [P1, P2]
	rev1, snap1 := client.Status().Snapshot()
	assert.Equal(t, int64(1), rev1)
	assert.Len(t, snap1, 2)
	assert.Equal(t, controlplane.PolicyStatusApplied, snap1["P1"].Status)
	assert.Equal(t, controlplane.PolicyStatusApplied, snap1["P2"].Status)

	// Step 2: Push revision 2 with policies [P1_modified, P3] (P2 is absent / deleted)
	p1Modified := &envoycorev3.TypedExtensionConfig{
		Name: "P1",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Value:   []byte("rule-v1-modified"),
		},
	}
	p3 := &envoycorev3.TypedExtensionConfig{
		Name: "P3",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.TraceFilterPolicy",
			Value:   []byte("rule-v3"),
		},
	}

	col2 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{p1Modified, p3},
	}
	col2Bytes, err := proto.Marshal(col2)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "2",
		Nonce:       "nonce-2",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources: []*anypb.Any{
			{
				TypeUrl: TelemetryCollectorTypeURL,
				Value:   col2Bytes,
			},
		},
	}

	// Verify client sends ACK for revision 2
	select {
	case ack2 := <-srv.reqCh:
		assert.Equal(t, "2", ack2.VersionInfo)
		assert.Equal(t, "nonce-2", ack2.ResponseNonce)
		assert.Nil(t, ack2.ErrorDetail)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ACK for revision 2")
	}

	// Verify watch received EventModified for P1, EventAdded for P3, EventDeleted for P2
	eventsRev2 := make(map[string]controlplane.PolicyWatchEvent)
	for i := 0; i < 3; i++ {
		select {
		case ev := <-watchCh:
			eventsRev2[ev.PolicyID] = ev
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for revision 2 events (got %d)", len(eventsRev2))
		}
	}

	// Verify P1 modified
	assert.Equal(t, controlplane.EventModified, eventsRev2["P1"].Type)
	assert.Equal(t, "P1", eventsRev2["P1"].PolicyID)
	assert.Equal(t, []byte("rule-v1-modified"), eventsRev2["P1"].Policy.TypedConfig.Value)

	// Verify P3 added
	assert.Equal(t, controlplane.EventAdded, eventsRev2["P3"].Type)
	assert.Equal(t, "P3", eventsRev2["P3"].PolicyID)
	assert.NotNil(t, eventsRev2["P3"].Policy)

	// Verify P2 deleted: PolicyID = "P2", Policy == nil, TypeURL preserved
	assert.Equal(t, controlplane.EventDeleted, eventsRev2["P2"].Type)
	assert.Equal(t, "P2", eventsRev2["P2"].PolicyID)
	assert.Nil(t, eventsRev2["P2"].Policy, "EventDeleted must have Policy == nil")
	assert.Equal(t, p2.TypedConfig.TypeUrl, eventsRev2["P2"].TypeURL)

	// Verify StatusRegistry has revision 2, [P1, P3], and P2 is pruned
	rev2, snap2 := client.Status().Snapshot()
	assert.Equal(t, int64(2), rev2)
	assert.Len(t, snap2, 2)
	assert.Contains(t, snap2, "P1")
	assert.Contains(t, snap2, "P3")
	assert.NotContains(t, snap2, "P2", "deleted policy P2 must be purged from StatusRegistry")
}

func TestXdsClient_StructuralPreValidationNACK(t *testing.T) {
	logger := zaptest.NewLogger(t)
	srv := newMockDiscoveryServer()
	endpoint, cleanup := startMockServer(t, srv)
	defer cleanup()

	cfg := Config{
		Endpoint:       endpoint,
		CollectorID:    "test-collector-id",
		FleetID:        "test-fleet-id",
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Logger:         logger,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	// Consume initial request
	<-srv.reqCh

	// Register a failing structural handler
	validationErr := errors.New("invalid service pipeline topology")
	client.RegisterStructuralHandler(func(ctx context.Context, update controlplane.PolicySnapshotUpdate) error {
		return validationErr
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh, err := client.Watch(ctx, "")
	require.NoError(t, err)
	<-watchCh // Initial EventResync

	// Push candidate revision 1
	p1 := &envoycorev3.TypedExtensionConfig{
		Name: "P1",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy",
			Value:   []byte("invalid-config"),
		},
	}
	col := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{p1},
	}
	colBytes, err := proto.Marshal(col)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Nonce:       "nonce-fail-1",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources: []*anypb.Any{
			{
				TypeUrl: TelemetryCollectorTypeURL,
				Value:   colBytes,
			},
		},
	}

	// Verify client sends NACK with InvalidArgument code
	select {
	case nack := <-srv.reqCh:
		assert.Equal(t, "", nack.VersionInfo, "version_info must retain previous version on NACK")
		assert.Equal(t, "nonce-fail-1", nack.ResponseNonce)
		require.NotNil(t, nack.ErrorDetail)
		assert.Equal(t, int32(codes.InvalidArgument), nack.ErrorDetail.Code)
		assert.Contains(t, nack.ErrorDetail.Message, "invalid service pipeline topology")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NACK")
	}

	// Verify no watch events emitted on rejected revision
	select {
	case ev := <-watchCh:
		t.Fatalf("unexpected watch event emitted on NACKed revision: %+v", ev)
	case <-time.After(100 * time.Millisecond):
		// No event emitted, as expected
	}

	// Verify StatusRegistry was NOT updated
	rev, snap := client.Status().Snapshot()
	assert.Equal(t, int64(0), rev)
	assert.Empty(t, snap)
}

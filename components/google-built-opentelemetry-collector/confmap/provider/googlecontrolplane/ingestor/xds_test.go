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
	"net"
	"testing"
	"time"

	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	statuspkg "google.golang.org/grpc/status"
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
		reqCh:       make(chan *discoveryv3.DiscoveryRequest, 10),
		respCh:      make(chan *discoveryv3.DiscoveryResponse, 10),
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

func TestXdsIngestor(t *testing.T) {
	logger := zaptest.NewLogger(t)
	srv := newMockDiscoveryServer()
	endpoint, cleanup := startMockServer(t, srv)
	defer cleanup()

	supportedURLs := []string{
		"type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
		"type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
	}

	cfg := XdsIngestorConfig{
		Endpoint:          endpoint,
		CollectorID:       "test-collector-uuid",
		FleetID:           "test-fleet-1234",
		Project:           "my-gcp-project",
		SupportedTypeURLs: supportedURLs,
		DialOptions:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		Logger:            logger,
	}

	ing := NewXdsIngestor(cfg)
	updateCh := make(chan PolicyUpdate, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ing.Start(ctx, func(u PolicyUpdate) error {
		updateCh <- u
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = ing.Stop(context.Background()) }()

	// 1. Verify capability negotiation on initial request
	var initReq *discoveryv3.DiscoveryRequest
	select {
	case initReq = <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	assert.Equal(t, TelemetryCollectorTypeURL, initReq.TypeUrl)
	assert.Equal(t, "", initReq.VersionInfo)
	assert.Equal(t, "", initReq.ResponseNonce)
	require.NotNil(t, initReq.Node)
	assert.Equal(t, "test-collector-uuid", initReq.Node.Id)
	assert.Equal(t, "test-fleet-1234", initReq.Node.Cluster)
	require.Len(t, initReq.Node.Extensions, 2)
	assert.Equal(t, supportedURLs[0], initReq.Node.Extensions[0].Name)
	assert.Equal(t, "telemetry.policy", initReq.Node.Extensions[0].Category)
	assert.Equal(t, supportedURLs[1], initReq.Node.Extensions[1].Name)

	// 2. Server sends DiscoveryResponse (v1)
	col1 := createSampleCollector("policy-xds-1")
	anyCol1, err := anypb.New(col1)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Nonce:       "nonce-1",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources:   []*anypb.Any{anyCol1},
	}

	// Ingestor receives update
	select {
	case u := <-updateCh:
		assert.Equal(t, "1", u.Revision)
		assert.Equal(t, int64(1), u.RevisionNumber)
		assert.Equal(t, "nonce-1", u.Nonce)
		require.Len(t, u.Collector.Policies, 1)
		assert.Equal(t, "policy-xds-1", u.Collector.Policies[0].Name)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for policy update v1")
	}

	// 3. Client ACKs v1
	err = ing.Acknowledge(ctx, PolicyAck{
		Revision:    "1",
		Nonce:       "nonce-1",
		ErrorDetail: nil,
	})
	require.NoError(t, err)

	// Server receives ACK
	var ackReq *discoveryv3.DiscoveryRequest
	select {
	case ackReq = <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ACK DiscoveryRequest")
	}

	assert.Equal(t, "1", ackReq.VersionInfo)
	assert.Equal(t, "nonce-1", ackReq.ResponseNonce)
	assert.Nil(t, ackReq.ErrorDetail)

	// 4. Server sends DiscoveryResponse (v2) with invalid policy
	col2 := createSampleCollector("policy-xds-bad")
	anyCol2, err := anypb.New(col2)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "2",
		Nonce:       "nonce-2",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources:   []*anypb.Any{anyCol2},
	}

	// Ingestor receives update v2
	select {
	case u := <-updateCh:
		assert.Equal(t, "2", u.Revision)
		assert.Equal(t, int64(2), u.RevisionNumber)
		assert.Equal(t, "nonce-2", u.Nonce)
		require.Len(t, u.Collector.Policies, 1)
		assert.Equal(t, "policy-xds-bad", u.Collector.Policies[0].Name)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for policy update v2")
	}

	// 5. Client NACKs v2
	nackErrDetail := &status.Status{
		Code:    int32(codes.InvalidArgument),
		Message: "structural validation failed for policy-xds-bad",
	}
	err = ing.Acknowledge(ctx, PolicyAck{
		Revision:    "2",
		Nonce:       "nonce-2",
		ErrorDetail: nackErrDetail,
	})
	require.NoError(t, err)

	// Server receives NACK
	var nackReq *discoveryv3.DiscoveryRequest
	select {
	case nackReq = <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NACK DiscoveryRequest")
	}

	// VersionInfo MUST remain "1" (the last acknowledged version)
	assert.Equal(t, "1", nackReq.VersionInfo)
	// ResponseNonce MUST be the rejected nonce "nonce-2"
	assert.Equal(t, "nonce-2", nackReq.ResponseNonce)
	require.NotNil(t, nackReq.ErrorDetail)
	assert.Equal(t, int32(codes.InvalidArgument), nackReq.ErrorDetail.Code)
	assert.Equal(t, "structural validation failed for policy-xds-bad", nackReq.ErrorDetail.Message)
}

func TestXdsIngestor_ReconnectBackoff(t *testing.T) {
	logger := zaptest.NewLogger(t)
	srv := newMockDiscoveryServer()
	endpoint, cleanup := startMockServer(t, srv)
	defer cleanup()

	cfg := XdsIngestorConfig{
		Endpoint:          endpoint,
		CollectorID:       "reconnect-collector",
		FleetID:           "reconnect-fleet",
		SupportedTypeURLs: []string{"type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"},
		DialOptions:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        50 * time.Millisecond,
		Logger:            logger,
	}

	ing := NewXdsIngestor(cfg)
	updateCh := make(chan PolicyUpdate, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ing.Start(ctx, func(u PolicyUpdate) error {
		updateCh <- u
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = ing.Stop(context.Background()) }()

	// Wait for initial connection
	select {
	case <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	// Send v1 and ACK it
	col1 := createSampleCollector("policy-reconnect-1")
	anyCol1, err := anypb.New(col1)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "v1-acked",
		Nonce:       "nonce-v1",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources:   []*anypb.Any{anyCol1},
	}

	select {
	case <-updateCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update")
	}

	require.NoError(t, ing.Acknowledge(ctx, PolicyAck{
		Revision:    "v1-acked",
		Nonce:       "nonce-v1",
		ErrorDetail: nil,
	}))

	// Drain ACK from server
	select {
	case <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ACK")
	}

	// Send v2 and NACK it to establish lastNonce = "nonce-v2"
	col2 := createSampleCollector("policy-reconnect-2")
	anyCol2, err := anypb.New(col2)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "v2-rejected",
		Nonce:       "nonce-v2",
		TypeUrl:     TelemetryCollectorTypeURL,
		Resources:   []*anypb.Any{anyCol2},
	}

	select {
	case <-updateCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for v2 update")
	}

	require.NoError(t, ing.Acknowledge(ctx, PolicyAck{
		Revision:    "v2-rejected",
		Nonce:       "nonce-v2",
		ErrorDetail: &status.Status{Code: int32(codes.InvalidArgument), Message: "rejected"},
	}))

	select {
	case <-srv.reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NACK")
	}

	// Simulate server-side abrupt disconnect
	srv.streamErrCh <- statuspkg.Error(codes.Unavailable, "simulated transient network drop")

	// Wait for reconnect: The mock server should receive a new initial DiscoveryRequest
	var reconnectReq *discoveryv3.DiscoveryRequest
	select {
	case reconnectReq = <-srv.reqCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reconnect initial DiscoveryRequest")
	}

	// Verify that the reconnect request preserves the last acknowledged version and last nonce!
	assert.Equal(t, "v1-acked", reconnectReq.VersionInfo, "Reconnect request must preserve last acknowledged version")
	assert.Equal(t, "nonce-v2", reconnectReq.ResponseNonce, "Reconnect request must preserve last response nonce")
	assert.Equal(t, "reconnect-collector", reconnectReq.Node.Id)
	assert.Equal(t, "reconnect-fleet", reconnectReq.Node.Cluster)
}

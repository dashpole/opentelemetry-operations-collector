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

package googlexdspolicy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane/xds"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
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

func TestGoogleXdsPolicyExtension(t *testing.T) {
	ctx := context.Background()

	// 1. Start mock xDS server
	srv := newMockDiscoveryServer()
	endpoint, cleanupSrv := startMockServer(t, srv)
	defer cleanupSrv()

	// 2. Setup observer logger to catch warnings
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	// 3. Configure googlexdspolicy extension
	factory := NewFactory()
	assert.Equal(t, component.MustNewType("googlexdspolicy"), factory.Type())

	supportedURLs := []string{
		"type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
		"type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
	}

	cfg := &Config{
		Endpoint:          endpoint,
		FleetID:           "test-fleet",
		ProjectID:         "test-project",
		CollectorID:       "collector-123",
		Insecure:          true,
		StartupTimeout:    2 * time.Second,
		SupportedTypeURLs: supportedURLs,
	}
	require.NoError(t, cfg.Validate())

	isolatedReg := controlplane.NewInformerRegistry(0)
	defer func() { _ = isolatedReg.Shutdown(ctx) }()

	set := extensiontest.NewNopSettings(typeStr)
	set.Logger = logger

	extAny, err := factory.Create(ctx, set, cfg)
	require.NoError(t, err)
	ext := extAny.(*googleXdsPolicyExtension).WithRegistry(isolatedReg)

	// 4. Start extension
	require.NoError(t, ext.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, ext.Shutdown(ctx))
	}()

	// 5. Verify DiscoveryRequest received and Node.extensions capability advertisement
	var initReq *discoveryv3.DiscoveryRequest
	select {
	case req := <-srv.reqCh:
		initReq = req
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	require.NotNil(t, initReq.Node)
	assert.Equal(t, "collector-123", initReq.Node.Id)
	assert.Equal(t, "test-fleet", initReq.Node.Cluster)

	extNames := make(map[string]bool)
	for _, e := range initReq.Node.Extensions {
		assert.Equal(t, "telemetry.policy", e.Category)
		extNames[e.Name] = true
	}
	assert.True(t, extNames["type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy"])
	assert.True(t, extNames["type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy"])

	// 6. Subscribe to Watch BEFORE sending policies
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watchCh, err := ext.Watch(watchCtx, "")
	require.NoError(t, err)

	// Consume initial EventResync
	resyncEv := <-watchCh
	assert.Equal(t, controlplane.EventResync, resyncEv.Type)

	// 7. Deliver DiscoveryResponse with filter policy AND a structural policy (GcpDestinationPolicy)
	polLog := &envoycorev3.TypedExtensionConfig{
		Name: "log-rule-1",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Value:   []byte(`{"action": "ACTION_DROP"}`),
		},
	}
	polStructural := &envoycorev3.TypedExtensionConfig{
		Name: "dest-policy-1",
		TypedConfig: &anypb.Any{
			TypeUrl: "type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy",
			Value:   []byte(`{"destination": "projects/p1/destinations/d1"}`),
		},
	}

	colResource := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{polLog, polStructural},
	}
	colAny, err := anypb.New(colResource)
	require.NoError(t, err)

	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Nonce:       "nonce-1",
		TypeUrl:     "type.googleapis.com/google.telemetry.xds.v1alpha1.TelemetryCollector",
		Resources:   []*anypb.Any{colAny},
	}

	// 8. Verify Ready() is closed
	select {
	case <-ext.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Informer.Ready()")
	}

	// 9. Verify List() delivers the policies
	policies, err := ext.List("")
	require.NoError(t, err)
	require.Len(t, policies, 2)

	// 10. Verify Watch() receives EventAdded for log-rule-1 and dest-policy-1
	received := make(map[string]controlplane.EventType)
	for i := 0; i < 2; i++ {
		select {
		case ev := <-watchCh:
			received[ev.PolicyID] = ev.Type
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for watch events (received %d)", len(received))
		}
	}
	assert.Equal(t, controlplane.EventAdded, received["log-rule-1"])
	assert.Equal(t, controlplane.EventAdded, received["dest-policy-1"])

	// 11. Verify standalone warning is emitted for dest-policy-1
	require.Eventually(t, func() bool {
		warns := logs.FilterMessageSnippet("Structural policy dest-policy-1 received but ignored; pipeline reconfiguration requires confmap.Provider")
		return warns.Len() > 0
	}, 3*time.Second, 20*time.Millisecond, "expected standalone warning for structural policy")

	// 12. Verify ACK DiscoveryRequest was sent by the client
	select {
	case ackReq := <-srv.reqCh:
		assert.Equal(t, "1", ackReq.VersionInfo)
		assert.Equal(t, "nonce-1", ackReq.ResponseNonce)
		assert.Nil(t, ackReq.ErrorDetail)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for xDS ACK DiscoveryRequest")
	}
}

func TestGoogleXdsPolicy_MetricsGauges(t *testing.T) {
	ctx := context.Background()

	srv := newMockDiscoveryServer()
	endpoint, cleanupSrv := startMockServer(t, srv)
	defer cleanupSrv()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		_ = mp.Shutdown(ctx)
	}()

	set := extensiontest.NewNopSettings(typeStr)
	set.TelemetrySettings.MeterProvider = mp
	set.Logger = zap.NewNop()

	isolatedReg := controlplane.NewInformerRegistry(0)
	defer func() { _ = isolatedReg.Shutdown(ctx) }()

	factory := NewFactory()
	cfg := &Config{
		Endpoint: endpoint,
		Insecure: true,
	}

	extAny, err := factory.Create(ctx, set, cfg)
	require.NoError(t, err)
	ext := extAny.(*googleXdsPolicyExtension).WithRegistry(isolatedReg)

	require.NoError(t, ext.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, ext.Shutdown(ctx))
	}()

	// Wait for connection initial DiscoveryRequest
	<-srv.reqCh

	// Directly update StatusRegistry to Revision 4 with applied and failed policies
	ext.handle.Status.SetRevisionAndStatuses(4, map[string]controlplane.PolicyStatusRecord{
		"policy-applied": {
			PolicyID: "policy-applied",
			TypeURL:  "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy",
			Status:   controlplane.PolicyStatusApplied,
		},
		"policy-failed": {
			PolicyID: "policy-failed",
			TypeURL:  "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy",
			Status:   controlplane.PolicyStatusFailed,
			Error:    "compilation error",
		},
	})

	// Collect metrics and assert
	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	rev, statuses := extractMetricsFromRM(&rm)
	assert.Equal(t, int64(4), rev)
	assert.Len(t, statuses, 2)
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.LogFilterPolicy:applied", statuses["policy-applied"])
	assert.Equal(t, "type.googleapis.com/google.telemetry.policy.v1alpha1.MetricFilterPolicy:failed", statuses["policy-failed"])
}

func extractMetricsFromRM(rm *metricdata.ResourceMetrics) (int64, map[string]string) {
	var rev int64
	statuses := make(map[string]string)

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "telemetry_policy_set_revision":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						rev = dp.Value
					}
				}
			case "telemetry_policy_status":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, dp := range gauge.DataPoints {
						var policyID, policyType, status string
						for _, attr := range dp.Attributes.ToSlice() {
							switch attr.Key {
							case "policy_id":
								policyID = attr.Value.AsString()
							case "policy_type":
								policyType = attr.Value.AsString()
							case "status":
								status = attr.Value.AsString()
							}
						}
						statuses[policyID] = policyType + ":" + status
					}
				}
			}
		}
	}
	return rev, statuses
}

func TestGoogleXdsPolicy_LifecycleAndValidation(t *testing.T) {
	// 1. Config validation tests
	cfgBadTimeout := &Config{
		Endpoint:       "localhost:8080",
		StartupTimeout: -1 * time.Second,
	}
	assert.Error(t, cfgBadTimeout.Validate(), "must reject negative startup_timeout")

	cfgAsymmetricCert := &Config{
		Endpoint:       "localhost:8080",
		ClientCertPath: "/path/to/cert",
	}
	assert.Error(t, cfgAsymmetricCert.Validate(), "must reject asymmetric cert without key")

	cfgAsymmetricKey := &Config{
		Endpoint:      "localhost:8080",
		ClientKeyPath: "/path/to/key",
	}
	assert.Error(t, cfgAsymmetricKey.Validate(), "must reject asymmetric key without cert")

	// 2. Pre-start Ready() and double Start() tests
	srv := newMockDiscoveryServer()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := grpc.NewServer()
	xdsv1alpha1.RegisterTelemetryDiscoveryServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Endpoint = lis.Addr().String()
	cfg.FleetID = "fleet-lifecycle"
	cfg.Insecure = true

	set := extensiontest.NewNopSettings(component.MustNewType("googlexdspolicy"))
	ext, err := factory.Create(context.Background(), set, cfg)
	require.NoError(t, err)

	xdsExt := ext.(*googleXdsPolicyExtension).WithDialOptions(grpc.WithInsecure())
	isolatedReg := controlplane.NewInformerRegistry(0)
	xdsExt = xdsExt.WithRegistry(isolatedReg)

	// Call Ready() BEFORE Start()
	readyCh := xdsExt.Ready()
	select {
	case <-readyCh:
		t.Fatal("Ready() must not be closed before Start()")
	default:
	}

	// Start extension
	err = xdsExt.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Double start should return error
	errDouble := xdsExt.Start(context.Background(), componenttest.NewNopHost())
	assert.Error(t, errDouble)
	assert.Contains(t, errDouble.Error(), "extension already started")

	// Deliver discovery response to trigger readiness
	srv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "rev-1",
		TypeUrl:     xds.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
		Resources:   []*anypb.Any{},
	}

	select {
	case <-readyCh:
		// Passed: Pre-start Ready() unblocked upon discovery response
	case <-time.After(3 * time.Second):
		t.Fatal("pre-start Ready() was not unblocked after discovery response")
	}

	// Shutdown
	err = xdsExt.Shutdown(context.Background())
	require.NoError(t, err)

	// List and Watch post-shutdown should return "extension not running"
	_, errList := xdsExt.List("")
	assert.Error(t, errList)
	assert.Contains(t, errList.Error(), "extension not running")

	_, errWatch := xdsExt.Watch(context.Background(), "")
	assert.Error(t, errWatch)
	assert.Contains(t, errWatch.Error(), "extension not running")
}

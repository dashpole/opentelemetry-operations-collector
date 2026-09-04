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

package googlecontrolplane_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/ingestor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
)

// TestMultiCycleReloadRefCounting verifies that across 5 consecutive structural reloads:
// 1. The underlying gRPC xDS connection is never dropped.
// 2. RefCount toggles between 2 and 1 (or 3 during reload overlap), never reaching 0 prematurely.
// 3. When the collector shuts down, RefCount reaches 0 and resources are cleanly released.
func TestMultiCycleReloadRefCounting(t *testing.T) {
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	xdsSrv := newMockDiscoveryServer()
	xdsEndpoint, stopXds := startMockDiscoveryServer(t, xdsSrv)
	defer stopXds()

	recvGrpcPort := getFreePort(t)
	recvHttpPort := getFreePort(t)
	baseConfigFile := writeBaseConfigFile(t, dest.port, recvGrpcPort, recvHttpPort)

	sharedReg := controlplane.NewInformerRegistry(5 * time.Second)

	// Send Initial Revision R1
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}
	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}
	colR1 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{destExt, srcExt},
	}
	colAnyR1, err := anypb.New(colR1)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Resources:   []*anypb.Any{colAnyR1},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
	}

	driverReg := driver.NewDefaultRegistry()
	providerFactory := googlecontrolplane.NewFactoryWithOptions(
		googlecontrolplane.WithProviderDriverRegistry(driverReg),
		googlecontrolplane.WithProviderInformerRegistry(sharedReg),
	)

	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactoriesWithInformerRegistry(nil, sharedReg),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:xds://%s?fleet_id=test-fleet&base_config=%s&startup_timeout=10s", xdsEndpoint, baseConfigFile),
				},
				ProviderFactories: []confmap.ProviderFactory{
					providerFactory,
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()

	require.Eventually(t, func() bool {
		select {
		case err := <-colErrCh:
			t.Fatalf("collector exited early: %v", err)
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Drain initial DiscoveryRequest and R1 ACK
	select {
	case req := <-xdsSrv.reqCh:
		assert.Equal(t, "", req.VersionInfo)
		assert.Equal(t, "", req.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	select {
	case ack := <-xdsSrv.reqCh:
		assert.Equal(t, "1", ack.VersionInfo)
		assert.Equal(t, "nonce-1", ack.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for R1 ACK")
	}

	// Canonical Key for connection
	canonicalKey := controlplane.CanonicalXdsKey(xdsEndpoint, "test-fleet", "", "", "", "", "", true)
	require.True(t, sharedReg.HasEntry(canonicalKey), "InformerRegistry must have entry for xDS endpoint")
	assert.Equal(t, 2, sharedReg.RefCount(canonicalKey), "Initial refCount must be 2 (1 Provider, 1 googlexdspolicy)")

	// Execute 5 consecutive structural reloads
	for i := 1; i <= 5; i++ {
		revStr := fmt.Sprintf("%d", i+1)
		nonceStr := fmt.Sprintf("nonce-%d", i+1)

		// Add a new distinct structural source policy to force configuration compilation and reload
		extraSrc := &envoycorev3.TypedExtensionConfig{
			Name: fmt.Sprintf("filelog-src-%d", i),
			TypedConfig: &anypb.Any{
				TypeUrl: driver.TypeURLFilelogSource,
			},
		}

		policies := []*envoycorev3.TypedExtensionConfig{destExt, srcExt, extraSrc}
		colUpdate := &xdsv1alpha1.TelemetryCollector{Policies: policies}
		colAny, err := anypb.New(colUpdate)
		require.NoError(t, err)

		xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
			VersionInfo: revStr,
			Resources:   []*anypb.Any{colAny},
			TypeUrl:     ingestor.TelemetryCollectorTypeURL,
			Nonce:       nonceStr,
		}

		// Await ACK for this revision
		select {
		case ack := <-xdsSrv.reqCh:
			assert.Equal(t, revStr, ack.VersionInfo)
			assert.Equal(t, nonceStr, ack.ResponseNonce)
			assert.Nil(t, ack.ErrorDetail)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for ACK on reload cycle %d", i)
		}

		// Verify refCount never drops to 0 during reload
		rc := sharedReg.RefCount(canonicalKey)
		assert.GreaterOrEqual(t, rc, 1, "RefCount must remain >= 1 throughout reload %d", i)

		// Wait for collector to re-stabilize in StateRunning
		require.Eventually(t, func() bool {
			select {
			case err := <-colErrCh:
				t.Fatalf("collector exited during reload cycle %d: %v", i, err)
			default:
			}
			return col.GetState() == otelcol.StateRunning
		}, 5*time.Second, 50*time.Millisecond)

		// Verify refCount toggles back to 2 upon re-stabilization
		require.Eventually(t, func() bool {
			return sharedReg.RefCount(canonicalKey) == 2
		}, 3*time.Second, 50*time.Millisecond, "RefCount must toggle back to 2 upon re-stabilization in reload cycle %d", i)

		// Verify zero transport reconnection occurs
		assert.Equal(t, int32(1), xdsSrv.streamCount.Load(), "Zero transport reconnection must occur during reload %d", i)
	}

	// Final Shutdown
	col.Shutdown()
	<-colErrCh

	require.Eventually(t, func() bool {
		return sharedReg.RefCount(canonicalKey) == 0
	}, 5*time.Second, 50*time.Millisecond, "RefCount must drop to 0 after collector shutdown")

	require.NoError(t, sharedReg.Shutdown(context.Background()))
	assert.Equal(t, 0, sharedReg.EntryCount())
}

// TestMixedPayloadDecoupling verifies that when an xDS response contains both a filter update and a structural update:
// 1. The filter rule takes effect immediately in policyprocessor.
// 2. The structural change triggers collector reconfiguration.
func TestMixedPayloadDecoupling(t *testing.T) {
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	xdsSrv := newMockDiscoveryServer()
	xdsEndpoint, stopXds := startMockDiscoveryServer(t, xdsSrv)
	defer stopXds()

	recvGrpcPort := getFreePort(t)
	recvHttpPort := getFreePort(t)
	baseConfigFile := writeBaseConfigFile(t, dest.port, recvGrpcPort, recvHttpPort)

	// Revision 1: Drop DEBUG logs (keep INFO)
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}
	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}
	filterR1 := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
			},
		},
	}
	anyFilterR1, err := anypb.New(filterR1)
	require.NoError(t, err)

	colR1 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "filter-logs", TypedConfig: anyFilterR1},
		},
	}
	colAnyR1, err := anypb.New(colR1)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Resources:   []*anypb.Any{colAnyR1},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
	}

	driverReg := driver.NewDefaultRegistry()
	providerFactory := googlecontrolplane.NewFactoryWithOptions(
		googlecontrolplane.WithProviderDriverRegistry(driverReg),
	)

	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactories(nil, nil),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:xds://%s?fleet_id=test-fleet&base_config=%s&startup_timeout=10s", xdsEndpoint, baseConfigFile),
				},
				ProviderFactories: []confmap.ProviderFactory{
					providerFactory,
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()
	defer func() {
		col.Shutdown()
		<-colErrCh
	}()

	require.Eventually(t, func() bool {
		select {
		case err := <-colErrCh:
			t.Fatalf("collector exited early: %v", err)
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Drain initial DiscoveryRequest and R1 ACK
	select {
	case req := <-xdsSrv.reqCh:
		assert.Equal(t, "", req.VersionInfo)
		assert.Equal(t, "", req.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	select {
	case ack := <-xdsSrv.reqCh:
		assert.Equal(t, "1", ack.VersionInfo)
		assert.Equal(t, "nonce-1", ack.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for R1 ACK")
	}

	// Send initial logs: DEBUG (drop), INFO (retain)
	recvAddr := fmt.Sprintf("127.0.0.1:%d", recvGrpcPort)
	cc, err := grpc.NewClient(recvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()
	logsClient := plogotlp.NewGRPCClient(cc)

	sendLogs := func(msgDebug, msgInfo string) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberDebug)
		r1.SetSeverityText("DEBUG")
		r1.Body().SetStr(msgDebug)

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberInfo)
		r2.SetSeverityText("INFO")
		r2.Body().SetStr(msgInfo)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 50*time.Millisecond)
	}

	sendLogs("debug-1", "info-1")

	// Verify only info-1 was received
	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"info-1"}, extractLogBodies(dest.logs.getLogs()))

	// Stage A: PURE FILTER UPDATE (Revision 2)
	// Change filter rule: drop INFO (keep DEBUG). No structural changes.
	// Filter policies must apply live in-place to policyprocessor with zero collector restart.
	dest.resetAll()
	filterR2 := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "INFO"},
			},
		},
	}
	anyFilterR2, err := anypb.New(filterR2)
	require.NoError(t, err)

	colR2 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "filter-logs", TypedConfig: anyFilterR2},
		},
	}
	colAnyR2, err := anypb.New(colR2)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "2",
		Resources:   []*anypb.Any{colAnyR2},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-2",
	}

	select {
	case ack := <-xdsSrv.reqCh:
		assert.Equal(t, "2", ack.VersionInfo)
		assert.Equal(t, "nonce-2", ack.ResponseNonce)
		assert.Nil(t, ack.ErrorDetail)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for R2 ACK")
	}

	// Verify filter takes effect immediately live in-place without restart: DEBUG retained, INFO dropped
	sendLogs("debug-2", "info-2")
	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"debug-2"}, extractLogBodies(dest.logs.getLogs()),
		"Filter-only update must apply live in-place to policyprocessor without restarting collector")
	assert.Equal(t, otelcol.StateRunning, col.GetState())

	// Stage B: STRUCTURAL UPDATE (Revision 3)
	// Add filelog-src structural policy while toggling filter rule back: drop DEBUG (keep INFO).
	// Structural changes MUST trigger an in-process collector reload.
	dest.resetAll()
	filterR3 := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
			},
		},
	}
	anyFilterR3, err := anypb.New(filterR3)
	require.NoError(t, err)

	extraSrc := &envoycorev3.TypedExtensionConfig{
		Name: "filelog-src-mixed",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLFilelogSource,
		},
	}

	colR3 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			extraSrc,
			{Name: "filter-logs", TypedConfig: anyFilterR3},
		},
	}
	colAnyR3, err := anypb.New(colR3)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "3",
		Resources:   []*anypb.Any{colAnyR3},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-3",
	}

	select {
	case ack := <-xdsSrv.reqCh:
		assert.Equal(t, "3", ack.VersionInfo)
		assert.Equal(t, "nonce-3", ack.ResponseNonce)
		assert.Nil(t, ack.ErrorDetail)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for R3 ACK")
	}

	// Wait for collector to re-stabilize in StateRunning after structural reload
	require.Eventually(t, func() bool {
		select {
		case err := <-colErrCh:
			t.Fatalf("collector exited during reload: %v", err)
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Send logs under Revision 3 rules: DEBUG dropped, INFO retained
	sendLogs("debug-3", "info-3")

	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"info-3"}, extractLogBodies(dest.logs.getLogs()))
}

// TestPreValidatorRejectionGate verifies that an xDS response containing both a new filter policy
// and an invalid structural update is NACKed atomically, leaving active rules intact (zero partial application).
func TestPreValidatorRejectionGate(t *testing.T) {
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	xdsSrv := newMockDiscoveryServer()
	xdsEndpoint, stopXds := startMockDiscoveryServer(t, xdsSrv)
	defer stopXds()

	recvGrpcPort := getFreePort(t)
	recvHttpPort := getFreePort(t)
	baseConfigFile := writeBaseConfigFile(t, dest.port, recvGrpcPort, recvHttpPort)

	driverReg := driver.NewDefaultRegistry()
	driverReg.RegisterDriver(&structuralViolationDriver{})

	// Revision 1: Valid configuration dropping DEBUG logs
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}
	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}
	filterR1 := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
			},
		},
	}
	anyFilterR1, err := anypb.New(filterR1)
	require.NoError(t, err)

	colR1 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "filter-logs", TypedConfig: anyFilterR1},
		},
	}
	colAnyR1, err := anypb.New(colR1)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Resources:   []*anypb.Any{colAnyR1},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
	}

	providerFactory := googlecontrolplane.NewFactoryWithOptions(
		googlecontrolplane.WithProviderDriverRegistry(driverReg),
	)

	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactories(nil, nil),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:xds://%s?fleet_id=test-fleet&base_config=%s&startup_timeout=10s", xdsEndpoint, baseConfigFile),
				},
				ProviderFactories: []confmap.ProviderFactory{
					providerFactory,
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()
	defer func() {
		col.Shutdown()
		<-colErrCh
	}()

	require.Eventually(t, func() bool {
		select {
		case err := <-colErrCh:
			t.Fatalf("collector exited early: %v", err)
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Drain initial DiscoveryRequest and R1 ACK
	select {
	case req := <-xdsSrv.reqCh:
		assert.Equal(t, "", req.VersionInfo)
		assert.Equal(t, "", req.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial DiscoveryRequest")
	}

	select {
	case ack := <-xdsSrv.reqCh:
		assert.Equal(t, "1", ack.VersionInfo)
		assert.Equal(t, "nonce-1", ack.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for R1 ACK")
	}

	recvAddr := fmt.Sprintf("127.0.0.1:%d", recvGrpcPort)
	cc, err := grpc.NewClient(recvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()
	logsClient := plogotlp.NewGRPCClient(cc)

	sendLogs := func(msgDebug, msgInfo string) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberDebug)
		r1.SetSeverityText("DEBUG")
		r1.Body().SetStr(msgDebug)

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberInfo)
		r2.SetSeverityText("INFO")
		r2.Body().SetStr(msgInfo)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 50*time.Millisecond)
	}

	sendLogs("debug-r1", "info-r1")
	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"info-r1"}, extractLogBodies(dest.logs.getLogs()))

	// Revision 2: MALFORMED STRUCTURAL + NEW FILTER POLICY
	// Filter: drop INFO (keep DEBUG)
	// Structural: structuralViolationDriver (triggers PreValidator failure)
	dest.resetAll()
	filterR2 := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "INFO"},
			},
		},
	}
	anyFilterR2, err := anypb.New(filterR2)
	require.NoError(t, err)

	invalidStructural := &envoycorev3.TypedExtensionConfig{
		Name: "invalid-structural-policy",
		TypedConfig: &anypb.Any{
			TypeUrl: typeURLStructuralViolation,
		},
	}

	colR2 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			invalidStructural,
			{Name: "filter-logs", TypedConfig: anyFilterR2},
		},
	}
	colAnyR2, err := anypb.New(colR2)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "2",
		Resources:   []*anypb.Any{colAnyR2},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-2",
	}

	// Verify NACK is sent
	select {
	case nack := <-xdsSrv.reqCh:
		require.NotNil(t, nack.ErrorDetail, "Expected NACK with ErrorDetail on structural validation failure")
		assert.Equal(t, int32(codes.InvalidArgument), nack.ErrorDetail.Code)
		assert.Contains(t, nack.ErrorDetail.Message, "undeclared_receiver_that_triggers_prevalidator_rejection")
		assert.Equal(t, "1", nack.VersionInfo, "VersionInfo must remain at last active revision 1")
		assert.Equal(t, "nonce-2", nack.ResponseNonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for xDS NACK")
	}

	// Verify collector runtime is STILL running
	assert.Equal(t, otelcol.StateRunning, col.GetState())

	// Verify Zero Partial Application:
	// Send logs again. Active rules from R1 (drop DEBUG, keep INFO) must STILL be active!
	// If R2's filter was partially applied, INFO would be dropped.
	sendLogs("debug-r1-still-dropped", "info-r1-still-kept")

	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"info-r1-still-kept"}, extractLogBodies(dest.logs.getLogs()),
		"Zero partial application: R1 filter rules must remain active after NACK")
}

// TestPipelineUnion_PreservesUserPipelinesAndResolvesAnchors verifies that:
// 1. User base configurations with component anchors ${googlecontrolplane:component//...} are resolved.
// 2. User pipelines run alongside synthesized policy pipelines in parallel without conflict.
func TestPipelineUnion_PreservesUserPipelinesAndResolvesAnchors(t *testing.T) {
	dest, stopDest := startMockOtlpDestination(t)
	defer stopDest()

	xdsSrv := newMockDiscoveryServer()
	xdsEndpoint, stopXds := startMockDiscoveryServer(t, xdsSrv)
	defer stopXds()

	tmpDir := t.TempDir()

	// Ports for user and policy pipelines
	userGrpcPort := getFreePort(t)
	policyGrpcPort := getFreePort(t)
	policyHttpPort := getFreePort(t)

	// User Base Config defining custom pipeline using anchors
	userBaseMap := map[string]any{
		"receivers": map[string]any{
			"otlp/user": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": fmt.Sprintf("127.0.0.1:%d", userGrpcPort),
					},
				},
			},
		},
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs/user": map[string]any{
					"receivers":  []any{"otlp/user"},
					"processors": []any{"${googlecontrolplane:component//global_policy_processor}"},
					"exporters":  []any{"${googlecontrolplane:component//active_destination_exporter}"},
				},
			},
		},
	}
	userData, err := yaml.Marshal(userBaseMap)
	require.NoError(t, err)
	userBasePath := filepath.Join(tmpDir, "user_base.yaml")
	require.NoError(t, os.WriteFile(userBasePath, userData, 0o600))

	// Policy xDS Base Config defining exporters and policy receivers
	policyBaseConfigFile := writeBaseConfigFile(t, dest.port, policyGrpcPort, policyHttpPort)

	// Revision 1: Drop DEBUG logs
	destExt := &envoycorev3.TypedExtensionConfig{
		Name: "gcp-dest",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLGcpDestination,
		},
	}
	srcExt := &envoycorev3.TypedExtensionConfig{
		Name: "otlp-src",
		TypedConfig: &anypb.Any{
			TypeUrl: driver.TypeURLOtlpSource,
		},
	}
	filterPolicy := &policyv1alpha1.LogFilterPolicy{
		Id:     "filter-logs-union",
		Action: policyv1alpha1.Action_ACTION_DROP.Enum(),
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{Exact: "DEBUG"},
			},
		},
	}
	anyFilter, err := anypb.New(filterPolicy)
	require.NoError(t, err)

	colR1 := &xdsv1alpha1.TelemetryCollector{
		Policies: []*envoycorev3.TypedExtensionConfig{
			destExt,
			srcExt,
			{Name: "filter-logs-union", TypedConfig: anyFilter},
		},
	}
	colAnyR1, err := anypb.New(colR1)
	require.NoError(t, err)

	xdsSrv.respCh <- &discoveryv3.DiscoveryResponse{
		VersionInfo: "1",
		Resources:   []*anypb.Any{colAnyR1},
		TypeUrl:     ingestor.TelemetryCollectorTypeURL,
		Nonce:       "nonce-1",
	}

	driverReg := driver.NewDefaultRegistry()
	providerFactory := googlecontrolplane.NewFactoryWithOptions(
		googlecontrolplane.WithProviderDriverRegistry(driverReg),
	)

	// Pass root xDS config first, then user base config
	colSettings := otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: createFactories(nil, nil),
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{
					fmt.Sprintf("googlecontrolplane:xds://%s?fleet_id=test-fleet&base_config=%s&startup_timeout=10s", xdsEndpoint, policyBaseConfigFile),
					fmt.Sprintf("file:%s", userBasePath),
				},
				ProviderFactories: []confmap.ProviderFactory{
					providerFactory,
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
	}

	col, err := otelcol.NewCollector(colSettings)
	require.NoError(t, err)

	colErrCh := make(chan error, 1)
	go func() {
		colErrCh <- col.Run(context.Background())
	}()
	defer func() {
		col.Shutdown()
		<-colErrCh
	}()

	require.Eventually(t, func() bool {
		return col.GetState() == otelcol.StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	// 1. Send telemetry to the USER receiver (otlp/user)
	userRecvAddr := fmt.Sprintf("127.0.0.1:%d", userGrpcPort)
	userCC, err := grpc.NewClient(userRecvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer userCC.Close()
	userLogsClient := plogotlp.NewGRPCClient(userCC)

	sendLogsClient := func(client plogotlp.GRPCClient, msgDebug, msgInfo string) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()

		r1 := sl.LogRecords().AppendEmpty()
		r1.SetSeverityNumber(plog.SeverityNumberDebug)
		r1.SetSeverityText("DEBUG")
		r1.Body().SetStr(msgDebug)

		r2 := sl.LogRecords().AppendEmpty()
		r2.SetSeverityNumber(plog.SeverityNumberInfo)
		r2.SetSeverityText("INFO")
		r2.Body().SetStr(msgInfo)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := client.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
			return err == nil
		}, 5*time.Second, 50*time.Millisecond)
	}

	sendLogsClient(userLogsClient, "user-debug-drop", "user-info-retain")

	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"user-info-retain"}, extractLogBodies(dest.logs.getLogs()),
		"User pipeline must route through policy/global and export to destination")

	// 2. Send telemetry to the POLICY receiver (otlp)
	dest.resetAll()
	policyRecvAddr := fmt.Sprintf("127.0.0.1:%d", policyGrpcPort)
	policyCC, err := grpc.NewClient(policyRecvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer policyCC.Close()
	policyLogsClient := plogotlp.NewGRPCClient(policyCC)

	sendLogsClient(policyLogsClient, "policy-debug-drop", "policy-info-retain")

	require.Eventually(t, func() bool {
		return len(dest.logs.getLogs()) > 0
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, []string{"policy-info-retain"}, extractLogBodies(dest.logs.getLogs()),
		"Synthesized policy pipeline must run in parallel and process logs independently")
}

func extractLogBodies(logs []plog.Logs) []string {
	var received []string
	for _, l := range logs {
		for i := 0; i < l.ResourceLogs().Len(); i++ {
			r := l.ResourceLogs().At(i)
			for j := 0; j < r.ScopeLogs().Len(); j++ {
				s := r.ScopeLogs().At(j)
				for k := 0; k < s.LogRecords().Len(); k++ {
					received = append(received, s.LogRecords().At(k).Body().AsString())
				}
			}
		}
	}
	return received
}

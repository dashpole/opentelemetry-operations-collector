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

package policyprocessor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	policyv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/policy/v1alpha1"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane"
)

type mockInformerExtension struct {
	controlplane.PolicyInformer
}

func (m *mockInformerExtension) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockInformerExtension) Shutdown(ctx context.Context) error {
	return nil
}

type mockHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h *mockHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

func newMockHost(exts map[component.ID]component.Component) *mockHost {
	return &mockHost{
		Host:       componenttest.NewNopHost(),
		extensions: exts,
	}
}

func makeLogPolicy(id string, action policyv1alpha1.Action, severityExact string) *v3.TypedExtensionConfig {
	act := action
	p := &policyv1alpha1.LogFilterPolicy{
		Id:     id,
		Action: &act,
		Matches: []*policyv1alpha1.LogMatcher{
			{
				Target: &policyv1alpha1.LogFieldSelector{
					Target: &policyv1alpha1.LogFieldSelector_RecordField{
						RecordField: policyv1alpha1.LogRecordField_LOG_RECORD_FIELD_SEVERITY_TEXT,
					},
				},
				Predicate: &policyv1alpha1.LogMatcher_Exact{
					Exact: severityExact,
				},
			},
		},
	}
	b, _ := proto.Marshal(p)
	return &v3.TypedExtensionConfig{
		Name: id,
		TypedConfig: &anypb.Any{
			TypeUrl: TypeURLLogFilterPolicy,
			Value:   b,
		},
	}
}

func makeMetricInstrumentPolicy(id string, action policyv1alpha1.Action, metricName string) *v3.TypedExtensionConfig {
	act := action
	p := &policyv1alpha1.MetricFilterPolicy{
		Id:     id,
		Action: &act,
		Matches: []*policyv1alpha1.MetricMatcher{
			{
				Target: &policyv1alpha1.MetricFieldSelector{
					Target: &policyv1alpha1.MetricFieldSelector_DescriptorField{
						DescriptorField: policyv1alpha1.MetricDescriptorField_METRIC_DESCRIPTOR_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.MetricMatcher_Exact{
					Exact: metricName,
				},
			},
		},
	}
	b, _ := proto.Marshal(p)
	return &v3.TypedExtensionConfig{
		Name: id,
		TypedConfig: &anypb.Any{
			TypeUrl: TypeURLMetricFilterPolicy,
			Value:   b,
		},
	}
}

func makeMetricDataPointPolicy(id string, action policyv1alpha1.Action, key, value string) *v3.TypedExtensionConfig {
	act := action
	p := &policyv1alpha1.MetricFilterPolicy{
		Id:     id,
		Action: &act,
		Matches: []*policyv1alpha1.MetricMatcher{
			{
				Target: &policyv1alpha1.MetricFieldSelector{
					Target: &policyv1alpha1.MetricFieldSelector_DatapointAttribute{
						DatapointAttribute: key,
					},
				},
				Predicate: &policyv1alpha1.MetricMatcher_Exact{
					Exact: value,
				},
			},
		},
	}
	b, _ := proto.Marshal(p)
	return &v3.TypedExtensionConfig{
		Name: id,
		TypedConfig: &anypb.Any{
			TypeUrl: TypeURLMetricFilterPolicy,
			Value:   b,
		},
	}
}

func makeTracePolicy(id string, action policyv1alpha1.Action, spanName string) *v3.TypedExtensionConfig {
	act := action
	p := &policyv1alpha1.TraceFilterPolicy{
		Id:     id,
		Action: &act,
		Matches: []*policyv1alpha1.TraceMatcher{
			{
				Target: &policyv1alpha1.TraceFieldSelector{
					Target: &policyv1alpha1.TraceFieldSelector_RecordField{
						RecordField: policyv1alpha1.SpanRecordField_SPAN_RECORD_FIELD_NAME,
					},
				},
				Predicate: &policyv1alpha1.TraceMatcher_Exact{
					Exact: spanName,
				},
			},
		},
	}
	b, _ := proto.Marshal(p)
	return &v3.TypedExtensionConfig{
		Name: id,
		TypedConfig: &anypb.Any{
			TypeUrl: TypeURLTraceFilterPolicy,
			Value:   b,
		},
	}
}

// 1. TestPolicyProcessor_MultiInformerPrecedence:
// Verify that earlier informers in informer_extensions take precedence over later informers
// when policyIDs collide (first-match-wins).
func TestPolicyProcessor_MultiInformerPrecedence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bFile := controlplane.NewPolicyBroadcaster()
	bXds := controlplane.NewPolicyBroadcaster()

	// Initial policies:
	// filepolicy sets "conflict-rule" to ACTION_KEEP for DEBUG
	bFile.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("conflict-rule", policyv1alpha1.Action_ACTION_KEEP, "DEBUG"),
	})
	// googlexdspolicy sets "conflict-rule" to ACTION_DROP for DEBUG
	bXds.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("conflict-rule", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
	})

	idFile := component.MustNewID("filepolicy")
	idXds := component.MustNewID("googlexdspolicy")

	host := newMockHost(map[component.ID]component.Component{
		idFile: &mockInformerExtension{bFile},
		idXds:  &mockInformerExtension{bXds},
	})

	// Order: [filepolicy, googlexdspolicy] -> filepolicy should win (ACTION_KEEP)
	cfg := &Config{
		InformerExtensions: []component.ID{idFile, idXds},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zaptest.NewLogger(t)

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	// Wait for compilation
	require.Eventually(t, func() bool {
		compiled := proc.compiled.Load()
		if compiled == nil || len(compiled.LogPolicies) != 1 {
			return false
		}
		return compiled.LogPolicies[0].ID == "conflict-rule" &&
			*compiled.LogPolicies[0].Policy.Action == policyv1alpha1.Action_ACTION_KEEP
	}, 3*time.Second, 20*time.Millisecond)

	// Send a DEBUG log: because filepolicy's ACTION_KEEP wins, it must be kept
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityText("DEBUG")

	out, err := proc.processLogs(ctx, ld)
	require.NoError(t, err)
	assert.Equal(t, 1, out.LogRecordCount(), "filepolicy (ACTION_KEEP) must override googlexdspolicy (ACTION_DROP)")

	// Now test reverse order: [googlexdspolicy, filepolicy] -> googlexdspolicy should win (ACTION_DROP)
	cfgRev := &Config{
		InformerExtensions: []component.ID{idXds, idFile},
		StartupTimeout:     1 * time.Second,
	}
	procRev, err := newPolicyProcessor(set, cfgRev)
	require.NoError(t, err)

	err = procRev.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = procRev.Shutdown(ctx) }()

	require.Eventually(t, func() bool {
		compiled := procRev.compiled.Load()
		if compiled == nil || len(compiled.LogPolicies) != 1 {
			return false
		}
		return compiled.LogPolicies[0].ID == "conflict-rule" &&
			*compiled.LogPolicies[0].Policy.Action == policyv1alpha1.Action_ACTION_DROP
	}, 3*time.Second, 20*time.Millisecond)

	ldRev := plog.NewLogs()
	lrRev := ldRev.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lrRev.SetSeverityText("DEBUG")

	outRev, err := procRev.processLogs(ctx, ldRev)
	require.NoError(t, err)
	assert.Equal(t, 0, outRev.LogRecordCount(), "googlexdspolicy (ACTION_DROP) must override filepolicy (ACTION_KEEP) when listed first")
}

// 2. TestPolicyProcessor_LiveUpdateZeroReload_AllSignals:
// Verify logs, metrics instruments+data points, and traces live update without pipeline reload or re-creating processor.
func TestPolicyProcessor_LiveUpdateZeroReload_AllSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := controlplane.NewPolicyBroadcaster()
	idXds := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		idXds: &mockInformerExtension{b},
	})

	// Initial policies:
	// Logs: drop DEBUG
	// Metrics: drop cpu.idle instrument; drop env:test data points
	// Traces: drop healthcheck
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("log-filter", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
		makeMetricInstrumentPolicy("metric-inst-filter", policyv1alpha1.Action_ACTION_DROP, "cpu.idle"),
		makeMetricDataPointPolicy("metric-dp-filter", policyv1alpha1.Action_ACTION_DROP, "env", "test"),
		makeTracePolicy("trace-filter", policyv1alpha1.Action_ACTION_DROP, "healthcheck"),
	})

	cfg := &Config{
		InformerExtensions: []component.ID{idXds},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 1 && len(c.MetricInstrumentPolicies) == 1 &&
			len(c.MetricDataPointPolicies) == 1 && len(c.TracePolicies) == 1
	}, 3*time.Second, 20*time.Millisecond)

	// Verify Phase 1 Telemetry Filtering:
	// Logs: DEBUG dropped, INFO kept
	ld1 := plog.NewLogs()
	lrs1 := ld1.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lrs1.AppendEmpty().SetSeverityText("DEBUG")
	lrs1.AppendEmpty().SetSeverityText("INFO")
	outLogs1, err := proc.processLogs(ctx, ld1)
	require.NoError(t, err)
	require.Equal(t, 1, outLogs1.LogRecordCount())
	assert.Equal(t, "INFO", outLogs1.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())

	// Metrics: cpu.idle dropped, cpu.busy env:test dropped, cpu.busy env:prod kept
	md1 := pmetric.NewMetrics()
	ms1 := md1.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	mIdle := ms1.AppendEmpty()
	mIdle.SetName("cpu.idle")
	mIdle.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)
	mBusy := ms1.AppendEmpty()
	mBusy.SetName("cpu.busy")
	dpTest := mBusy.SetEmptyGauge().DataPoints().AppendEmpty()
	dpTest.Attributes().PutStr("env", "test")
	dpTest.SetDoubleValue(2.0)
	dpProd := mBusy.Gauge().DataPoints().AppendEmpty()
	dpProd.Attributes().PutStr("env", "prod")
	dpProd.SetDoubleValue(3.0)

	outMetrics1, err := proc.processMetrics(ctx, md1)
	require.NoError(t, err)
	require.Equal(t, 1, outMetrics1.MetricCount())
	require.Equal(t, "cpu.busy", outMetrics1.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	dps1 := outMetrics1.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	require.Equal(t, 1, dps1.Len())
	assert.Equal(t, "prod", dps1.At(0).Attributes().AsRaw()["env"])

	// Traces: healthcheck dropped, api_call kept
	td1 := ptrace.NewTraces()
	spans1 := td1.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	spans1.AppendEmpty().SetName("healthcheck")
	spans1.AppendEmpty().SetName("api_call")
	outTraces1, err := proc.processTraces(ctx, td1)
	require.NoError(t, err)
	require.Equal(t, 1, outTraces1.SpanCount())
	assert.Equal(t, "api_call", outTraces1.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())

	// LIVE UPDATE (Phase 2): Mutate all rules via EventModified
	// Logs: drop INFO instead of DEBUG
	// Metrics: drop cpu.busy instrument; drop env:prod data points
	// Traces: drop api_call instead of healthcheck
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("log-filter", policyv1alpha1.Action_ACTION_DROP, "INFO"),
		makeMetricInstrumentPolicy("metric-inst-filter", policyv1alpha1.Action_ACTION_DROP, "cpu.busy"),
		makeMetricDataPointPolicy("metric-dp-filter", policyv1alpha1.Action_ACTION_DROP, "env", "prod"),
		makeTracePolicy("trace-filter", policyv1alpha1.Action_ACTION_DROP, "api_call"),
	})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		if c == nil || len(c.LogPolicies) != 1 || len(c.MetricInstrumentPolicies) != 1 ||
			len(c.MetricDataPointPolicies) != 1 || len(c.TracePolicies) != 1 {
			return false
		}
		return c.LogPolicies[0].Policy.GetMatches()[0].GetExact() == "INFO" &&
			c.MetricInstrumentPolicies[0].Policy.GetMatches()[0].GetExact() == "cpu.busy" &&
			c.TracePolicies[0].Policy.GetMatches()[0].GetExact() == "api_call"
	}, 3*time.Second, 20*time.Millisecond)

	// Verify Phase 2 Telemetry Filtering under live mutated rules:
	// Logs: DEBUG now kept, INFO now dropped
	ld2 := plog.NewLogs()
	lrs2 := ld2.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lrs2.AppendEmpty().SetSeverityText("DEBUG")
	lrs2.AppendEmpty().SetSeverityText("INFO")
	outLogs2, err := proc.processLogs(ctx, ld2)
	require.NoError(t, err)
	require.Equal(t, 1, outLogs2.LogRecordCount())
	assert.Equal(t, "DEBUG", outLogs2.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())

	// Metrics: cpu.busy dropped, cpu.idle kept, env:prod dropped, env:test kept
	md2 := pmetric.NewMetrics()
	ms2 := md2.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	mBusy2 := ms2.AppendEmpty()
	mBusy2.SetName("cpu.busy")
	mBusy2.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)
	mIdle2 := ms2.AppendEmpty()
	mIdle2.SetName("cpu.idle")
	dpTest2 := mIdle2.SetEmptyGauge().DataPoints().AppendEmpty()
	dpTest2.Attributes().PutStr("env", "test")
	dpTest2.SetDoubleValue(2.0)
	dpProd2 := mIdle2.Gauge().DataPoints().AppendEmpty()
	dpProd2.Attributes().PutStr("env", "prod")
	dpProd2.SetDoubleValue(3.0)

	outMetrics2, err := proc.processMetrics(ctx, md2)
	require.NoError(t, err)
	require.Equal(t, 1, outMetrics2.MetricCount())
	require.Equal(t, "cpu.idle", outMetrics2.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	dps2 := outMetrics2.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	require.Equal(t, 1, dps2.Len())
	assert.Equal(t, "test", dps2.At(0).Attributes().AsRaw()["env"])

	// Traces: healthcheck now kept, api_call now dropped
	td2 := ptrace.NewTraces()
	spans2 := td2.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	spans2.AppendEmpty().SetName("healthcheck")
	spans2.AppendEmpty().SetName("api_call")
	outTraces2, err := proc.processTraces(ctx, td2)
	require.NoError(t, err)
	require.Equal(t, 1, outTraces2.SpanCount())
	assert.Equal(t, "healthcheck", outTraces2.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
}

// 3. TestPolicyProcessor_EventDeletedRestore:
// Push EventAdded to drop a stream, verify dropped; push EventDeleted, verify stream restored without reload.
func TestPolicyProcessor_EventDeletedRestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := controlplane.NewPolicyBroadcaster()
	id := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		id: &mockInformerExtension{b},
	})

	cfg := &Config{
		InformerExtensions: []component.ID{id},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	// Add rule to drop DEBUG logs
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("drop-debug", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
	})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 1
	}, 3*time.Second, 20*time.Millisecond)

	// Send DEBUG log: dropped
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetSeverityText("DEBUG")
	out, err := proc.processLogs(ctx, ld)
	require.NoError(t, err)
	assert.Equal(t, 0, out.LogRecordCount(), "DEBUG log must be dropped by active policy")

	// Delete policy
	b.UpdatePolicies([]*v3.TypedExtensionConfig{})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 0
	}, 3*time.Second, 20*time.Millisecond)

	// Send DEBUG log again: restored immediately!
	ldRestored := plog.NewLogs()
	ldRestored.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetSeverityText("DEBUG")
	outRestored, err := proc.processLogs(ctx, ldRestored)
	require.NoError(t, err)
	assert.Equal(t, 1, outRestored.LogRecordCount(), "DEBUG log must be restored after EventDeleted")
}

// 4. TestPolicyProcessor_EventResync:
// Verify EventResync calls informer.List(""), reconciles store, recompiles rules, and swaps pointer.
func TestPolicyProcessor_EventResync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := controlplane.NewPolicyBroadcaster()
	id := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		id: &mockInformerExtension{b},
	})

	// Initial policies in broadcaster
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("resync-1", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
		makeLogPolicy("resync-2", policyv1alpha1.Action_ACTION_DROP, "INFO"),
	})

	cfg := &Config{
		InformerExtensions: []component.ID{id},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 2
	}, 3*time.Second, 20*time.Millisecond)

	// Update broadcaster store directly without delta events, then broadcast EventResync
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("resync-2", policyv1alpha1.Action_ACTION_DROP, "INFO"),
		makeLogPolicy("resync-3", policyv1alpha1.Action_ACTION_DROP, "WARN"),
	})

	// Explicitly broadcast EventResync
	b.Broadcast(controlplane.PolicyWatchEvent{Type: controlplane.EventResync})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		if c == nil || len(c.LogPolicies) != 2 {
			return false
		}
		hasResync2 := false
		hasResync3 := false
		for _, p := range c.LogPolicies {
			if p.ID == "resync-2" {
				hasResync2 = true
			}
			if p.ID == "resync-3" {
				hasResync3 = true
			}
		}
		return hasResync2 && hasResync3
	}, 3*time.Second, 20*time.Millisecond)

	// Check that resync-1 is no longer in store
	proc.store.mu.Lock()
	_, resync1Exists := proc.store.sources[id.String()]["resync-1"]
	proc.store.mu.Unlock()
	assert.False(t, resync1Exists, "resync-1 must be purged by EventResync reconciliation")
}

// 5. TestPolicyProcessor_ConcurrentLiveMutation:
// 1,000 concurrent goroutines sending data, 50Hz rule mutations for 3s under go test -race, 0 races, 0 panics.
func TestPolicyProcessor_ConcurrentLiveMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := controlplane.NewPolicyBroadcaster()
	id := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		id: &mockInformerExtension{b},
	})

	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("log-race", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
		makeMetricInstrumentPolicy("metric-race", policyv1alpha1.Action_ACTION_DROP, "cpu.load"),
		makeTracePolicy("trace-race", policyv1alpha1.Action_ACTION_DROP, "ping"),
	})

	cfg := &Config{
		InformerExtensions: []component.ID{id},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	const numWorkers = 1000
	var stopFlag atomic.Int32
	var workerWg sync.WaitGroup

	// 1,000 concurrent goroutines sending logs, metrics, traces
	workerWg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer workerWg.Done()
			sev := "DEBUG"
			if workerID%2 == 0 {
				sev = "INFO"
			}
			mName := "cpu.load"
			if workerID%2 == 0 {
				mName = "mem.used"
			}
			spanName := "ping"
			if workerID%2 == 0 {
				spanName = "login"
			}

			for stopFlag.Load() == 0 {
				// Logs
				ld := plog.NewLogs()
				ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetSeverityText(sev)
				_, _ = proc.processLogs(ctx, ld)

				// Metrics
				md := pmetric.NewMetrics()
				m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
				m.SetName(mName)
				m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)
				_, _ = proc.processMetrics(ctx, md)

				// Traces
				td := ptrace.NewTraces()
				td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(spanName)
				_, _ = proc.processTraces(ctx, td)
			}
		}(i)
	}

	// 50Hz rule mutations for 3 seconds
	mutationTicker := time.NewTicker(20 * time.Millisecond) // 50Hz
	defer mutationTicker.Stop()

	testDuration := time.After(3 * time.Second)
	step := 0

mutationLoop:
	for {
		select {
		case <-testDuration:
			break mutationLoop
		case <-mutationTicker.C:
			step++
			switch step % 4 {
			case 0:
				b.UpdatePolicies([]*v3.TypedExtensionConfig{
					makeLogPolicy("log-race", policyv1alpha1.Action_ACTION_DROP, "INFO"),
					makeMetricInstrumentPolicy("metric-race", policyv1alpha1.Action_ACTION_DROP, "mem.used"),
					makeTracePolicy("trace-race", policyv1alpha1.Action_ACTION_DROP, "login"),
				})
			case 1:
				b.UpdatePolicies([]*v3.TypedExtensionConfig{
					makeLogPolicy("log-race", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
					makeMetricInstrumentPolicy("metric-race", policyv1alpha1.Action_ACTION_DROP, "cpu.load"),
					makeTracePolicy("trace-race", policyv1alpha1.Action_ACTION_DROP, "ping"),
				})
			case 2:
				b.Broadcast(controlplane.PolicyWatchEvent{Type: controlplane.EventResync})
			case 3:
				b.UpdatePolicies([]*v3.TypedExtensionConfig{
					makeLogPolicy("log-race", policyv1alpha1.Action_ACTION_KEEP, "DEBUG"),
					makeMetricInstrumentPolicy("metric-race", policyv1alpha1.Action_ACTION_KEEP, "cpu.load"),
					makeTracePolicy("trace-race", policyv1alpha1.Action_ACTION_KEEP, "ping"),
				})
			}
		}
	}

	stopFlag.Store(1)
	workerWg.Wait()
}

// 6. TestPolicyProcessor_StoreRollbackOnError:
// Verify malformed policy fails compilation, is rolled back from store, active rules are retained,
// and subsequent valid policy compiles successfully proving the store wasn't poisoned.
func TestPolicyProcessor_StoreRollbackOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := controlplane.NewPolicyBroadcaster()
	id := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		id: &mockInformerExtension{b},
	})

	cfg := &Config{
		InformerExtensions: []component.ID{id},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	// Step 1: Push valid rule1 (drop DEBUG)
	b.UpdatePolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("rule1", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
	})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 1 && c.LogPolicies[0].ID == "rule1"
	}, 3*time.Second, 20*time.Millisecond)

	// Step 2: Push malformed policy rule2 (invalid regex in matches)
	b.Broadcast(controlplane.PolicyWatchEvent{
		Type:     controlplane.EventAdded,
		PolicyID: "rule2",
		TypeURL:  TypeURLLogFilterPolicy,
		Policy: &v3.TypedExtensionConfig{
			Name: "rule2",
			TypedConfig: &anypb.Any{
				TypeUrl: TypeURLLogFilterPolicy,
				Value:   []byte(`{"action": "ACTION_DROP", "matches": [{"target": {"record_field": "LOG_RECORD_FIELD_SEVERITY_TEXT"}, "regex": "["}]}`),
			},
		},
	})

	// Wait briefly to allow processing
	time.Sleep(100 * time.Millisecond)

	// Verify rule2 is NOT in store (was rolled back)
	proc.store.mu.Lock()
	_, rule2Exists := proc.store.sources[id.String()]["rule2"]
	proc.store.mu.Unlock()
	assert.False(t, rule2Exists, "rule2 must have been rolled back from store")

	// Verify rule1 is STILL active in compiled
	cAfterErr := proc.compiled.Load()
	require.NotNil(t, cAfterErr)
	require.Equal(t, 1, len(cAfterErr.LogPolicies))
	assert.Equal(t, "rule1", cAfterErr.LogPolicies[0].ID)

	// Step 3: Push subsequent valid rule3 (drop INFO)
	b.Broadcast(controlplane.PolicyWatchEvent{
		Type:     controlplane.EventAdded,
		PolicyID: "rule3",
		TypeURL:  TypeURLLogFilterPolicy,
		Policy:   makeLogPolicy("rule3", policyv1alpha1.Action_ACTION_DROP, "INFO"),
	})

	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		if c == nil || len(c.LogPolicies) != 2 {
			return false
		}
		return (c.LogPolicies[0].ID == "rule1" && c.LogPolicies[1].ID == "rule3") ||
			(c.LogPolicies[0].ID == "rule3" && c.LogPolicies[1].ID == "rule1")
	}, 3*time.Second, 20*time.Millisecond)

	// Verify both rule1 and rule3 enforce filtering
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lrs.AppendEmpty().SetSeverityText("DEBUG") // dropped by rule1
	lrs.AppendEmpty().SetSeverityText("INFO")  // dropped by rule3
	lrs.AppendEmpty().SetSeverityText("WARN")  // kept

	out, err := proc.processLogs(ctx, ld)
	require.NoError(t, err)
	require.Equal(t, 1, out.LogRecordCount())
	assert.Equal(t, "WARN", out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
}

// reconnectingMockInformer implements PolicyInformer with support for simulating channel closures.
type reconnectingMockInformer struct {
	mu       sync.Mutex
	policies map[string]*v3.TypedExtensionConfig
	activeCh chan controlplane.PolicyWatchEvent
	readyCh  chan struct{}
}

func newReconnectingMockInformer(initial []*v3.TypedExtensionConfig) *reconnectingMockInformer {
	pMap := make(map[string]*v3.TypedExtensionConfig)
	for _, p := range initial {
		pMap[p.Name] = p
	}
	r := &reconnectingMockInformer{
		policies: pMap,
		readyCh:  make(chan struct{}),
	}
	close(r.readyCh)
	return r
}

func (r *reconnectingMockInformer) Ready() <-chan struct{} {
	return r.readyCh
}

func (r *reconnectingMockInformer) List(typeURL string) ([]*v3.TypedExtensionConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*v3.TypedExtensionConfig, 0, len(r.policies))
	for _, p := range r.policies {
		out = append(out, proto.Clone(p).(*v3.TypedExtensionConfig))
	}
	return out, nil
}

func (r *reconnectingMockInformer) Watch(ctx context.Context, typeURL string) (<-chan controlplane.PolicyWatchEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan controlplane.PolicyWatchEvent, 100)
	r.activeCh = ch
	// Initial EventResync
	ch <- controlplane.PolicyWatchEvent{Type: controlplane.EventResync}
	return ch, nil
}

func (r *reconnectingMockInformer) SetPolicies(newPolicies []*v3.TypedExtensionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policies = make(map[string]*v3.TypedExtensionConfig)
	for _, p := range newPolicies {
		r.policies[p.Name] = p
	}
}

func (r *reconnectingMockInformer) CloseActiveChannel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activeCh != nil {
		close(r.activeCh)
		r.activeCh = nil
	}
}

func (r *reconnectingMockInformer) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (r *reconnectingMockInformer) Shutdown(ctx context.Context) error {
	return nil
}

// 7. TestPolicyProcessor_WatchReconnectionAndReconciliation:
// Verify unexpected channel closure while processor running automatically reconnects with backoff,
// reconciles on EventResync, cleans deleted rule1, applies rule2.
func TestPolicyProcessor_WatchReconnectionAndReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockInf := newReconnectingMockInformer([]*v3.TypedExtensionConfig{
		makeLogPolicy("rule1", policyv1alpha1.Action_ACTION_DROP, "DEBUG"),
	})

	id := component.MustNewID("googlexdspolicy")
	host := newMockHost(map[component.ID]component.Component{
		id: mockInf,
	})

	cfg := &Config{
		InformerExtensions: []component.ID{id},
		StartupTimeout:     1 * time.Second,
	}
	set := processortest.NewNopSettings(processortest.NopType)
	set.Logger = zap.NewNop()

	proc, err := newPolicyProcessor(set, cfg)
	require.NoError(t, err)

	err = proc.Start(ctx, host)
	require.NoError(t, err)
	defer func() { _ = proc.Shutdown(ctx) }()

	// Verify rule1 is active
	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		return c != nil && len(c.LogPolicies) == 1 && c.LogPolicies[0].ID == "rule1"
	}, 3*time.Second, 20*time.Millisecond)

	// While running: mutate informer state to remove rule1 and add rule2 (drop ERROR)
	mockInf.SetPolicies([]*v3.TypedExtensionConfig{
		makeLogPolicy("rule2", policyv1alpha1.Action_ACTION_DROP, "ERROR"),
	})

	// Simulate unexpected watch channel closure while collector is running
	mockInf.CloseActiveChannel()

	// Verify processor reconnects with backoff, handles EventResync, cleans rule1, applies rule2
	require.Eventually(t, func() bool {
		c := proc.compiled.Load()
		if c == nil || len(c.LogPolicies) != 1 {
			return false
		}
		return c.LogPolicies[0].ID == "rule2"
	}, 5*time.Second, 50*time.Millisecond)

	// Send telemetry to verify rule1 is gone and rule2 is active:
	// DEBUG log (previously dropped by rule1) should now be KEPT!
	// ERROR log (newly dropped by rule2) should now be DROPPED!
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lrs.AppendEmpty().SetSeverityText("DEBUG")
	lrs.AppendEmpty().SetSeverityText("ERROR")

	out, err := proc.processLogs(ctx, ld)
	require.NoError(t, err)
	require.Equal(t, 1, out.LogRecordCount())
	assert.Equal(t, "DEBUG", out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
}

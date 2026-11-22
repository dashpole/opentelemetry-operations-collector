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

package metricaggregationprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
)

func TestSeriesShardCacheLineSize(t *testing.T) {
	require.Equal(t, uintptr(64), unsafe.Sizeof(seriesShard{}), "seriesShard must be padded to 64 B cache line")
}

func TestFNV1a128MatchesStdlib(t *testing.T) {
	inputs := [][]byte{
		nil,
		{},
		[]byte("a"),
		[]byte("http.server.request.duration"),
		[]byte("host=node-1;service=checkout;env=prod;long_payload_0123456789abcdefghijklmnopqrstuvwxyz"),
		{0x00, 0xff, 0x80, 0x7f, 0x01, 0xfe},
	}
	for _, in := range inputs {
		got := fnv1a128(in)
		h := fnv.New128a()
		_, err := h.Write(in)
		require.NoError(t, err)
		sum := h.Sum(nil)
		wantHi := binary.BigEndian.Uint64(sum[:8])
		wantLo := binary.BigEndian.Uint64(sum[8:])
		require.Equal(t, [2]uint64{wantHi, wantLo}, got, "input=%q", string(in))
	}
}

func makeDeltaSumMetric(host, scopeName, metricName, unit string, ts time.Time, val float64, attrs map[string]string, isInt bool) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	rm.Resource().Attributes().PutStr("host.name", host)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	sm.Scope().SetVersion("1.0.0")
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	m.SetUnit(unit)
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.SetIsMonotonic(true)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	for k, v := range attrs {
		dp.Attributes().PutStr(k, v)
	}
	if isInt {
		dp.SetIntValue(int64(val))
	} else {
		dp.SetDoubleValue(val)
	}
	return md
}

func findMetric(md pmetric.Metrics, name string) (pmetric.Metric, bool) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				if m.Name() == name {
					return m, true
				}
			}
		}
	}
	return pmetric.Metric{}, false
}

func collectMetricMap(batches []pmetric.Metrics) map[string]pmetric.Metric {
	out := make(map[string]pmetric.Metric)
	for _, md := range batches {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			rm := md.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					out[m.Name()] = m
				}
			}
		}
	}
	return out
}

func TestPassthroughUntouchedTypes(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second
	cfg.PassthroughInput = false

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()
	p.clock = func() time.Time { return base }

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// 1. Cumulative Sum (MUST pass through untouched, NEVER summed!)
	mCum := sm.Metrics().AppendEmpty()
	mCum.SetName("cum.counter")
	mCum.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	mCum.Sum().SetIsMonotonic(true)
	dpCum := mCum.Sum().DataPoints().AppendEmpty()
	dpCum.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpCum.SetIntValue(100)

	// 2. Unspecified temporality Sum (MUST pass through untouched)
	mUnspec := sm.Metrics().AppendEmpty()
	mUnspec.SetName("unspec.counter")
	mUnspec.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityUnspecified)
	dpU := mUnspec.Sum().DataPoints().AppendEmpty()
	dpU.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpU.SetDoubleValue(50)

	// 3. Histogram (MUST pass through untouched)
	mHist := sm.Metrics().AppendEmpty()
	mHist.SetName("req.hist")
	dpH := mHist.SetEmptyHistogram().DataPoints().AppendEmpty()
	dpH.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpH.SetCount(5)

	// 4. ExponentialHistogram (MUST pass through untouched)
	mExp := sm.Metrics().AppendEmpty()
	mExp.SetName("req.exphist")
	dpE := mExp.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	dpE.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpE.SetCount(3)

	// 5. Summary (MUST pass through untouched)
	mSum := sm.Metrics().AppendEmpty()
	mSum.SetName("req.summary")
	dpS := mSum.SetEmptySummary().DataPoints().AppendEmpty()
	dpS.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpS.SetCount(7)

	// 6. Eligible Delta Sum (MUST be aggregated and replaced when PassthroughInput=false)
	mDelta := sm.Metrics().AppendEmpty()
	mDelta.SetName("req.delta")
	mDelta.SetUnit("By")
	mDelta.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpD := mDelta.Sum().DataPoints().AppendEmpty()
	dpD.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpD.SetDoubleValue(40)

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	require.Len(t, sink.AllMetrics(), 1)
	out := sink.AllMetrics()[0]

	for _, kept := range []string{"cum.counter", "unspec.counter", "req.hist", "req.exphist", "req.summary"} {
		_, ok := findMetric(out, kept)
		require.True(t, ok, "expected %s to pass through untouched", kept)
		_, bad := findMetric(out, kept+".window.sum")
		require.False(t, bad, "metric %s must NOT be aggregated", kept)
	}

	// Original delta metric replaced (passthrough_input=false).
	_, hasOrig := findMetric(out, "req.delta")
	require.False(t, hasOrig, "req.delta should be replaced when passthrough_input=false")

	// Rolled-up Gauge snapshots emitted with proper type & units.
	mSumOut, ok := findMetric(out, "req.delta.window.sum")
	require.True(t, ok)
	require.Equal(t, pmetric.MetricTypeGauge, mSumOut.Type())
	require.Equal(t, "By", mSumOut.Unit())
	require.InDelta(t, 40.0, mSumOut.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)

	mCntOut, ok := findMetric(out, "req.delta.window.count")
	require.True(t, ok)
	require.Equal(t, pmetric.MetricTypeGauge, mCntOut.Type())
	require.Equal(t, "1", mCntOut.Unit())
	require.Equal(t, int64(1), mCntOut.Gauge().DataPoints().At(0).IntValue())

	mRateOut, ok := findMetric(out, "req.delta.window.rate")
	require.True(t, ok)
	require.Equal(t, pmetric.MetricTypeGauge, mRateOut.Type())
	require.Equal(t, "By/s", mRateOut.Unit())
	require.InDelta(t, 4.0, mRateOut.Gauge().DataPoints().At(0).DoubleValue(), 1e-12) // 40 / 10s
}

func TestBaselineVsChange_PassthroughInputToggle(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	for _, passthrough := range []bool{false, true} {
		cfg := createDefaultConfig().(*Config)
		cfg.WindowSize = 10 * time.Second
		cfg.BucketDuration = time.Second
		cfg.IdleTTL = 20 * time.Second
		cfg.PassthroughInput = passthrough

		sink := &consumertest.MetricsSink{}
		p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
		md := makeDeltaSumMetric("host-1", "scope", "reqs", "", base, 5, nil, true)
		require.NoError(t, p.ConsumeMetrics(context.Background(), md))

		out := sink.AllMetrics()[0]
		_, hasOrig := findMetric(out, "reqs")
		require.Equal(t, passthrough, hasOrig, "passthrough=%v", passthrough)
		_, hasRollup := findMetric(out, "reqs.window.sum")
		require.True(t, hasRollup)
		mRate, ok := findMetric(out, "reqs.window.rate")
		require.True(t, ok)
		require.Equal(t, "1/s", mRate.Unit(), "empty unit rate must default to 1/s")
	}
}

func TestStreamIdentityIsolationAndComplexAttributes(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()

	// Two hosts sending the SAME metric name and identical datapoint attributes MUST NOT collide.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("host-A", "scope", "bytes", "By", base, 100, map[string]string{"k": "v"}, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("host-B", "scope", "bytes", "By", base, 200, map[string]string{"k": "v"}, false)))
	// Different scope name on host-A MUST NOT collide.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("host-A", "scope-2", "bytes", "By", base, 300, map[string]string{"k": "v"}, false)))

	require.Equal(t, int64(3), p.Stats().ActiveSeries)

	// Verify -0.0 and +0.0 double attributes map to the SAME stream identity,
	// two DISTINCT NaN payloads map to the SAME canonical NaN identity (non-tautological),
	// and distinct non-zero doubles (1.5 vs 2.5) map to DIFFERENT identities.
	buildComplex := func(zeroVal float64, nanVal float64, nonZero float64) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("complex.gauge")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
		dp.SetDoubleValue(10)
		dp.Attributes().PutDouble("z", zeroVal)
		dp.Attributes().PutDouble("n", nanVal)
		dp.Attributes().PutDouble("d", nonZero)
		dp.Attributes().PutBool("b", true)
		dp.Attributes().PutInt("i", 7)
		dp.Attributes().PutEmpty("e")
		dp.Attributes().PutEmptyBytes("raw").FromRaw([]byte{1, 2, 3})
		sl := dp.Attributes().PutEmptySlice("arr")
		sl.AppendEmpty().SetStr("x")
		mp := dp.Attributes().PutEmptyMap("nested")
		mp.PutBool("flag", false)
		return md
	}

	negZero := math.Copysign(0, -1)
	nanA := math.Float64frombits(0x7ff8000000000001)
	nanB := math.Float64frombits(0xfff800000000dead)
	require.NotEqual(t, math.Float64bits(nanA), math.Float64bits(nanB))
	require.NoError(t, p.ConsumeMetrics(context.Background(), buildComplex(0.0, nanA, 1.5)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), buildComplex(negZero, nanB, 1.5)))
	require.Equal(t, int64(4), p.Stats().ActiveSeries, "-0.0/+0.0 and distinct NaN payloads must map to same series")

	require.NoError(t, p.ConsumeMetrics(context.Background(), buildComplex(0.0, nanA, 2.5)))
	require.Equal(t, int64(5), p.Stats().ActiveSeries, "distinct non-zero double attributes (1.5 vs 2.5) must not collide")

	// Verify canonical key encoding (including ValueTypeBytes, Slice, nested Map)
	// and fnv1a128 hashing are deterministic across repeated encodes on a reused
	// keyBuilder (allocation regression testing lives in BenchmarkKeyBuilderCanonical).
	prebuilt := buildComplex(0.0, nanA, 1.5)
	prebuiltRM := prebuilt.ResourceMetrics().At(0)
	prebuiltSM := prebuiltRM.ScopeMetrics().At(0)
	prebuiltM := prebuiltSM.Metrics().At(0)
	prebuiltAttrs := prebuiltM.Gauge().DataPoints().At(0).Attributes()
	kb := acquireKeyBuilder()
	defer releaseKeyBuilder(kb)
	encodeOnce := func() (string, [2]uint64) {
		kb.buf = kb.buf[:0]
		kb.entries = kb.entries[:0]
		kb.appendResourcePrefix(prebuiltRM.Resource(), prebuiltRM.SchemaUrl())
		kb.appendScopePrefix(prebuiltSM.Scope(), prebuiltSM.SchemaUrl())
		kb.appendMetricPrefix(prebuiltM, pmetric.AggregationTemporalityUnspecified, false)
		kb.appendDataPointAttrs(prebuiltAttrs)
		return string(kb.buf), fnv1a128(kb.buf)
	}
	firstKey, firstHash := encodeOnce()
	secondKey, secondHash := encodeOnce()
	require.NotEmpty(t, firstKey)
	require.Equal(t, firstKey, secondKey, "canonical key encoding must be deterministic across buffer reuse")
	require.Equal(t, firstHash, secondHash, "fnv1a128 hash must be deterministic across buffer reuse")
}

func TestGaugeRateAllowFlagAndElapsedWindowMode(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()

	// Default allow_gauge_rate=false suppresses rate on Gauge inputs.
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 60 * time.Second
	cfg.BucketDuration = 5 * time.Second
	cfg.IdleTTL = 120 * time.Second
	cfg.RateMode = RateModeElapsedWindow
	cfg.AllowGaugeRate = false

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	mdGauge := pmetric.NewMetrics()
	g := mdGauge.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("cpu.temp")
	g.SetUnit("Cel")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dp.SetDoubleValue(50)
	require.NoError(t, p.ConsumeMetrics(context.Background(), mdGauge))
	_, hasRate := findMetric(sink.AllMetrics()[0], "cpu.temp.window.rate")
	require.False(t, hasRate, "gauge rate must be omitted when allow_gauge_rate=false")

	// Elapsed window warmup rate on Delta Sum:
	// - t+0s: denom clamped to bucket_duration=5s (0 < 5s)
	// - t+2s: denom clamped to bucket_duration=5s (d < bktU branch!)
	// - t+15s: denom = 15s (between bucket_duration and window_size)
	// - t+45s (keep window alive), then t+70s: elapsed=70s > 60s -> clamped to window_size=60s (d > winU branch!)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "req", "1", base, 50, nil, false)))
	mR, ok := findMetric(sink.AllMetrics()[1], "req.window.rate")
	require.True(t, ok)
	require.InDelta(t, 50.0/5.0, mR.Gauge().DataPoints().At(0).DoubleValue(), 1e-12) // 50 / 5s

	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "req", "1", base.Add(2*time.Second), 25, nil, false)))
	mR, _ = findMetric(sink.AllMetrics()[2], "req.window.rate")
	require.InDelta(t, 75.0/5.0, mR.Gauge().DataPoints().At(0).DoubleValue(), 1e-12) // d=2s < bktU=5s -> clamp to 5s

	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "req", "1", base.Add(15*time.Second), 75, nil, false)))
	mR, _ = findMetric(sink.AllMetrics()[3], "req.window.rate")
	require.InDelta(t, 150.0/15.0, mR.Gauge().DataPoints().At(0).DoubleValue(), 1e-12) // 150 / 15s

	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "req", "1", base.Add(45*time.Second), 30, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "req", "1", base.Add(70*time.Second), 120, nil, false)))
	// At t+70s (bucket 14), window covers buckets [3..14] = [15s..75s): points at t+0 (50) and t+2 (25) expired,
	// leaving t+15 (75) + t+45 (30) + t+70 (120) = 225. firstSeen=t+0 -> elapsed=70s > 60s -> clamped to 60s!
	mR, _ = findMetric(sink.AllMetrics()[5], "req.window.rate")
	require.InDelta(t, 225.0/60.0, mR.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)

	// Now enable allow_gauge_rate=true and verify gauge rate emits.
	cfg2 := createDefaultConfig().(*Config)
	cfg2.WindowSize = 10 * time.Second
	cfg2.BucketDuration = time.Second
	cfg2.IdleTTL = 20 * time.Second
	cfg2.AllowGaugeRate = true
	sink2 := &consumertest.MetricsSink{}
	p2 := newMetricAggregationProcessor(cfg2, zap.NewNop(), sink2)
	mdG2 := pmetric.NewMetrics()
	g2 := mdG2.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g2.SetName("cpu.temp")
	dp2 := g2.SetEmptyGauge().DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dp2.SetDoubleValue(50)
	require.NoError(t, p2.ConsumeMetrics(context.Background(), mdG2))
	mGR, ok := findMetric(sink2.AllMetrics()[0], "cpu.temp.window.rate")
	require.True(t, ok)
	require.InDelta(t, 5.0, mGR.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestNoRecordedValueMissingTimestampAndNaNDoNotConsumeCardinality(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second
	cfg.MaxSeries = 2
	cfg.FallbackToArrivalTime = false

	now := time.Unix(1700000000, 0).UTC()
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.clock = func() time.Time { return now }

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.gauge")
	dps := m.SetEmptyGauge().DataPoints()

	// 1. NoRecordedValue flag (must NOT ingest 0)
	dp1 := dps.AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp1.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	dp1.Attributes().PutStr("id", "nrv")

	// 2. Empty value type (must NOT ingest 0)
	dpEmpty := dps.AppendEmpty()
	dpEmpty.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dpEmpty.Attributes().PutStr("id", "empty")

	// 3. Timestamp == 0 with FallbackToArrivalTime=false (must drop)
	dpZeroTS := dps.AppendEmpty()
	dpZeroTS.SetDoubleValue(42)
	dpZeroTS.Attributes().PutStr("id", "zero_ts")

	// 4. NaN, +Inf, overflow spam with unique labels (must NOT consume max_series=2!)
	for i, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1e200} {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.SetDoubleValue(bad)
		dp.Attributes().PutStr("id", fmt.Sprintf("bad-%d", i))
	}

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	stats := p.Stats()
	require.Equal(t, int64(0), stats.ActiveSeries, "dropped/invalid datapoints must NOT consume max_series slots")
	require.Equal(t, uint64(2), stats.DroppedNoRecordedValue)
	require.Equal(t, uint64(1), stats.DroppedMissingTimestamp)
	require.Equal(t, uint64(3), stats.DroppedNonFinite)
	require.Equal(t, uint64(1), stats.DroppedOverflow)

	// Confirm both legitimate series slots are still free!
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h1", "s", "ok", "", now, 10, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h2", "s", "ok", "", now, 20, nil, false)))
	require.Equal(t, int64(2), p.Stats().ActiveSeries)

	// Test FallbackToArrivalTime=true stamps Timestamp==0 with clock.Now().
	cfg2 := createDefaultConfig().(*Config)
	cfg2.WindowSize = 10 * time.Second
	cfg2.BucketDuration = time.Second
	cfg2.IdleTTL = 20 * time.Second
	cfg2.FallbackToArrivalTime = true
	sink2 := &consumertest.MetricsSink{}
	p2 := newMetricAggregationProcessor(cfg2, zap.NewNop(), sink2)
	p2.clock = func() time.Time { return now }

	mdZ := pmetric.NewMetrics()
	dpZ := mdZ.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	mdZ.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName("ts_fallback")
	dpZ.SetDoubleValue(99)
	require.NoError(t, p2.ConsumeMetrics(context.Background(), mdZ))
	mOut, ok := findMetric(sink2.AllMetrics()[0], "ts_fallback.window.sum")
	require.True(t, ok)
	require.Equal(t, pcommon.NewTimestampFromTime(now), mOut.Gauge().DataPoints().At(0).Timestamp())
}

func TestMaxFutureSkewAgainstWallClockWithoutQuietDeadlock(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 10 * time.Minute
	cfg.MaxFutureSkew = 30 * time.Second

	now := time.Unix(1700000000, 0).UTC()
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.clock = func() time.Time { return now }

	// Ingest initial valid point at T=0.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", now, 10, nil, false)))

	// Rogue point 1 year in the future MUST be dropped against clock.Now() without wiping window.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", now.Add(365*24*time.Hour), 9999, nil, false)))
	require.Equal(t, uint64(1), p.Stats().DroppedFutureSkew)

	// uint64 timestamp > math.MaxInt64 MUST also be rejected cleanly.
	mdHuge := pmetric.NewMetrics()
	mh := mdHuge.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mh.SetName("m")
	dpHuge := mh.SetEmptyGauge().DataPoints().AppendEmpty()
	dpHuge.SetTimestamp(pcommon.Timestamp(math.MaxUint64))
	dpHuge.SetDoubleValue(1)
	require.NoError(t, p.ConsumeMetrics(context.Background(), mdHuge))
	require.Equal(t, uint64(2), p.Stats().DroppedFutureSkew)

	// Quiet-series anti-deadlock check: advance wall clock by 2 minutes (> MaxFutureSkew=30s, < IdleTTL=10m).
	// A new valid datapoint at the new wall-clock time MUST be accepted, NOT rejected against old headBucket!
	now = now.Add(2 * time.Minute)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", now, 77, nil, false)))
	require.Equal(t, uint64(2), p.Stats().DroppedFutureSkew)
	mOut, ok := findMetric(sink.AllMetrics()[len(sink.AllMetrics())-1], "m.window.sum")
	require.True(t, ok)
	require.InDelta(t, 77.0, mOut.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestConcurrentCardinalityCapStrictCAS(t *testing.T) {
	const maxSeries = 250
	const totalSeries = 2 * maxSeries // 500 unique streams across 16 goroutines
	const workers = 16

	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 60 * time.Second
	cfg.MaxSeries = maxSeries
	cfg.EmitInterval = time.Hour // avoid noisy per-batch emission during concurrency stress

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()
	p.clock = func() time.Time { return base }

	var wg sync.WaitGroup
	ids := make(chan int, totalSeries)
	for i := 0; i < totalSeries; i++ {
		ids <- i
	}
	close(ids)

	errCh := make(chan error, totalSeries)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				md := makeDeltaSumMetric(fmt.Sprintf("host-%d", id), "scope", "reqs", "", base, float64(id), nil, false)
				if err := p.ConsumeMetrics(context.Background(), md); err != nil {
					errCh <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	stats := p.Stats()
	require.Equal(t, int64(maxSeries), stats.ActiveSeries, "strict CAS cap must never overshoot max_series")
	require.Equal(t, uint64(totalSeries-maxSeries), stats.DroppedCardinality)

	// Confirm actual sum of shard map lengths matches atomic ActiveSeries.
	actual := 0
	for i := range p.shards {
		actual += len(p.shards[i].series)
	}
	require.Equal(t, maxSeries, actual)
}

func TestIdleEvictionAndEmptyWindowEmissionSemantics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 30 * time.Second
	cfg.Engine = engine.KindTwoStackMonoid
	cfg.Aggregations = []AggregationType{AggSum, AggCount, AggMean, AggStdDev, AggRate, AggMin, AggMax}
	cfg.EmitInterval = time.Hour // manual deterministic Flush

	now := time.Unix(1700000000, 0).UTC()
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.clock = func() time.Time { return now }

	// Add 2 points into bucket 0 (t=0) and bucket 1 (t=1s).
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h1", "s", "lat", "ms", now.Add(100*time.Millisecond), 10, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h1", "s", "lat", "ms", now.Add(1100*time.Millisecond), 30, nil, false)))

	// Flush at exact bucket-aligned boundary t+10s -> Flush queries now-1ns covering [0s, 10s)!
	require.NoError(t, p.Flush(context.Background(), now.Add(10*time.Second)))
	got := collectMetricMap(sink.AllMetrics())
	require.InDelta(t, 40.0, got["lat.window.sum"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
	require.Equal(t, int64(2), got["lat.window.count"].Gauge().DataPoints().At(0).IntValue())
	require.InDelta(t, 20.0, got["lat.window.mean"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
	require.InDelta(t, math.Sqrt(200), got["lat.window.stddev"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
	require.InDelta(t, 4.0, got["lat.window.rate"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
	require.InDelta(t, 10.0, got["lat.window.min"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
	require.InDelta(t, 30.0, got["lat.window.max"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)

	// Flush at t+20s (> W=10s, < IdleTTL=30s): series is still tracked, window is EMPTY (count==0)!
	// MUST emit 0 for count/sum/rate and OMIT mean/stddev/min/max (never fabricate 0.0!).
	sink.Reset()
	require.NoError(t, p.Flush(context.Background(), now.Add(20*time.Second)))
	emptyMap := collectMetricMap(sink.AllMetrics())
	require.Equal(t, int64(0), emptyMap["lat.window.count"].Gauge().DataPoints().At(0).IntValue())
	require.Equal(t, 0.0, emptyMap["lat.window.sum"].Gauge().DataPoints().At(0).DoubleValue())
	require.Equal(t, 0.0, emptyMap["lat.window.rate"].Gauge().DataPoints().At(0).DoubleValue())
	for _, omitted := range []string{"lat.window.mean", "lat.window.stddev", "lat.window.min", "lat.window.max"} {
		_, present := emptyMap[omitted]
		require.False(t, present, "%s MUST be omitted on empty window (count==0)", omitted)
	}
	require.Equal(t, int64(1), p.Stats().ActiveSeries)

	// Flush at t+35s (>= IdleTTL=30s): series MUST be evicted and NOT emitted.
	sink.Reset()
	require.NoError(t, p.Flush(context.Background(), now.Add(35*time.Second)))
	require.Empty(t, sink.AllMetrics())
	require.Equal(t, int64(0), p.Stats().ActiveSeries)
	require.Equal(t, uint64(1), p.Stats().IdleEvicted)

	// Empty-window pruning check: when ONLY mean/stddev are configured and count==0,
	// Flush must NOT emit empty ResourceMetrics shells.
	cfgOnlyMean := createDefaultConfig().(*Config)
	cfgOnlyMean.WindowSize = 10 * time.Second
	cfgOnlyMean.BucketDuration = time.Second
	cfgOnlyMean.IdleTTL = 30 * time.Second
	cfgOnlyMean.Aggregations = []AggregationType{AggMean, AggStdDev}
	cfgOnlyMean.EmitInterval = time.Hour
	sinkM := &consumertest.MetricsSink{}
	pm := newMetricAggregationProcessor(cfgOnlyMean, zap.NewNop(), sinkM)
	pm.clock = func() time.Time { return now }
	require.NoError(t, pm.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h1", "s", "lat", "ms", now, 10, nil, false)))
	require.NoError(t, pm.Flush(context.Background(), now.Add(20*time.Second)))
	require.Empty(t, sinkM.AllMetrics(), "empty ResourceMetrics shells must be pruned when all aggs omitted")
}

func TestMetricNamesAllowlistAndBatchDeduplication(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second
	cfg.MetricNames = []string{"wanted"}
	cfg.VarianceMode = engine.VariancePopulation

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Unlisted metric passes through untouched.
	mSkip := sm.Metrics().AppendEmpty()
	mSkip.SetName("unlisted")
	dpS := mSkip.SetEmptyGauge().DataPoints().AppendEmpty()
	dpS.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpS.SetDoubleValue(5)

	// Listed metric receives 3 datapoints for the SAME series in one batch.
	// Output MUST deduplicate to 1 post-batch snapshot (count=3, sum=60, mean=20, pop stddev=sqrt(200/3)).
	mWant := sm.Metrics().AppendEmpty()
	mWant.SetName("wanted")
	dps := mWant.SetEmptyGauge().DataPoints()
	for i, v := range []float64{10, 20, 30} {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
		dp.SetDoubleValue(v)
	}

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	out := sink.AllMetrics()[0]
	_, hasUnlisted := findMetric(out, "unlisted")
	require.True(t, hasUnlisted)
	mMean, ok := findMetric(out, "wanted.window.mean")
	require.True(t, ok)
	require.Equal(t, 1, mMean.Gauge().DataPoints().Len(), "batch datapoints for same series must deduplicate to 1 rollup point")
	require.InDelta(t, 20.0, mMean.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)

	mSD, ok := findMetric(out, "wanted.window.stddev")
	require.True(t, ok)
	require.InDelta(t, math.Sqrt(200.0/3.0), mSD.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestPeriodicEmitLoopAndCleanShutdown(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 60 * time.Second
	cfg.EmitInterval = 15 * time.Millisecond

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	ctx := context.Background()

	require.NoError(t, p.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, p.Shutdown(ctx))
	}()

	now := time.Now()
	require.NoError(t, p.ConsumeMetrics(ctx, makeDeltaSumMetric("h1", "s", "periodic.req", "1", now, 42, nil, false)))

	require.Eventually(t, func() bool {
		return len(sink.AllMetrics()) > 0
	}, 2*time.Second, 10*time.Millisecond, "expected periodic emitter to flush rollup metrics")

	got := collectMetricMap(sink.AllMetrics())
	require.InDelta(t, 42.0, got["periodic.req.window.sum"].Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestNonMonotonicDeltaSumAndLateDropCounter(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 5 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 10 * time.Second

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()

	// Non-monotonic delta sum (+10, -4 -> sum=6, count=2, mean=3).
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("queue.delta")
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.SetIsMonotonic(false)
	dp1 := sum.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(10 * time.Second)))
	dp1.SetDoubleValue(10)
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(10*time.Second + 100*time.Millisecond)))
	dp2.SetDoubleValue(-4)
	// Expired late point at t+0 (10s behind headBucket -> dropped).
	dpLate := sum.DataPoints().AppendEmpty()
	dpLate.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpLate.SetDoubleValue(999)

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	require.Equal(t, uint64(1), p.Stats().DroppedLate)
	mSum, ok := findMetric(sink.AllMetrics()[0], "queue.delta.window.sum")
	require.True(t, ok)
	require.InDelta(t, 6.0, mSum.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestRateOnlyConfigPreservesGaugesWithoutBurningCardinality(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second
	cfg.Aggregations = []AggregationType{AggRate}
	cfg.AllowGaugeRate = false
	cfg.MaxSeries = 1
	cfg.PassthroughInput = false

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()
	p.clock = func() time.Time { return base }

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	mg := sm.Metrics().AppendEmpty()
	mg.SetName("cpu.util")
	dpg := mg.SetEmptyGauge().DataPoints().AppendEmpty()
	dpg.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dpg.SetDoubleValue(0.75)

	mdSum := sm.Metrics().AppendEmpty()
	mdSum.SetName("reqs")
	mdSum.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dps := mdSum.Sum().DataPoints().AppendEmpty()
	dps.SetTimestamp(pcommon.NewTimestampFromTime(base))
	dps.SetDoubleValue(20)

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	out := sink.AllMetrics()[0]
	// Gauge MUST pass through untouched and consume 0 max_series slots!
	_, hasGauge := findMetric(out, "cpu.util")
	require.True(t, hasGauge, "Gauge must pass through when only AggRate is configured and allow_gauge_rate=false")
	// Delta Sum gets the single max_series=1 slot and emits reqs.window.rate.
	require.Equal(t, int64(1), p.Stats().ActiveSeries)
	require.Equal(t, uint64(0), p.Stats().DroppedCardinality)
	mRate, ok := findMetric(out, "reqs.window.rate")
	require.True(t, ok)
	require.InDelta(t, 2.0, mRate.Gauge().DataPoints().At(0).DoubleValue(), 1e-12)
}

func TestFlushPositiveClockSkewValidTimestampsAndElapsedRate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 60 * time.Second
	cfg.MaxFutureSkew = time.Minute
	cfg.RateMode = RateModeElapsedWindow
	cfg.EmitInterval = time.Hour

	base := time.Unix(1700000000, 0).UTC()
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.clock = func() time.Time { return base }

	// Host with +15s skew sends points at base+15s and base+20s (both within max_future_skew=1m).
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("skewed", "s", "req", "1", base.Add(15*time.Second), 10, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("skewed", "s", "req", "1", base.Add(20*time.Second), 40, nil, false)))

	// Flush at wall-clock base (earlier than base+20s!).
	require.NoError(t, p.Flush(context.Background(), base))
	got := collectMetricMap(sink.AllMetrics())
	dpSum := got["req.window.sum"].Gauge().DataPoints().At(0)
	require.LessOrEqual(t, dpSum.StartTimestamp(), dpSum.Timestamp(), "StartTimestamp must never exceed Timestamp")
	require.Equal(t, pcommon.NewTimestampFromTime(base.Add(20*time.Second)), dpSum.Timestamp())
	// Elapsed span is (base+20s) - (base+15s) = 5s -> rate = 50 / 5 = 10/s (NOT 50 / 1 = 50/s!).
	dpRate := got["req.window.rate"].Gauge().DataPoints().At(0)
	require.InDelta(t, 10.0, dpRate.DoubleValue(), 1e-12)
}

func TestAlgebraicNaiveEmptyWindowSumClampedToZero(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 3 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 30 * time.Second
	cfg.Engine = engine.KindAlgebraicNaive
	cfg.EmitInterval = time.Hour

	base := time.Unix(1700000000, 0).UTC()
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.clock = func() time.Time { return base }

	// 1e16 absorbs 1.0 in naive float64, causing phantom -1.0 when window empties.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", base, 1e16, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", base.Add(time.Second), 1.0, nil, false)))
	require.NoError(t, p.Flush(context.Background(), base.Add(4*time.Second)))
	sink.Reset()
	require.NoError(t, p.Flush(context.Background(), base.Add(5*time.Second)))

	// Verify the underlying algebraic_naive engine actually produces phantom -1.0
	// residual drift (making this clamping test non-tautological).
	var rawSum float64
	foundSeries := false
	for i := range p.shards {
		for _, entry := range p.shards[i].series {
			rawSum = entry.win.Snapshot().Sum
			foundSeries = true
		}
	}
	require.True(t, foundSeries, "expected active algebraic_naive series before idle_ttl")
	require.InDelta(t, -1.0, rawSum, 1e-12, "algebraic_naive raw Snapshot must expose phantom -1.0 drift on empty window")

	got := collectMetricMap(sink.AllMetrics())
	require.Equal(t, int64(0), got["m.window.count"].Gauge().DataPoints().At(0).IntValue())
	require.Equal(t, 0.0, got["m.window.sum"].Gauge().DataPoints().At(0).DoubleValue(), "empty window sum must be clamped to 0.0")
	require.Equal(t, 0.0, got["m.window.rate"].Gauge().DataPoints().At(0).DoubleValue())
}

func TestIdleTTLActiveInDefaultSyncModeAndMonotonicRefoldStats(t *testing.T) {
	// 1. Lazy eviction in default sync mode (EmitInterval == 0, WITHOUT Start()).
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 5 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 10 * time.Second
	cfg.MaxSeries = 2
	cfg.EmitInterval = 0

	curTime := time.Unix(1700000000, 0).UTC()
	var clockMu sync.Mutex
	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	p.lazySweepCooldown = 0
	p.clock = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return curTime
	}

	// Series A triggers a cancellation refold (1e17 spike + 1.0 + 2.0, then spike evicts).
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hA", "s", "m", "", curTime, 1e17, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hA", "s", "m", "", curTime.Add(time.Second), 1.0, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hA", "s", "m", "", curTime.Add(2*time.Second), 2.0, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hA", "s", "m", "", curTime.Add(5*time.Second), 3.0, nil, false)))
	refoldsBefore := p.Stats().Refolds
	require.GreaterOrEqual(t, refoldsBefore, uint64(1), "expected hA to trigger refold")

	// Fill second slot (max_series=2).
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hB", "s", "m", "", curTime, 10.0, nil, false)))
	require.Equal(t, int64(2), p.Stats().ActiveSeries)

	// While still fresh (not idle), third series MUST be dropped.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hC", "s", "m", "", curTime, 20.0, nil, false)))
	require.Equal(t, uint64(1), p.Stats().DroppedCardinality)

	// Advance clock past IdleTTL=10s -> lazy eviction reclaims idle hA and hB in default sync mode!
	// Adding two fresh series (hC and hD) guarantees BOTH hA and hB are evicted even if hC
	// hashes to the same shard as hA (or hB) and reclaims only that shard on the first insert.
	clockMu.Lock()
	curTime = curTime.Add(15 * time.Second)
	clockMu.Unlock()
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hC", "s", "m", "", curTime, 20.0, nil, false)))
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hD", "s", "m", "", curTime, 30.0, nil, false)))
	stats := p.Stats()
	require.Equal(t, int64(2), stats.ActiveSeries, "lazy sweep must evict idle hA & hB and admit hC & hD")
	require.Equal(t, uint64(2), stats.IdleEvicted)
	require.Equal(t, uint64(1), stats.DroppedCardinality, "neither hC nor hD should be dropped after IdleTTL")
	// Refold counter MUST remain strictly monotonic after hA was evicted!
	require.Equal(t, refoldsBefore, stats.Refolds, "refold stats must not drop backwards on idle eviction")
	require.GreaterOrEqual(t, stats.CancelRefolds, uint64(1))

	// 2. Background idleReaperLoop in default sync mode (EmitInterval == 0, IdleTTL > 0) with Start()/Shutdown().
	cfg2 := createDefaultConfig().(*Config)
	cfg2.WindowSize = 5 * time.Second
	cfg2.BucketDuration = time.Second
	cfg2.IdleTTL = 10 * time.Second
	cfg2.EmitInterval = 0

	var bgNow atomic.Int64
	bgNow.Store(time.Unix(1700000000, 0).UnixNano())
	p2 := newMetricAggregationProcessor(cfg2, zap.NewNop(), &consumertest.MetricsSink{})
	p2.reaperInterval = 10 * time.Millisecond
	p2.clock = func() time.Time { return time.Unix(0, bgNow.Load()) }

	require.NoError(t, p2.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p2.Shutdown(context.Background())) }()

	t0 := time.Unix(0, bgNow.Load())
	require.NoError(t, p2.ConsumeMetrics(context.Background(), makeDeltaSumMetric("bg1", "s", "m", "", t0, 5, nil, false)))
	require.Equal(t, int64(1), p2.Stats().ActiveSeries)

	bgNow.Store(t0.Add(15 * time.Second).UnixNano())
	require.Eventually(t, func() bool {
		return p2.Stats().ActiveSeries == 0 && p2.Stats().IdleEvicted == 1
	}, 2*time.Second, 10*time.Millisecond, "background idleReaperLoop must evict stale series in sync mode")
}

func TestMultiSeriesSameMetricCacheHitAndConcurrentConsumeFlush(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 60 * time.Second

	sink := &consumertest.MetricsSink{}
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
	base := time.Unix(1700000000, 0).UTC()
	p.clock = func() time.Time { return base }

	// Single batch with 3 distinct series sharing (Resource, Scope, Metric) exercises
	// getOrCreateGauge cache hit inside appendSamples.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("host", "h1")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("svc")
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.dur")
	m.SetUnit("ms")
	dps := m.SetEmptyGauge().DataPoints()
	for i, route := range []string{"/a", "/b", "/c"} {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
		dp.Attributes().PutStr("route", route)
		dp.SetDoubleValue(float64((i + 1) * 10))
	}
	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	mSum, ok := findMetric(sink.AllMetrics()[0], "http.dur.window.sum")
	require.True(t, ok)
	require.Equal(t, 3, mSum.Gauge().DataPoints().Len(), "all 3 routes must append to the same output metric")

	// Concurrent ConsumeMetrics + Flush + Stats on overlapping series under -race.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for step := 0; step < 30; step++ {
				ts := base.Add(time.Duration(step) * 200 * time.Millisecond)
				batch := makeDeltaSumMetric("shared", "svc", "hot", "1", ts, float64(step+id), map[string]string{"k": fmt.Sprintf("%d", id%3)}, false)
				assert.NoError(t, p.ConsumeMetrics(context.Background(), batch))
			}
		}(g)
	}
	for f := 0; f < 3; f++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for step := 0; step < 15; step++ {
				assert.NoError(t, p.Flush(context.Background(), base.Add(time.Duration(step)*time.Second)))
				_ = p.Stats()
			}
		}()
	}
	wg.Wait()

	// Verify every emitted datapoint across concurrent ConsumeMetrics + Flush calls
	// satisfies the OTLP interval invariant StartTimestamp <= Timestamp.
	checked := 0
	for _, batch := range sink.AllMetrics() {
		for i := 0; i < batch.ResourceMetrics().Len(); i++ {
			rm := batch.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					if m.Type() != pmetric.MetricTypeGauge {
						continue
					}
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						require.LessOrEqual(t, dp.StartTimestamp(), dp.Timestamp(),
							"metric %s emitted inverted interval: StartTimestamp=%d > Timestamp=%d",
							m.Name(), dp.StartTimestamp(), dp.Timestamp())
						checked++
					}
				}
			}
		}
	}
	require.Greater(t, checked, 0, "expected emitted datapoints during concurrent stress")
}

func TestLazySweepPerShardCooldownAndMonotonicOtherShardSweep(t *testing.T) {
	// 1. Verify shouldSweepOtherShards is strictly monotonic (no backward rewind / ping-pong)
	//    and handles Unix epoch 0 cleanly.
	cfg0 := createDefaultConfig().(*Config)
	p0 := newMetricAggregationProcessor(cfg0, zap.NewNop(), &consumertest.MetricsSink{})
	require.True(t, p0.shouldSweepOtherShards(0), "first sweep at Unix epoch 0 must succeed")
	require.False(t, p0.shouldSweepOtherShards(0), "duplicate sweep at epoch 0 within cooldown must skip")
	require.False(t, p0.shouldSweepOtherShards(-int64(time.Second)), "stale earlier timestamp must not rewind lastLazySweep")
	require.True(t, p0.shouldSweepOtherShards(int64(2*time.Second)), "sweep past 1s cooldown must succeed")

	pMono := newMetricAggregationProcessor(cfg0, zap.NewNop(), &consumertest.MetricsSink{})
	tA := time.Unix(1700000000, 0).UnixNano()
	tB := tA + int64(2*time.Second)
	triggered := 0
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			if pMono.shouldSweepOtherShards(tB) {
				triggered++
			}
		} else {
			if pMono.shouldSweepOtherShards(tA) {
				triggered++
			}
		}
	}
	require.Equal(t, 1, triggered, "alternating tB/tA must trigger at most 1 sweep, never ping-pong 40/40")

	// 2. Verify per-shard lazy sweep cooldown throttles rescans under saturation
	//    and reclaims idle entries once the cooldown elapses.
	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 5 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 10 * time.Second
	cfg.MaxSeries = 2
	cfg.EmitInterval = 0

	base := time.Unix(1700000000, 0).UTC()
	cur := base
	p := newMetricAggregationProcessor(cfg, zap.NewNop(), &consumertest.MetricsSink{})
	p.clock = func() time.Time { return cur }

	// Fill both slots: hA at t=0, hB at t=5s.
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hA", "s", "m", "", cur, 1, nil, false)))
	cur = base.Add(5 * time.Second)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hB", "s", "m", "", cur, 2, nil, false)))

	// At t=9.5s (neither idle yet), excess drop hC runs the initial sweep across all shards,
	// stamping lastSweepNanos = 9.5s and lastLazySweep = 9.5s.
	cur = base.Add(9500 * time.Millisecond)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hC", "s", "m", "", cur, 3, nil, false)))
	require.Equal(t, uint64(0), p.Stats().IdleEvicted)
	require.Equal(t, uint64(1), p.Stats().DroppedCardinality)

	// Confirm per-shard cooldown rejects stale and within-cooldown timestamps.
	for i := range p.shards {
		require.False(t, p.shouldSweepShardLocked(&p.shards[i], base.Add(9*time.Second).UnixNano()))
		require.False(t, p.shouldSweepShardLocked(&p.shards[i], base.Add(10200*time.Millisecond).UnixNano()))
	}

	// At t=10.2s, hA is idle (>= 10s), but only 0.7s < 1s cooldown elapsed since the 9.5s sweep.
	// Neither local shard sweep nor cross-shard sweep should fire yet.
	cur = base.Add(10200 * time.Millisecond)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hD", "s", "m", "", cur, 4, nil, false)))
	require.Equal(t, uint64(0), p.Stats().IdleEvicted, "cooldown must prevent unthrottled per-shard rescan within 1s")
	require.Equal(t, uint64(2), p.Stats().DroppedCardinality)

	// At t=10.6s (1.1s >= 1s cooldown), sweep runs, evicts idle hA, and admits hE.
	cur = base.Add(10600 * time.Millisecond)
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("hE", "s", "m", "", cur, 5, nil, false)))
	require.Equal(t, uint64(1), p.Stats().IdleEvicted, "idle hA must be evicted once cooldown elapses")
	require.Equal(t, uint64(2), p.Stats().DroppedCardinality)
	require.Equal(t, int64(2), p.Stats().ActiveSeries)
}

func TestFlushBackwardClockNeverInvertsStartTimestamp(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	cases := []struct {
		name      string
		eng       engine.Kind
		win       time.Duration
		bkt       time.Duration
		advFlush  time.Duration
		backFlush time.Duration
	}{
		{
			name:      "algebraic_chan_B1_2ms_ntp_step_back",
			eng:       engine.KindAlgebraicChan,
			win:       5 * time.Second,
			bkt:       5 * time.Second,
			advFlush:  5001 * time.Millisecond,
			backFlush: 4999 * time.Millisecond,
		},
		{
			name:      "algebraic_chan_B10_backward_flush",
			eng:       engine.KindAlgebraicChan,
			win:       10 * time.Second,
			bkt:       time.Second,
			advFlush:  14 * time.Second,
			backFlush: 0,
		},
		{
			name:      "raw_rescan_backward_flush",
			eng:       engine.KindRawRescan,
			win:       10 * time.Second,
			bkt:       time.Second,
			advFlush:  15 * time.Second,
			backFlush: 2 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.Engine = tc.eng
			cfg.WindowSize = tc.win
			cfg.BucketDuration = tc.bkt
			cfg.IdleTTL = time.Hour
			cfg.EmitInterval = time.Hour

			sink := &consumertest.MetricsSink{}
			p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
			p.clock = func() time.Time { return base }

			require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "1", base, 10, nil, false)))
			require.NoError(t, p.Flush(context.Background(), base.Add(tc.advFlush)))
			sink.Reset()
			require.NoError(t, p.Flush(context.Background(), base.Add(tc.backFlush)))

			got := collectMetricMap(sink.AllMetrics())
			require.NotEmpty(t, got)
			for name, m := range got {
				dps := m.Gauge().DataPoints()
				for i := 0; i < dps.Len(); i++ {
					dp := dps.At(i)
					require.LessOrEqual(t, dp.StartTimestamp(), dp.Timestamp(),
						"%s emitted inverted interval: start=%d > ts=%d", name, dp.StartTimestamp(), dp.Timestamp())
				}
			}
			require.Equal(t, int64(0), got["m.window.count"].Gauge().DataPoints().At(0).IntValue())
			require.Equal(t, 0.0, got["m.window.sum"].Gauge().DataPoints().At(0).DoubleValue())
			require.Equal(t, 0.0, got["m.window.rate"].Gauge().DataPoints().At(0).DoubleValue())
		})
	}
}

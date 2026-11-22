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
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
)

type discardMetricsConsumer struct {
	datapoints atomic.Int64
}

func (*discardMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *discardMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	c.datapoints.Add(int64(md.DataPointCount()))
	return nil
}

var benchHashSink [2]uint64

// warmKeyBuilderPool runs double GC (normalizing heap state across engines so
// prior rawRing.grow() GC cycles do not asymmetrically evict keyBuilderPool),
// then pre-populates count warm keyBuilders into keyBuilderPool so any P
// can obtain or steal a pre-sized buffer without triggering pool-cold allocs.
func warmKeyBuilderPool(count int) {
	runtime.GC()
	runtime.GC()
	kbs := make([]*keyBuilder, count)
	for i := range kbs {
		kb := &keyBuilder{
			buf:     make([]byte, 0, 512),
			entries: make([]kvEntry, 0, 16),
		}
		kbs[i] = kb
	}
	for _, kb := range kbs {
		keyBuilderPool.Put(kb)
	}
}

func BenchmarkKeyBuilderCanonical(b *testing.B) {
	buildFlat := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
		rm.Resource().Attributes().PutStr("host.name", "node-042")
		rm.Resource().Attributes().PutStr("service.name", "checkout")
		rm.Resource().Attributes().PutStr("cloud.region", "us-central1")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
		sm.Scope().SetVersion("0.56.0")
		m := sm.Metrics().AppendEmpty()
		m.SetName("http.server.request.duration")
		m.SetUnit("ms")
		dp := m.SetEmptySum().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("http.route", "/api/v1/orders")
		dp.Attributes().PutStr("http.request.method", "POST")
		dp.Attributes().PutInt("http.response.status_code", 200)
		dp.Attributes().PutBool("tls", true)
		return md
	}
	buildComplex := func() pmetric.Metrics {
		md := buildFlat()
		dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
		dp.Attributes().PutEmptyBytes("raw").FromRaw([]byte{0x01, 0x02, 0x03, 0x04})
		sl := dp.Attributes().PutEmptySlice("tags")
		sl.AppendEmpty().SetStr("prod")
		sl.AppendEmpty().SetInt(42)
		nested := dp.Attributes().PutEmptyMap("nested")
		nested.PutStr("k1", "v1")
		nested.PutDouble("k2", 1.5)
		return md
	}

	cases := []struct {
		name string
		md   pmetric.Metrics
	}{
		{name: "flat", md: buildFlat()},
		{name: "complex", md: buildComplex()},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			rm := tc.md.ResourceMetrics().At(0)
			sm := rm.ScopeMetrics().At(0)
			m := sm.Metrics().At(0)
			dp := m.Sum().DataPoints().At(0)

			kb := acquireKeyBuilder()
			defer releaseKeyBuilder(kb)
			// Warm buffer capacity once (mirrors steady-state pooled keyBuilder).
			kb.appendResourcePrefix(rm.Resource(), rm.SchemaUrl())
			kb.appendScopePrefix(sm.Scope(), sm.SchemaUrl())
			kb.appendMetricPrefix(m, pmetric.AggregationTemporalityDelta, true)
			kb.appendDataPointAttrs(dp.Attributes())

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				kb.buf = kb.buf[:0]
				kb.entries = kb.entries[:0]
				kb.appendResourcePrefix(rm.Resource(), rm.SchemaUrl())
				kb.appendScopePrefix(sm.Scope(), sm.SchemaUrl())
				kb.appendMetricPrefix(m, pmetric.AggregationTemporalityDelta, true)
				kb.appendDataPointAttrs(dp.Attributes())
				benchHashSink = fnv1a128(kb.buf)
			}
		})
	}
}

// buildRotatingBatches builds 10 batches of 1,000 datapoints covering 10,000
// distinct series (100 hosts x 100 routes) sharing realistic OTLP structure.
func buildRotatingBatches(hosts, routes int, baseTS time.Time) []pmetric.Metrics {
	const batchCount = 10
	hostsPerBatch := hosts / batchCount // 10 hosts * 100 routes = 1,000 dps/batch
	batches := make([]pmetric.Metrics, batchCount)
	ts := pcommon.NewTimestampFromTime(baseTS)

	for bi := 0; bi < batchCount; bi++ {
		md := pmetric.NewMetrics()
		for hi := 0; hi < hostsPerBatch; hi++ {
			hostID := bi*hostsPerBatch + hi
			rm := md.ResourceMetrics().AppendEmpty()
			rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
			rm.Resource().Attributes().PutStr("host.name", fmt.Sprintf("node-%03d", hostID))
			rm.Resource().Attributes().PutStr("service.name", "payments")
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("otel.http")
			sm.Scope().SetVersion("1.0.0")
			m := sm.Metrics().AppendEmpty()
			m.SetName("http.server.requests")
			m.SetUnit("1")
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			sum.SetIsMonotonic(true)
			dps := sum.DataPoints()
			dps.EnsureCapacity(routes)
			for r := 0; r < routes; r++ {
				dp := dps.AppendEmpty()
				dp.SetTimestamp(ts)
				dp.Attributes().PutStr("http.route", fmt.Sprintf("/api/v1/r%03d", r))
				dp.Attributes().PutStr("method", "GET")
				dp.SetDoubleValue(float64(10 + (hostID+r)%90))
			}
		}
		batches[bi] = md
	}
	return batches
}

// stampBatchTSAndVal stamps timestamp ts and a non-constant deterministic value
// matching warmup variance so steady-state M2 stays strictly positive across rotations
// (preventing artificial synchronized M2 -> 0 cancellation refolds).
func stampBatchTSAndVal(md pmetric.Metrics, ts pcommon.Timestamp, tick int) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				dps := ms.At(k).Sum().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					dp := dps.At(d)
					dp.SetTimestamp(ts)
					dp.SetDoubleValue(float64(10 + (tick+d)%90))
				}
			}
		}
	}
}

func BenchmarkConsumeMetricsBatch(b *testing.B) {
	const (
		totalSeries = 10000
		batchSize   = 1000
		hosts       = 100
		routes      = 100
		warmupTicks = 62 // ticks 0..61 (base+0s .. base+305s) -> exact 60 live samples/buckets [2..61]
	)
	base := time.Unix(1700000000, 0).UTC()
	ctx := context.Background()

	for _, mode := range []string{"periodic", "sync"} {
		for _, kind := range engine.AllKinds() {
			name := fmt.Sprintf("mode=%s/batch=%d/engine=%s", mode, batchSize, kind)
			b.Run(name, func(b *testing.B) {
				cfg := createDefaultConfig().(*Config)
				cfg.Engine = kind
				cfg.WindowSize = 5 * time.Minute
				cfg.BucketDuration = 5 * time.Second
				cfg.MaxSeries = totalSeries
				cfg.IdleTTL = 0 // disabled during monotonic simulated-time microbenchmark
				if mode == "periodic" {
					cfg.EmitInterval = time.Hour
					cfg.PassthroughInput = true
				} else {
					cfg.EmitInterval = 0
					cfg.PassthroughInput = false
				}

				sink := &discardMetricsConsumer{}
				p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
				simNow := base
				p.clock = func() time.Time { return simNow }

				batches := buildRotatingBatches(hosts, routes, base)

				// Register all 10,000 series once via ConsumeMetrics, then warm the remaining
				// 61 ticks directly on each series window (exact 60 live samples/buckets in
				// [2..61], cap=64 in rawRing) so benchmark setup stays fast while steady-state
				// evictions run on every measured iteration with 0 ring-growth allocations.
				for bi := range batches {
					in := batches[bi]
					if mode == "sync" {
						in = pmetric.NewMetrics()
						batches[bi].CopyTo(in)
					}
					if err := p.ConsumeMetrics(ctx, in); err != nil {
						b.Fatal(err)
					}
				}
				baseNanos := base.UnixNano()
				for si := range p.shards {
					for _, entry := range p.shards[si].series {
						for tick := 1; tick < warmupTicks; tick++ {
							ts := baseNanos + int64(tick)*int64(5*time.Second)
							entry.win.Add(ts, float64(10+(tick%90)))
						}
						entry.lastArrivalNanos = baseNanos + int64(warmupTicks-1)*int64(5*time.Second)
					}
				}
				if got := p.Stats().ActiveSeries; got != totalSeries {
					b.Fatalf("expected %d active series after warmup, got %d", totalSeries, got)
				}

				// Start curTS at the last warmed tick (base + 61*5s = base+305s) so i=0
				// (bi==0) advances +5s to base+310s (bucket 62), advancing 1 bucket per
				// 10-batch rotation and retaining exact 60 live samples/buckets on every op.
				tick := warmupTicks - 1
				curTS := base.Add(time.Duration(tick) * 5 * time.Second)

				// Normalize GC and sync.Pool across all 5 engines before timing so
				// rawRing.grow() warmup GC does not asymmetrically evict keyBuilderPool.
				warmKeyBuilderPool(runtime.GOMAXPROCS(0))

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bi := i % len(batches)
					if bi == 0 {
						tick++
						curTS = curTS.Add(5 * time.Second)
						simNow = curTS
					}
					pTS := pcommon.NewTimestampFromTime(curTS)
					if mode == "periodic" {
						stampBatchTSAndVal(batches[bi], pTS, tick)
						if err := p.ConsumeMetrics(ctx, batches[bi]); err != nil {
							b.Fatal(err)
						}
					} else {
						b.StopTimer()
						work := pmetric.NewMetrics()
						batches[bi].CopyTo(work)
						b.StartTimer()
						// Keep stampBatchTSAndVal inside the timer identically to mode=periodic.
						stampBatchTSAndVal(work, pTS, tick)
						if err := p.ConsumeMetrics(ctx, work); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				if got := p.Stats().DroppedLate; got != 0 {
					b.Fatalf("unexpected late drops in ConsumeMetricsBatch (%s/%s): %d", mode, kind, got)
				}
				// Verify sample series retains exact 60 live buckets/samples (not 59).
				for si := range p.shards {
					checked := false
					for _, entry := range p.shards[si].series {
						if c := entry.win.Snapshot().Count; c != 60 {
							b.Fatalf("expected 60 live samples/buckets in steady state (%s/%s), got %d", mode, kind, c)
						}
						checked = true
						break
					}
					if checked {
						break
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/dp")
			})
		}
	}
}

func BenchmarkConsumeMetricsParallel(b *testing.B) {
	const (
		minHosts    = 100
		routes      = 100
		batchSize   = 100
		warmupTicks = 62 // ticks 0..61 (base+0s .. base+305s) -> exact 60 live samples/buckets [2..61]
	)
	base := time.Unix(1700000000, 0).UTC()
	ctx := context.Background()

	numWorkers := runtime.GOMAXPROCS(0)
	// Ensure hosts >= numWorkers so every worker owns a strictly disjoint set of
	// pre-allocated host batches (0 in-timer clones and 0 cross-worker timestamp
	// races on any host, including >100 vCPUs; on <=100 vCPUs hosts==100, totalSeries==10,000).
	hosts := minHosts
	if numWorkers > hosts {
		hosts = numWorkers
	}
	totalSeries := hosts * routes

	for _, kind := range engine.AllKinds() {
		b.Run(fmt.Sprintf("batch=%d/engine=%s", batchSize, kind), func(b *testing.B) {
			cfg := createDefaultConfig().(*Config)
			cfg.Engine = kind
			cfg.WindowSize = 5 * time.Minute
			cfg.BucketDuration = 5 * time.Second
			cfg.MaxSeries = totalSeries
			cfg.IdleTTL = 0
			cfg.EmitInterval = time.Hour
			cfg.PassthroughInput = true

			sink := &discardMetricsConsumer{}
			p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
			p.clock = func() time.Time { return base }

			// Pre-build one dedicated 100-dp batch per host BEFORE ResetTimer so
			// setup allocations do not pollute RunParallel measurements.
			workerBatches := make([]pmetric.Metrics, hosts)
			for hostID := 0; hostID < hosts; hostID++ {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
				rm.Resource().Attributes().PutStr("host.name", fmt.Sprintf("node-%03d", hostID))
				rm.Resource().Attributes().PutStr("service.name", "payments")
				sm := rm.ScopeMetrics().AppendEmpty()
				sm.Scope().SetName("otel.http")
				sm.Scope().SetVersion("1.0.0")
				m := sm.Metrics().AppendEmpty()
				m.SetName("http.server.requests")
				m.SetUnit("1")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sum.SetIsMonotonic(true)
				dps := sum.DataPoints()
				dps.EnsureCapacity(routes)
				for r := 0; r < routes; r++ {
					dp := dps.AppendEmpty()
					dp.Attributes().PutStr("http.route", fmt.Sprintf("/api/v1/r%03d", r))
					dp.Attributes().PutStr("method", "GET")
					dp.SetDoubleValue(float64(10 + r%90))
				}
				workerBatches[hostID] = md
			}

			// Register all series once via ConsumeMetrics, then warm the remaining
			// 61 ticks directly on each window (exact 60 live samples/buckets [2..61],
			// cap=64 in rawRing) so steady-state evictions run on every iteration.
			initTS := pcommon.NewTimestampFromTime(base)
			for hostID := 0; hostID < hosts; hostID++ {
				dps := workerBatches[hostID].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
				for r := 0; r < routes; r++ {
					dps.At(r).SetTimestamp(initTS)
				}
				if err := p.ConsumeMetrics(ctx, workerBatches[hostID]); err != nil {
					b.Fatal(err)
				}
			}
			baseNanos := base.UnixNano()
			for si := range p.shards {
				for _, entry := range p.shards[si].series {
					for tick := 1; tick < warmupTicks; tick++ {
						ts := baseNanos + int64(tick)*int64(5*time.Second)
						entry.win.Add(ts, float64(10+(tick%90)))
					}
					entry.lastArrivalNanos = baseNanos + int64(warmupTicks-1)*int64(5*time.Second)
				}
			}

			// Per-host monotonic tick counter initialized to warmupTicks-1 (61) so the
			// first increment lands on tick 62 (base+310s, exact 1-bucket step, 60 live buckets).
			hostTicks := make([]int64, hosts)
			for h := 0; h < hosts; h++ {
				hostTicks[h] = warmupTicks - 1
			}

			// Pre-build strictly disjoint host partitions BEFORE ResetTimer so every
			// worker exercises its owned series round-robin with 0 allocations and 0 races.
			ownedByWorker := make([][]int, numWorkers)
			for h := 0; h < hosts; h++ {
				w := h % numWorkers
				ownedByWorker[w] = append(ownedByWorker[w], h)
			}

			// Normalize GC and pre-warm sync.Pool across all P slots before timing.
			warmKeyBuilderPool(numWorkers * 2)

			var workerSeq atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				wid := int(workerSeq.Add(1) - 1)
				owned := ownedByWorker[wid]
				pos := 0
				for pb.Next() {
					h := owned[pos]
					pos++
					if pos == len(owned) {
						pos = 0
					}
					hostTicks[h]++
					tVal := hostTicks[h]
					pTS := pcommon.NewTimestampFromTime(base.Add(time.Duration(tVal) * 5 * time.Second))
					md := workerBatches[h]
					dps := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
					for r := 0; r < routes; r++ {
						dp := dps.At(r)
						dp.SetTimestamp(pTS)
						dp.SetDoubleValue(float64(10 + (int(tVal)+r)%90))
					}
					if err := p.ConsumeMetrics(ctx, md); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			if got := p.Stats().DroppedLate; got != 0 {
				b.Fatalf("unexpected late drops in ConsumeMetricsParallel (%s): %d", kind, got)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/dp")
		})
	}
}

func BenchmarkProcessorFlush10kSeries(b *testing.B) {
	const (
		totalSeries   = 10000
		hosts         = 100
		routes        = 100
		windowSeconds = 300 // 300 live events/series = 3,000,000 live events (10k/s rate)
		fillSeconds   = 330 // 300s window + 30s slide (matches BenchmarkSnapshot: wraps raw_* rings & keeps twostack_monoid mid-cycle)
	)
	base := time.Unix(1700000000, 0).UTC()
	ctx := context.Background()

	for _, kind := range engine.AllKinds() {
		b.Run(fmt.Sprintf("series=%d/engine=%s", totalSeries, kind), func(b *testing.B) {
			cfg := createDefaultConfig().(*Config)
			cfg.Engine = kind
			cfg.WindowSize = 5 * time.Minute
			cfg.BucketDuration = 5 * time.Second
			cfg.MaxSeries = totalSeries
			cfg.IdleTTL = 10 * time.Minute // keep > 330s so Flush(base+330s) never evicts
			cfg.EmitInterval = time.Hour
			cfg.PassthroughInput = true

			sink := &discardMetricsConsumer{}
			p := newMetricAggregationProcessor(cfg, zap.NewNop(), sink)
			p.clock = func() time.Time { return base }

			// Register all 10,000 series once through ConsumeMetrics, then populate the remaining
			// 329 seconds directly on each series window (retaining exact 300 live events/series
			// while keeping twostack_monoid mid-cycle and raw_* rings wrapped).
			batches := buildRotatingBatches(hosts, routes, base)
			for _, md := range batches {
				if err := p.ConsumeMetrics(ctx, md); err != nil {
					b.Fatal(err)
				}
			}
			baseNanos := base.UnixNano()
			for si := range p.shards {
				for _, entry := range p.shards[si].series {
					for sec := 1; sec < fillSeconds; sec++ {
						ts := baseNanos + int64(sec)*int64(time.Second)
						entry.win.Add(ts, float64(100+(sec%50)))
					}
					entry.lastArrivalNanos = baseNanos + int64(fillSeconds-1)*int64(time.Second)
				}
			}

			flushTime := base.Add(time.Duration(fillSeconds) * time.Second)
			sink.datapoints.Store(0)

			// Verify one Flush emits exact 10k * 5 = 50,000 gauge datapoints and retains exact 300 live events/series.
			if err := p.Flush(ctx, flushTime); err != nil {
				b.Fatal(err)
			}
			if got := sink.datapoints.Load(); got != totalSeries*5 {
				b.Fatalf("expected %d emitted gauge datapoints per Flush, got %d", totalSeries*5, got)
			}
			for si := range p.shards {
				checked := false
				for _, entry := range p.shards[si].series {
					if c := entry.win.Snapshot().Count; c != windowSeconds {
						b.Fatalf("expected %d live events/series after 330s fill (%s), got %d", windowSeconds, kind, c)
					}
					checked = true
					break
				}
				if checked {
					break
				}
			}

			runtime.GC()
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.Flush(ctx, flushTime); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if got := p.Stats().ActiveSeries; got != totalSeries {
				b.Fatalf("Flush unexpectedly evicted active series: got %d, want %d", got, totalSeries)
			}
			wantTotalDPs := int64(b.N+1) * totalSeries * 5
			if got := sink.datapoints.Load(); got != wantTotalDPs {
				b.Fatalf("expected %d total emitted gauge datapoints across %d+1 flushes, got %d", wantTotalDPs, b.N, got)
			}
		})
	}
}

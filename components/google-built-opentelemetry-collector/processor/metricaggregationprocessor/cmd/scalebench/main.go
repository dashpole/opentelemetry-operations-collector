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

// Command scalebench runs strictly isolated single-configuration memory, GC,
// ingest throughput, and query sweep measurements across 10k active metric
// identities for the sliding-window engines and full OTel Collector processor.
//
// Invoke one fresh OS process per (engine, buckets, rate) configuration so Go
// runtime heap spans, GC counts, and pause histograms are uncontaminated.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	maproc "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/metadata"
)

type engineResult struct {
	Mode               string  `json:"mode"`
	Engine             string  `json:"engine"`
	Series             int     `json:"series"`
	WindowSec          int     `json:"window_sec"`
	BucketSec          int     `json:"bucket_sec"`
	Buckets            int     `json:"buckets"`
	RateEventsPerSec   int     `json:"rate_events_per_sec"`
	EventsPerSeriesSec int     `json:"events_per_series_sec"`
	WindowEvents       uint64  `json:"window_events"`
	SlideSec           int     `json:"slide_sec"`
	SlideEvents        uint64  `json:"slide_events"`
	FillDurationMs     float64 `json:"fill_duration_ms"`
	FillThroughputEPS  float64 `json:"fill_throughput_eps"`
	SlideDurationMs    float64 `json:"slide_duration_ms"`
	SlideThroughputEPS float64 `json:"slide_throughput_eps"`
	QuerySweepMs       float64 `json:"query_sweep_ms"`
	QuerySweepCount    uint64  `json:"query_sweep_count"`
	QuerySweepSum      float64 `json:"query_sweep_sum"`
	QuerySweepMeanSD   float64 `json:"query_sweep_mean_sd"`
	BaseHeapAllocBytes uint64  `json:"base_heap_alloc_bytes"`
	LiveHeapAllocBytes uint64  `json:"live_heap_alloc_bytes"`
	DeltaHeapBytes     int64   `json:"delta_heap_bytes"`
	DeltaHeapMiB       float64 `json:"delta_heap_mib"`
	BaseHeapInuseBytes uint64  `json:"base_heap_inuse_bytes"`
	LiveHeapInuseBytes uint64  `json:"live_heap_inuse_bytes"`
	DeltaHeapInuseMiB  float64 `json:"delta_heap_inuse_mib"`
	BaseHeapObjects    uint64  `json:"base_heap_objects"`
	LiveHeapObjects    uint64  `json:"live_heap_objects"`
	TotalAllocDeltaB   uint64  `json:"total_alloc_delta_bytes"`
	TotalNumGC         uint32  `json:"total_num_gc"`
	FillNumGC          uint32  `json:"fill_num_gc"`
	SlideNumGC         uint32  `json:"slide_num_gc"`
	TotalPauseNs       uint64  `json:"total_pause_ns"`
	SlidePauseNs       uint64  `json:"slide_pause_ns"`
	MaxPauseNs         uint64  `json:"max_pause_ns"`
	SlideMaxPauseNs    uint64  `json:"slide_max_pause_ns"`
	PauseTruncated     bool    `json:"pause_ring_truncated"`
	TotalEvictions     uint64  `json:"total_evictions"`
	TotalRefolds       uint64  `json:"total_refolds"`
	TotalCancelRefolds uint64  `json:"total_cancel_refolds"`
	TotalPeriodRefolds uint64  `json:"total_periodic_refolds"`
	TotalDroppedLate   uint64  `json:"total_dropped_late"`
}

type processorMetaResult struct {
	Mode               string  `json:"mode"`
	Engine             string  `json:"engine"`
	Series             int     `json:"series"`
	Buckets            int     `json:"buckets"`
	BaseHeapAllocBytes uint64  `json:"base_heap_alloc_bytes"`
	LiveHeapAllocBytes uint64  `json:"live_heap_alloc_bytes"`
	DeltaHeapBytes     int64   `json:"delta_heap_bytes"`
	DeltaHeapMiB       float64 `json:"delta_heap_mib"`
	BytesPerSeries     float64 `json:"bytes_per_series"`
	BaseHeapInuseBytes uint64  `json:"base_heap_inuse_bytes"`
	LiveHeapInuseBytes uint64  `json:"live_heap_inuse_bytes"`
	DeltaHeapInuseMiB  float64 `json:"delta_heap_inuse_mib"`
	BaseHeapObjects    uint64  `json:"base_heap_objects"`
	LiveHeapObjects    uint64  `json:"live_heap_objects"`
	ObjectsPerSeries   float64 `json:"objects_per_series"`
}

type nopConsumer struct{}

func (nopConsumer) Capabilities() consumer.Capabilities                  { return consumer.Capabilities{} }
func (nopConsumer) ConsumeMetrics(context.Context, pmetric.Metrics) error { return nil }

func readForcedGCStats() runtime.MemStats {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms
}

func maxPauseInWindow(before, after *runtime.MemStats) (uint64, bool) {
	delta := after.NumGC - before.NumGC
	if delta == 0 {
		return 0, false
	}
	truncated := delta > 256
	n := delta
	if n > 256 {
		n = 256
	}
	var maxP uint64
	for i := uint32(0); i < n; i++ {
		idx := (after.NumGC - 1 - i) % 256
		if p := after.PauseNs[idx]; p > maxP {
			maxP = p
		}
	}
	return maxP, truncated
}

func main() {
	mode := flag.String("mode", "engine", "benchmark mode: engine or processor-meta")
	engStr := flag.String("engine", "algebraic_chan", "engine kind: raw_rescan|raw_inc|algebraic_naive|algebraic_chan|twostack_monoid")
	series := flag.Int("series", 10000, "number of active metric identities")
	windowSec := flag.Int("window", 300, "sliding window width in seconds")
	bucketSec := flag.Int("bucket", 5, "bucket duration in seconds")
	rate := flag.Int("rate", 10000, "total arrival rate across all series in events/sec (must be a multiple of series)")
	slideSec := flag.Int("slide", 60, "steady-state sliding eviction duration in seconds after filling the window")
	flag.Parse()

	kind := engine.Kind(*engStr)
	if !kind.Valid() {
		fmt.Fprintf(os.Stderr, "invalid engine %q\n", *engStr)
		os.Exit(2)
	}
	if *series <= 0 || *windowSec <= 0 || *bucketSec <= 0 || *windowSec%*bucketSec != 0 {
		fmt.Fprintf(os.Stderr, "invalid window/bucket/series parameters\n")
		os.Exit(2)
	}

	switch *mode {
	case "engine":
		if *rate <= 0 || *rate%*series != 0 {
			fmt.Fprintf(os.Stderr, "rate (%d) must be a positive multiple of series (%d)\n", *rate, *series)
			os.Exit(2)
		}
		if *slideSec < 0 {
			fmt.Fprintf(os.Stderr, "slide (%d) must be >= 0\n", *slideSec)
			os.Exit(2)
		}
		if *slideSec%*bucketSec != 0 {
			fmt.Fprintf(os.Stderr, "slide (%d) must be an exact multiple of bucket (%d) so continuous and bucketed boundaries align\n", *slideSec, *bucketSec)
			os.Exit(2)
		}
		res := runEngine(kind, *series, *windowSec, *bucketSec, *rate, *slideSec)
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(res); err != nil {
			fmt.Fprintf(os.Stderr, "encode error: %v\n", err)
			os.Exit(1)
		}
	case "processor-meta":
		res := runProcessorMeta(kind, *series, *windowSec, *bucketSec)
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(res); err != nil {
			fmt.Fprintf(os.Stderr, "encode error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (valid: engine, processor-meta)\n", *mode)
		os.Exit(2)
	}
}

func runEngine(kind engine.Kind, series, windowSec, bucketSec, rate, slideSec int) engineResult {
	winDur := time.Duration(windowSec) * time.Second
	bktDur := time.Duration(bucketSec) * time.Second
	buckets := windowSec / bucketSec
	eps := rate / series
	if int64(time.Second)%int64(eps) != 0 {
		fmt.Fprintf(os.Stderr, "1s must divide evenly by events/sec/series=%d\n", eps)
		os.Exit(2)
	}
	stepNanos := int64(time.Second) / int64(eps)
	baseNanos := int64(1700000000) * int64(time.Second)

	mBase := readForcedGCStats()

	windows := make([]engine.Window, series)
	for s := 0; s < series; s++ {
		w, err := engine.NewWindow(kind, winDur, bktDur)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewWindow failed: %v\n", err)
			os.Exit(1)
		}
		windows[s] = w
	}

	// Phase A: Fill the full window [0s, windowSec) time-interleaved across all 10k series
	// so CPU cache working-set effects match real Collector ingestion.
	fillStart := time.Now()
	for sec := 0; sec < windowSec; sec++ {
		secBase := baseNanos + int64(sec)*int64(time.Second)
		for sub := 0; sub < eps; sub++ {
			ts := secBase + int64(sub)*stepNanos
			subOff := float64(sub) * 0.25
			for s := 0; s < series; s++ {
				val := 100.0 + float64((s+sec)%50) + subOff
				if st := windows[s].Add(ts, val); st != engine.AddOK {
					fmt.Fprintf(os.Stderr, "unexpected Add status=%v in fill phase\n", st)
					os.Exit(1)
				}
			}
		}
	}
	fillDur := time.Since(fillStart)

	var mPreSlide runtime.MemStats
	runtime.ReadMemStats(&mPreSlide)

	// Phase B: Steady-state sliding eviction [windowSec, windowSec+slideSec).
	slideStart := time.Now()
	for sec := windowSec; sec < windowSec+slideSec; sec++ {
		secBase := baseNanos + int64(sec)*int64(time.Second)
		for sub := 0; sub < eps; sub++ {
			ts := secBase + int64(sub)*stepNanos
			subOff := float64(sub) * 0.25
			for s := 0; s < series; s++ {
				val := 100.0 + float64((s+sec)%50) + subOff
				if st := windows[s].Add(ts, val); st != engine.AddOK {
					fmt.Fprintf(os.Stderr, "unexpected Add status=%v in slide phase\n", st)
					os.Exit(1)
				}
			}
		}
	}
	slideDur := time.Since(slideStart)

	var mPostSlide runtime.MemStats
	runtime.ReadMemStats(&mPostSlide)

	// Phase C: One full 10k-series Query sweep at exact bucket boundary
	// (windowSec + slideSec)*1s - 1ns so continuous (now-W, now] and bucketed
	// [k-B, k-1] cover the exact same event range [slideSec, windowSec+slideSec).
	queryTS := baseNanos + int64(windowSec+slideSec)*int64(time.Second) - 1
	qStart := time.Now()
	var totalCount uint64
	var totalSum float64
	var sumSD float64
	for s := 0; s < series; s++ {
		snap := windows[s].Query(queryTS)
		totalCount += snap.Count
		totalSum += snap.Sum
		sumSD += snap.StdDev(engine.VarianceSample)
	}
	queryDur := time.Since(qStart)

	wantWindowEvents := uint64(rate) * uint64(windowSec)
	if totalCount != wantWindowEvents {
		fmt.Fprintf(os.Stderr, "count mismatch for %s: got %d, want %d\n", kind, totalCount, wantWindowEvents)
		os.Exit(1)
	}

	var totEvict, totRefold, totCancel, totPeriodic, totLate uint64
	for s := 0; s < series; s++ {
		d := windows[s].Diagnostics()
		totEvict += d.Evictions
		totRefold += d.Refolds
		totCancel += d.CancelRefolds
		totPeriodic += d.PeriodicRefolds
		totLate += d.DroppedLate
	}

	// Capture live retained heap after double GC while keeping windows alive.
	mLive := readForcedGCStats()
	runtime.KeepAlive(windows)

	maxP, trunc := maxPauseInWindow(&mBase, &mPostSlide)
	slideMaxP, _ := maxPauseInWindow(&mPreSlide, &mPostSlide)
	deltaHeap := int64(mLive.HeapAlloc) - int64(mBase.HeapAlloc)
	deltaInuse := int64(mLive.HeapInuse) - int64(mBase.HeapInuse)
	slideEvents := uint64(rate) * uint64(slideSec)
	fillEPS := 0.0
	if fillDur > 0 {
		fillEPS = float64(wantWindowEvents) / fillDur.Seconds()
	}
	slideEPS := 0.0
	if slideDur > 0 && slideEvents > 0 {
		slideEPS = float64(slideEvents) / slideDur.Seconds()
	}

	return engineResult{
		Mode:               "engine",
		Engine:             string(kind),
		Series:             series,
		WindowSec:          windowSec,
		BucketSec:          bucketSec,
		Buckets:            buckets,
		RateEventsPerSec:   rate,
		EventsPerSeriesSec: eps,
		WindowEvents:       wantWindowEvents,
		SlideSec:           slideSec,
		SlideEvents:        slideEvents,
		FillDurationMs:     math.Round(fillDur.Seconds()*1e5) / 100.0,
		FillThroughputEPS:  math.Round(fillEPS),
		SlideDurationMs:    math.Round(slideDur.Seconds()*1e5) / 100.0,
		SlideThroughputEPS: math.Round(slideEPS),
		QuerySweepMs:       math.Round(queryDur.Seconds()*1e6) / 1000.0,
		QuerySweepCount:    totalCount,
		QuerySweepSum:      totalSum,
		QuerySweepMeanSD:   sumSD / float64(series),
		BaseHeapAllocBytes: mBase.HeapAlloc,
		LiveHeapAllocBytes: mLive.HeapAlloc,
		DeltaHeapBytes:     deltaHeap,
		DeltaHeapMiB:       float64(deltaHeap) / (1024 * 1024),
		BaseHeapInuseBytes: mBase.HeapInuse,
		LiveHeapInuseBytes: mLive.HeapInuse,
		DeltaHeapInuseMiB:  float64(deltaInuse) / (1024 * 1024),
		BaseHeapObjects:    mBase.HeapObjects,
		LiveHeapObjects:    mLive.HeapObjects,
		// TotalAllocDeltaB covers [mBase, mPostSlide] before Phase C QuerySweep and
		// the final forced double-GC, so low-alloc runs can see tiny (~112B-5.6KB)
		// runtime GC bookkeeping differences relative to DeltaHeapBytes.
		TotalAllocDeltaB:   mPostSlide.TotalAlloc - mBase.TotalAlloc,
		// TotalNumGC and FillNumGC start at mBase (captured BEFORE upfront 10k-series
		// NewWindow allocation), so bucketed engines' 3-7 GCs reflect one-time
		// initialization allocations while SlideNumGC stays 0 during steady-state sliding.
		TotalNumGC:         mPostSlide.NumGC - mBase.NumGC,
		FillNumGC:          mPreSlide.NumGC - mBase.NumGC,
		SlideNumGC:         mPostSlide.NumGC - mPreSlide.NumGC,
		TotalPauseNs:       mPostSlide.PauseTotalNs - mBase.PauseTotalNs,
		SlidePauseNs:       mPostSlide.PauseTotalNs - mPreSlide.PauseTotalNs,
		MaxPauseNs:         maxP,
		SlideMaxPauseNs:    slideMaxP,
		PauseTruncated:     trunc,
		TotalEvictions:     totEvict,
		TotalRefolds:       totRefold,
		TotalCancelRefolds: totCancel,
		TotalPeriodRefolds: totPeriodic,
		TotalDroppedLate:   totLate,
	}
}

func runProcessorMeta(kind engine.Kind, series, windowSec, bucketSec int) processorMetaResult {
	buckets := windowSec / bucketSec
	f := maproc.NewFactory()
	cfg := f.CreateDefaultConfig().(*maproc.Config)
	cfg.Engine = kind
	cfg.WindowSize = time.Duration(windowSec) * time.Second
	cfg.BucketDuration = time.Duration(bucketSec) * time.Second
	cfg.MaxSeries = series
	cfg.IdleTTL = 0
	cfg.EmitInterval = time.Hour
	cfg.PassthroughInput = true

	mBase := readForcedGCStats()

	proc, err := f.CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, nopConsumer{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CreateMetrics failed: %v\n", err)
		os.Exit(1)
	}

	// Register `series` distinct realistic streams (100 hosts x series/100 routes, or fallback).
	hosts := 100
	if series < hosts {
		hosts = series
	}
	routesPerHost := series / hosts
	rem := series % hosts
	ts := pcommon.NewTimestampFromTime(time.Unix(1700000000, 0).UTC())
	ctx := context.Background()

	for h := 0; h < hosts; h++ {
		count := routesPerHost
		if h < rem {
			count++
		}
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
		rm.Resource().Attributes().PutStr("host.name", fmt.Sprintf("node-%03d", h))
		rm.Resource().Attributes().PutStr("service.name", "payments")
		rm.Resource().Attributes().PutStr("cloud.region", "us-central1")
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
		dps.EnsureCapacity(count)
		for r := 0; r < count; r++ {
			dp := dps.AppendEmpty()
			dp.SetTimestamp(ts)
			dp.Attributes().PutStr("http.route", fmt.Sprintf("/api/v1/r%03d", r))
			dp.Attributes().PutStr("method", "GET")
			dp.SetDoubleValue(1.0)
		}
		if err := proc.ConsumeMetrics(ctx, md); err != nil {
			fmt.Fprintf(os.Stderr, "ConsumeMetrics failed: %v\n", err)
			os.Exit(1)
		}
	}

	mLive := readForcedGCStats()
	runtime.KeepAlive(proc)

	deltaHeap := int64(mLive.HeapAlloc) - int64(mBase.HeapAlloc)
	deltaInuse := int64(mLive.HeapInuse) - int64(mBase.HeapInuse)
	deltaObjs := int64(mLive.HeapObjects) - int64(mBase.HeapObjects)
	return processorMetaResult{
		Mode:               "processor-meta",
		Engine:             string(kind),
		Series:             series,
		Buckets:            buckets,
		BaseHeapAllocBytes: mBase.HeapAlloc,
		LiveHeapAllocBytes: mLive.HeapAlloc,
		DeltaHeapBytes:     deltaHeap,
		DeltaHeapMiB:       float64(deltaHeap) / (1024 * 1024),
		BytesPerSeries:     float64(deltaHeap) / float64(series),
		BaseHeapInuseBytes: mBase.HeapInuse,
		LiveHeapInuseBytes: mLive.HeapInuse,
		DeltaHeapInuseMiB:  float64(deltaInuse) / (1024 * 1024),
		BaseHeapObjects:    mBase.HeapObjects,
		LiveHeapObjects:    mLive.HeapObjects,
		ObjectsPerSeries:   float64(deltaObjs) / float64(series),
	}
}

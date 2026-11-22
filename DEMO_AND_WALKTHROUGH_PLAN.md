# Demo & Walkthrough Plan: `metricaggregationprocessor`

## 60-Second Overview

- **Why this matters**: Naive sliding-window metric aggregation buffers raw datapoints over window `W`. At `500k events/sec` across `10,000` active series over a 5-minute window (`150M` live events), raw buffering consumes `2.44 GiB` (`2,502 MiB`) of live Go heap and causes `1.78s` query scans.
- **What we built**: A standalone OpenTelemetry Collector `processor.Metrics` prototype (`components/google-built-opentelemetry-collector/processor/metricaggregationprocessor`) implementing five streaming engines behind one symmetric interface, centered on **`algebraic_chan`** (32-byte locally anchored Welford buckets + `O(1)` Reverse Chan eviction + high-water-mark re-fold guards).
- **Headline result**: `algebraic_chan` keeps live heap flat at **`21.83 MiB`** (`B=60`, **`114.6x` smaller**, `99.13%` savings) and **`95.08 MiB`** (`B=300`, **`26.3x` smaller**, `96.20%` savings), sustains **`4.53M events/sec`** bare (`7.65M dp/s` across 24 cores in periodic mode `emit_interval > 0` with `0` steady-state ingest allocations), and matches a 256-bit exact oracle to `~1e-16` where naive algebraic subtraction produces negative variance and phantom empty-window sums.
- **Decision / Next Step**: Adopt `algebraic_chan` as the default engine (`twostack_monoid` optional for `min`/`max`) and close the P0/P1 items in `PROTOTYPE_LEARNINGS.md` (`manifest.yaml` wiring, `mdatagen` telemetry, `Resource`/`Scope` interning).

---

## Option A: Interactive Terminal Walkthrough (Reproducible Live Demo)

Run all steps from the repository root on branch `proto/metric-aggregation-algebraic-eviction`. Every command uses repo-relative paths and a fresh `mktemp -d` directory so any teammate can reproduce the exact proofs locally.

```bash
cd components/google-built-opentelemetry-collector/processor/metricaggregationprocessor
DEMO_TMP="$(mktemp -d)"
```

### Step 1: Prove Naive Algebraic Subtraction Fails and `algebraic_chan` Recovers (~2s)

Runs the side-by-side adversarial suite against the 256-bit `math/big.Float` exact recomputation oracle:

```bash
GOWORK=off go test -race -v -run TestBaselineVsChange_AlgebraicNaiveFails ./internal/engine
```

**What to point out in the output**:
- `high_offset_1e9_catastrophic_variance_cancellation`: `algebraic_naive` computes negative raw variance (`-2.407e+03`, `sd = 0.0`), while `algebraic_chan` and `twostack_monoid` match exact `sd = 0.299156271430`.
- `high_offset_1e12_catastrophic_variance_cancellation`: `algebraic_naive` blows up to `sd = 45,442.56` (`161,000x` error vs exact `0.281472978444`), while `algebraic_chan` and `twostack_monoid` match to machine precision (`~1e-16`).
- `spike_eviction_residual_drift_and_phantom_empty_sum`: After a `1e17` spike evicts, `algebraic_naive` loses `100%` of the remaining sum (`0.0` vs `3.0`) and keeps a phantom `-3.0` sum on an empty window; `algebraic_chan` triggers `1` cancellation re-fold and returns exact `3.0` then `0.0`.
- `decaying_staircase_naive_fails`: `algebraic_naive` drops to negative variance (`-15.27`), while `algebraic_chan` (`4` refolds) and `twostack_monoid` (`2` flips) match exact `sd = 0.051298917604`.

### Step 2: Verify Collector Telemetry Contract, Strict Cardinality Cap & Leak-Free Lifecycle (~2s)

```bash
GOWORK=off go test -race -v \
  -run 'TestPassthroughUntouchedTypes|TestGaugeRateAllowFlagAndElapsedWindowMode|TestConcurrentCardinalityCapStrictCAS|TestIdleEvictionAndEmptyWindowEmissionSemantics|TestMaxFutureSkewAgainstWallClockWithoutQuietDeadlock|TestPeriodicEmitLoopAndCleanShutdown' .
```

**What these tests verify**:
- `TestPassthroughUntouchedTypes` and `TestGaugeRateAllowFlagAndElapsedWindowMode`: Cumulative sums, unspecified-temporality sums, histograms, exponential histograms, and summaries pass through untouched; eligible `Gauge` and `Sum(Delta)` inputs emit strictly as `Gauge` snapshots.
- `TestConcurrentCardinalityCapStrictCAS`: 16 concurrent goroutines inserting `500` unique streams against `max_series: 250` cap strictly at `ActiveSeries == 250` with `DroppedCardinality == 250` and zero data races.
- `TestIdleEvictionAndEmptyWindowEmissionSemantics`: Silent series emit `0` only for `sum`/`count`/`rate` (omitting undefined `mean`/`stddev`) until `idle_ttl` evicts them cleanly (`ActiveSeries == 0`, `IdleEvicted == 1`).
- `TestMaxFutureSkewAgainstWallClockWithoutQuietDeadlock`: Far-future timestamps (`1` year ahead and `math.MaxUint64`) are dropped against `clock.Now()` (`DroppedFutureSkew == 2`) without deadlocking normal traffic after a 2-minute quiet gap.
- `TestPeriodicEmitLoopAndCleanShutdown`: Starting and stopping the periodic emission goroutine exits cleanly with `goleak` confirming `0` leaked goroutines.

### Step 3: Live Flat-Heap vs. Linear-Heap Comparison (`cmd/scalebench`)

Build the standalone scale benchmark binary once:

```bash
GOWORK=off go build -o "$DEMO_TMP/scalebench" ./cmd/scalebench
```

**Fast live demo (~25s total, `10k/s` to `100k/s` across `10,000` series, `W=300s`, `B=60`, `--slide=30`)**:

```bash
for rate in 10000 100000; do
  echo "--- raw_inc @ ${rate} eps ---"
  "$DEMO_TMP/scalebench" --mode=engine --engine=raw_inc --series=10000 --window=300 --bucket=5 --slide=30 --rate="$rate"
  echo "--- algebraic_chan @ ${rate} eps ---"
  "$DEMO_TMP/scalebench" --mode=engine --engine=algebraic_chan --series=10000 --window=300 --bucket=5 --slide=30 --rate="$rate"
done
```

**Expected live numbers (`milestone_3_evidence.md` Sections 2.1-2.2)**:

| Engine | Rate (`10k` Series, `W=300s`) | Live Events | `delta_heap_mib` | `slide_throughput_eps` | `query_sweep_ms` |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `raw_inc` | `10k/s` | `3M` | `80.58 MiB` | `2.54M eps` | `2.10 ms` |
| `raw_inc` | `100k/s` | `30M` | **`627.45 MiB`** (`7.79x`) | `1.49M eps` | `3.02 ms` |
| `algebraic_chan` (`B=60`) | `10k/s` | `3M` | **`21.83 MiB`** | `4.65M eps` | `2.21 ms` |
| `algebraic_chan` (`B=60`) | `100k/s` | `30M` | **`21.83 MiB`** (`1.00x`) | `5.20M eps` | `1.92 ms` |

**Full `500k/s` (`150M` live events) reproduction (~3.5 to 4 min for `raw_inc` + `raw_rescan` + `algebraic_chan`)**:

```bash
for eng in raw_rescan raw_inc algebraic_chan; do
  "$DEMO_TMP/scalebench" --mode=engine --engine="$eng" --series=10000 --window=300 --bucket=5 --slide=30 --rate=500000
done
```

Expected at `500k/s`: `raw_rescan` hits `2,502.00 MiB` (`1,780.53 ms` query sweep), `raw_inc` hits `2,502.46 MiB` (`0.95M eps` slide throughput), and `algebraic_chan` stays at **`21.83 MiB`** (`4.53M eps` slide throughput, `2.15 ms` query sweep, `114.6x` smaller heap).

### Step 4: Demonstrate `O(1)` Out-of-Order Advantage vs. `twostack_monoid` (~15s)

```bash
GOWORK=off go test -run=^$ \
  -bench='BenchmarkSnapshot/B=60/rate=50eps|BenchmarkAddOOO/B=300/ooo=15pct' \
  -benchmem -count=3 -benchtime=200ms ./internal/engine
```

**What to point out (`milestone_3_evidence.md` Section 3)**:
- `Snapshot/B=60/rate=50eps`: `algebraic_chan` (`53.47 ns`) is **`2,498x` faster** than `raw_rescan` (`133,576 ns`) and **`2.83x` faster** than `twostack_monoid` (`151.20 ns`).
- `AddOOO/B=300/ooo=15pct`: `algebraic_chan` stays flat at **`50.17 ns`**, whereas `twostack_monoid` climbs to **`289.05 ns`** (`5.76x` slower due to `O(B - k)` front-stack rebuilds) and `raw_inc` runs at `118.50 ns` (`2.36x` slower). All five report `0 B/op`, `0 allocs/op`.

Clean up temporary build output:

```bash
rm -rf "$DEMO_TMP"
```

---

## Option B: AI-Narrated Walkthrough Video Storyboard & Script

- **Format**: 1080p (`1920x1080`, 30 fps) dark-theme animated engineering explainer (charts, architecture diagrams, terminal cards).
- **Target Duration**: ~3 minutes 50 seconds (6 scenes, ~570 spoken words at ~150 wpm).
- **TTS Hygiene**: Every Spoken Narration block below uses plain spoken English with zero raw code identifiers, backticks, file paths, or URLs.

### Scene 1: The Memory Problem (0:00 to 0:36)

- **Visual Layout**: Title card (`O(1) Sliding-Window Metric Rollups in the OpenTelemetry Collector`) transitioning into an animated bar chart of live Go heap for `10,000` active series over a 5-minute window as traffic climbs from `10k/s` (`80 MiB`) to `100k/s` (`627 MiB`) to `500k/s` (`2,502 MiB` / `2.44 GiB` across `150M` buffered datapoints, plus `5,000 MiB` / `4.88 GiB` total allocation churn).
- **Spoken Narration**:
  > When an OpenTelemetry Collector computes five-minute sliding-window rollups, such as rolling sums, rates, averages, or standard deviations, the straightforward approach buffers every raw datapoint in a ring per time series. That scales memory linearly with event rate. At five hundred thousand events per second across ten thousand time series, a single five-minute window holds one hundred fifty million raw points. That takes over two point four gibibytes of live heap and triggers nearly two-second query scans. We built a prototype processor to cut that down to bounded memory and constant-time eviction.

### Scene 2: Why Naive Algebraic Subtraction Breaks (0:36 to 1:12)

- **Visual Layout**: Split-screen numerical failure table contrasting Exact 256-bit Oracle vs. Naive Subtraction across three adversarial cases:
  1. Offset `10^9 + [0, 1]` -> Raw Variance `-2,407` (negative!)
  2. Offset `10^12 + [0, 1]` -> StdDev `45,442.56` vs exact `0.2815` (`161,000x` error)
  3. `10^17` spike + `1.0` + `2.0` (after spike evicts) -> Sum `0.0` (exact `3.0`), then after `1.0` and `2.0` evict (empty window) -> phantom `-3.0`
- **Spoken Narration**:
  > Streaming theory says a sliding window is just the difference between two running totals. When an old bucket expires, you simply subtract it in constant time. In real sixty-four-bit floating-point math, however, naive subtraction breaks down badly. On metrics with large base offsets around one trillion, subtracting squared totals produces standard deviation errors over one hundred sixty thousand times too large, or even negative variance. Worse, when a huge traffic spike expires, floating-point absorption erases normal traffic and leaves a phantom negative sum even on an empty window.

### Scene 3: Locally Anchored Guarded Chan Eviction (1:12 to 1:52)

- **Visual Layout**: Animated ring-buffer diagram showing `B` fixed 32-byte slots (`count`, `anchor`, `meanShifted`, `m2`, two per 64-byte cache line), the deferred open head bucket, the closed accumulator, Reverse Chan eviction on window advance, and the two high-water-mark guards (`peakM2` and scale-aware `peakSumMag`) triggering a clean forward re-fold only when a spike or decaying staircase exits. Callout badge: `0 cancellation refolds` on `N(0, 1)` and bipolar `±1` streams.
- **Spoken Narration**:
  > Our recommended engine solves this with three ideas. First, we store fixed sub-window buckets, each thirty-two bytes wide, with its own local anchor value. Shifting points relative to each bucket anchor keeps floating-point precision near sixteen significant digits even at offsets of one trillion. Second, incoming events update only the active head bucket, and expiring buckets subtract cleanly using reverse parallel variance updates. Third, per-series high-water marks detect when a big spike or decaying staircase leaves the window and trigger one clean forward re-fold. On steady Gaussian or alternating positive-and-negative streams, cancellation re-folds drop all the way to zero.

### Scene 4: Five-Engine Scale Bakeoff (1:52 to 2:42)

- **Visual Layout**: Three side-by-side benchmark charts from `cmd/scalebench` and `benchstat`:
  1. **Live Heap (`10k/s` to `500k/s`)**: Flat `21.83 MiB` (`B=60`) and `95.08 MiB` (`B=300`) lines vs. skyrocketing `2,502 MiB` raw curves (`114.6x` / `26.3x` reduction; `314.96 MiB` on Two-Stack at `B=300`, `3.3x` larger than `algebraic_chan` including `37.84 MiB` of Go allocator rounding).
  2. **Steady-State Slide Throughput (`500k/s`)**: `4.53M eps` (`algebraic_chan`) vs `0.95M eps` (`raw_inc`, `4.79x` faster) and `2.15 ms` vs `1,780.53 ms` query sweep (`828x` faster than `raw_rescan`).
  3. **15% Out-of-Order Latency (`B=300`)**: `50.17 ns` flat vs `289.05 ns` on Two-Stack (`5.76x` slower).
- **Spoken Narration**:
  > We benchmarked five engines head to head across ten thousand live time series from ten thousand up to five hundred thousand events per second, with zero extrapolation. With five-second buckets, our guarded algebraic engine held live heap completely flat at twenty-two mebibytes, over one hundred fourteen times smaller than raw buffering, while sliding nearly five times faster than incremental raw buffering and querying over eight hundred times faster than rescanning. We also tested a zero-subtraction two-stack engine. It is a great optional mode if you need exact minimum and maximum values, but at three hundred buckets it uses over three times more memory than our guarded algebraic engine, worsened by thirty-eight mebibytes of Go allocator rounding, and late out-of-order arrivals run nearly six times slower.

### Scene 5: Full Collector Processor Results (2:42 to 3:22)

- **Visual Layout**: Pipeline diagram (`OTLP Metrics -> 64-Shard Store -> Periodic Gauge Flush`) with three metric callouts:
  - `7.65M dp/s` across 24 cores (`130.8 ns/dp`, `0 B/op`, `0 allocs/op`)
  - CPU Breakdown pie: `~86%` OTLP key encoding/hashing/map lookup vs `~14%` engine (`10k` rotating series)
  - Periodic vs. Sync bar: `0` allocs/batch (`1.57 ms`) vs `31,720` allocs/batch (`7.50 ms`, `4.79x` slower)
- **Spoken Narration**:
  > Next, we wired all five engines into a full OpenTelemetry Collector metrics processor with sixty-four cache-line-aligned shards, strict atomic cardinality caps, and idle series eviction. In periodic emission mode, ingestion hits six hundred thirty-eight thousand datapoints per second on one core and seven point six five million across twenty-four cores, with zero heap allocations per batch. Profiling also surfaced a key architectural lesson: eighty-six percent of ingestion time goes to OpenTelemetry protocol attribute sorting and hashing, not the aggregation math, and synchronous per-batch emission is nearly five times slower than periodic flushing. High-throughput deployments should always emit periodically.

### Scene 6: Decision & Next Steps (3:22 to 3:50)

- **Visual Layout**: Roadmap checklist card:
  1. Ship guarded algebraic engine as default (Two-Stack optional for `min`/`max`)
  2. Wire into Collector distribution manifest and `mdatagen` self-telemetry
  3. Intern shared `Resource` and `Scope` metadata (`-37%` to `-46%` total processor objects)
  4. Optional follow-ups: Logs-to-Metrics connector, 128-bit integer mode, sliding quantile sketches
- **Spoken Narration**:
  > The prototype gives us a clear path forward. Guarded algebraic eviction delivers constant-time windowing, flat bounded memory, and full floating-point accuracy without sacrificing out-of-order performance. Before productionizing this processor in the Google-Built OpenTelemetry Collector, our top follow-ups are wiring it into the distribution manifest, adding OpenTelemetry self-telemetry for cardinality and late-drop monitoring, and interning shared resource and scope metadata to cut per-series object overhead nearly in half. Thanks for watching.

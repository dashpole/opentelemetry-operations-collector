# Prototype Learnings: `metricaggregationprocessor` (O(1) Sliding-Window Rollups)

## Executive Decision

**Recommendation: Adopt `algebraic_chan` as the default sliding-window rollup engine (`(Recommended)`), with `twostack_monoid` available as an optional mode when exact `min`/`max` or zero-subtraction guarantees matter more than `O(1)` late out-of-order updates.**

In the OpenTelemetry Collector, naive sliding-window metric aggregations buffer raw datapoints per time series over window `W`, scaling heap linearly with arrival rate (`O(N * W)`). To solve that without losing floating-point accuracy, we built a standalone Collector metrics processor (`metricaggregationprocessor`) and ran a symmetric five-engine bakeoff: two raw-buffering baselines (`raw_rescan` and `raw_inc`) against three fixed-bucket `O(B)` engines (`algebraic_naive`, `algebraic_chan`, and `twostack_monoid`).

Across 10,000 active metric series over a 5-minute (`300s`) sliding window up to `500k events/sec` (`150M` live events, zero extrapolation), `algebraic_chan` proves all three prototype hypotheses:

- **Flat `O(B)` memory (`114.6x` smaller at `500k/s`)**: Live retained heap stays flat at `21.83 MiB` (`B=60`, 5s buckets, `-0.026%` drift from `10k/s` to `500k/s`) and `95.08 MiB` (`B=300`, 1s buckets, `-0.000%` drift), compared to `2,502.00 MiB` (`2.443 GiB`, `31.23x` growth) on `raw_rescan` and `2,502.46 MiB` (`2.444 GiB`, `31.06x` growth) on `raw_inc`. That cuts live heap by `99.13%` (`B=60`) and `96.20%` (`B=300`).
- **Machine-epsilon stability where naive subtraction breaks**: `algebraic_chan` matches the 256-bit `big.Float` exact recomputation oracle to `~1e-16` relative standard deviation error at `1e9` and `1e12` offsets (`5.77e-16` and `3.84e-16`) and recovers cleanly from `1e12`/`1e17` spikes and decaying staircases. Naive algebraic subtraction (`algebraic_naive`) fails all three with negative variance, `161,000x` stddev blowups, and phantom `-3.0` sums on empty windows.
- **High throughput with zero steady-state ingest allocations**: Bare `algebraic_chan` sustains `4.53M events/sec` steady-state sliding (`B=60`) and `3.85M events/sec` (`B=300`) on one core (`4.79x` and `4.07x` higher throughput than `raw_inc`'s `0.95M events/sec`, with `0.000 B/op` and `0.000 allocs/op`). Inside the full Collector processor (`metricaggregationprocessor`) with periodic emission (`emit_interval > 0`), ingestion reaches `638k dp/s` single-core (`1.567 µs/dp`) and `7.65M dp/s` across 24 cores (`130.8 ns/dp`) with `0.000 B/op` and `0.000 allocs/op`.

---

## 1. Engine Comparison & Selection Matrix

All five engines were evaluated symmetrically under identical 64-shard locking, key encoding, overflow guards, and synthetic streams (`milestone_1_evidence.md`, `milestone_2_evidence.md`, `milestone_3_evidence.md`).

| Engine | Live Heap at `500k/s` (`10k` Series, `W=300s`) | Slide Tput (`500k/s`) | `10k`-Series Query (`500k/s`) | `15%` OOO Latency (`B=60` / `B=300`) | Numerical Precision | Exact `Min`/`Max` | Decision |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`algebraic_chan`** | `21.83 MiB` (`B=60`) / `95.08 MiB` (`B=300`) | `4.53M` / `3.85M eps` | `2.15 ms` / `2.35 ms` | `49.64 ns` / `50.17 ns` | `~1e-16` (guarded refold) | No | **`(Recommended)` Default** |
| `twostack_monoid` | `61.05 MiB` (`B=60`) / `314.96 MiB` (`B=300`) | `4.50M` / `4.18M eps` | `4.52 ms` / `4.37 ms` | `100.45 ns` / `289.05 ns` | `~1e-16` (zero subtraction) | Yes | Optional mode for `min`/`max` |
| `algebraic_naive` | `16.49 MiB` (`B=60`) / `79.97 MiB` (`B=300`) | `6.23M` / `5.96M eps` | `0.85 ms` / `0.48 ms` | `44.98 ns` / `45.21 ns` | **Fails** (negative var, drift) | No | Reject (unsafe under `float64`) |
| `raw_inc` | `2,502.46 MiB` (`2.444 GiB`) | `0.95M eps` | `6.85 ms` | `113.55 ns` / `118.50 ns` | `~1e-16` (shifted refold) | No | Reject (`O(N * W)` heap) |
| `raw_rescan` | `2,502.00 MiB` (`2.443 GiB`) | `2.01M eps` | `1,780.53 ms` | `47.98 ns` / `52.97 ns` | Exact reference | Yes | Reject (`O(N * W)` heap & query) |

### Deferred 6th Architecture: FlatFAT / Binary Segment Tree

`twostack_monoid` trades `O(B - k)` worst-case front-stack rebuilds on late out-of-order arrivals for zero-subtraction `min`/`max` support (`289.05 ns` at `B=300, 15% OOO`, which is `5.76x` slower than `algebraic_chan`'s `50.17 ns`). If a future workload needs **both** zero-subtraction `min`/`max` (or mergeable quantile sketches) **and** sub-linear worst-case out-of-order latency at large `B`, a flat fixed-size binary segment tree (FlatFAT) over `B` buckets maintains `O(log B)` worst-case out-of-order updates and `O(1)` root queries in `2B` nodes (`~96 B/bucket`, matching `twostack_monoid`'s footprint). We did not need to build a 6th engine here because `algebraic_chan` already covers `sum`, `count`, `mean`, `stddev`, and `rate` in `O(1)` time and `3.3x` less memory at `B=300`.

---

## 2. What Worked vs. What Surprised Us

### What Worked Well

1. **Per-bucket local anchoring + deferred head bucket (`chanBucket`, 32 bytes)**: Storing `{count, anchor, meanShifted, m2}` fits two buckets per 64-byte cache line. Updating only the open `head` bucket on in-order arrivals avoids touching the closed accumulator until a bucket rolls over, keeping single-series L1-hot `Add` (`44.87 ns` at `50 eps`) statistically indistinguishable from `algebraic_naive` (`42.57 ns`, `p=0.132`).
2. **High-water-mark guards with scale-aware bipolar protection (`algebraic_chan.go`)**: Tracking `peakM2` and `peakSumMag` since the last re-fold caught both single `1e12`/`1e17` spikes and decaying `[20^7..1]` staircases while producing **`0` cancellation refolds** across `5,000` buckets of `N(0, 1)` (`0.162%` periodic refolds) and `2,000` buckets of alternating bipolar `±1` (`0.155%` at `B=59/60`, `0.000%` at `B=299/300`). Periodic `1e12` spike eviction (`1` per `3,000` events) added only `2.80 ns` amortized over `algebraic_naive` (`48.12 ns` vs `45.32 ns`, `p=0.180`).
3. **Cache-line-aligned 64-shard store + zero-alloc canonical key builder**: Padding `seriesShard` to `64 bytes` and pooling `keyBuilder` with `slices.SortStableFunc` and hardware-accelerated `bits.Mul64` 128-bit FNV-1a (`identity.go`) eliminated steady-state ingest allocations (`0.000 B/op`, `0.000 allocs/op` on both flat and complex `Bytes`/`Slice`/nested `Map` attributes) and scaled to `7.65M dp/s` across 24 cores.
4. **Strict atomic CAS cardinality cap (`tryReserveSeries`)**: Lock-free CAS reservation across 64 shards held `ActiveSeries` strictly `<= MaxSeries` under 16-goroutine `-race` stress (`250` active, `250` dropped, zero overshoot) without a global mutex.

### What Surprised Us

1. **Naive DBSP subtraction fails immediately on everyday telemetry offsets**: Subtracting `sum` and `sumSq` in `float64` assumes exact group arithmetic. On high-offset series like Unix timestamps or monotonic gauges (`1e9 + [0, 1]` and `1e12 + [0, 1]`), `algebraic_naive` computed negative raw variance (`-2.407e+03`, clamping stddev to `0.0`) at `1e9` and `+2.065e+09` (`45,442.56` vs exact `0.2815`, a `161,000x` error) at `1e12`. After a `1e17` spike followed by `1.0` and `2.0`, once the spike evicted, `algebraic_naive` lost `100%` of the remaining sum (`0.0` vs `3.0`), and once `1.0` and `2.0` also evicted (`count == 0`), it retained a phantom `-3.0` sum on an empty window.
2. **Unshifted `24 B/bucket` Welford also failed at `1e12`**: An earlier design stored unshifted bucket means. Inside a single bucket at `1e12 + [0, 1]`, `float64` ULP quantization (`1.22e-4`) caused `2.66e-5` relative stddev error, failing our `1e-7` gate. Adding 8 bytes per bucket for a local `anchor` (`32 B/bucket`) made `x - b.anchor` exact by Sterbenz's Lemma and dropped error to `3.84e-16`.
3. **Go 1.27 allocator size classes amplified `twostack_monoid`'s memory cost at `B=300`**: At `B=300`, `twostack_monoid` allocates two `14,400 B` slices (`buckets` and `frontAgg`). Go's `mspan` allocator rounds each slice up to `16,384 B`, wasting `3,968 B/series` (`37.84 MiB` across `10k` series) for `314.96 MiB` total. By contrast, `algebraic_chan`'s single `9,600 B` slice rounds to `9,728 B` (`+1.3%` slack, `95.08 MiB` total), making `algebraic_chan` `3.3x` smaller than `twostack_monoid` at `B=300`.
4. **OTLP decoding and key hashing consume `~86%` of single-core processor CPU**: Bare `algebraic_chan` takes `~45 ns/dp` L1-hot (`~3%`) and `~215` to `221 ns/dp` across 10,000 rotating series (`~14%` of `1,567 ns/dp` in `ConsumeMetricsBatch/mode=periodic`). Canonical key serialization, attribute sorting, hashing, shard locking, and Go `map[string]` lookup account for the remaining `~86%`. Further engine micro-optimization will barely move end-to-end Collector CPU; key-hashing and lookup optimizations drive steady-state ingest CPU, while metadata interning cuts live heap object count and cold series creation cost.
5. **Synchronous emission (`emit_interval: 0`) dominates ingest latency and allocations**: Emitting output `Gauge` rollups inside `ConsumeMetrics` increased batch latency `4.79x` (`7.501 ms` vs `1.567 ms` per `1,000`-dp batch) and added `31,720 allocs/batch` (`1.670 MiB/batch`) just building `5,000` output `pmetric.Gauge` datapoints. Periodic emission (`emit_interval > 0`) eliminates steady-state ingest allocations completely.
6. **Uncoordinated lazy `IdleTTL` sweeps turn cardinality drops into an `O(N)` cliff**: Whenever `max_series` saturated with `idle_ttl > 0`, scanning shards for idle series on every dropped datapoint turned over-cap drops into full-shard scans. Gating both per-shard (`lastSweepNanos`) and cross-shard (`lastLazySweep`) sweeps with strictly monotonic cooldowns keeps within-cooldown drops `O(1)` and prevents backward-clock ping-pong thrashing (verified in `TestLazySweepPerShardCooldownAndMonotonicOtherShardSweep`, where 40 alternating backward/forward timestamps trigger exactly `1` sweep instead of `40`).
7. **Benchmark setup traps distorted early microbenchmarks**: Loop-body integer division (`(i/eventsPerBucket)%2`) added `IDIVQ` overhead in `BenchmarkAddBipolar`; filling `300s` instead of `330s` in `BenchmarkSnapshot` and `BenchmarkProcessorFlush10kSeries` parked `twostack_monoid` on its exact stack-flip boundary (`splitID == headBucket`), skipping its 3-way combine and keeping `raw_*` rings unwrapped; and constant post-warmup batch values caused artificial synchronized `M2 -> 0` refolds. Fixing all three made the final `benchstat` comparison fair and reproducible.

---

## 3. Design Discrepancies & Recommended Spec Updates

Real implementation exposed five places where the initial mental model or early problem statement needed sharper contracts. Any downstream design doc, RFC, or Collector component spec should codify these explicitly:

1. **Replace pure Abelian subtraction with guarded locally anchored Reverse Chan**: The initial problem statement modeled sliding windows as `S_new = S_old - x_evicted` over an Abelian group. Because IEEE-754 `float64` lacks exact inverses under spikes and high offsets, the specification must mandate locally anchored buckets (`32 B`), Reverse Chan downdates, exact zero resets (`nt <= nb`), high-water-mark re-fold guards (`peakM2` and scale-aware `peakSumMag`), a periodic re-fold cap (`10 * B`), and pre-ingestion rejection of `NaN`, `±Inf`, and `|x| > 1e140`.
2. **Codify bucket boundary alignment (`k*Delta - 1`) and `-1/(2B)` mid-bucket bias**: Using `B` active slots `[headBucket - B + 1, headBucket]` gives `0` boundary error against continuous `(now - W, now]` when queried at `now = k*Delta - 1` (implemented in `Flush` by subtracting `1 ns` on bucket-aligned ticks). Querying at arbitrary mid-bucket timestamps under-covers the window by `-Delta/2` on average (`-0.825%` measured mean signed error at `B=60`, `-0.158%` at `B=300`, `-0.075%` at `B=600`). Specs should document this trade-off explicitly (and note that `B + 1` slots flips bias to `+Delta/2`).
3. **Enforce Collector telemetry safety contracts**:
   - Emit all rolling outputs strictly as **`Gauge`** snapshots (never overlapping `Sum(Delta)`), naming outputs `<name>.window.{sum,count,mean,stddev,rate,min,max}` with `<unit>/s` (or `1/s`) for `rate`.
   - Pass `Sum(Cumulative)`, `Sum(Unspecified)`, `Histogram`, `ExponentialHistogram`, and `Summary` metrics through untouched (users chain after `cumulativetodeltaprocessor` for cumulative counters).
   - On empty windows (`count == 0` before `idle_ttl`), emit `0` only for `count`, `sum`, and `rate` (clamping any raw `algebraic_naive` residual to `0`), and **omit** `mean`, `stddev`, `min`, and `max` (pruning empty `ResourceMetrics` shells).
   - Skip `NoRecordedValue` datapoints and drop `Timestamp == 0` by default (`fallback_to_arrival_time: false`) before reserving a `max_series` slot.
4. **Guarantee `StartTimestamp <= Timestamp` under clock skew**: `Flush` must clamp `emitTS = max(nowNanos, entry.win.LastActivityNanos(), snap.WindowStartNanos)` so host clock skew, backward NTP steps, or concurrent flushes never emit inverted OTLP timestamps or under-clamped `elapsed_window` rates.
5. **Separate test vs. production defaults for `emit_interval` and `max_future_skew`**: The prototype keeps `emit_interval: 0s` and `max_future_skew: 0` as defaults so offline replay and unit tests stay deterministic. Production documentation and default Collector configs should recommend `emit_interval: 5s` (or `10s`, matching `bucket_duration`) and `max_future_skew: 1m` (strictly checked against wall-clock `clock.Now()`, never against a series' own `headBucket` which would deadlock quiet series).

---

## 4. Productionization Gap Ledger

These intentional prototype shortcuts must be addressed before enabling `metricaggregationprocessor` in production Google-Built OpenTelemetry Collector (GBOC) or upstream OpenTelemetry Collector Contrib builds:

| # | Area | Prototype Shortcut | Required Production Hardening | Priority |
| :--- | :--- | :--- | :--- | :---: |
| **1** | **Distro & Policy Wiring** | Standalone Go module (`manifest.yaml`, `registry.yaml`, and `specs/` untouched). | Wire factory into GBOC distribution manifest, component registry, and Telemetry Policy translation (if exposed via policy). | P0 |
| **2** | **Internal Observability (`mdatagen`)** | Counters exposed only via Go `Stats()` struct (`ActiveSeries`, drops, `IdleEvicted`, refolds). | Generate OTel self-telemetry metrics via `mdatagen` (`metadata.yaml`) so operators can alert on `dropped_cardinality`, `dropped_late`, and `cancel_refolds`. | P0 |
| **3** | **Metadata Interning (`Resource` / `Scope`)** | Clones `pcommon.Resource`, `pcommon.InstrumentationScope`, and datapoint attributes per series (all non-engine processor metadata totals `9.47 MiB` / `993 B/series`, `15.56` objects/series across `10k` series). | Intern immutable `Resource` and `Scope` structs keyed by `resKey`/`scopeKey` with refcounts. Eliminates `~37%` to `46%` of total `processor-meta` objects (`~42%` to `51%` of metadata-only objects on 3-resource-attribute workloads, and higher on Kubernetes fleets with 10+ resource attributes). | P1 |
| **4** | **Production Defaults & Graceful Drain** | `emit_interval: 0s` and `max_future_skew: 0` defaults; `Shutdown` stops loops without a final flush. | Document/tune production defaults (`emit_interval > 0`, `max_future_skew > 0`) and add an optional `flush_on_shutdown` flag to drain final rollups cleanly. | P1 |
| **5** | **`Flush` Allocation Overhead** | `Flush` builds fresh `pmetric.Metrics` (`56.81 ms`, `17.48 MiB`, `316.8k allocs` for `50k` gauge datapoints across `10k` series). | Pre-size `DataPoints()` slices per `(scope, metric)` group and consider chunked batching (`max_flush_batch_size`) so downstream exporters do not spike on a single giant OTLP payload. | P1 |
| **6** | **Exact `int64 > 2^53` Accumulation** | Converts `int64` datapoints to `float64` (loses LSBs above `2^53 = 9.007e15`, verified in `milestone_1_evidence.md`). | Add an optional 128-bit integer accumulator path for `sum`/`count` when lossless integer precision above `2^53` is required. | P2 |
| **7** | **Logs-to-Metrics Connector** | `processor.Metrics` only; log-to-metric rollups deferred. | Wrap `internal/engine` in a `connector.LogsToMetrics` component sharing the exact same sharded store and `algebraic_chan` engine. | P2 |
| **8** | **Sliding Quantiles & FlatFAT** | Scalar statistics only (`sum`, `count`, `mean`, `stddev`, `rate`, plus `min`/`max` on `twostack_monoid`). | Evaluate mergeable quantile sketches (DDSketch or ExponentialHistogram buckets) over `twostack_monoid` or a `2B`-node FlatFAT segment tree if sliding `p95`/`p99` is required. | P2 |
| **9** | **Restart Persistence** | State is in-memory only; collector restarts begin a warmup window. | Acceptable for sliding-window monitoring (documented via `rate_mode: elapsed_window`); checkpointing via `file_storage` extension is only needed if multi-hour windows are introduced. | P3 |

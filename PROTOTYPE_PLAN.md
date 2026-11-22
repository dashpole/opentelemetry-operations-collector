# Prototype Plan: `metricaggregationprocessor`: O(1) Sliding-Window Metric Rollup via Algebraic Eviction

## 1. Executive Summary & Core Hypothesis

Stateful sliding-window metric aggregations in the OpenTelemetry Collector (such as 5-minute rolling sums, rates, means, and standard deviations) often buffer raw event records `(timestamp, value)` per series over window $W$. That naive design scales heap memory as $O(N \cdot W)$ where $N$ is the event arrival rate, and triggers high Go GC pressure or OOM under high throughput (100k to 500k events/sec across 10k active metric identities: up to 150M raw datapoints inside a single 5-minute window, or `2.40 GB` (`2.235 GiB`) of raw payload alone before slice capacity slack).

This prototype evaluates **five streaming sliding-window aggregation engines** under identical hashing, sharding, and synthetic workloads to answer three concrete technical questions:

1. **Memory Bound**: Does discretizing window $W$ into $B$ fixed sub-window buckets (e.g., $W = 5\text{m}$, $B = 60$ 5s buckets or $B = 300$ 1s buckets) bound per-series heap usage strictly to $O(B)$ across `10k/s` to `500k/s` event rates (flat heap vs. linear growth in raw-event buffering)?
2. **Numerical Stability under IEEE-754**: Naive DBSP algebraic subtraction ($S_{\text{new}} = S_{\text{old}} - x_{\text{evicted}}$, $\text{Var} = \frac{1}{n}\sum x_i^2 - \bar{x}^2$) assumes exact Abelian group arithmetic, which fails under IEEE-754 `float64` due to (a) catastrophic variance cancellation on high-offset series ($10^9 + \varepsilon$, $10^{12} + \varepsilon$), (b) irreversible floating-point residual drift when a large traffic spike expires, and (c) multi-step precision loss on decaying staircase spikes. Does **per-bucket locally anchored Welford + Reverse Chan algebraic downdate with high-water-mark-triggered local re-fold (`algebraic_chan`)** recover machine-epsilon accuracy (`< 1e-9` relative for `sum`/`mean` and `< 1e-7` relative for `stddev`, empirically verified `~1e-16` at offsets up to `1e12`) with negligible amortized CPU cost compared to both naive subtraction and a zero-subtraction **Two-Stack monoid queue (`twostack_monoid`)**?
3. **Throughput, Out-of-Order Cost & Discretization Trade-off**: What is the end-to-end throughput, latency, GC pause, out-of-order insertion cost, and sub-bucket discretization error across all five engines when integrated into a real OpenTelemetry Collector `processor.Metrics` component?

**Recommended Architecture**: **`algebraic_chan`** (deferred-head per-bucket locally anchored Welford accumulator + $O(1)$ Reverse Chan algebraic eviction + high-water-mark and periodic $O(B)$ forward Chan re-fold), benchmarked symmetrically against `raw_rescan`, `raw_inc`, `algebraic_naive`, and `twostack_monoid` (with `twostack_monoid` offered as an optional engine mode when exact `min`/`max` or zero-subtraction guarantees are preferred over $O(1)$ late out-of-order updates).

---

## 2. Ground-Truth Context: Existing Collector Components

Existing OpenTelemetry Collector Contrib and GBOC processors do not provide bounded-memory, exact sliding-window rollup statistics (`sum`, `count`, `mean`, `stddev`, `rate`):

| Existing Component | Window Model | Gap for Sliding-Window Rollups |
| :--- | :--- | :--- |
| `intervalprocessor` | Tumbling window (resets state on every flush tick; sums monotonic deltas via `aggregateDeltaDataPoints` and keeps latest cumulative points) | No overlapping sliding window; no variance/stddev; clears state on each interval flush. |
| `deltatorateprocessor` | Single-point (`value / (ts - start_ts)`) on Delta Sum only | Stateless per datapoint; no multi-point windowing. |
| `rollingspanlatencyprocessor` (upstream Contrib only, not in GBOC) | Exponentially Weighted Moving Average (EWMA) over traces | Decaying approximation rather than exact fixed-width sliding window $W$; traces-only. |
| `metricstransformprocessor` | Intra-batch label aggregation only | No cross-batch or temporal windowing. |
| `cumulativetodeltaprocessor` | Point-to-point cumulative-to-delta conversion | Emits deltas; does not aggregate over a sliding window (composes upstream of this processor). |

---

## 3. Scope, Intentional Simplifications & Non-Goals

### In Scope
- Isolated OpenTelemetry Collector `processor.Metrics` module under `components/google-built-opentelemetry-collector/processor/metricaggregationprocessor` (`go.opentelemetry.io/collector/*` `v1.71.0` / `v0.165.0` matching `googlepolicyprocessor/go.mod`).
- **5 symmetric streaming engines** behind a shared `WindowEngine` interface and identical 64-shard stream lookup table (canonical key equality with 128-bit FNV-1a `[2]uint64` hash for shard routing and map keys):
  - Engine identifiers (consistent across config, code, tests, and benchmarks): `raw_rescan`, `raw_inc`, `algebraic_naive`, `algebraic_chan`, `twostack_monoid`.
  - Input support: `Gauge` and `Sum(Delta)` inputs (`int64` and `float64` datapoints).
  - Output aggregations: `sum`, `count`, `mean`, `stddev` (configurable `variance_mode`: `sample` default returning `0` for `n < 2`, or `population`), and `rate` (`sum / effectiveWindowSeconds`). `twostack_monoid` and `raw_rescan` additionally compute exact `min` and `max`.
  - Safe pass-through for `Sum(Cumulative)`, `Sum(Unspecified)`, `Histogram`, `ExponentialHistogram`, and `Summary` metrics (never erroneously summing cumulative counter totals; chains cleanly after `cumulativetodeltaprocessor`).
  - Rejection of non-finite inputs (`NaN`, `±Inf`) and extreme overflow-prone finite inputs (`|x| > 1e140`, preventing `(delta)^2` overflow above `math.MaxFloat64` since `(2e140)^2 * 1e9 = 4e289 << 1.8e308`) with explicit drop counters before touching running accumulators.
  - Bounded cardinality (`max_series` hard cap enforced strictly across 64 shards via atomic CAS reservation with rollback on duplicate-key race) and idle series expiration (`idle_ttl`), plus optional `max_future_skew` guard against far-future timestamps wiping active windows.
  - Real `runtime.MemStats` heap scaling measurements across **10k metric identities** from **10k/s to 500k/s** for a fixed **5-minute (`300s`) sliding window** (`3M` to `150M` real simulated events, no extrapolation).

### Intentional Prototype Simplifications (Out of Scope)
- **Not wired into distribution manifests**: Standalone module; `specs/google-built-opentelemetry-collector.yaml`, `google-built-opentelemetry-collector/manifest.yaml`, and `components/google-built-opentelemetry-collector/registry.yaml` are intentionally untouched during prototyping.
- **Logs-to-Metrics / `connector.LogsToMetrics`**: Log-to-metric conversion shares the exact same `WindowEngine` core, but wiring a separate `connector` interface (`plog.Logs -> pmetric.Metrics`) is deferred to productionization.
- **Histogram / Quantile Rollups**: Sketch-based sliding percentiles (e.g., DDSketch/HDR buckets) are deferred to follow-up evaluation (though `twostack_monoid` naturally supports mergeable sketches).
- **Persistence / Checkpoint Recovery across Restarts**: State is in-memory only.
- **Exact `int64` Accumulation Mode**: For `int64` values with magnitude `> 2^53`, conversion to `float64` loses least-significant bits; this precision boundary is documented and explicitly tested in M1 Gate 3, with a dedicated 128-bit integer accumulator path tracked in the productionization gap ledger.

---

## 4. Collector Telemetry Contract & Clock Semantics

1. **Stream Identity**: Uniquely keyed by `(Resource.Attributes + Resource.SchemaUrl, Scope.Name + Scope.Version + Scope.Attributes + Scope.SchemaUrl, Metric.Name + Metric.Unit + Metric.Type + Metric.Temporality, DataPoint.Attributes)` using canonical sorted encoding hashed with 128-bit FNV-1a (`[2]uint64`) plus full string equality check inside the shard map to prevent collisions (ensuring identical metric names from different hosts/resources never collide).
2. **Output Schema**:
   - Every emitted sliding-window rollup is emitted as a **`Gauge` snapshot** (never as overlapping `Sum(Delta)`, which breaks OTel/Prometheus/Cloud Monitoring disjoint-interval delta invariants).
   - Metric naming suffixes: `<name>.window.sum` (input unit), `<name>.window.count` (unit `1`), `<name>.window.mean` (input unit), `<name>.window.stddev` (input unit), `<name>.window.rate` (unit `<unit>/s` if non-empty, else `1/s`), `<name>.window.min` / `<name>.window.max` (input unit).
   - Timestamps: `StartTimestamp = windowStart` (oldest active bucket start or exact `emitTime - W`), `Timestamp = emitTime`.
   - Empty-window (`count == 0`) silent ticks before `idle_ttl`: emit `0` only for `count`, `sum`, and `rate`, and **omit** `mean`, `stddev`, `min`, and `max` (never fabricate `0.0` for undefined sample location/dispersion statistics on empty windows).
   - `DataPoint.Flags().NoRecordedValue()`: Skipped cleanly (never ingested as value `0`, incremented in `dropped_no_recorded_value` counter).
   - `DataPoint.Timestamp() == 0`: Controlled by `fallback_to_arrival_time` (default `false`: drops datapoint and increments `dropped_missing_timestamp` counter so missing timestamps do not silently corrupt event-time windows; when `true`, stamps with `clock.Now()`).
3. **Warmup `rate` & `Gauge` `rate` Semantics**:
   - Controlled by `rate_mode`: `fixed_window` (default: `sum / window_size.Seconds()`) vs `elapsed_window` (`sum / max(bucket_duration, min(elapsed, window_size)).Seconds()`), where `elapsed = now - series.firstSeenEventTime` (per-series event-time age, resetting whenever the window empties to `count == 0`, whether by a full window gap `steps >= B` or partial advance). Note: during startup before a full window `W` has elapsed, `fixed_window` under-reports rate proportionally to `elapsed / W`.
   - `rate` is emitted for `Sum(Delta)` by default; optional `allow_gauge_rate` (default `false`) enables emitting `rate` for `Gauge` inputs (since `sum / W` on a gauge depends on scrape frequency and is dimensionally distinct from a counter rate).
4. **Clock, Silent Series Eviction & `max_future_skew`**:
   - The processor holds an injectable `clock func() time.Time` (default `time.Now`).
   - Every `Query(nowNanos)` and periodic `emit_interval` tick explicitly invokes `Advance(nowNanos)` on active series so silent series evict expired buckets (emitting `0` for `count`/`sum`/`rate` and omitting `mean`/`stddev`/`min`/`max` once `count == 0` until `idle_ttl` deletes the series). On bucket-aligned `emit_interval` ticks targeting exact completed window `[(k - B)Δ, kΔ)` of width `W`, callers query at **`nowNanos = k*Δ - 1`** (inclusive nanosecond end of bucket `k - 1`, matching continuous half-open `(now - W, now]`); querying at `nowNanos = k*Δ` immediately opens empty bucket `k` and yields width `W - Δ` (mean bias `-Δ/2` across arbitrary mid-bucket queries).
   - `max_future_skew` (default `0` = disabled for deterministic offline/replay tests): when `> 0`, incoming datapoints with `ts > clock.Now() + max_future_skew` are rejected (incrementing `dropped_future_skew`) strictly against wall-clock `clock.Now()`, NEVER against a series' own `headBucket` (preventing permanent deadlock after a quiet period shorter than `idle_ttl`).
   - Series state carries an explicit `initialized bool` so the first event (even with early or epoch-adjacent timestamps) initializes `headBucket` without being compared against `0`.

---

## 5. Candidate Engine Architectures (Fair Symmetric Bakeoff)

All 5 engines share the exact same key identity, 64-shard `sync.Mutex` map, `max_series` CAS reservation, non-finite/overflow rejection, and synthetic input streams. No candidate receives generic Go tuning denied to baselines.

| Engine | Memory / Series (`B=60` / `B=300`, 10k series `internal/engine` estimate) | In-Order Ingest | Out-of-Order Late Ingest | Window Eviction | Query | Exact `Min`/`Max`? | Numerical Accuracy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`raw_rescan`** (Baseline A) | $O(N \cdot W)$ (`16 B/event`: ring deque + OOO min-heap) | $O(1)$ amortized | $O(\log(N \cdot W))$ (min-heap) | $O(k_{\text{in-order}} + k_{\text{ooo}} \log(N \cdot W))$ (half-open `(now - W, now]`, evict `ts <= now - W`) | $O(N \cdot W)$ (anchored Welford / compensated two-pass scan) | Yes | Reference exact continuous window (`~1e-16` at `1e9` & `1e12`, verified against 256-bit `big.Float` oracle) |
| **`raw_inc`** (Baseline B) | $O(N \cdot W)$ (`16 B/event`: ring deque + OOO min-heap) | $O(1)$ amortized | $O(\log(N \cdot W))$ (min-heap) | $O(k_{\text{in-order}} + k_{\text{ooo}} \log(N \cdot W))$ incremental `seriesAnchor`-shifted Reverse Chan ($O(N \cdot W)$ worst-case on re-fold; periodic cap every `10,000` evictions or high-water guard) | $O(1)$ | No | Fair precision parity (`~1e-16` shifted); isolates $O(N \cdot W)$ memory scaling from rescan CPU cost |
| **`algebraic_naive`** (Candidate 1) | $O(B)$ (`24 B/bucket`: ~15 MiB / ~78 MiB) | $O(1)$ (0 divisions) | $O(1)$ | $O(1)$ per bucket (`S -= b.sum`, `Sq -= b.sumSq`) | $O(1)$ | No | **HIGH RISK**: Catastrophic cancellation at `1e9 + ε` & `1e12 + ε`; permanent residual drift after spikes |
| **`algebraic_chan`** (Candidate 2, **Recommended**) | $O(B)$ (`32 B/bucket` locally anchored: ~20 MiB / ~96 MiB, Go class `9728 B`) | $O(1)$ (1 Welford update on `head` only; deferred fold into `closedAcc`) | $O(1)$ (head-only if `eventBucket == headBucket`; bucket + `closedAcc` if closed) | $O(1)$ Reverse Chan (`evict_then_close`) + high-water-mark & periodic $O(B)$ re-fold | $O(1)$ (1 Chan merge of `closedAcc` + `head`) | No | **MACHINE-EPSILON**: Exact zero reset; `~1e-16` at `1e9` & `1e12`; automatic spike & staircase recovery (`rel err <= 1e-9` sum/mean, `<= 1e-7` stddev) |
| **`twostack_monoid`** (Candidate 3) | $O(B)$ (`96 B/bucket`: 48 B locally anchored raw bucket incl. `min`/`max` + 48 B front-stack suffix agg: ~59 MiB / ~315 MiB, Go class `32,768 B` / `2x16,384 B` rounding cliff) | $O(1)$ (Welford on `head` only) | Let `k = headBucket - eventBucket` in `[1, B-1]`: $O(1)$ on `back` hit (commutative update to `backAgg`); $O(B - k)$ on `front` hit (worst case $O(B)$ at `k=1` post-flip; expected `1 + k(B-k-1)/(B-1)`, peaking `~B/4` at `k=B/2`) | $O(1)$ amortized (Two-Stack / DABA reverse on empty front) | $O(1)$ (Chan combine of front top + back agg + `head`) | **Yes** | **ZERO SUBTRACTION**: Forward Chan combine only (`~1e-16` at `1e9` & `1e12`) |

*Note on `twostack_monoid` OOO complexity*: If a future workload requires both zero subtraction (`min`/`max`) AND worst-case sub-linear OOO latency, a FlatFAT / binary segment tree over `B` buckets gives $O(\log B)$ worst-case OOO updates in `~96 B/bucket` (`2B` nodes). That alternative is documented in `PROTOTYPE_LEARNINGS.md` rather than adding a 6th engine here.

### Key Mathematical & State-Transition Details for `algebraic_chan`

1. **Bucket Discretization, Ring Indexing & Window Boundary Sign (`B` Slots)**:
   - Floor-divide timestamp nanos by `bucketNanos`: `bucketID = floorDiv(tsNanos, bucketNanos)`. Bucket `k` covers the half-open interval `[kΔ, (k+1)Δ)`.
   - The ring stores **`B` active slots** `[headBucket - B + 1, headBucket]`.
   - `closedAcc` tracks the exact union of closed buckets `[headBucket - B + 1, headBucket - 1]` (shifted relative to per-series `anchor`).
   - `head` (`buckets[posMod(headBucket, B)]`) tracks the currently open bucket `headBucket`.
   - **Exactness vs. Sub-Bucket Boundary Bias**:
     - Continuous baselines (`raw_rescan` and `raw_inc`) define the exact half-open sliding window **`(now - W, now]`** (evicting `ts <= now - W`).
     - Exact **0 boundary discretization error** occurs at `now = k*Δ - 1` (the inclusive nanosecond end of completed bucket `k - 1`, so `floorDiv(now, Δ) = k - 1`), where `B` completed buckets `[k - B, k - 1]` span `[(k - B)Δ, kΔ)` and match `(now - W, now]` for every integer nanosecond timestamp! Note: passing `now = k*Δ` advances `headBucket` to empty bucket `k` and evicts bucket `k - B`, yielding width `W - Δ`.
     - At an arbitrary mid-bucket timestamp `t = (k + f)Δ` (`0 <= f < 1`) AFTER advancing `headBucket = k`, the active slots `[k - B + 1, k]` cover `[(k - B + 1)Δ, (k + f)Δ]`, which has width **`W - (1 - f)Δ` in `[W - Δ, W)`** (under-covering on average by **`-Δ/2`**, i.e., biasing `sum` / `count` low by `-1/(2B)` unless normalized by effective span!). At `t = kΔ` (`f = 0`), right after advancing to `k`, width is `W - Δ` (`-1.67%` for `B=60`, `-0.33%` for `B=300`).
     - *(Alternative documented for comparison)*: Using `B + 1` slots (`[h - B, h]`) retains the oldest bucket past `t - W`, giving width `[W, W + Δ)` (mean over-coverage `+Δ/2`, `+1` bucket per series). This prototype uses `B` slots with explicit reporting of mid-bucket under-coverage for `B ∈ {60, 300, 600}`.

2. **Per-Bucket Locally Anchored Welford Storage (`32 B/bucket`)**:
   - Each ring slot stores `{count uint64, anchor float64, meanShifted float64, m2 float64}` (`32 bytes` exact, 0 padding, 2 buckets per 64-byte cache line).
   - First point inserted into an empty bucket sets `b.anchor = x`, `b.count = 1`, `b.meanShifted = 0`, `b.m2 = 0`. Subsequent points compute `xs := x - b.anchor` and run Welford (`delta = xs - b.meanShifted`, `b.meanShifted += delta / float64(b.count)`, `b.m2 += delta * (xs - b.meanShifted)`).
   - Why per-bucket local anchor instead of unshifted `24 B/bucket`? At offset `1e12 + [0, 1]`, unshifted Welford quantizes `b.mean` to `math.ulp(1e12) = 1.22e-4` inside the bucket, yielding `2.66e-5` stddev error (failing `1e-7`). With per-bucket local anchoring, by Sterbenz's Lemma `x - b.anchor` is exact (`0` rounding error) and keeps stddev relative error at `~1e-16` (`3.84e-16` measured at `1e12` and `5.77e-16` at `1e9` across 100 pts/bucket in M1!).
   - When shifting bucket `b` into `closedAcc` relative to `seriesAnchor`:
     `bMeanRel := (b.anchor - seriesAnchor) + b.meanShifted`.

3. **Forward Chan Merge & Reverse Chan Algebraic Downdate**:
   - Always use safe `float64` ordering `(float64(na) / float64(nt)) * float64(nb)` (NEVER `na * nb` in integer arithmetic, which overflows `MaxInt64` above `3e9` counts!).
   - Forward merge of `b` into `closedAcc` (`A`):
     - If `A.count == 0`: set `seriesAnchor = b.anchor`, `A = {b.count, b.meanShifted, b.m2}`.
     - Else: `nt = na + nb`, `delta = bMeanRel - A.meanShifted`, `A.meanShifted += delta * (float64(nb) / float64(nt))`, `A.m2 += b.m2 + delta*delta*(float64(na)/float64(nt))*float64(nb)`, `A.count = nt`.
   - Reverse Chan downdate (evicting `b` from `closedAcc` `T`):
     - If `nt <= nb`: **exact zero reset**: `closedAcc = {0, 0, 0}`, `seriesAnchor = 0`, `peakM2 = 0`, `peakSumMag = 0`, `evictionsSinceRefold = 0`.
     - Else (`na = nt - nb > 0`):
       `bMeanRel = (b.anchor - seriesAnchor) + b.meanShifted`
       `oldMean = T.meanShifted`
       `newMean = oldMean - (float64(nb)/float64(na))*(bMeanRel - oldMean)`
       `newM2 = T.m2 - b.m2 - (bMeanRel - oldMean)*(bMeanRel - newMean)*float64(nb)`

4. **High-Water-Mark Cancellation Guards & Scale-Aware Bipolar Protection**:
   - Track per-series high-water marks since last re-fold / zero reset (`+16 B/series`, `0 B/bucket`):
     - `peakM2 = max(peakM2, closedAcc.m2)` (updated on every forward merge or late insert into `closedAcc`).
     - `peakSumMag = max(peakSumMag, |float64(closedAcc.count) * closedAcc.meanShifted|)` (updated on every forward merge or late insert into `closedAcc`).
   - On every non-empty eviction where `na > 0`, trigger `needsRefold = true` if ANY of:
     1. **Non-finite / Negative Guard** (explicitly handles `NaN` since `NaN < x` is `false`!):
        `!isFinite(newMean) || !isFinite(newM2) || newM2 < 0`
     2. **M2 High-Water-Mark Guard** (catches both single spikes AND decaying staircase ramps like `[20^7, 20^6, ..., 1]`!):
        `peakM2 > 0 && !(newM2 >= 1e-3*peakM2)`
     3. **Scale-Aware Mean High-Water-Mark Guard** (catches decaying mean ramps without thrashing on bipolar `±1` streams!):
        `scale := max(|float64(na)*newMean|, math.Sqrt(max(0, float64(na)*newM2)))`
        `peakSumMag > 0 && !(scale >= 1e-3*peakSumMag)`
     4. **Periodic Drift Cap**: `evictionsSinceRefold >= 10 * B` (amortized `0.1` Chan merges per bucket tick; guarantees bounded drift over infinite uptime).
   - *Sparse-scrape / `na == 1` note*: Under `evict_then_close`, when a window holds only 2 distinct single-point buckets (`B = 3` or sparse `100s` to `120s` scrapes on `W = 300s`), evicting 1 leaves `na == 1` with `newM2 = 0`, tripping the M2 guard (`100%` refold rate on those sparse evictions). Keeping the guard active at `na == 1` is intentional to guarantee post-spike accuracy; sparse scrapes already execute `steps ~ O(B)` slot zeroing at low frequency, so total CPU impact is negligible.

5. **Exact State Transitions (`evict_then_close`, Multi-Step Advance, OOO Head Guard)**:
   - **OOO inside `headBucket`** (`eventBucket == headBucket`, even if `ts < lastTs`): update `buckets[headSlot]` **ONLY** (NEVER touch `closedAcc`, avoiding double counting!).
   - **Late event inside closed range** (`headBucket - B + 1 <= eventBucket <= headBucket - 1`): update `buckets[slot]` via local-anchor Welford AND update `closedAcc` via shifted Welford (`x - seriesAnchor`, initializing `seriesAnchor = x` if `closedAcc.count == 0`), updating `peakM2` and `peakSumMag`.
   - **Expired late event** (`eventBucket <= headBucket - B`): drop and increment `droppedLate` counter.
   - **Advance `steps = eventBucket - headBucket >= B` (including `B == 1` whenever `steps >= 1`)**: `clear(buckets)`, reset `closedAcc = {}`, `seriesAnchor = 0`, `peakM2 = 0`, `peakSumMag = 0`, `evictionsSinceRefold = 0`, set `headBucket = eventBucket`.
   - **Advance `1 <= steps < B` (`evict_then_close` ordering)**:
     1. Loop `s = 1..steps`: `evictID := oldHead + s - B`, `slot := posMod(evictID, B)`.
        - If `!needsRefold` and `buckets[slot].count > 0`: run Reverse Chan downdate against `closedAcc` (updating `evictionsSinceRefold++`, setting `needsRefold = true` if any guard trips).
        - Zero `buckets[slot] = bucket{}` unconditionally (even after `needsRefold == true`, short-circuiting further `reverseChan` calls on corrupted state!).
     2. Set `headBucket = eventBucket`.
     3. If `needsRefold`: unconditionally reset `closedAcc = {}, seriesAnchor = 0, peakM2 = 0, peakSumMag = 0, evictionsSinceRefold = 0` first (so an all-empty surviving range after multi-step short-circuit clears cleanly), then run ONE forward Chan re-fold over `[newHead - B + 1, oldHead]` (picking the **newest** non-empty bucket in that range as the fresh `seriesAnchor` so re-anchoring jumps immediately to current traffic instead of an expiring spike!), updating `peakM2 = closedAcc.m2` and `peakSumMag = |n * closedAcc.meanShifted|`.
     4. Else (`!needsRefold`): close `oldHead` by forward Chan-merging `buckets[posMod(oldHead, B)]` into `closedAcc` (if non-empty) and updating `peakM2`, `peakSumMag`.
   - **Read `Sum()`**: `float64(totalCount) * (totalAnchor + totalMeanShifted)` (adds `anchor + meanShifted` before multiplying so near-canceling opposite-sign anchors remain exact via Sterbenz's Lemma and preserve the exact `Sum() == float64(Count()) * Mean()` invariant).

---

## 6. Milestones & Falsifiable Exit Gates

### Milestone 1 (M1): Core Engine Bakeoff, Numerical Precision & Oracle Parity
- **Deliverable**: `internal/engine/` implementing all 5 engines (`raw_rescan`, `raw_inc`, `algebraic_naive`, `algebraic_chan`, `twostack_monoid`) plus an exact arbitrary-precision / two-pass recomputation oracle (`oracle.go`, using `math/big.Float` 256-bit precision!) and test suite (`engine_test.go`).
- **Verification Command**: `go test -race -v ./internal/engine/...` (captured in `milestone_1_evidence.md`).
- **Falsifiable Exit Gates**:
  1. **Oracle Parity (Scale-Aware Tolerance)**: On bucket-aligned windows queried at `now = k*Δ - 1` (covering `[k - B, k - 1]`, matching continuous half-open `(now - W, now]`), **`raw_rescan`**, **`raw_inc`**, **`algebraic_chan`**, and **`twostack_monoid`** all match the 256-bit `big.Float` oracle with:
     - `count`: exact (`0` diff).
     - `mean`: `|got - want| <= max(1e-9 * |want|, 1e-9 * max(1.0, stddev_true))`.
     - `sum`: `|got - want| <= max(1e-9 * |want|, 1e-9 * float64(n) * max(1.0, stddev_true))`.
     - `stddev`: `|got - want| <= max(1e-7 * |want|, 1e-12)`.
     Tested across: steady-state traffic, `1e12` traffic spikes + eviction, **decaying staircase spikes (`[20^7, 20^6, ..., 1]`)**, **high-offset `1e9 + [0, 1]` and `1e12 + [0, 1]`**, zero-mean Gaussian `N(0, 1)`, **alternating bipolar `±1`**, `10,000`-bucket long-horizon drift, and `15%` out-of-order streams (including OOO within `headBucket`!).
  2. **Baseline-vs-Change Proof (Non-Tautological)**: Explicitly assert that `algebraic_naive` **FAILS** the exact same spike-eviction, decaying-staircase, and high-offset `1e9 + [0, 1]` / `1e12 + [0, 1]` checks (producing negative variance / `>100%` error or residual drift after window empties / spike expires), whereas `algebraic_chan` and `twostack_monoid` **PASS**.
  3. **Safety, Thrashing & Boundary Cases**:
     - `NaN`, `±Inf`, and extreme finite `±1e200` (`> 1e140`) inputs rejected cleanly (drop counters incremented, zero accumulator poisoning).
     - Time gaps (`dt = 10*W`, `dt = 365 days`) complete in $O(B)$ without looping `dt / Δ`.
     - `B = 1` works cleanly; late events `<= headBucket - B` dropped without slot aliasing; explicit `Advance(now)` evicts silent series to `0`.
     - `int64 > 2^53` boundary test explicitly documents `float64` ULP loss (`(1<<53)+1 == (1<<53)` in `float64`).
     - Zero-thrashing check (for `B > 10`, tested at even `B = 60` / `300` and odd `B = 59` / `299`): `refoldCount` stays **`< 5%`** of evictions on `N(0, 1)` over `5,000` buckets AND **`< 1%`** (`0` cancellation refolds + periodic cap `1/(10*B)` only) on alternating bipolar `±1` over `2,000` buckets.
  4. **Sub-Bucket Discretization Error Quantified**: Report measured max & mean relative error (including systematic `-Δ/2` sign) vs. continuous exact oracle for `B ∈ {60, 300, 600}`.

### Milestone 2 (M2): OTel Collector `metricaggregationprocessor` Integration
- **Deliverable**: Full `processor.Metrics` component in `components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/`:
  - `factory.go`, `config.go`, `processor.go`, `doc.go`, `metadata.yaml`, `internal/metadata/generated_status.go`, `go.mod`, `go.sum`, `Makefile`.
  - 64-shard concurrency-safe store with full stream identity key (`Resource + Scope + Metric + DataPoint.Attributes`) and atomic CAS `max_series` cap.
  - Configurable `window_size`, `bucket_duration`, `max_buckets` (default `10,000`), `engine` (`algebraic_chan` default, `twostack_monoid`, `algebraic_naive`, `raw_rescan`, `raw_inc`), `aggregations` (`sum`, `count`, `mean`, `stddev`, `rate`, `min`, `max`), `variance_mode` (`sample` | `population`), `rate_mode` (`fixed_window` | `elapsed_window`), `allow_gauge_rate`, `max_series`, `idle_ttl`, `max_future_skew`, `fallback_to_arrival_time`, `emit_interval` (or synchronous `on_consume` mode for deterministic tests), `passthrough_input`, and `metric_names` allowlist.
  - Full unit, lifecycle (`goleak`), config validation, concurrency (`-race`), `NoRecordedValue`, `Timestamp == 0`, cumulative/unspecified pass-through, and cardinality-eviction tests.
- **Verification Command**: `go test -race -v ./...` inside `metricaggregationprocessor` (captured in `milestone_2_evidence.md`).
- **Falsifiable Exit Gates**:
  1. Passes `go test -race ./...` including `goleak` goroutine leak verification and `componenttest.CheckConfigStruct`.
  2. Cumulative sums, unspecified-temporality sums, histograms, exponential histograms, and summaries pass through untouched; `NoRecordedValue` datapoints are skipped; all rolled-up metrics emit as **`Gauge`** snapshots with proper units and timestamps.
  3. Bounded cardinality: concurrent insertion of `2 * max_series` unique streams across multiple goroutines caps active series **strictly `<= max_series`** (zero CAS overshoot under `-race`) and increments `dropped_cardinality`; idle series expire after `idle_ttl` and silent series advance to zero on `emit_interval`.
  4. Baseline-vs-Change Proof: Verify `Validate()` rejects invalid configs (`window_size <= 0`, `bucket_duration <= 0`, `window_size % bucket_duration != 0`, `B > max_buckets`, unknown `engine`, requesting `min`/`max` on unsupported engines like `algebraic_chan`/`algebraic_naive`/`raw_inc`), verify `passthrough_input=false` vs `true`, and verify `max_future_skew` blocks rogue far-future timestamp resets against `clock.Now()` without deadlocking quiet series.

### Milestone 3 (M3): Scale Memory Sweep (10k to 500k/s across 10k Series) & Serial `benchstat` Suite
- **Deliverable**:
  - `internal/engine/bench_test.go`: strictly serial microbenchmarks comparing `raw_rescan`, `raw_inc`, `algebraic_naive`, `algebraic_chan`, and `twostack_monoid` on in-order ingest, query, out-of-order (`0%`, `5%`, `15%`), spike eviction, and bipolar `±1` streams.
  - `bench_test.go` (root processor package): end-to-end `pmetric.Metrics` `ConsumeMetrics` batch latency, allocs/batch, and metadata heap retention across `10k` series (explicitly isolating engine cost vs `pmetric`/attribute-hashing overhead).
  - `cmd/scalebench/main.go`: dedicated standalone memory & GC scaling harness operating directly on `internal/engine` (`uint64` series IDs) across **10,000 active series** over a **fixed 5-minute (`300s`) sliding window** at rates **`10k/s` (3M events), `50k/s` (15M), `100k/s` (30M), `250k/s` (75M), `500k/s` (150M)** for both `B=60` (5s buckets) and `B=300` (1s buckets). Measures real `runtime.MemStats` (`HeapAlloc` after double `runtime.GC()`, `HeapObjects`, `NumGC`, `PauseTotalNs`, `MaxPauseNs`).
  - Explicit `scalebench` query cadence: ingest full 300s window (measuring live heap + GC pauses + ingest throughput), THEN execute one full query pass across all 10k series per simulated 5s tick (or separately timed 10k-series sweep) so `raw_rescan`'s $O(N \cdot W)$ query cost is measured cleanly without stalling for hours.
- **Verification Commands**:
  - Strictly serial `go test -bench=. -benchmem -count=6 -benchtime=200ms ./internal/engine` + `benchstat` (comparing engines head-to-head via sub-benchmark files or `benchstat -row` format).
  - Strictly serial `go test -bench=. -benchmem -count=6 -benchtime=200ms .` (end-to-end processor).
  - Strictly serial `go run ./cmd/scalebench` (one engine/rate per fresh subprocess or GC-isolated run, captured in `milestone_3_evidence.md`).
- **Falsifiable Exit Gates**:
  1. **Flat Heap Scaling (`internal/engine`, 10k series, `W = 300s`)**: Across `10k/s -> 500k/s` (`50x` rate increase), `algebraic_chan` live `HeapAlloc` changes by **`< 5%`** (expected **`~20 MiB`** for `B=60` and **`~96 MiB`** for `B=300`, Go size class `9728 B`), while `raw_rescan` and `raw_inc` scale linearly to **`>= 2.2 GiB` (`>= 2.4 GB` decimal)** at `500k/s` (expected scaling ratio `~32x` for power-of-2 ring growth `512 -> 16,384` slots, or `~46x` for exact-sized growth).
  2. **Throughput & Allocations (`internal/engine`)**: `algebraic_chan` sustains **`>= 500k events/sec`** across 10k series on this host (`0 allocs/op` in steady state), with `benchstat` comparisons reported across all 5 engines.
  3. **End-to-End Collector Batch Overhead**: Measure and report `pmetric.Metrics` `ConsumeMetrics` `ns/datapoint`, `B/batch`, `allocs/batch`, and metadata `HeapAlloc` overhead for 10k cloned series (`Resource`/`Scope`/`DataPoint` attributes).
  4. **Out-of-Order Tradeoff Measured**: Quantify `algebraic_chan` ($O(1)$) vs `twostack_monoid` ($O(B-k)$ front rebuild) vs `raw_inc` ($O(\log(N \cdot W))$ min-heap) under `5%` and `15%` late arrivals.

---

## 7. Adversarial Review Gate Schedule

- **Gate 1 (Plan Audit)**: `adversarial-review-loop` (`single-review --mode=self-review`) on `PROTOTYPE_PLAN.md` before writing code.
- **Gate 2 (Milestone Audits)**: `adversarial-review-loop` on each completed milestone (`M1`, `M2`, `M3`) code diff + `milestone_<k>_evidence.md`.
- **Gate 3 (Learnings & Walkthrough Audit)**: `adversarial-review-loop` across `PROTOTYPE_LEARNINGS.md` and `DEMO_AND_WALKTHROUGH_PLAN.md` before final parking.

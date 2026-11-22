# Milestone 1 Evidence: Core Engine Bakeoff, Numerical Precision & Oracle Parity

**Commands Executed**:
```bash
cd components/google-built-opentelemetry-collector/processor/metricaggregationprocessor
GOWORK=off go test -race -v ./internal/engine/...
GOWORK=off go test -coverprofile=/tmp/m1_cov.out ./internal/engine/...
```
**Result**: `PASS` (`9.088s` with `-race`, `1.673s` non-race, **`98.1%`** statement coverage, `0` race conditions, all 5 engines verified against the 256-bit `math/big.Float` exact recomputation oracle).

---

## 1. Struct Memory Layout Verification (`TestStructSizes`)

| Struct | Engine | `unsafe.Sizeof` | Cache Line Packing |
| :--- | :--- | :---: | :--- |
| `rawSample` | `raw_rescan`, `raw_inc` | `16 B` | 4 samples / 64 B cache line |
| `naiveBucket` | `algebraic_naive` | `24 B` | unshifted `(count, sum, sumSq)` |
| `chanBucket` | `algebraic_chan` | **`32 B`** | 2 buckets / 64 B cache line (locally anchored `(count, anchor, meanShifted, m2)`) |
| `monoBucket` | `twostack_monoid` | `48 B` | locally anchored `(count, anchor, meanShifted, m2, min, max)`; `96 B/bucket` across `buckets` + `frontAgg` |

---

## 2. Oracle Parity Across All Workloads (`TestOracleParity_AllWorkloads`)

All bucket-aligned queries evaluated at `now = k*Delta - 1` (covering exact completed window `[k - B, k - 1]`, matching continuous half-open `(now - W, now]`) against 256-bit `big.Float` exact recomputation (`ComputeExactOracle`). Scale-aware tolerances: `mean <= max(1e-9*|want|, 1e-9*max(1, sd))`, `sum <= max(1e-9*|want|, 1e-9*n*max(1, sd))`, `stddev <= max(1e-7*|want|, 1e-12)`, `M2 >= 0.0` strictly, `count` exact `0` diff, `min`/`max` exact on `raw_rescan` and `twostack_monoid`.

| Workload | `raw_rescan` maxRelMean / maxRelSD | `raw_inc` maxRelMean / maxRelSD (refolds) | `algebraic_chan` maxRelMean / maxRelSD (refolds) | `twostack_monoid` maxRelMean / maxRelSD (flips) |
| :--- | :---: | :---: | :---: | :---: |
| Steady uniform `[0, 100]` (`B=60`) | `1.56e-15` / `7.19e-16` | `4.35e-16` / `1.57e-15` (0) | **`1.46e-16` / `5.99e-16`** (0) | `1.48e-16` / `3.75e-16` (3) |
| Spike `1e12` then evict (`B=60`) | `5.20e-15` / `5.72e-16` | `4.50e-16` / `4.38e-16` (1 cancel) | **`2.59e-16` / `2.44e-16`** (1 cancel) | `2.59e-16` / `2.44e-16` (3) |
| First-bucket spike `1e12` re-anchor (`B=60`) | `8.13e-16` / `6.93e-16` | `5.17e-14` / `6.49e-16` (1 cancel) | **`8.14e-14` / `3.72e-16`** (1 cancel) | `8.14e-14` / `3.72e-16` (2) |
| Decaying staircase `[20^7..1]` (`B=10`)* | `3.39e-07` / `3.35e-16` | `1.70e-06` / `2.26e-14` (4 cancel) | **`1.70e-06` / `3.36e-14`** (4 cancel) | `9.54e-07` / `1.68e-16` (4) |
| High-offset `1e9 + [0, 1]` (`100 pts/b`, `B=60`) | `0.00e+00` / `2.30e-15` | `0.00e+00` / `3.07e-15` (0) | **`0.00e+00` / `5.77e-16`** (0) | `0.00e+00` / `3.78e-16` (2) |
| High-offset `1e12 + [0, 1]` (`100 pts/b`, `B=60`) | `0.00e+00` / `2.31e-15` | `0.00e+00` / `1.55e-15` (0) | **`0.00e+00` / `3.84e-16`** (0) | `0.00e+00` / `3.86e-16` (2) |
| Zero-mean Gaussian `N(0, 1)` (`B=60`) | `8.69e-12` / `8.51e-16` | `1.67e-11` / `1.29e-15` (0) | **`7.77e-12` / `4.68e-16`** (0) | `7.88e-13` / `4.26e-16` (3) |
| Alternating bipolar `±1` (`B=59` odd) | `2.35e-14` / `2.22e-16` | `2.35e-14` / `2.22e-16` (0) | **`2.66e-15` / `2.22e-16`** (0) | `2.66e-15` / `2.22e-16` (3) |
| Alternating bipolar `±1` (`B=60` even) | `1.87e-14` / `2.22e-16` | `1.87e-14` / `2.22e-16` (0) | **`1.41e-15` / `2.22e-16`** (0) | `1.41e-15` / `2.22e-16` (3) |
| Opposite-sign canceling `(+1e12/3, -1e12/3 + 1e-3)` (`B=60`)** | `1.50e+00` / `9.16e-16` | `1.50e+00` / `9.16e-16` (0) | **`4.56e-15` / `1.83e-16`** (0) | `1.25e-01` / `1.83e-16` (2) |
| Long horizon `10,000` buckets (`1e10` spikes/500) | `1.78e-16` / `4.42e-16` | `5.33e-16` / `2.79e-15` (20 cancel) | **`1.80e-16` / `1.74e-15`** (20 cancel) | `1.79e-16` / `3.85e-16` (169) |
| 15% out-of-order (incl. `headBucket` OOO, `B=30`) | `7.92e-16` / `4.83e-16` | `5.55e-16` / `1.15e-15` (3 cancel) | **`4.45e-16` / `7.46e-16`** (3 cancel) | `6.18e-16` / `5.41e-16` (5 flips; `backHits=259`, `frontRebuilds=206`) |

*\*Note on `decaying_staircase` unscaled `maxRelMean`*: Because symmetric `±v` pairs cancel in the mean while `stddev` remains orders of magnitude larger, unscaled `|got - want| / |want|` peaks at two distinct steps:
- At `step=10` (`want.Mean = 0.045`, `want.StdDev ≈ 2.08e7`, `meanTol ≈ 0.0208`): `raw_inc` and `algebraic_chan` peak at **`1.70e-6`** (min margin **`2.73e5x`** vs scale-aware tolerance). At this same step, `twostack_monoid` is `3.53e-9` (`1.31e8x` margin) and `raw_rescan` is `7.09e-9` (`6.52e7x` margin).
- At `step=8` (`want.Mean ≈ 0.0167`, `want.StdDev ≈ 4.40e8`, `meanTol ≈ 0.440`): `twostack_monoid` peaks at **`9.54e-7`** (min margin **`2.77e7x`**) and `raw_rescan` peaks at **`3.39e-7`** (`7.78e7x` margin; `raw_rescan`'s overall minimum margin is **`4.55e7x`** at `step=12`).
All four accurate engines pass with **`>= 2.73e5x`** margin against the scale-aware mean tolerance.

*\*\*Note on `sterbenz_cancel` unscaled `maxRelMean`*: With `a = 1e12/3.0` (`ulp(a) = 2^-14 = 6.1035e-5`), `b = -a + 1e-3` rounds in IEEE-754 to `-a + 16*ulp(a)`, giving exact even-window mean `4.8828125e-4` (`2^-11`) while `stddev ≈ 3.333e11`. Because `1 ULP` of `a` is `12.5%` of `4.88e-4`, unscaled `|got - want| / |want|` reaches `1.50` on `raw_rescan`/`raw_inc` and `0.125` on `twostack_monoid` (while `algebraic_chan` achieved `4.56e-15`). Measured minimum margins vs the scale-aware tolerance (`meanTol = 1e-9 * 3.333e11 = 333.3`) across all queried steps are **`3.84e5x`** on `raw_rescan`/`raw_inc`, **`5.46e6x`** on `twostack_monoid`, and **`1.20e7x`** on `algebraic_chan`.

---

## 3. Baseline-vs-Change Proof (Non-Tautological): `algebraic_naive` Failures vs. Candidates

Executed in `TestBaselineVsChange_AlgebraicNaiveFails` (verifying `naive`, `algebraic_chan`, AND `twostack_monoid` side-by-side on every case):

| Adversarial Case | Exact Oracle | `algebraic_naive` (Fails) | `algebraic_chan` (Passes) | `twostack_monoid` (Passes) |
| :--- | :--- | :--- | :--- | :--- |
| High-offset `1e9 + [0, 1]` | `sd = 0.299156271430` | **`raw_var = -2.407e+03`** (negative!), `sd = 0.0` (`100%` error) | `sd = 0.299156271430` | `sd = 0.299156271430` |
| High-offset `1e12 + [0, 1]` | `sd = 0.281472978444` | **`raw_var = +2.065e+09`**, `sd = 45,442.56` (**`161,000x` error!**) | `sd = 0.281472978444` | `sd = 0.281472978444` |
| `1e17` spike + `1.0` + `2.0` after spike evicts | `sum = 3.0` | **`sum = 0.0`** (`100%` absorbed & lost) | `sum = 3.0` (**`cancelRefolds = 1`**) | `sum = 3.0` |
| Same window after emptying (`count == 0`) | `sum = 0.0` | **`phantom sum = -3.0`** on empty window! | `sum = 0.0` | `sum = 0.0` |
| Decaying staircase `[20^7..1]` after decay | `sd = 0.051298917604` | **`raw_var = -15.27`** (negative!), `sd = 0.0` (`100%` error) | `sd = 0.051298917604` (`refolds = 4`) | `sd = 0.051298917604` (`flips = 2`) |

---

## 4. Safety, Non-Thrashing & Boundary Verification (`TestSafetyAndBoundaries`)

- **Non-finite & overflow rejection**: `NaN`, `+Inf`, `-Inf` (`DroppedNonFinite = 3`) and `+1e200`, `-1e200`, `+1e141` (`DroppedOverflow = 3`) rejected cleanly on all 5 engines with zero accumulator poisoning (`Count = 2`, `Sum = 30.0`, `Mean = 15.0`).
- **Time-gap fast path (`10*W` + `365 days` / `31.5M` buckets, measured under `-race`)**:
  - `raw_rescan`: `23.857µs`
  - `raw_inc`: `27.826µs`
  - `algebraic_naive`: `112.094µs`
  - `algebraic_chan`: `332.807µs`
  - `twostack_monoid`: `133.4µs`
- **Signed `int64` timestamp extreme safety**:
  - `math.MinInt64 + 2s` with `W = 10s`: unsigned distance arithmetic prevents positive cutoff wrap; `WindowStartNanos` saturates at `math.MinInt64` across all 5 engines.
  - `-5e18 -> +5e18` jump at `bucketDur = 1ns`: unsigned step count prevents negative overflow; old samples evicted cleanly and late `-5e18` rejected as `AddDroppedLate`.
  - Exact `math.MinInt64` descending loops in `refoldClosed`, `twoStackMonoid` stack flip, and front-stack OOO rebuild terminate via slot counts without underflow wrap.
- **`peakSumMag` guard independent of `peakM2`**: Bulk `5,000`-point burst eviction triggers `CancelRefolds >= 1` on both `algebraic_chan` and `raw_inc` while surviving `M2` stays well above `1e-3 * peakM2` and eviction count stays below `rawPeriodicRefoldInterval` (`10,000`).
- **`raw_inc` periodic refold & `rawRing.grow` wrap-around**: `12,000` steady evictions hit `PeriodicRefolds >= 1` (`CancelRefolds = 0`); wrap-around copy branch verified on `head = 10, size = 16 -> 20`.
- **Empty surviving multi-step refold consistency & `FirstSeenNanos` reset**: Jumping `7` buckets clearing all remaining data (`1e12`, `1.0`, `2.0`) leaves `Refolds = 0, CancelRefolds = 0`, `HasFirstSeen = false`, `FirstSeenNanos = 0` consistently across both `raw_inc` and `algebraic_chan`; verified uninitialized `Advance`/`Snapshot`, `LastActivityNanos`, `Initialized`, and partial-advance empty reset across all 5 engines.
- **`B = 1` & Late-event slot aliasing**: Late events at `headBucket - B` rejected with `AddDroppedLate`, `0` slot corruption.
- **Negative pre-epoch timestamps**: `floorDiv`/`posMod` exact on `-10s .. -8s` across all accurate engines.
- **`int64 > 2^53` boundary**: Documented `float64` precision floor (`(1<<53)+1 == 1<<53`, mean of `{2^53, 2^53+2}` rounds to `9007199254740992`).
- **Boundary shift check**: `Query(k*Delta - 1)` retains all `B = 5` completed buckets (`Count = 5`); `Query(k*Delta)` opens bucket `k` and evicts bucket `k - B` (`Count = 4`, width `W - Delta`).
- **Zero-thrashing refold rate (`algebraic_chan`)**:
  - `N(0, 1)` over `5,000` buckets (`B = 60`): `evictions = 4940`, **`cancelRefolds = 0`**, `periodicRefolds = 8` (`0.162% << 5%`).
  - Bipolar `±1` over `2,000` buckets (`B > 10`):
    - `B = 59`: `cancel = 0`, `periodic = 3`, total **`0.155%`** (`<< 1%`)
    - `B = 60`: `cancel = 0`, `periodic = 3`, total **`0.155%`** (`<< 1%`)
    - `B = 299`: `cancel = 0`, `periodic = 0`, total **`0.000%`**
    - `B = 300`: `cancel = 0`, `periodic = 0`, total **`0.000%`**
- **Sparse-scrape `B = 3` (`na == 1`) accuracy**: Triggers `47/47` cancellation refolds as documented, maintaining exact `maxRelMean = 1.67e-16` and `maxRelSD = 0.00e+00`.

---

## 5. Sub-Bucket Discretization Error vs. Continuous Oracle (`TestSubBucketDiscretizationError`)

Measured on fixed `W = 5m (300s)` at constant rate (`20 events/s`, `6,000` events/window, sampled over `[310s, 330s)` covering `4` full `5s` cycles) across `B` slots (`[headBucket-B+1, headBucket]`):

| Bucket Count `B` | Bucket Width `Delta` | Mean Signed Err (Theory `-1/(2B)`) | Min Signed Err | Max Abs Err (Theory `<= 1/B`) |
| :---: | :---: | :---: | :---: | :---: |
| `60` | `5s` | **`-0.8250%`** (`-0.8333%`) | `-1.6500%` | `1.6500%` (`<= 1.6667%`) |
| `300` | `1s` | **`-0.1583%`** (`-0.1667%`) | `-0.3167%` | `0.3167%` (`<= 0.3333%`) |
| `600` | `500ms` | **`-0.0750%`** (`-0.0833%`) | `-0.1500%` | `0.1500%` (`<= 0.1667%`) |

All M1 Exit Gates passed.

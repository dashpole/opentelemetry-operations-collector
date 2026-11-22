# Milestone 3 Evidence: Scale Memory Sweep (`10k` to `500k/s`) & Serial `benchstat` Suite

**Environment**:
- Host CPU: `Intel(R) Xeon(R) CPU @ 2.20GHz` (`24` vCPUs, `linux/amd64`), `94 GiB` RAM
- Toolchain: `go version go1.27.3 linux/amd64`, `golang.org/x/perf/cmd/benchstat`
- Execution discipline: **strictly serial** (one benchmark suite or isolated `scalebench` subprocess at a time; zero concurrent CPU competition).
- Unit test & vet verification after M3 benchmark/harness updates:
  - `GOWORK=off go vet ./...`: clean (0 issues).
  - `GOWORK=off go test -race -count=1 ./...`: `metricaggregationprocessor` (`1.263s`) + `internal/engine` (`9.063s`) = **100% PASS** (`cmd/scalebench` and `internal/metadata` have no test files).
  - High-vCPU parallel disjoint-ownership smoke check (`-cpu=128 -benchtime=2x`): `BenchmarkConsumeMetricsParallel/batch=100/engine=algebraic_chan-128` passed with `0` panics and `0` late drops (`hosts` scales dynamically to `max(100, GOMAXPROCS)`).

**Commands Executed**:
```bash
cd components/google-built-opentelemetry-collector/processor/metricaggregationprocessor
BENCHSTAT="$(go env GOPATH)/bin/benchstat"
GOWORK=off go vet ./...
GOWORK=off go test -race -count=1 ./...

# 1. Core engine microbenchmarks (50 sub-benchmarks, -count=6, strictly serial)
GOWORK=off go test -run=^$ -bench=. -benchmem -count=6 -benchtime=200ms ./internal/engine > /tmp/bench_engine_v2.txt
"$BENCHSTAT" -col "/engine@(raw_rescan raw_inc algebraic_naive algebraic_chan twostack_monoid)" /tmp/bench_engine_v2.txt
"$BENCHSTAT" -col "/engine@(algebraic_chan raw_rescan raw_inc algebraic_naive twostack_monoid)" /tmp/bench_engine_v2.txt

# 2. End-to-end processor microbenchmarks (22 sub-benchmarks, -count=6, strictly serial)
GOWORK=off go test -run=^$ -bench=. -benchmem -count=6 -benchtime=200ms . > /tmp/bench_proc_v3.txt
"$BENCHSTAT" -col "/engine@(raw_rescan raw_inc algebraic_naive algebraic_chan twostack_monoid)" /tmp/bench_proc_v3.txt
"$BENCHSTAT" -col "/engine@(algebraic_chan raw_rescan raw_inc algebraic_naive twostack_monoid)" /tmp/bench_proc_v3.txt
"$BENCHSTAT" -filter ".name:KeyBuilderCanonical" /tmp/bench_proc_v3.txt

# 3. Real 10k-series scale memory, GC, and throughput sweep (48 isolated OS subprocesses)
GOWORK=off go build -o /tmp/scalebench ./cmd/scalebench
# Verified --slide=7 --bucket=5 exits non-zero (must be exact multiple of bucket) and re-verified
# algebraic_chan 10k/s parity smoke run matches /tmp/scalebench_results.jsonl on exact count/sum/sd
# and HeapInuse (21.91 MiB), with delta_heap_bytes within 0.05% (22,883,952 B vs 22,895,568 B).
# Note: --slide=30 explicitly overrides cmd/scalebench/main.go's 60s default.
# Processor metadata heap (8 isolated runs):
#   /tmp/scalebench --mode=processor-meta --engine=<eng> --series=10000 --window=300 --bucket=<5|1>
# Engine sweep (40 isolated runs: W=300s fill + 30s steady-state slide across 10k interleaved series,
#   rates 10k, 50k, 100k, 250k, 500k events/sec -> 3M to 150M live window events, no extrapolation):
#   /tmp/scalebench --mode=engine --engine=<eng> --series=10000 --window=300 --bucket=<5|1> --slide=30 --rate=<10000|50000|100000|250000|500000>
```

---

## 1. Exit Gate Verification Summary

| Exit Gate (`PROTOTYPE_PLAN.md` Section 6 M3) | Threshold | Measured Result | Status |
| :--- | :--- | :--- | :---: |
| **1. Flat Heap Scaling (`10k` series, `W=300s`, `10k/s -> 500k/s`)** | `algebraic_chan` live heap change `< 5%` (`~20-22 MiB` `B=60`, `~95-96 MiB` `B=300`); `raw_rescan`/`raw_inc` `>= 2.2 GiB` (`>= 2.4 GB` decimal) at `500k/s` | `algebraic_chan`: **`21.83 MiB -> 21.83 MiB` (`-0.026%`, `1.00x`)** at `B=60`, **`95.08 MiB -> 95.08 MiB` (`-0.000%`, `1.00x`)** at `B=300`. `raw_rescan`: **`80.11 MiB -> 2,502.00 MiB` (`2.443 GiB`, `2.624 GB` decimal, `31.23x`)**; `raw_inc`: **`80.58 MiB -> 2,502.46 MiB` (`2.444 GiB`, `2.624 GB` decimal, `31.06x`)**. At `500k/s`, `algebraic_chan` `B=60` saves **`99.13%`** live heap (**`114.6x` smaller**); `B=300` saves **`96.20%`** (**`26.3x` smaller**). | **PASS** |
| **2. Throughput & Allocations (`internal/engine`)** | `algebraic_chan >= 500k events/sec` across `10k` series, `0 allocs/op` steady state | **`4.53M events/sec`** steady-state slide (`5.26M` fill) at `B=60` and **`3.85M events/sec`** (`5.04M` fill) at `B=300` on a single core (**`7.7x` to `9.1x` above target**, **`4.07x` to `4.79x` faster than `raw_inc`** under 10k-series cache pressure). **`0.000 B/op`, `0.000 allocs/op`** across all 50 `internal/engine` sub-benchmarks. | **PASS** |
| **3. End-to-End Collector Batch & Metadata Overhead** | Measure `ConsumeMetrics` `ns/dp`, `B/op`, `allocs/op`, and `10k`-series metadata `HeapAlloc` | Periodic `ConsumeMetrics` (`batch=1000`, `algebraic_chan`): **`1.567 µs/dp`** single-core (`~638k dp/s`), **`0 B/op`, `0 allocs/op`**; 24-core parallel: **`130.8 ns/dp`** (**`7.65M dp/s`**), **`0 B/op`, `0 allocs/op`**. Sync `ConsumeMetrics`: **`7.501 µs/dp`**, `1.670 MiB/batch`, `31.72k allocs/batch`. Processor `10k`-series metadata overhead (excluding `scalebench`'s `163,840 B` `[]engine.Window` harness slice): **`9.46` to `9.49 MiB` (`992` to `995 B/series`, `~15.6` extra heap objects/series)** above bare engine allocations. | **PASS** |
| **4. Out-of-Order Tradeoff Quantified (`5%` & `15%` late)** | Measure `algebraic_chan` (`O(1)`) vs `twostack_monoid` (`O(B-k)`) vs `raw_inc` (`O(log(N*W))`) | `B=60, 5% OOO`: `48.67 ns` (`algebraic_chan`) vs `68.39 ns` (`twostack_monoid`, **`+40.5%`**) vs `99.60 ns` (`raw_inc`, **`+104.7%`**). `B=60, 15% OOO`: **`49.64 ns`** vs **`100.45 ns` (`2.02x`)** vs **`113.55 ns` (`2.29x`)**. `B=300, 15% OOO`: **`50.17 ns`** (flat `O(1)`) vs **`289.05 ns` (`5.76x` slower!)** vs **`118.50 ns` (`2.36x`)**. `0` late drops on all engines. | **PASS** |

---

## 2. Scale Memory, GC, Throughput & Query Sweep (`cmd/scalebench`)

Each row ran in its **own fresh OS process** simulating `10,000` active metric identities interleaved in time order over a `5m` (`300s`) sliding window (**`3M` to `150M` real live events**, zero extrapolation) followed by `30s` of steady-state sliding with active eviction (**`300k` to `15M` real sliding events**), then one full `10,000`-series `Query(330s - 1ns)` sweep at the exact aligned bucket boundary.

- **Zero cross-engine numerical discrepancies across all 40 runs**: At every rate (`10k` to `500k/s`), all 5 engines matched exact `query_sweep_count == rate * 300`, `max_sum_diff == 0.0`, and `max_sd_diff == 0.0` (`query_sweep_mean_sd` exact to all 15 decimal digits: `14.454981435281029` at `10k/s`, `14.440014173800588` at `50k/s`, `14.451132714968015` at `100k/s`, `14.544009205186123` at `250k/s`, `14.875495858124928` at `500k/s`). Note: `scalebench` uses low-offset values (`100..150`) without spikes so `algebraic_naive` agrees here; adversarial failure proofs for `algebraic_naive` (`1e9`/`1e12` offsets, `1e17` spike, decaying staircase) are documented in `milestone_1_evidence.md`.
- **Zero truncated GC pause rings (`pause_ring_truncated == false`)** and **zero late drops (`total_dropped_late == 0`)** across all 40 engine runs.
- **Note on `Total GCs` vs `Slide GCs`**: `TotalNumGC` (and `FillNumGC`) span `[mBase, mPostSlide]`, where `mBase` is captured *before* upfront `NewWindow` allocation of all `10,000` series. Thus the `3` to `7` GCs on bucketed engines (`algebraic_naive`, `algebraic_chan`, `twostack_monoid`) are strictly one-time initialization GCs (`0` GCs during Phase B steady-state sliding, matching `0.000 B/op`, `0.000 allocs/op` in Section 3).
- Note on `TotAlloc`: `total_alloc_delta_bytes` is snapshotted at `[mBase, mPostSlide]` (immediately after Phase B sliding, before `QuerySweep` and final forced double-GC). On 5 low-allocation bucketed runs, `delta_heap_bytes` exceeded `total_alloc_delta_bytes` by `112 B` to `5,576 B` (`<= 0.03%`) due to Go runtime bookkeeping during forced GC.

### 2.1 Continuous Baselines (`raw_rescan` and `raw_inc`, `W = 300s`, `slide = 30s`)

| Engine | Rate (`eps`) | Live Window Events | Live `HeapAlloc` Delta (`MiB`) | `HeapInuse` Delta (`MiB`) | Total Alloc Churn (`MiB`) | Total GCs (Slide GCs) | Total GC Pause (`ms`) | Max GC Pause (`ms`) | Fill Tput (`M eps`) | Slide Tput (`M eps`) | `10k`-Series Query Sweep (`ms`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `raw_rescan` | `10k/s` | `3M` | `80.11` | `80.18` | `155.80` | `7` (`0`) | `1.604` | `0.453` | `5.29` | `4.30` | `41.773` |
| `raw_rescan` | `50k/s` | `15M` | `314.49` | `314.55` | `624.55` | `11` (`0`) | `2.883` | `0.757` | `3.97` | `3.61` | `174.431` |
| `raw_rescan` | `100k/s` | `30M` | `626.99` | `627.05` | `1,249.55` | `13` (`0`) | `4.201` | `0.780` | `2.61` | `1.87` | `416.461` |
| `raw_rescan` | `250k/s` | `75M` | `1,251.99` | `1,252.05` | `2,499.55` | `14` (`0`) | `5.128` | `0.783` | `2.42` | `2.00` | `957.204` |
| `raw_rescan` | `500k/s` | `150M` | **`2,502.00`** (`2.443 GiB`) | `2,502.07` | `4,999.56` | `16` (`0`) | `5.833` | `1.116` | `2.13` | `2.01` | **`1,780.530`** |
| `raw_inc` | `10k/s` | `3M` | `80.58` | `80.67` | `156.26` | `7` (`0`) | `2.166` | `0.394` | `3.75` | `2.54` | `2.103` |
| `raw_inc` | `50k/s` | `15M` | `314.97` | `315.05` | `625.03` | `11` (`0`) | `3.921` | `0.589` | `2.65` | `1.59` | `2.627` |
| `raw_inc` | `100k/s` | `30M` | `627.45` | `627.54` | `1,250.01` | `13` (`0`) | `4.919` | `0.931` | `2.37` | `1.49` | `3.016` |
| `raw_inc` | `250k/s` | `75M` | `1,252.46` | `1,252.55` | `2,500.01` | `14` (`0`) | `6.197` | `0.915` | `1.97` | `1.33` | `4.829` |
| `raw_inc` | `500k/s` | `150M` | **`2,502.46`** (`2.444 GiB`) | `2,502.53` | `5,000.01` | `16` (`0`) | `9.014` | `1.925` | `1.61` | **`0.95`** | `6.845` |

### 2.2 Bucketed Engines (`B = 60`, `5s` buckets, `W = 300s`, `slide = 30s`)

| Engine | Rate (`eps`) | Live Window Events | Live `HeapAlloc` Delta (`MiB`) | `HeapInuse` Delta (`MiB`) | Total Alloc Churn (`MiB`) | Total GCs (Slide GCs) | Total GC Pause (`ms`) | Max GC Pause (`ms`) | Fill Tput (`M eps`) | Slide Tput (`M eps`) | `10k`-Series Query Sweep (`ms`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `algebraic_naive` | `10k/s` | `3M` | `16.48` | `17.52` | `16.48` | `3` (`0`) | `1.091` | `0.598` | `4.59` | `3.61` | `1.579` |
| `algebraic_naive` | `50k/s` | `15M` | `16.49` | `17.52` | `16.49` | `3` (`0`) | `2.023` | `1.134` | `4.37` | `4.51` | `0.773` |
| `algebraic_naive` | `100k/s` | `30M` | `16.49` | `17.52` | `16.49` | `3` (`0`) | `1.053` | `0.553` | `3.80` | `3.89` | `0.770` |
| `algebraic_naive` | `250k/s` | `75M` | `16.48` | `17.52` | `16.48` | `3` (`0`) | `0.693` | `0.284` | `4.29` | `4.05` | `0.820` |
| `algebraic_naive` | `500k/s` | `150M` | `16.49` | `17.52` | `16.49` | `3` (`0`) | `0.715` | `0.260` | `5.54` | `6.23` | `0.845` |
| **`algebraic_chan`** | `10k/s` | `3M` | **`21.83`** | `21.91` | `21.83` | `3` (`0`) | `0.411` | `0.164` | `5.36` | `4.65` | `2.205` |
| **`algebraic_chan`** | `50k/s` | `15M` | **`21.83`** | `21.91` | `21.83` | `3` (`0`) | `3.171` | `1.958` | `5.87` | `5.65` | `1.991` |
| **`algebraic_chan`** | `100k/s` | `30M` | **`21.83`** | `21.91` | `21.83` | `3` (`0`) | `1.070` | `0.440` | `5.46` | `5.20` | `1.920` |
| **`algebraic_chan`** | `250k/s` | `75M` | **`21.83`** | `21.91` | `21.83` | `3` (`0`) | `0.775` | `0.354` | `5.75` | `5.98` | `1.993` |
| **`algebraic_chan`** | `500k/s` | `150M` | **`21.83`** | `21.91` | `21.83` | `3` (`0`) | `0.796` | `0.569` | `5.26` | **`4.53`** | **`2.150`** |
| `twostack_monoid` | `10k/s` | `3M` | `61.04` | `61.12` | `61.04` | `4` (`0`) | `2.505` | `0.963` | `1.90` | `2.13` | `8.448` |
| `twostack_monoid` | `50k/s` | `15M` | `61.06` | `61.12` | `61.06` | `4` (`0`) | `4.327` | `2.294` | `2.41` | `2.78` | `6.655` |
| `twostack_monoid` | `100k/s` | `30M` | `61.04` | `61.18` | `61.04` | `4` (`0`) | `4.652` | `3.824` | `2.55` | `3.07` | `6.363` |
| `twostack_monoid` | `250k/s` | `75M` | `61.04` | `61.12` | `61.04` | `4` (`0`) | `0.551` | `0.154` | `5.26` | `5.32` | `4.152` |
| `twostack_monoid` | `500k/s` | `150M` | `61.05` | `61.12` | `61.05` | `4` (`0`) | `0.639` | `0.226` | `4.71` | `4.50` | `4.520` |

### 2.3 Bucketed Engines (`B = 300`, `1s` buckets, `W = 300s`, `slide = 30s`)

| Engine | Rate (`eps`) | Live Window Events | Live `HeapAlloc` Delta (`MiB`) | `HeapInuse` Delta (`MiB`) | Total Alloc Churn (`MiB`) | Total GCs (Slide GCs) | Total GC Pause (`ms`) | Max GC Pause (`ms`) | Fill Tput (`M eps`) | Slide Tput (`M eps`) | `10k`-Series Query Sweep (`ms`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `algebraic_naive` | `10k/s` | `3M` | `79.97` | `80.02` | `79.97` | `5` (`0`) | `1.298` | `0.344` | `3.51` | `3.54` | `0.641` |
| `algebraic_naive` | `50k/s` | `15M` | `79.97` | `80.02` | `79.97` | `5` (`0`) | `1.184` | `0.395` | `4.74` | `4.84` | `1.030` |
| `algebraic_naive` | `100k/s` | `30M` | `79.97` | `80.02` | `79.97` | `5` (`0`) | `0.906` | `0.222` | `5.04` | `5.80` | `0.741` |
| `algebraic_naive` | `250k/s` | `75M` | `79.97` | `80.02` | `79.97` | `5` (`0`) | `0.744` | `0.208` | `5.37` | `5.39` | `0.591` |
| `algebraic_naive` | `500k/s` | `150M` | `79.97` | `80.02` | `79.97` | `5` (`0`) | `0.903` | `0.305` | `5.46` | `5.96` | `0.482` |
| **`algebraic_chan`** | `10k/s` | `3M` | **`95.08`** | `96.14` | `95.08` | `5` (`0`) | `0.758` | `0.227` | `3.83` | `3.11` | `2.692` |
| **`algebraic_chan`** | `50k/s` | `15M` | **`95.07`** | `96.14` | `95.07` | `5` (`0`) | `0.823` | `0.317` | `5.03` | `4.55` | `2.120` |
| **`algebraic_chan`** | `100k/s` | `30M` | **`95.08`** | `96.14` | `95.08` | `5` (`0`) | `1.291` | `0.345` | `4.81` | `5.11` | `2.447` |
| **`algebraic_chan`** | `250k/s` | `75M` | **`95.08`** | `96.14` | `95.08` | `5` (`0`) | `2.023` | `0.670` | `4.95` | `4.97` | `2.345` |
| **`algebraic_chan`** | `500k/s` | `150M` | **`95.08`** | `96.14` | `95.08` | `5` (`0`) | `1.648` | `0.657` | `5.04` | **`3.85`** | **`2.347`** |
| `twostack_monoid` | `10k/s` | `3M` | `314.96` | `315.03` | `314.96` | `7` (`0`) | `2.247` | `0.890` | `2.08` | `2.47` | `3.493` |
| `twostack_monoid` | `50k/s` | `15M` | `314.96` | `315.03` | `314.96` | `6` (`0`) | `1.097` | `0.294` | `3.48` | `3.76` | `3.780` |
| `twostack_monoid` | `100k/s` | `30M` | `314.95` | `315.03` | `314.95` | `7` (`0`) | `1.235` | `0.247` | `3.88` | `4.16` | `4.910` |
| `twostack_monoid` | `250k/s` | `75M` | `314.97` | `315.03` | `314.97` | `7` (`0`) | `2.056` | `0.793` | `4.31` | `4.39` | `5.407` |
| `twostack_monoid` | `500k/s` | `150M` | **`314.96`** | `315.03` | `314.96` | `7` (`0`) | `1.155` | `0.200` | `4.42` | `4.18` | `4.366` |

### 2.4 Summary Ratios at `500k/s` (`150M` Live Events) & Scaling Ratios (`10k/s -> 500k/s`)

| Engine (`B`) | `10k/s -> 500k/s` Heap Change | Heap vs `raw_rescan` (`500k/s`) | Heap vs `raw_inc` (`500k/s`) | Alloc Churn vs `raw_inc` | Query Sweep vs `raw_rescan` | Steady-State Slide vs `raw_inc` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `raw_rescan` (`W=300s`) | **`+3,023.1%` (`31.23x`)** | `1.00x` (`2,502.00 MiB`) | `1.00x` | `1.00x` (`4,999.56 MiB`) | `1.0x` (`1,780.53 ms`) | `2.12x` |
| `raw_inc` (`W=300s`) | **`+3,005.5%` (`31.06x`)** | `1.00x` (`2,502.46 MiB`) | `1.00x` | `1.00x` (`5,000.01 MiB`) | `260.1x` faster (`6.85 ms`) | `1.00x` (`0.95M eps`) |
| `algebraic_naive` (`B=60`) | **`+0.064%` (`1.00x`)** | **`151.7x` smaller (`-99.34%`)** | `151.7x` smaller | `303.1x` less | `2,107.1x` faster (`0.85 ms`) | `6.58x` faster |
| **`algebraic_chan` (`B=60`)** | **`-0.026%` (`1.00x`)** | **`114.6x` smaller (`-99.13%`)** | **`114.6x` smaller** | **`229.1x` less** | **`828.2x` faster (`2.15 ms`)** | **`4.79x` faster (`4.53M eps`)** |
| `twostack_monoid` (`B=60`) | **`+0.008%` (`1.00x`)** | `41.0x` smaller (`-97.56%`) | `41.0x` smaller | `81.9x` less | `393.9x` faster (`4.52 ms`) | `4.76x` faster |
| `algebraic_naive` (`B=300`) | **`+0.000%` (`1.00x`)** | `31.3x` smaller (`-96.80%`) | `31.3x` smaller | `62.5x` less | `3,694.0x` faster (`0.48 ms`) | `6.30x` faster |
| **`algebraic_chan` (`B=300`)** | **`-0.000%` (`1.00x`)** | **`26.3x` smaller (`-96.20%`)** | **`26.3x` smaller** | **`52.6x` less** | **`758.6x` faster (`2.35 ms`)** | **`4.07x` faster (`3.85M eps`)** |
| `twostack_monoid` (`B=300`) | **`-0.000%` (`1.00x`)** | `7.9x` smaller (`-87.41%`) | `7.9x` smaller | `15.9x` less | `407.8x` faster (`4.37 ms`) | `4.42x` faster |

### 2.5 Exact Go 1.27 Allocator Size-Class Reconciliation (`10,000` Series)

Every measured byte reconciles directly to Go 1.27 `mspan` size classes plus the `163,840 B` (`20 * 8 KiB`) backing array of `[]engine.Window` (`10,000 * 16 B`):

| Engine (`B`) | Window Struct Class | Backing Slice(s) Raw -> Go Class | Exact Predicted Total (`B`) | Measured `delta_heap_bytes` (`B`) | Notes |
| :--- | :---: | :--- | :---: | :---: | :--- |
| `algebraic_naive` (`B=60`) | `176 B` | `1 x (1,440 -> 1,536 B)` | `17,283,840` | `17,283,840` (`250k`) / `17,295,120` (`500k`) | Exact byte match |
| `algebraic_naive` (`B=300`) | `176 B` | `1 x (7,200 -> 8,192 B)` | `83,843,840` | `83,855,120` | `+992 B/series` class slack (`+13.8%`) |
| **`algebraic_chan` (`B=60`)** | `224 B` | `1 x (1,920 -> 2,048 B)` | `22,883,840` | `22,889,640` (`500k`) | `19.53 MiB` buckets + `2.14 MiB` structs + `0.16 MiB` slice = **`21.83 MiB`** |
| **`algebraic_chan` (`B=300`)** | `224 B` | `1 x (9,600 -> 9,728 B)` | `99,683,840` | `99,700,696` (`500k`) | Tight `+1.3%` class fit (`9,600 -> 9,728 B`) = **`95.08 MiB`** |
| `twostack_monoid` (`B=60`) | `240 B` | `2 x (2,880 -> 3,072 B)` | `64,003,840` | `64,015,232` (`500k`) | `3` objects/series (`30,018` objects) |
| **`twostack_monoid` (`B=300`)** | `240 B` | **`2 x (14,400 -> 16,384 B)`** | `330,243,840` | `330,255,216` (`500k`) | **Go allocator cliff**: `+3,968 B/series` slack (`37.84 MiB` wasted across `10k` series!) |
| `raw_rescan` (`10k/s` / `500k/s`) | `192 B` | `8,192 B` (`512` slots) / `262,144 B` (`16,384` slots) | `84,003,840` / `2,623,523,840` | `84,004,064` / `2,623,535,232` | Power-of-2 ring scaling `32x` (`512 -> 16,384`) |
| `raw_inc` (`10k/s` / `500k/s`) | `240 B` | `8,192 B` / `262,144 B` | `84,483,840` / `2,624,003,840` | `84,495,120` / `2,624,015,216` | `2.624 GB` decimal (`2.444 GiB` binary); `5.24 GB` churn across doublings |

### 2.6 Full Collector Processor Metadata Heap Overhead (`10,000` Cloned Series)

Measured via `--mode=processor-meta` (`100` hosts x `100` routes = `10,000` active series with 3 resource attributes, scope name/version, schema URL, and 2 datapoint attributes per series; `1` datapoint ingested per series). `Bare Engine HeapAlloc` is computed from exact Go 1.27 per-series engine size classes (Table 2.5, excluding `scalebench`'s `163,840 B` `[]engine.Window` harness slice which does not exist in `metricAggregationProcessor`, and using `cap=16` = `256 B` ring backing slice for `raw_rescan`/`raw_inc` after 1 datapoint):

| Engine | `B` | Total Processor `HeapAlloc` (`MiB`) | Total `HeapInuse` (`MiB`) | Bare Engine `HeapAlloc` (`MiB`) | Metadata Overhead (`MiB`) | Metadata Overhead (`B/series`) | Total `B/series` | Heap Objects / Series (Meta Only) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `raw_rescan` (`1 pt/series`, `cap=16`) | `60` | `13.73` | `14.57` | `4.27` | **`9.46`** | `991.9` | `1,439.9` | `17.56` (`15.56`) |
| `raw_inc` (`1 pt/series`, `cap=16`) | `60` | `14.20` | `15.17` | `4.73` | **`9.47`** | `993.1` | `1,489.1` | `17.56` (`15.56`) |
| `algebraic_naive` | `60` | `25.81` | `27.53` | `16.33` | **`9.49`** | `994.8` | `2,706.8` | `17.57` (`15.57`) |
| **`algebraic_chan`** | `60` | **`31.13`** | `31.96` | `21.67` | **`9.47`** | **`992.5`** | `3,264.5` | `17.56` (`15.56`) |
| `twostack_monoid` | `60` | `70.35` | `71.09` | `60.88` | **`9.47`** | `993.1` | `7,377.1` | `18.57` (`15.57`) |
| `algebraic_naive` | `300` | `89.28` | `90.18` | `79.80` | **`9.48`** | `993.7` | `9,361.7` | `17.57` (`15.57`) |
| **`algebraic_chan`** | `300` | **`104.38`** | `106.52` | `94.91` | **`9.47`** | **`993.0`** | `10,945.0` | `17.56` (`15.56`) |
| `twostack_monoid` | `300` | `324.26` | `325.04` | `314.79` | **`9.48`** | `993.6` | `34,001.6` | `18.56` (`15.56`) |

Takeaway: Across all 8 configurations, cloning `pcommon.Resource`, `pcommon.InstrumentationScope`, and `pcommon.Map` datapoint attributes per series adds a uniform **`9.46` to `9.49 MiB`** (`992` to `995 B/series`, `15.56` to `15.57` metadata objects/series) above the bare engine. Interning shared `Resource` and `Scope` structs across series sharing the same host/scope avoids `~6.5` to `8` cloned objects/series on this 3-resource-attribute workload (**`~37%` to `46%`** of total `processor-meta` objects, or **`~42%` to `51%`** of metadata-only objects, rising further on Kubernetes fleets with 10+ resource attributes).

---

## 3. Verbatim `benchstat` Output: Core `internal/engine`

### 3.1 Baseline = `raw_rescan`

```text
goos: linux
goarch: amd64
pkg: github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                               │    raw_rescan    │                raw_inc                │           algebraic_naive           │           algebraic_chan            │            twostack_monoid            │
                               │      sec/op      │    sec/op      vs base                │    sec/op     vs base               │    sec/op     vs base               │    sec/op      vs base                │
AddInOrder/B=60/rate=10eps-24        29.41n ± 11%    77.40n ± 14%  +163.19% (p=0.002 n=6)   43.68n ±  9%  +48.52% (p=0.002 n=6)   47.68n ±  8%  +62.12% (p=0.002 n=6)    52.85n ±  7%   +79.70% (p=0.002 n=6)
AddInOrder/B=60/rate=50eps-24        30.09n ± 10%    88.91n ±  6%  +195.51% (p=0.002 n=6)   42.57n ±  7%  +41.48% (p=0.002 n=6)   44.87n ±  4%  +49.16% (p=0.002 n=6)    49.98n ±  6%   +66.11% (p=0.002 n=6)
AddInOrder/B=300/rate=50eps-24       27.73n ± 10%    91.01n ± 23%  +228.24% (p=0.002 n=6)   44.20n ±  2%  +59.40% (p=0.002 n=6)   47.24n ±  3%  +70.41% (p=0.002 n=6)    54.39n ±  5%   +96.18% (p=0.002 n=6)
Snapshot/B=60/rate=10eps-24       26916.50n ±  2%    17.77n ±  7%   -99.93% (p=0.002 n=6)   29.81n ± 12%  -99.89% (p=0.002 n=6)   49.99n ± 12%  -99.81% (p=0.002 n=6)   127.10n ±  3%   -99.53% (p=0.002 n=6)
Snapshot/B=60/rate=50eps-24      133576.00n ±  2%    17.03n ±  5%   -99.99% (p=0.002 n=6)   29.45n ±  4%  -99.98% (p=0.002 n=6)   53.47n ±  8%  -99.96% (p=0.002 n=6)   151.20n ± 16%   -99.89% (p=0.002 n=6)
AddOOO/B=60/ooo=5pct-24              44.47n ± 27%    99.60n ± 15%  +124.00% (p=0.002 n=6)   44.41n ±  6%        ~ (p=0.937 n=6)   48.67n ±  6%        ~ (p=0.093 n=6)    68.39n ±  4%   +53.81% (p=0.002 n=6)
AddOOO/B=60/ooo=15pct-24             47.98n ±  9%   113.55n ± 15%  +136.64% (p=0.002 n=6)   44.98n ±  9%   -6.26% (p=0.041 n=6)   49.64n ±  7%        ~ (p=0.240 n=6)   100.45n ±  3%  +109.34% (p=0.002 n=6)
AddOOO/B=300/ooo=15pct-24            52.97n ± 11%   118.50n ± 17%  +123.73% (p=0.002 n=6)   45.21n ±  7%  -14.64% (p=0.002 n=6)   50.17n ±  6%        ~ (p=0.093 n=6)   289.05n ±  5%  +445.74% (p=0.002 n=6)
AddSpikeEviction/B=60-24             28.80n ± 10%    86.49n ±  5%  +200.26% (p=0.002 n=6)   45.32n ±  5%  +57.33% (p=0.002 n=6)   48.12n ±  9%  +67.07% (p=0.002 n=6)    53.38n ±  9%   +85.30% (p=0.002 n=6)
AddBipolar/B=60-24                   28.09n ±  7%    75.89n ±  9%  +170.20% (p=0.002 n=6)   45.20n ±  6%  +60.92% (p=0.002 n=6)   47.63n ±  3%  +69.61% (p=0.002 n=6)    49.34n ± 23%   +75.68% (p=0.002 n=6)
geomean                              155.1n          66.40n         -57.19%                 40.98n        -73.58%                 48.70n        -68.60%                  82.40n         -46.88%

                               │  raw_rescan  │              raw_inc               │          algebraic_naive           │           algebraic_chan           │          twostack_monoid           │
                               │     B/op     │    B/op     vs base                │    B/op     vs base                │    B/op     vs base                │    B/op     vs base                │
AddInOrder/B=60/rate=10eps-24    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddInOrder/B=60/rate=50eps-24    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddInOrder/B=300/rate=50eps-24   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
Snapshot/B=60/rate=10eps-24      0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
Snapshot/B=60/rate=50eps-24      0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=60/ooo=5pct-24          0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=60/ooo=15pct-24         0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=300/ooo=15pct-24        0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddSpikeEviction/B=60-24         0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddBipolar/B=60-24               0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
geomean                                     ²               +0.00%               ²               +0.00%               ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean

                               │  raw_rescan  │              raw_inc               │          algebraic_naive           │           algebraic_chan           │          twostack_monoid           │
                               │  allocs/op   │ allocs/op   vs base                │ allocs/op   vs base                │ allocs/op   vs base                │ allocs/op   vs base                │
AddInOrder/B=60/rate=10eps-24    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddInOrder/B=60/rate=50eps-24    0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddInOrder/B=300/rate=50eps-24   0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
Snapshot/B=60/rate=10eps-24      0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
Snapshot/B=60/rate=50eps-24      0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=60/ooo=5pct-24          0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=60/ooo=15pct-24         0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddOOO/B=300/ooo=15pct-24        0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddSpikeEviction/B=60-24         0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
AddBipolar/B=60-24               0.000 ± 0%     0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹   0.000 ± 0%       ~ (p=1.000 n=6) ¹
geomean                                     ²               +0.00%               ²               +0.00%               ²               +0.00%               ²               +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean
```

### 3.2 Baseline = `algebraic_chan` (`sec/op` summary)

```text
                               │ algebraic_chan │                 raw_rescan                  │                raw_inc                │           algebraic_naive           │            twostack_monoid            │
                               │     sec/op     │      sec/op       vs base                   │    sec/op      vs base                │    sec/op     vs base               │    sec/op      vs base                │
AddInOrder/B=60/rate=10eps-24      47.68n ±  8%       29.41n ± 11%      -38.32% (p=0.002 n=6)    77.40n ± 14%   +62.34% (p=0.002 n=6)   43.68n ±  9%   -8.39% (p=0.026 n=6)    52.85n ±  7%   +10.84% (p=0.002 n=6)
AddInOrder/B=60/rate=50eps-24      44.87n ±  4%       30.09n ± 10%      -32.96% (p=0.002 n=6)    88.91n ±  6%   +98.12% (p=0.002 n=6)   42.57n ±  7%        ~ (p=0.132 n=6)    49.98n ±  6%   +11.36% (p=0.002 n=6)
AddInOrder/B=300/rate=50eps-24     47.24n ±  3%       27.73n ± 10%      -41.32% (p=0.002 n=6)    91.01n ± 23%   +92.62% (p=0.002 n=6)   44.20n ±  2%   -6.46% (p=0.002 n=6)    54.39n ±  5%   +15.12% (p=0.002 n=6)
Snapshot/B=60/rate=10eps-24        49.99n ± 12%    26916.50n ±  2%   +53749.15% (p=0.002 n=6)    17.77n ±  7%   -64.46% (p=0.002 n=6)   29.81n ± 12%  -40.36% (p=0.002 n=6)   127.10n ±  3%  +154.28% (p=0.002 n=6)
Snapshot/B=60/rate=50eps-24        53.47n ±  8%   133576.00n ±  2%  +249714.85% (p=0.002 n=6)    17.03n ±  5%   -68.15% (p=0.002 n=6)   29.45n ±  4%  -44.92% (p=0.002 n=6)   151.20n ± 16%  +182.78% (p=0.002 n=6)
AddOOO/B=60/ooo=5pct-24            48.67n ±  6%       44.47n ± 27%            ~ (p=0.093 n=6)    99.60n ± 15%  +104.66% (p=0.002 n=6)   44.41n ±  6%   -8.73% (p=0.004 n=6)    68.39n ±  4%   +40.53% (p=0.002 n=6)
AddOOO/B=60/ooo=15pct-24           49.64n ±  7%       47.98n ±  9%            ~ (p=0.240 n=6)   113.55n ± 15%  +128.77% (p=0.002 n=6)   44.98n ±  9%   -9.38% (p=0.002 n=6)   100.45n ±  3%  +102.38% (p=0.002 n=6)
AddOOO/B=300/ooo=15pct-24          50.17n ±  6%       52.97n ± 11%            ~ (p=0.093 n=6)   118.50n ± 17%  +136.22% (p=0.002 n=6)   45.21n ±  7%   -9.88% (p=0.009 n=6)   289.05n ±  5%  +476.20% (p=0.002 n=6)
AddSpikeEviction/B=60-24           48.12n ±  9%       28.80n ± 10%      -40.15% (p=0.002 n=6)    86.49n ±  5%   +79.72% (p=0.002 n=6)   45.32n ±  5%        ~ (p=0.180 n=6)    53.38n ±  9%   +10.91% (p=0.002 n=6)
AddBipolar/B=60-24                 47.63n ±  3%       28.09n ±  7%      -41.04% (p=0.002 n=6)    75.89n ±  9%   +59.31% (p=0.002 n=6)   45.20n ±  6%        ~ (p=0.065 n=6)    49.34n ± 23%         ~ (p=0.093 n=6)
geomean                            48.70n             155.1n           +218.51%                  66.40n         +36.35%                 40.98n        -15.86%                  82.40n         +69.20%
```

Key microbenchmark observations:
- **In-order `Add` (L1-hot single series)**: `algebraic_chan` (`44.87 ns` at `50 eps`) is statistically indistinguishable from `algebraic_naive` (`42.57 ns`, `p=0.132`), has `49.5%` lower latency than `raw_inc` (`88.91 ns`, `+98.12%` slower, `p=0.002`, which pays Reverse Chan on every single datapoint), and has `10.2%` lower latency than `twostack_monoid` (`49.98 ns`, `+11.36%` slower, `p=0.002`). `raw_rescan` (`30.09 ns`, `-32.96%` lower latency) only appends to its ring and defers all math to `Snapshot`.
- **`Snapshot` query (`15,000` live events, mid-cycle `splitID < headBucket` for `twostack_monoid`)**: `raw_rescan` takes `133,576 ns` (`133.6 µs`); `algebraic_chan` takes **`53.47 ns`** (**`2,498x` faster** than `raw_rescan` and **`2.83x` faster** than `twostack_monoid`'s `151.20 ns` 3-way front+back+head combine).
- **Out-of-order late arrivals inside `[1, B-1]` closed buckets**: `algebraic_chan` stays flat `O(1)` regardless of `B` (`48.67 ns` at `B=60, 5%`; `49.64 ns` at `B=60, 15%`; `50.17 ns` at `B=300, 15%`). `twostack_monoid` degrades linearly with `B` due to `O(B-k)` front-stack suffix rebuilds (`68.39 ns` at `B=60, 5%`; `100.45 ns` at `B=60, 15%` = `2.02x` slower; **`289.05 ns` at `B=300, 15%` = `5.76x` slower**). `raw_inc` stays `O(log(N*W))` (`113.55` to `118.50 ns`, `2.29x` to `2.36x` slower).
- **Spike eviction & Bipolar streams**: Replacing loop-body integer modulo/division with a branch-friendly countdown removed harness `IDIVQ` overhead cleanly. Periodic `1e12` spike eviction (`1` spike per `3,000` events, verified to trigger measured `CancelRefolds >= b.N / spikePeriod`) runs in `48.12 ns` on `algebraic_chan` vs `45.32 ns` on `algebraic_naive` (`+2.80 ns`, statistically indistinguishable at `p=0.180`). Alternating bipolar `±1` runs in `47.63 ns` on `algebraic_chan` vs `45.20 ns` on `algebraic_naive` (`p=0.065`) with **`0` cancellation refolds** verified across all 6 runs.

---

## 4. Verbatim `benchstat` Output: End-to-End `metricaggregationprocessor`

Setup note: `ConsumeMetricsBatch` (`batch=1000`, `10k` active series) and `ConsumeMetricsParallel` (`batch=100`, `24` parallel goroutines with pre-built disjoint host partitions scaling `hosts = max(100, GOMAXPROCS)`) warm `62` ticks at `5s` spacing (`60` live samples/buckets `[2..61]` per series, `cap=64` in `rawRing`, fitting inside CPU cache), normalize `sync.Pool` via `warmKeyBuilderPool` before timing, and stamp non-constant values (`stampBatchTSAndVal`) so steady-state `M2` stays strictly positive. Under that `1 dp / 5s` pacing, every single datapoint advances `headBucket` by `1` (evicting `1` bucket per event instead of `1` bucket per `50-250` events as at `10-50 eps/series`). `ProcessorFlush10kSeries` populates `330` seconds (`300s` window + `30s` slide, retaining exact `300` live events/series = `3,000,000` live events at `10k/s` while wrapping `raw_*` rings and keeping `twostack_monoid` mid-cycle with `splitID < headBucket`) and emits `50,000` OTLP `Gauge` datapoints (`5` aggregations x `10,000` series) per `Flush`.

### 4.1 `KeyBuilderCanonical` (Zero-Alloc Isolation: Flat & Complex Attributes)

```text
goos: linux
goarch: amd64
pkg: github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                               │ /tmp/bench_proc_v3.txt │
                               │         sec/op         │
KeyBuilderCanonical/flat-24                 1.663µ ± 2%
KeyBuilderCanonical/complex-24              2.388µ ± 1%
geomean                                     1.993µ

                               │ /tmp/bench_proc_v3.txt │
                               │          B/op          │
KeyBuilderCanonical/flat-24                0.000 ± 0%
KeyBuilderCanonical/complex-24             0.000 ± 0%
geomean                                               ¹
¹ summaries must be >0 to compute geomean

                               │ /tmp/bench_proc_v3.txt │
                               │       allocs/op        │
KeyBuilderCanonical/flat-24                0.000 ± 0%
KeyBuilderCanonical/complex-24             0.000 ± 0%
geomean                                               ¹
¹ summaries must be >0 to compute geomean
```

### 4.2 Full Processor Benchmarks (Baseline = `raw_rescan`)

```text
goos: linux
goarch: amd64
pkg: github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                                                │  raw_rescan  │               raw_inc               │           algebraic_naive           │           algebraic_chan            │           twostack_monoid           │
                                                │    sec/op    │    sec/op     vs base               │    sec/op     vs base               │    sec/op     vs base               │    sec/op     vs base               │
ConsumeMetricsBatch/mode=periodic/batch=1000-24   1.503m ±  9%   1.591m ±  7%   +5.88% (p=0.041 n=6)   1.355m ±  9%   -9.82% (p=0.009 n=6)   1.567m ±  3%   +4.25% (p=0.009 n=6)   1.765m ± 18%  +17.43% (p=0.002 n=6)
ConsumeMetricsBatch/mode=sync/batch=1000-24       6.882m ± 12%   6.200m ± 17%   -9.91% (p=0.041 n=6)   7.146m ± 11%   +3.83% (p=0.041 n=6)   7.501m ± 36%        ~ (p=0.093 n=6)   9.637m ± 43%        ~ (p=0.818 n=6)
ConsumeMetricsParallel/batch=100-24               17.16µ ± 40%   18.25µ ± 51%        ~ (p=0.240 n=6)   12.67µ ± 20%        ~ (p=0.132 n=6)   13.07µ ± 15%        ~ (p=0.589 n=6)   16.25µ ± 18%        ~ (p=1.000 n=6)
ProcessorFlush10kSeries/series=10000-24           87.94m ± 14%   54.26m ±  6%  -38.30% (p=0.002 n=6)   52.87m ±  5%  -39.89% (p=0.002 n=6)   56.81m ±  3%  -35.40% (p=0.002 n=6)   56.42m ±  9%  -35.85% (p=0.002 n=6)
geomean                                           1.988m         1.768m        -11.06%                 1.596m        -19.71%                 1.719m        -13.53%                 1.987m         -0.04%

                                                │  raw_rescan  │              raw_inc               │           algebraic_naive           │           algebraic_chan           │           twostack_monoid           │
                                                │    sec/dp    │    sec/dp     vs base              │    sec/dp     vs base               │    sec/dp     vs base              │    sec/dp     vs base               │
ConsumeMetricsBatch/mode=periodic/batch=1000-24   1.503µ ±  8%   1.591µ ±  7%  +5.89% (p=0.041 n=6)   1.356µ ±  9%   -9.78% (p=0.009 n=6)   1.567µ ±  3%  +4.26% (p=0.009 n=6)   1.765µ ± 18%  +17.47% (p=0.002 n=6)
ConsumeMetricsBatch/mode=sync/batch=1000-24       6.883µ ± 12%   6.201µ ± 17%  -9.91% (p=0.041 n=6)   7.146µ ± 11%   +3.82% (p=0.041 n=6)   7.501µ ± 36%       ~ (p=0.093 n=6)   9.637µ ± 43%        ~ (p=0.818 n=6)
ConsumeMetricsParallel/batch=100-24               171.7n ± 40%   182.5n ± 51%       ~ (p=0.240 n=6)   126.7n ± 20%        ~ (p=0.132 n=6)   130.8n ± 15%       ~ (p=0.589 n=6)   162.5n ± 18%        ~ (p=1.000 n=6)
geomean                                           1.211µ         1.216µ        +0.46%                 1.071µ        -11.58%                 1.154µ        -4.70%                 1.403µ        +15.90%

                                                │   raw_rescan   │                raw_inc                │            algebraic_naive             │             algebraic_chan             │            twostack_monoid             │
                                                │      B/op      │     B/op      vs base                 │     B/op      vs base                  │     B/op      vs base                  │     B/op      vs base                  │
ConsumeMetricsBatch/mode=periodic/batch=1000-24     0.000 ± 0%       0.000 ± 0%        ~ (p=1.000 n=6) ¹     0.000 ± 0%       ~ (p=1.000 n=6) ¹       0.000 ± 0%       ~ (p=1.000 n=6) ¹       0.000 ± 0%       ~ (p=1.000 n=6) ¹
ConsumeMetricsBatch/mode=sync/batch=1000-24       1.670Mi ± 0%     1.670Mi ± 0%        ~ (p=0.859 n=6)     1.670Mi ± 0%       ~ (p=0.258 n=6)       1.670Mi ± 0%  -0.00% (p=0.015 n=6)       1.670Mi ± 0%  -0.01% (p=0.002 n=6)
ConsumeMetricsParallel/batch=100-24                0.5000 ±  ?      1.0000 ±  ?        ~ (p=1.000 n=6)      0.0000 ± 0%       ~ (p=0.182 n=6)        0.0000 ± 0%       ~ (p=0.182 n=6)        0.0000 ± 0%       ~ (p=0.182 n=6)
ProcessorFlush10kSeries/series=10000-24           17.48Mi ± 0%     17.48Mi ± 0%        ~ (p=0.234 n=6)     17.48Mi ± 0%  +0.00% (p=0.002 n=6)       17.48Mi ± 0%  +0.00% (p=0.002 n=6)       17.48Mi ± 0%  +0.00% (p=0.002 n=6)
geomean                                                        ²                 +18.92%               ²                 ?                    ² ³                 ?                    ² ³                 ?                    ² ³
¹ all samples are equal
² summaries must be >0 to compute geomean
³ ratios must be >0 to compute geomean

                                                │  raw_rescan   │               raw_inc               │           algebraic_naive           │           algebraic_chan            │           twostack_monoid           │
                                                │   allocs/op   │  allocs/op   vs base                │  allocs/op   vs base                │  allocs/op   vs base                │  allocs/op   vs base                │
ConsumeMetricsBatch/mode=periodic/batch=1000-24    0.000 ± 0%      0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹
ConsumeMetricsBatch/mode=sync/batch=1000-24       31.72k ± 0%     31.72k ± 0%       ~ (p=1.000 n=6) ¹   31.72k ± 0%       ~ (p=1.000 n=6) ¹   31.72k ± 0%       ~ (p=1.000 n=6) ¹   31.72k ± 0%       ~ (p=1.000 n=6) ¹
ConsumeMetricsParallel/batch=100-24                0.000 ± 0%      0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹    0.000 ± 0%       ~ (p=1.000 n=6) ¹
ProcessorFlush10kSeries/series=10000-24           316.8k ± 0%     316.8k ± 0%       ~ (p=1.000 n=6) ¹   316.8k ± 0%       ~ (p=1.000 n=6) ¹   316.8k ± 0%       ~ (p=1.000 n=6) ¹   316.8k ± 0%       ~ (p=1.000 n=6) ¹
geomean                                                       ²                +0.00%               ²                +0.00%               ²                +0.00%               ²                +0.00%               ²
¹ all samples are equal
² summaries must be >0 to compute geomean
```

### 4.3 Full Processor Benchmarks (Baseline = `algebraic_chan`, `sec/op` summary)

```text
                                                │ algebraic_chan │             raw_rescan              │               raw_inc               │           algebraic_naive           │           twostack_monoid           │
                                                │     sec/op     │    sec/op     vs base               │    sec/op     vs base               │    sec/op     vs base               │    sec/op     vs base               │
ConsumeMetricsBatch/mode=periodic/batch=1000-24     1.567m ±  3%   1.503m ±  9%   -4.08% (p=0.009 n=6)   1.591m ±  7%        ~ (p=0.699 n=6)   1.355m ±  9%  -13.50% (p=0.002 n=6)   1.765m ± 18%  +12.64% (p=0.002 n=6)
ConsumeMetricsBatch/mode=sync/batch=1000-24         7.501m ± 36%   6.882m ± 12%        ~ (p=0.093 n=6)   6.200m ± 17%  -17.34% (p=0.009 n=6)   7.146m ± 11%        ~ (p=0.180 n=6)   9.637m ± 43%        ~ (p=0.937 n=6)
ConsumeMetricsParallel/batch=100-24                 13.07µ ± 15%   17.16µ ± 40%        ~ (p=0.589 n=6)   18.25µ ± 51%  +39.57% (p=0.015 n=6)   12.67µ ± 20%        ~ (p=0.394 n=6)   16.25µ ± 18%  +24.27% (p=0.002 n=6)
ProcessorFlush10kSeries/series=10000-24             56.81m ±  3%   87.94m ± 14%  +54.80% (p=0.002 n=6)   54.26m ±  6%   -4.49% (p=0.041 n=6)   52.87m ±  5%   -6.94% (p=0.004 n=6)   56.42m ±  9%        ~ (p=0.699 n=6)
geomean                                             1.719m         1.988m        +15.65%                 1.768m         +2.85%                 1.596m         -7.15%                 1.987m        +15.60%
```

End-to-end architectural takeaways:
1. **OTLP decoding & hashing dominate single-core ingestion (`~86%` on rotating 10k series, up to `~97%` L1-hot)**: In periodic mode (`EmitInterval > 0`), `algebraic_chan` processes `1,000`-dp batches in `1.567 ms` (`1.567 µs/dp`, `~638k dp/s` single-core, **`0.000 B/op`, `0.000 allocs/op`** across all 5 engines). On L1-hot single series, bare `algebraic_chan` takes `~45 ns/dp` (`~3%`); under 10k-series rotating cache misses (`scalebench` Table 2.2), bare `algebraic_chan` takes `~215` to `221 ns/dp` (`~14%`), so canonical key encoding, sorting, `fnv1a128` hashing, shard locking, and `map[string]` lookup account for `~86%` of total per-datapoint ingest latency. Against `algebraic_chan` (`1.567 ms`), `twostack_monoid` (`1.765 ms`) is `+12.64%` slower (`p=0.002`), `raw_inc` (`1.591 ms`) is statistically indistinguishable (`p=0.699`), and `raw_rescan` (`1.503 ms`, L1-fit `cap=64` ring) has `-4.08%` lower latency (`p=0.009`).
2. **Parallel multi-core throughput reaches `7.65M dp/s`**: With 64 cache-line-aligned shards (`64 B` `seriesShard`) and pre-built disjoint host partitions, `ConsumeMetricsParallel` scales to `130.8 ns/dp` (`7.65M dp/s`) on `algebraic_chan` with **`0.000 B/op`, `0.000 allocs/op`** (`raw_inc` is `+39.57%` slower at `p=0.015`, `twostack_monoid` is `+24.27%` slower at `p=0.002`, while `raw_rescan` `p=0.589` and `algebraic_naive` `p=0.394` are statistically indistinguishable). At realistic `500k/s` (`50 eps/series`, `250` events per `5s` bucket), `scalebench` proves `algebraic_chan` achieves **`4.79x` higher steady-state slide throughput** than `raw_inc` (`4.532M` vs `0.946M eps`) and **`2.26x` higher** than `raw_rescan` (`4.532M` vs `2.010M eps`).
3. **Periodic emission (`emit_interval > 0`) vs synchronous (`emit_interval == 0`)**: Synchronous emission spends `~5.93 ms` extra (`7.501 ms` vs `1.567 ms`, **`4.79x` higher latency**) and allocates **`1.670 MiB` (`31,720 allocs/batch`)** strictly constructing `5,000` output `pmetric.Gauge` datapoints per `1,000`-dp input batch. Production high-throughput deployments (`100k` to `500k/s`) should use `emit_interval > 0` (e.g., `5s` or `10s`) to eliminate steady-state ingest allocations completely (`31.7k -> 0 allocs/batch`).
4. **`Flush` (`10k` series, `3M` live events at `10k/s`, `330s` fill)**: Constructing `50,000` output `pmetric.Gauge` datapoints takes **`~53` to `57 ms`** (`17.48 MiB`, `316.8k allocs`) across `O(1)` engines (`algebraic_chan` `56.81 ms` vs `twostack_monoid` `56.42 ms`, statistically tied `p=0.699`), while `raw_rescan` adds **`+31.13 ms`** (`87.94 ms`, `+54.80%` slower than `algebraic_chan`, i.e., `algebraic_chan` has **`35.40%` lower latency**, `p=0.002`) rescanning `3M` events. At `500k/s` (`150M` live events = `50x` larger), `scalebench` proves `raw_rescan`'s pure scan jumps to **`1,780.53 ms` (`1.78s`)**, vs **`2.15 ms`** for `algebraic_chan` (**`828.2x` faster**).

All M3 Exit Gates passed.

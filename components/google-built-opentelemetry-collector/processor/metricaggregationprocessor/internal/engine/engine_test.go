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

package engine

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestStructSizes(t *testing.T) {
	require.Equal(t, uintptr(16), unsafe.Sizeof(rawSample{}), "rawSample must be 16 B")
	require.Equal(t, uintptr(24), unsafe.Sizeof(naiveBucket{}), "naiveBucket must be 24 B")
	require.Equal(t, uintptr(32), unsafe.Sizeof(chanBucket{}), "chanBucket must be 32 B (half cache line)")
	require.Equal(t, uintptr(48), unsafe.Sizeof(monoBucket{}), "monoBucket must be 48 B")
}

func assertParity(t *testing.T, label string, got Snapshot, want OracleStats, checkMinMax bool) {
	t.Helper()
	require.Equal(t, want.Count, got.Count, "%s: count mismatch", label)
	if want.Count == 0 {
		require.Equal(t, 0.0, got.Sum, "%s: empty sum must be 0", label)
		require.Equal(t, 0.0, got.StdDev(VarianceSample), "%s: empty stddev must be 0", label)
		return
	}

	scale := math.Max(1.0, want.StdDev)
	meanTol := math.Max(1e-9*math.Abs(want.Mean), 1e-9*scale)
	sumTol := math.Max(1e-9*math.Abs(want.Sum), 1e-9*float64(want.Count)*scale)
	sdTol := math.Max(1e-7*math.Abs(want.StdDev), 1e-12)
	popTol := math.Max(1e-7*math.Abs(want.PopStdDev), 1e-12)

	require.LessOrEqual(t, math.Abs(got.Mean-want.Mean), meanTol,
		"%s: mean got=%.17g want=%.17g diff=%.3e tol=%.3e", label, got.Mean, want.Mean, math.Abs(got.Mean-want.Mean), meanTol)
	require.LessOrEqual(t, math.Abs(got.Sum-want.Sum), sumTol,
		"%s: sum got=%.17g want=%.17g diff=%.3e tol=%.3e", label, got.Sum, want.Sum, math.Abs(got.Sum-want.Sum), sumTol)
	require.GreaterOrEqual(t, got.M2, 0.0,
		"%s: negative M2 got=%.17g want=%.17g", label, got.M2, want.M2)
	require.LessOrEqual(t, math.Abs(got.StdDev(VarianceSample)-want.StdDev), sdTol,
		"%s: sample stddev got=%.17g want=%.17g diff=%.3e tol=%.3e", label, got.StdDev(VarianceSample), want.StdDev, math.Abs(got.StdDev(VarianceSample)-want.StdDev), sdTol)
	require.LessOrEqual(t, math.Abs(got.StdDev(VariancePopulation)-want.PopStdDev), popTol,
		"%s: pop stddev got=%.17g want=%.17g diff=%.3e tol=%.3e", label, got.StdDev(VariancePopulation), want.PopStdDev, math.Abs(got.StdDev(VariancePopulation)-want.PopStdDev), popTol)

	if checkMinMax {
		require.True(t, got.HasMinMax, "%s: expected HasMinMax=true", label)
		require.Equal(t, want.Min, got.Min, "%s: min mismatch", label)
		require.Equal(t, want.Max, got.Max, "%s: max mismatch", label)
	}
}

type event struct {
	ts  int64
	val float64
}

// runBucketAlignedOracle runs a series of bucket-grouped events through the
// 4 accurate engines (raw_rescan, raw_inc, algebraic_chan, twostack_monoid)
// and verifies every query at now = k*Delta - 1 against the 256-bit big.Float oracle.
func runBucketAlignedOracle(t *testing.T, name string, buckets int, bucketDur time.Duration, stepEvents [][]event, queryEvery int) {
	t.Helper()
	windowDur := time.Duration(buckets) * bucketDur
	delta := bucketDur.Nanoseconds()

	kinds := []Kind{KindRawRescan, KindRawInc, KindAlgebraicChan, KindTwoStackMonoid}
	engines := make(map[Kind]Window, len(kinds))
	for _, k := range kinds {
		w, err := NewWindow(k, windowDur, bucketDur)
		require.NoError(t, err)
		engines[k] = w
	}

	var allEvents []event
	maxRelMean := map[Kind]float64{}
	maxRelSum := map[Kind]float64{}
	maxRelSD := map[Kind]float64{}

	for step, evs := range stepEvents {
		for _, ev := range evs {
			for _, k := range kinds {
				st := engines[k].Add(ev.ts, ev.val)
				require.Equal(t, AddOK, st, "%s engine=%s step=%d ts=%d val=%g", name, k, step, ev.ts, ev.val)
			}
			allEvents = append(allEvents, ev)
		}

		if queryEvery > 1 && (step+1)%queryEvery != 0 && step != len(stepEvents)-1 {
			continue
		}

		// Query at exclusive right boundary minus 1ns: k*Delta - 1 covers buckets [k-B, k-1],
		// matching exact continuous half-open interval (now - W, now].
		queryTS := int64(step+1)*delta - 1
		cutoff := queryTS - windowDur.Nanoseconds()

		var liveVals []float64
		for _, ev := range allEvents {
			if ev.ts > cutoff && ev.ts <= queryTS {
				liveVals = append(liveVals, ev.val)
			}
		}
		want := ComputeExactOracle(liveVals)

		for _, k := range kinds {
			got := engines[k].Query(queryTS)
			assertParity(t, fmt.Sprintf("%s/%s/step=%d", name, k, step), got, want, k.SupportsMinMax())
			if want.Count > 0 {
				if math.Abs(want.Mean) > 1e-6 {
					if r := math.Abs(got.Mean-want.Mean) / math.Abs(want.Mean); r > maxRelMean[k] {
						maxRelMean[k] = r
					}
				}
				if math.Abs(want.Sum) > 1e-6 {
					if r := math.Abs(got.Sum-want.Sum) / math.Abs(want.Sum); r > maxRelSum[k] {
						maxRelSum[k] = r
					}
				}
				if want.StdDev > 1e-6 {
					if r := math.Abs(got.StdDev(VarianceSample)-want.StdDev) / want.StdDev; r > maxRelSD[k] {
						maxRelSD[k] = r
					}
				}
			}
		}
	}

	for _, k := range kinds {
		d := engines[k].Diagnostics()
		t.Logf("[%s] %-16s maxRelMean=%.2e maxRelSum=%.2e maxRelSD=%.2e refolds=%d (cancel=%d periodic=%d) flips=%d evicts=%d",
			name, k, maxRelMean[k], maxRelSum[k], maxRelSD[k], d.Refolds, d.CancelRefolds, d.PeriodicRefolds, d.StackFlips, d.Evictions)
	}
}

func TestOracleParity_AllWorkloads(t *testing.T) {
	bucketDur := time.Second
	delta := bucketDur.Nanoseconds()

	t.Run("steady_state_uniform", func(t *testing.T) {
		rng := rand.New(rand.NewSource(101))
		steps := make([][]event, 200)
		for s := range steps {
			base := int64(s) * delta
			for i := 0; i < 10; i++ {
				steps[s] = append(steps[s], event{ts: base + int64(i)*10_000_000, val: rng.Float64() * 100})
			}
		}
		runBucketAlignedOracle(t, "steady", 60, bucketDur, steps, 1)
	})

	t.Run("spike_1e12_then_evict", func(t *testing.T) {
		steps := make([][]event, 180)
		for s := range steps {
			base := int64(s) * delta
			if s == 20 {
				steps[s] = append(steps[s], event{ts: base + 100, val: 1e12})
			} else {
				for i := 0; i < 5; i++ {
					steps[s] = append(steps[s], event{ts: base + int64(i)*100_000, val: 0.1 + 0.01*float64(i)})
				}
			}
		}
		runBucketAlignedOracle(t, "spike_1e12", 60, bucketDur, steps, 1)
	})

	t.Run("first_bucket_spike_reanchor", func(t *testing.T) {
		// Spike arrives as the very first event so initial anchor = 1e12.
		steps := make([][]event, 150)
		for s := range steps {
			base := int64(s) * delta
			if s == 0 {
				steps[s] = append(steps[s], event{ts: base + 1, val: 1e12})
			} else {
				for i := 0; i < 4; i++ {
					steps[s] = append(steps[s], event{ts: base + int64(i)*1000, val: 0.1 + 0.05*float64(i)})
				}
			}
		}
		runBucketAlignedOracle(t, "first_spike_1e12", 60, bucketDur, steps, 1)
	})

	t.Run("decaying_staircase_spikes", func(t *testing.T) {
		// Adversarial cool-down ramp: each bucket drops M2 by 400x (ratio 2.5e-3 > 1e-3),
		// defeating single-step M2 guards. High-water-mark peakM2 must catch it.
		levels := []float64{
			1.28e9, // 20^7
			6.4e7,  // 20^6
			3.2e6,  // 20^5
			1.6e5,  // 20^4
			8e3,    // 20^3
			4e2,    // 20^2
			20,     // 20^1
			1,      // 20^0
		}
		const B = 10
		steps := make([][]event, 40)
		for s := range steps {
			base := int64(s) * delta
			if s < len(levels) {
				v := levels[s]
				steps[s] = append(steps[s], event{ts: base + 1, val: v}, event{ts: base + 2, val: -v})
			} else {
				steps[s] = append(steps[s], event{ts: base + 1, val: 0.1}, event{ts: base + 2, val: 0.2})
			}
		}
		runBucketAlignedOracle(t, "decaying_staircase", B, bucketDur, steps, 1)
	})

	t.Run("high_offset_1e9", func(t *testing.T) {
		rng := rand.New(rand.NewSource(202))
		steps := make([][]event, 150)
		for s := range steps {
			base := int64(s) * delta
			for i := 0; i < 100; i++ {
				steps[s] = append(steps[s], event{ts: base + int64(i)*1_000_000, val: 1e9 + rng.Float64()})
			}
		}
		runBucketAlignedOracle(t, "offset_1e9", 60, bucketDur, steps, 5)
	})

	t.Run("high_offset_1e12", func(t *testing.T) {
		rng := rand.New(rand.NewSource(303))
		steps := make([][]event, 150)
		for s := range steps {
			base := int64(s) * delta
			for i := 0; i < 100; i++ {
				steps[s] = append(steps[s], event{ts: base + int64(i)*1_000_000, val: 1e12 + rng.Float64()})
			}
		}
		runBucketAlignedOracle(t, "offset_1e12", 60, bucketDur, steps, 5)
	})

	t.Run("zero_mean_gaussian", func(t *testing.T) {
		rng := rand.New(rand.NewSource(404))
		steps := make([][]event, 200)
		for s := range steps {
			base := int64(s) * delta
			for i := 0; i < 10; i++ {
				steps[s] = append(steps[s], event{ts: base + int64(i)*10_000_000, val: rng.NormFloat64()})
			}
		}
		runBucketAlignedOracle(t, "zero_mean_gaussian", 60, bucketDur, steps, 2)
	})

	t.Run("alternating_bipolar_even_and_odd_B", func(t *testing.T) {
		for _, B := range []int{59, 60} {
			steps := make([][]event, 200)
			for s := range steps {
				base := int64(s) * delta
				v := 1.0
				if s%2 == 1 {
					v = -1.0
				}
				for i := 0; i < 10; i++ {
					steps[s] = append(steps[s], event{ts: base + int64(i)*10_000_000, val: v})
				}
			}
			runBucketAlignedOracle(t, fmt.Sprintf("bipolar_B%d", B), B, bucketDur, steps, 5)
		}
	})

	t.Run("opposite_sign_canceling_sum_sterbenz", func(t *testing.T) {
		// Opposite-sign near-cancellation: +1e12/3 and -1e12/3 + 1e-3 nearly cancel.
		// Grouping float64(n)*(anchor + meanShifted) stays Sterbenz-exact.
		steps := make([][]event, 120)
		a := 1e12 / 3.0
		b := -a + 1e-3
		for s := range steps {
			base := int64(s) * delta
			val := a
			if s%2 == 1 {
				val = b
			}
			for i := 0; i < 50; i++ {
				steps[s] = append(steps[s], event{ts: base + int64(i)*1000, val: val})
			}
		}
		runBucketAlignedOracle(t, "sterbenz_cancel", 60, bucketDur, steps, 5)
	})

	t.Run("long_horizon_10000_buckets", func(t *testing.T) {
		// Verify zero unbounded drift across 10,000 bucket evictions with periodic spikes.
		rng := rand.New(rand.NewSource(505))
		const B = 60
		steps := make([][]event, 10000)
		for s := range steps {
			base := int64(s) * delta
			if s%500 == 0 {
				steps[s] = append(steps[s], event{ts: base + 1, val: 1e10})
			} else {
				steps[s] = append(steps[s],
					event{ts: base + 1, val: 10.0 + rng.NormFloat64()},
					event{ts: base + 2, val: 10.0 + rng.NormFloat64()},
				)
			}
		}
		runBucketAlignedOracle(t, "long_10k", B, bucketDur, steps, 250)
	})

	t.Run("out_of_order_15pct_including_head_bucket", func(t *testing.T) {
		rng := rand.New(rand.NewSource(606))
		const B = 30
		windowDur := time.Duration(B) * bucketDur
		kinds := []Kind{KindRawRescan, KindRawInc, KindAlgebraicChan, KindTwoStackMonoid}
		engines := make(map[Kind]Window, len(kinds))
		for _, k := range kinds {
			w, err := NewWindow(k, windowDur, bucketDur)
			require.NoError(t, err)
			engines[k] = w
		}

		maxRelMean := map[Kind]float64{}
		maxRelSum := map[Kind]float64{}
		maxRelSD := map[Kind]float64{}

		var accepted []event
		for s := 0; s < 160; s++ {
			base := int64(s) * delta
			// Main in-order events inside headBucket
			evs := []event{
				{ts: base + 500_000_000, val: 10 + rng.Float64()*5},
				// OOO inside headBucket (ts < lastTs but same bucket!)
				{ts: base + 100_000_000, val: 10 + rng.Float64()*5},
				{ts: base + 700_000_000, val: 10 + rng.Float64()*5},
				{ts: base + 200_000_000, val: 10 + rng.Float64()*5},
			}
			// Inject 15%+ late events landing across front and back stacks of twoStackMonoid.
			if s >= 5 {
				maxBack := B - 1
				if s < maxBack {
					maxBack = s
				}
				for _, lag := range []int{1, maxBack / 2, maxBack} {
					if lag > 0 && lag < B && s-lag >= 0 {
						lateTS := int64(s-lag)*delta + 300_000_000
						val := 20 + rng.Float64()*10
						if s%40 == 0 && lag == maxBack {
							val = 1e9 // late spike into oldest surviving bucket
						}
						evs = append(evs, event{ts: lateTS, val: val})
					}
				}
			}

			for _, ev := range evs {
				for _, k := range kinds {
					st := engines[k].Add(ev.ts, ev.val)
					require.Equal(t, AddOK, st, "engine=%s step=%d ev=%+v", k, s, ev)
				}
				accepted = append(accepted, ev)
			}

			queryTS := int64(s+1)*delta - 1
			cutoff := queryTS - windowDur.Nanoseconds()
			var live []float64
			for _, ev := range accepted {
				if ev.ts > cutoff && ev.ts <= queryTS {
					live = append(live, ev.val)
				}
			}
			want := ComputeExactOracle(live)
			for _, k := range kinds {
				got := engines[k].Query(queryTS)
				assertParity(t, fmt.Sprintf("ooo/%s/step=%d", k, s), got, want, k.SupportsMinMax())
				if want.Count > 0 {
					if math.Abs(want.Mean) > 1e-6 {
						if r := math.Abs(got.Mean-want.Mean) / math.Abs(want.Mean); r > maxRelMean[k] {
							maxRelMean[k] = r
						}
					}
					if math.Abs(want.Sum) > 1e-6 {
						if r := math.Abs(got.Sum-want.Sum) / math.Abs(want.Sum); r > maxRelSum[k] {
							maxRelSum[k] = r
						}
					}
					if want.StdDev > 1e-6 {
						if r := math.Abs(got.StdDev(VarianceSample)-want.StdDev) / want.StdDev; r > maxRelSD[k] {
							maxRelSD[k] = r
						}
					}
				}
			}
		}

		// Confirm twoStackMonoid actually exercised BOTH back hits and front rebuilds.
		tsDiag := engines[KindTwoStackMonoid].Diagnostics()
		require.Greater(t, tsDiag.BackOOOHits, uint64(0), "expected twostack back OOO hits")
		require.Greater(t, tsDiag.FrontOOORebuilds, uint64(0), "expected twostack front OOO rebuilds")
		require.Greater(t, tsDiag.StackFlips, uint64(0), "expected twostack flips")
		for _, k := range kinds {
			d := engines[k].Diagnostics()
			t.Logf("[ooo_15pct] %-16s maxRelMean=%.2e maxRelSum=%.2e maxRelSD=%.2e refolds=%d (cancel=%d periodic=%d) flips=%d evicts=%d",
				k, maxRelMean[k], maxRelSum[k], maxRelSD[k], d.Refolds, d.CancelRefolds, d.PeriodicRefolds, d.StackFlips, d.Evictions)
		}
		t.Logf("[ooo_15pct] twostack backHits=%d frontRebuilds=%d flips=%d",
			tsDiag.BackOOOHits, tsDiag.FrontOOORebuilds, tsDiag.StackFlips)
	})
}

func TestBaselineVsChange_AlgebraicNaiveFails(t *testing.T) {
	bucketDur := time.Second
	delta := bucketDur.Nanoseconds()
	windowDur := 60 * time.Second

	t.Run("high_offset_1e9_catastrophic_variance_cancellation", func(t *testing.T) {
		rng := rand.New(rand.NewSource(707))
		naive, err := NewWindow(KindAlgebraicNaive, windowDur, bucketDur)
		require.NoError(t, err)
		chanW, err := NewWindow(KindAlgebraicChan, windowDur, bucketDur)
		require.NoError(t, err)
		twoStack, err := NewWindow(KindTwoStackMonoid, windowDur, bucketDur)
		require.NoError(t, err)

		var vals []float64
		for s := 0; s < 60; s++ {
			for i := 0; i < 10; i++ {
				x := 1e9 + rng.Float64()
				ts := int64(s)*delta + int64(i)*1000
				vals = append(vals, x)
				require.Equal(t, AddOK, naive.Add(ts, x))
				require.Equal(t, AddOK, chanW.Add(ts, x))
				require.Equal(t, AddOK, twoStack.Add(ts, x))
			}
		}
		want := ComputeExactOracle(vals)
		qts := int64(60)*delta - 1

		naiveSnap := naive.Query(qts)
		rawVar := naiveSnap.RawVariance(VarianceSample)
		naiveSD := naiveSnap.StdDev(VarianceSample)
		naiveRelErr := math.Abs(naiveSD-want.StdDev) / want.StdDev
		t.Logf("1e9+[0,1]: oracle_sd=%.12f | naive_raw_var=%.3e naive_sd=%.6f (rel_err=%.2e) | chan_sd=%.12f | twostack_sd=%.12f",
			want.StdDev, rawVar, naiveSD, naiveRelErr, chanW.Query(qts).StdDev(VarianceSample), twoStack.Query(qts).StdDev(VarianceSample))

		// Non-tautological assertion: algebraic_naive MUST fail (>100% error or negative variance).
		require.True(t, rawVar < 0 || naiveRelErr > 0.5,
			"expected algebraic_naive catastrophic cancellation on 1e9+[0,1], got rawVar=%g relErr=%g", rawVar, naiveRelErr)

		// Candidates MUST pass within tight oracle tolerances.
		assertParity(t, "chan_1e9", chanW.Query(qts), want, false)
		assertParity(t, "twostack_1e9", twoStack.Query(qts), want, true)
	})

	t.Run("high_offset_1e12_catastrophic_variance_cancellation", func(t *testing.T) {
		rng := rand.New(rand.NewSource(808))
		naive, err := NewWindow(KindAlgebraicNaive, windowDur, bucketDur)
		require.NoError(t, err)
		chanW, err := NewWindow(KindAlgebraicChan, windowDur, bucketDur)
		require.NoError(t, err)
		twoStack, err := NewWindow(KindTwoStackMonoid, windowDur, bucketDur)
		require.NoError(t, err)

		var vals []float64
		for s := 0; s < 60; s++ {
			for i := 0; i < 10; i++ {
				x := 1e12 + rng.Float64()
				ts := int64(s)*delta + int64(i)*1000
				vals = append(vals, x)
				require.Equal(t, AddOK, naive.Add(ts, x))
				require.Equal(t, AddOK, chanW.Add(ts, x))
				require.Equal(t, AddOK, twoStack.Add(ts, x))
			}
		}
		want := ComputeExactOracle(vals)
		qts := int64(60)*delta - 1
		naiveSnap := naive.Query(qts)
		naiveRelErr := math.Abs(naiveSnap.StdDev(VarianceSample)-want.StdDev) / want.StdDev
		t.Logf("1e12+[0,1]: oracle_sd=%.12f | naive_raw_var=%.3e naive_sd=%.6e (rel_err=%.2e) | chan_sd=%.12f | twostack_sd=%.12f",
			want.StdDev, naiveSnap.RawVariance(VarianceSample), naiveSnap.StdDev(VarianceSample), naiveRelErr,
			chanW.Query(qts).StdDev(VarianceSample), twoStack.Query(qts).StdDev(VarianceSample))

		require.True(t, naiveSnap.RawVariance(VarianceSample) < 0 || naiveRelErr >= 1.0,
			"expected algebraic_naive total loss on 1e12+[0,1], got rawVar=%g relErr=%g", naiveSnap.RawVariance(VarianceSample), naiveRelErr)
		assertParity(t, "chan_1e12", chanW.Query(qts), want, false)
		assertParity(t, "twostack_1e12", twoStack.Query(qts), want, true)
	})

	t.Run("spike_eviction_residual_drift_and_phantom_empty_sum", func(t *testing.T) {
		naive, err := NewWindow(KindAlgebraicNaive, 10*time.Second, bucketDur)
		require.NoError(t, err)
		chanW, err := NewWindow(KindAlgebraicChan, 10*time.Second, bucketDur)
		require.NoError(t, err)
		twoStack, err := NewWindow(KindTwoStackMonoid, 10*time.Second, bucketDur)
		require.NoError(t, err)

		// Bucket 0: 1e17 spike (ulp(1e17) = 16.0, so both 1.0 and 2.0 are completely
		// absorbed by 1e17 in float64!). Bucket 2 (2.0) forces BOTH bucket 0 (1e17)
		// and bucket 1 (1.0) into closedAcc simultaneously on algebraic_chan so
		// eviction MUST recover via CancelRefolds == 1 (not an empty-closedAcc reset!).
		require.Equal(t, AddOK, naive.Add(0*delta+1, 1e17))
		require.Equal(t, AddOK, chanW.Add(0*delta+1, 1e17))
		require.Equal(t, AddOK, twoStack.Add(0*delta+1, 1e17))

		require.Equal(t, AddOK, naive.Add(1*delta+1, 1.0))
		require.Equal(t, AddOK, chanW.Add(1*delta+1, 1.0))
		require.Equal(t, AddOK, twoStack.Add(1*delta+1, 1.0))

		require.Equal(t, AddOK, naive.Add(2*delta+1, 2.0))
		require.Equal(t, AddOK, chanW.Add(2*delta+1, 2.0))
		require.Equal(t, AddOK, twoStack.Add(2*delta+1, 2.0))

		// Advance so bucket 0 (1e17) expires while buckets 1 (1.0) and 2 (2.0) remain live!
		qts := int64(11)*delta - 1 // covers buckets [1, 10]
		ns := naive.Query(qts)
		cs := chanW.Query(qts)
		ts := twoStack.Query(qts)
		t.Logf("after 1e17 spike evicts (true sum=3.0): naive_sum=%.6g | chan_sum=%.17g (cancelRefolds=%d) | twostack_sum=%.17g",
			ns.Sum, cs.Sum, chanW.Diagnostics().CancelRefolds, ts.Sum)

		// Non-tautological failure on naive: 1e17 + 1.0 + 2.0 == 1e17, then 1e17 - 1e17 == 0.0 (100% error!).
		require.Equal(t, 0.0, ns.Sum, "expected algebraic_naive to lose absorbed 1.0+2.0 (sum == 0)")
		require.Equal(t, uint64(1), chanW.Diagnostics().CancelRefolds, "expected algebraic_chan to recover via cancellation refold")
		require.InDelta(t, 3.0, cs.Sum, 1e-12)
		require.InDelta(t, 3.0, ts.Sum, 1e-12)

		// Now advance past bucket 2 so window becomes completely empty (count == 0)!
		// Naive subtracts 1.0 and 2.0 from 0.0 and leaves a PHANTOM NEGATIVE SUM (-3.0) on an EMPTY window!
		emptyTS := int64(13)*delta - 1
		nsEmpty := naive.Query(emptyTS)
		csEmpty := chanW.Query(emptyTS)
		tsEmpty := twoStack.Query(emptyTS)
		t.Logf("when window empties (count=0): naive_phantom_sum=%.6g | chan_sum=%.6g | twostack_sum=%.6g",
			nsEmpty.Sum, csEmpty.Sum, tsEmpty.Sum)
		require.Equal(t, uint64(0), nsEmpty.Count)
		require.Equal(t, -3.0, nsEmpty.Sum, "expected phantom negative sum -3.0 on empty algebraic_naive window")
		require.Equal(t, 0.0, csEmpty.Sum)
		require.Equal(t, 0.0, tsEmpty.Sum)
	})

	t.Run("decaying_staircase_naive_fails", func(t *testing.T) {
		naive, err := NewWindow(KindAlgebraicNaive, 10*time.Second, bucketDur)
		require.NoError(t, err)
		chanW, err := NewWindow(KindAlgebraicChan, 10*time.Second, bucketDur)
		require.NoError(t, err)
		twoStack, err := NewWindow(KindTwoStackMonoid, 10*time.Second, bucketDur)
		require.NoError(t, err)
		levels := []float64{1.28e9, 6.4e7, 3.2e6, 1.6e5, 8e3, 4e2, 20, 1}
		for s := 0; s < 20; s++ {
			base := int64(s) * delta
			if s < len(levels) {
				v := levels[s]
				require.Equal(t, AddOK, naive.Add(base+1, v))
				require.Equal(t, AddOK, naive.Add(base+2, -v))
				require.Equal(t, AddOK, chanW.Add(base+1, v))
				require.Equal(t, AddOK, chanW.Add(base+2, -v))
				require.Equal(t, AddOK, twoStack.Add(base+1, v))
				require.Equal(t, AddOK, twoStack.Add(base+2, -v))
			} else {
				require.Equal(t, AddOK, naive.Add(base+1, 0.1))
				require.Equal(t, AddOK, naive.Add(base+2, 0.2))
				require.Equal(t, AddOK, chanW.Add(base+1, 0.1))
				require.Equal(t, AddOK, chanW.Add(base+2, 0.2))
				require.Equal(t, AddOK, twoStack.Add(base+1, 0.1))
				require.Equal(t, AddOK, twoStack.Add(base+2, 0.2))
			}
		}
		// At step 19, query covers buckets [10..19] (all [0.1, 0.2] pairs, true M2 = 0.05).
		qts := int64(20)*delta - 1
		var vals []float64
		for i := 0; i < 10; i++ {
			vals = append(vals, 0.1, 0.2)
		}
		want := ComputeExactOracle(vals)
		ns := naive.Query(qts)
		cs := chanW.Query(qts)
		ts := twoStack.Query(qts)
		t.Logf("staircase after decay: want_sd=%.12f | naive_raw_var=%.3e naive_sd=%.3e | chan_sd=%.12f (refolds=%d) | twostack_sd=%.12f (flips=%d)",
			want.StdDev, ns.RawVariance(VarianceSample), ns.StdDev(VarianceSample), cs.StdDev(VarianceSample), chanW.Diagnostics().Refolds, ts.StdDev(VarianceSample), twoStack.Diagnostics().StackFlips)
		require.True(t, ns.RawVariance(VarianceSample) < 0 || math.Abs(ns.StdDev(VarianceSample)-want.StdDev)/want.StdDev >= 1.0,
			"expected algebraic_naive to fail decaying staircase, got rawVar=%g sd=%g", ns.RawVariance(VarianceSample), ns.StdDev(VarianceSample))
		assertParity(t, "chan_staircase", cs, want, false)
		assertParity(t, "twostack_staircase", ts, want, true)
	})
}

func TestSafetyAndBoundaries(t *testing.T) {
	bucketDur := time.Second
	delta := bucketDur.Nanoseconds()

	t.Run("non_finite_and_extreme_overflow_rejected", func(t *testing.T) {
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 10*time.Second, bucketDur)
			require.NoError(t, err)

			require.Equal(t, AddOK, w.Add(1*delta, 10.0))
			require.Equal(t, AddDroppedNonFinite, w.Add(2*delta, math.NaN()))
			require.Equal(t, AddDroppedNonFinite, w.Add(2*delta, math.Inf(1)))
			require.Equal(t, AddDroppedNonFinite, w.Add(2*delta, math.Inf(-1)))
			require.Equal(t, AddDroppedOverflow, w.Add(2*delta, 1e200))
			require.Equal(t, AddDroppedOverflow, w.Add(2*delta, -1e200))
			require.Equal(t, AddDroppedOverflow, w.Add(2*delta, 1e141))
			require.Equal(t, AddOK, w.Add(3*delta, 20.0))

			d := w.Diagnostics()
			require.Equal(t, uint64(3), d.DroppedNonFinite, "engine=%s", k)
			require.Equal(t, uint64(3), d.DroppedOverflow, "engine=%s", k)

			snap := w.Query(4*delta - 1)
			require.Equal(t, uint64(2), snap.Count, "engine=%s", k)
			require.InDelta(t, 30.0, snap.Sum, 1e-12, "engine=%s", k)
			require.InDelta(t, 15.0, snap.Mean, 1e-12, "engine=%s", k)
			require.False(t, math.IsNaN(snap.M2), "engine=%s poisoned with NaN", k)
		}
	})

	t.Run("huge_time_gaps_fast_path", func(t *testing.T) {
		yearGap := int64(365 * 24 * time.Hour)
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 5*time.Minute, time.Second)
			require.NoError(t, err)

			start := time.Now()
			require.Equal(t, AddOK, w.Add(10*delta, 42.0))
			// Jump 10*W
			require.Equal(t, AddOK, w.Add(10*delta+int64(50*time.Minute), 7.0))
			// Jump 365 days (31.5M buckets - must complete in O(B), not O(steps)!)
			require.Equal(t, AddOK, w.Add(10*delta+int64(50*time.Minute)+yearGap, 99.0))
			elapsed := time.Since(start)
			t.Logf("[huge_gap] %-16s elapsed=%v", k, elapsed)
			require.Less(t, elapsed, 50*time.Millisecond, "engine=%s stalled on 365-day jump (%v)", k, elapsed)

			snap := w.Snapshot()
			require.Equal(t, uint64(1), snap.Count, "engine=%s", k)
			require.InDelta(t, 99.0, snap.Mean, 1e-12, "engine=%s", k)
		}
	})

	t.Run("single_bucket_B1_and_late_drop", func(t *testing.T) {
		for _, k := range []Kind{KindAlgebraicNaive, KindAlgebraicChan, KindTwoStackMonoid} {
			w, err := NewWindow(k, time.Second, time.Second)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(10*delta+100, 5.0))
			require.Equal(t, AddOK, w.Add(10*delta+200, 15.0))
			// Advance to bucket 11 -> bucket 10 completely evicted.
			require.Equal(t, AddOK, w.Add(11*delta+100, 30.0))
			// Late event for bucket 10 must be dropped without aliasing slot 0.
			require.Equal(t, AddDroppedLate, w.Add(10*delta+500, 999.0))
			snap := w.Snapshot()
			require.Equal(t, uint64(1), snap.Count, "engine=%s", k)
			require.InDelta(t, 30.0, snap.Sum, 1e-12, "engine=%s", k)
		}
	})

	t.Run("late_event_boundary_no_slot_aliasing", func(t *testing.T) {
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 5*time.Second, time.Second)
			require.NoError(t, err)
			// Establish headBucket = 10 (active buckets [6..10] for bucketed engines).
			require.Equal(t, AddOK, w.Add(10*delta+500_000_000, 100.0))
			// Bucket 5 (ts = 5*delta + 1) aliases slot 0 == bucket 10 % 5! Must be rejected.
			require.Equal(t, AddDroppedLate, w.Add(5*delta+1, 999999.0), "engine=%s", k)
			require.Equal(t, uint64(1), w.Diagnostics().DroppedLate, "engine=%s", k)
			require.Equal(t, uint64(1), w.Snapshot().Count, "engine=%s", k)
			require.InDelta(t, 100.0, w.Snapshot().Mean, 1e-12, "engine=%s", k)
		}
	})

	t.Run("advance_evicts_silent_series_to_zero", func(t *testing.T) {
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 5*time.Second, time.Second)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(1*delta, 50.0))
			snap := w.Query(20 * delta)
			require.Equal(t, uint64(0), snap.Count, "engine=%s", k)
			require.False(t, snap.HasFirstSeen, "engine=%s should reset HasFirstSeen after empty gap", k)
		}
	})

	t.Run("negative_pre_epoch_timestamps", func(t *testing.T) {
		for _, k := range []Kind{KindRawRescan, KindRawInc, KindAlgebraicChan, KindTwoStackMonoid} {
			w, err := NewWindow(k, 5*time.Second, time.Second)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(-10*delta+100, 2.0))
			require.Equal(t, AddOK, w.Add(-9*delta+100, 4.0))
			require.Equal(t, AddOK, w.Add(-8*delta+100, 6.0))
			snap := w.Query(-7*delta - 1)
			require.Equal(t, uint64(3), snap.Count, "engine=%s", k)
			require.InDelta(t, 4.0, snap.Mean, 1e-12, "engine=%s", k)
		}
	})

	t.Run("int64_above_2_to_53_precision_boundary", func(t *testing.T) {
		// Documented float64 precision boundary: int64 > 2^53 loses least-significant bit.
		const base int64 = 1 << 53
		require.Equal(t, float64(base), float64(base+1), "IEEE-754 float64 merges 2^53 and 2^53+1")
		require.NotEqual(t, float64(base), float64(base+2), "IEEE-754 float64 distinguishes 2^53 and 2^53+2 (ulp=2)")

		w, err := NewWindow(KindAlgebraicChan, 5*time.Second, time.Second)
		require.NoError(t, err)
		require.Equal(t, AddOK, w.Add(1*delta, float64(base)))
		require.Equal(t, AddOK, w.Add(2*delta, float64(base+2)))
		snap := w.Query(3*delta - 1)
		require.Equal(t, uint64(2), snap.Count)
		require.Equal(t, float64(base+1), snap.Mean) // (2^53 + (2^53+2))/2 == 2^53 + 1? No, 2^53+1 rounds to 2^53 (ties to even)!
		t.Logf("int64 > 2^53 boundary: mean of {2^53, 2^53+2} in float64 rounds to %.0f (exact int avg 2^53+1 is unrepresentable)", snap.Mean)
	})

	t.Run("refold_non_thrashing_gaussian_and_bipolar", func(t *testing.T) {
		// 1. N(0, 1) over 5,000 buckets: refoldCount < 5% of evictions.
		rng := rand.New(rand.NewSource(909))
		wGauss, err := NewWindow(KindAlgebraicChan, 60*time.Second, time.Second)
		require.NoError(t, err)
		for s := 0; s < 5000; s++ {
			base := int64(s) * delta
			for i := 0; i < 10; i++ {
				require.Equal(t, AddOK, wGauss.Add(base+int64(i)*1000, rng.NormFloat64()))
			}
		}
		dg := wGauss.Diagnostics()
		rateGauss := float64(dg.Refolds) / float64(dg.Evictions)
		t.Logf("N(0,1) 5000 buckets (B=60): evictions=%d refolds=%d (cancel=%d periodic=%d) rate=%.3f%%",
			dg.Evictions, dg.Refolds, dg.CancelRefolds, dg.PeriodicRefolds, rateGauss*100)
		require.Less(t, rateGauss, 0.05, "gaussian refold rate exceeded 5%%")

		// 2. Alternating bipolar +/-1 over 2,000 buckets for B > 10 (even & odd):
		// 0 cancellation refolds, total refolds < 1% (periodic cap 1/(10*B) only).
		for _, B := range []int{59, 60, 299, 300} {
			w, err := NewWindow(KindAlgebraicChan, time.Duration(B)*time.Second, time.Second)
			require.NoError(t, err)
			for s := 0; s < 2000; s++ {
				base := int64(s) * delta
				v := 1.0
				if s%2 == 1 {
					v = -1.0
				}
				for i := 0; i < 10; i++ {
					require.Equal(t, AddOK, w.Add(base+int64(i)*1000, v))
				}
			}
			d := w.Diagnostics()
			rate := float64(d.Refolds) / float64(d.Evictions)
			t.Logf("bipolar +/-1 B=%d: evictions=%d refolds=%d (cancel=%d periodic=%d) rate=%.3f%%",
				B, d.Evictions, d.Refolds, d.CancelRefolds, d.PeriodicRefolds, rate*100)
			require.Equal(t, uint64(0), d.CancelRefolds, "expected 0 cancellation refolds for B=%d", B)
			require.Less(t, rate, 0.01, "bipolar refold rate exceeded 1%% for B=%d", B)
		}
	})

	t.Run("sparse_scrape_na_equals_1_accuracy", func(t *testing.T) {
		// Sparse-scrape edge case: B=3 bipolar +/-1 leaves na==1 on each eviction,
		// tripping the M2 guard as documented while keeping accuracy exact.
		steps := make([][]event, 50)
		for s := range steps {
			v := 1.0
			if s%2 == 1 {
				v = -1.0
			}
			steps[s] = []event{{ts: int64(s)*delta + 1, val: v}}
		}
		runBucketAlignedOracle(t, "sparse_B3", 3, bucketDur, steps, 1)
		// Also check exact right boundary difference: Query(k*Delta - 1) covers [k-B, k-1]
		// (exact width W), whereas Query(k*Delta) opens empty bucket k (width W - Delta).
		wBoundary, err := NewWindow(KindAlgebraicChan, 5*time.Second, time.Second)
		require.NoError(t, err)
		for s := 0; s < 5; s++ {
			require.Equal(t, AddOK, wBoundary.Add(int64(s)*delta+500_000_000, 10.0))
		}
		atEndMinus1 := wBoundary.Query(5*delta - 1)
		require.Equal(t, uint64(5), atEndMinus1.Count, "Query(k*Delta - 1) must keep all B=5 completed buckets")
		atBoundary := wBoundary.Query(5 * delta)
		require.Equal(t, uint64(4), atBoundary.Count, "Query(k*Delta) opens bucket k and evicts bucket k-B (width W-Delta)")
	})

	t.Run("int64_timestamp_extremes_no_overflow_or_infinite_loop", func(t *testing.T) {
		// 1. MinInt64 + 2s with W = 10s: must not underflow cutoff positive or drop live points.
		baseMin := int64(math.MinInt64) + int64(2*time.Second)
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 10*time.Second, time.Second)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(baseMin, 10.0), "engine=%s", k)
			snap := w.Query(baseMin + int64(time.Second))
			require.Equal(t, uint64(1), snap.Count, "engine=%s evicted live point near MinInt64", k)
			require.Equal(t, int64(math.MinInt64), snap.WindowStartNanos, "engine=%s WindowStartNanos wrapped", k)
			require.Equal(t, AddOK, w.Add(baseMin+int64(2*time.Second), 20.0), "engine=%s deadlocked near MinInt64", k)
			require.Equal(t, uint64(2), w.Snapshot().Count, "engine=%s", k)
		}

		// 2. Huge jump across 0 (-5e18 -> +5e18) at bucketDur = 1ns: must not overflow steps negative.
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 10*time.Nanosecond, time.Nanosecond)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(-5_000_000_000_000_000_000, 10.0), "engine=%s", k)
			require.Equal(t, AddOK, w.Add(5_000_000_000_000_000_000, 20.0), "engine=%s", k)
			snap := w.Snapshot()
			require.Equal(t, uint64(1), snap.Count, "engine=%s failed to clear across -5e18 -> +5e18", k)
			require.InDelta(t, 20.0, snap.Mean, 1e-12, "engine=%s", k)
			// Late event at -5e18 after jumping to +5e18 must be dropped cleanly.
			require.Equal(t, AddDroppedLate, w.Add(-5_000_000_000_000_000_000, 99.0), "engine=%s", k)
		}

		// 3. Bucketed engines at exact math.MinInt64 (bucketDur = 1ns): descending loops
		// in refoldClosed and twoStackMonoid flip/front-OOO must terminate without wraparound.
		for _, k := range []Kind{KindAlgebraicChan, KindTwoStackMonoid} {
			w, err := NewWindow(k, 5*time.Nanosecond, time.Nanosecond)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(math.MinInt64, 1e12), "engine=%s", k)
			require.Equal(t, AddOK, w.Add(math.MinInt64+1, 1.0), "engine=%s", k)
			require.Equal(t, AddOK, w.Add(math.MinInt64+2, 2.0), "engine=%s", k)
			// Late front-stack OOO at MinInt64+1
			require.Equal(t, AddOK, w.Add(math.MinInt64+1, 3.0), "engine=%s", k)
			// Advance past MinInt64 (triggers refold / flip starting down to MinInt64+1)
			snap := w.Query(math.MinInt64 + 5)
			require.Equal(t, uint64(3), snap.Count, "engine=%s", k)
			require.InDelta(t, 6.0, snap.Sum, 1e-12, "engine=%s", k)
		}

		// 4. Exact boundary check for windowStartBucketNanos on non-power-of-two bucketDur:
		// oldest == math.MinInt64/bucketNanos MUST NOT prematurely clamp; oldest-1 MUST clamp.
		const bn = int64(time.Second)
		q := math.MinInt64 / bn // -9223372036 (exact multiple -9223372036000000000 > MinInt64)
		require.Equal(t, q*bn, windowStartBucketNanos(q+9, 10, bn))
		require.Equal(t, int64(math.MinInt64), windowStartBucketNanos(q+8, 10, bn))
		// Power-of-two bucketDur=1ns: exact MinInt64 returns MinInt64 without overflow.
		require.Equal(t, int64(math.MinInt64), windowStartBucketNanos(math.MinInt64, 1, 1))
		// Large bucketDur=5e18 ns, B=1, ts=-1 -> bucket -1 -> WindowStartNanos = -5e18 (not MinInt64!).
		for _, k := range []Kind{KindAlgebraicNaive, KindAlgebraicChan, KindTwoStackMonoid} {
			w, err := NewWindow(k, 5_000_000_000_000_000_000*time.Nanosecond, 5_000_000_000_000_000_000*time.Nanosecond)
			require.NoError(t, err)
			require.Equal(t, AddOK, w.Add(-1, 42.0), "engine=%s", k)
			require.Equal(t, int64(-5_000_000_000_000_000_000), w.Snapshot().WindowStartNanos, "engine=%s prematurely clamped", k)
		}
	})

	t.Run("peakSumMag_guard_independent_of_peakM2", func(t *testing.T) {
		// Triggers peakSumMag guard while M2 ratio stays > 1e-3 AND eviction count stays
		// below rawPeriodicRefoldInterval (10,000) so raw_inc hits CancelRefolds, not PeriodicRefolds:
		// Seed anchor=0.0, load 5 buckets of 1,000 pts at 1.0 (sumShifted=5,000, M2=1.0),
		// add final buckets with {0.0, 1.0} (M2=0.5 per bucket), then evict the 5 bulk buckets.
		// Surviving M2 >= 0.5 (> 1e-3 * 1.0) while scale drops from 5,000 to ~2 (< 1e-3 * 5,000 = 5.0).
		chanW, err := NewWindow(KindAlgebraicChan, 15*time.Second, time.Second)
		require.NoError(t, err)
		rawW, err := NewWindow(KindRawInc, 15*time.Second, time.Second)
		require.NoError(t, err)

		require.Equal(t, AddOK, chanW.Add(0*delta+1, 0.0))
		require.Equal(t, AddOK, rawW.Add(0*delta+1, 0.0))
		for s := 1; s <= 5; s++ {
			base := int64(s) * delta
			for i := 0; i < 1000; i++ {
				require.Equal(t, AddOK, chanW.Add(base+int64(i)*1000, 1.0))
				require.Equal(t, AddOK, rawW.Add(base+int64(i)*1000, 1.0))
			}
		}
		require.Equal(t, AddOK, chanW.Add(6*delta+1, 0.0))
		require.Equal(t, AddOK, chanW.Add(6*delta+2, 1.0))
		require.Equal(t, AddOK, rawW.Add(6*delta+1, 0.0))
		require.Equal(t, AddOK, rawW.Add(6*delta+2, 1.0))
		require.Equal(t, AddOK, chanW.Add(7*delta+1, 0.0))
		require.Equal(t, AddOK, chanW.Add(7*delta+2, 1.0))
		require.Equal(t, AddOK, rawW.Add(7*delta+1, 0.0))
		require.Equal(t, AddOK, rawW.Add(7*delta+2, 1.0))

		for step := 15; step <= 20; step++ {
			qts := int64(step+1)*delta - 1
			chanW.Advance(qts)
			rawW.Advance(qts)
		}
		require.GreaterOrEqual(t, chanW.Diagnostics().CancelRefolds, uint64(1), "expected peakSumMag cancellation refold on algebraic_chan")
		require.GreaterOrEqual(t, rawW.Diagnostics().CancelRefolds, uint64(1), "expected peakSumMag cancellation refold on raw_inc")
		cSnap := chanW.Snapshot()
		rSnap := rawW.Snapshot()
		require.Equal(t, uint64(4), cSnap.Count)
		require.Equal(t, uint64(4), rSnap.Count)
		require.InDelta(t, 2.0, cSnap.Sum, 1e-12)
		require.InDelta(t, 2.0, rSnap.Sum, 1e-12)
	})

	t.Run("raw_inc_periodic_refold_and_ring_wraparound_grow", func(t *testing.T) {
		rawW, err := NewWindow(KindRawInc, 10*time.Second, time.Second)
		require.NoError(t, err)
		for i := 0; i < 12000; i++ {
			ts := int64(i) * int64(100*time.Millisecond)
			val := 50.0 + float64(i%5)
			require.Equal(t, AddOK, rawW.Add(ts, val))
		}
		d := rawW.Diagnostics()
		require.GreaterOrEqual(t, d.PeriodicRefolds, uint64(1), "expected raw_inc periodic refold past 10,000 evictions")
		require.Equal(t, uint64(0), d.CancelRefolds)

		// Exercise rawRing.grow wrap-around copy branch explicitly (head > 0 when full).
		var ring rawRing
		for i := 0; i < 16; i++ {
			ring.pushBack(rawSample{ts: int64(i), val: float64(i)})
		}
		for i := 0; i < 10; i++ {
			ring.popFront()
		}
		for i := 16; i < 30; i++ {
			ring.pushBack(rawSample{ts: int64(i), val: float64(i)})
		}
		require.Equal(t, 20, ring.size)
		for i := 0; i < 20; i++ {
			require.Equal(t, int64(10+i), ring.at(i).ts)
		}
	})

	t.Run("empty_refold_sibling_diagnostic_consistency_and_lifecycle_accessors", func(t *testing.T) {
		chanW, err := NewWindow(KindAlgebraicChan, 10*time.Second, time.Second)
		require.NoError(t, err)
		rawW, err := NewWindow(KindRawInc, 10*time.Second, time.Second)
		require.NoError(t, err)

		for _, w := range []Window{chanW, rawW} {
			require.False(t, w.Initialized())
			require.Equal(t, int64(0), w.LastActivityNanos())
			w.Advance(100 * delta)
			require.Equal(t, Snapshot{}, w.Snapshot())
		}

		require.Equal(t, AddOK, chanW.Add(0*delta+1, 1e12))
		require.Equal(t, AddOK, chanW.Add(1*delta+1, 1.0))
		require.Equal(t, AddOK, chanW.Add(2*delta+1, 2.0))
		require.Equal(t, AddOK, rawW.Add(0*delta+1, 1e12))
		require.Equal(t, AddOK, rawW.Add(1*delta+1, 1.0))
		require.Equal(t, AddOK, rawW.Add(2*delta+1, 2.0))

		chanW.Advance(5*delta + 1)
		rawW.Advance(5*delta + 1)
		// Jump 7 buckets (< B=10): evicts buckets [ -4 .. 2 ], clearing 0 (1e12), 1 (1.0), 2 (2.0)!
		chanW.Advance(12*delta + 1)
		rawW.Advance(12*delta + 1)

		require.Equal(t, uint64(0), chanW.Snapshot().Count)
		require.Equal(t, uint64(0), rawW.Snapshot().Count)
		require.Equal(t, int64(0), chanW.Snapshot().FirstSeenNanos, "stale FirstSeenNanos after clear")
		require.Equal(t, int64(0), rawW.Snapshot().FirstSeenNanos, "stale FirstSeenNanos after clear")
		require.False(t, chanW.Snapshot().HasFirstSeen)
		require.False(t, rawW.Snapshot().HasFirstSeen)
		require.Equal(t, uint64(0), chanW.Diagnostics().Refolds, "empty surviving refold should not increment Refolds")
		require.Equal(t, uint64(0), chanW.Diagnostics().CancelRefolds)
		require.Equal(t, uint64(0), rawW.Diagnostics().Refolds)
		require.Equal(t, uint64(0), rawW.Diagnostics().CancelRefolds)

		// Verify uninitialized Advance/Snapshot, LastActivityNanos, Initialized, and
		// partial-advance empty reset with FirstSeenNanos==0 across all 5 engines.
		for _, k := range AllKinds() {
			w, err := NewWindow(k, 5*time.Second, time.Second)
			require.NoError(t, err)
			require.False(t, w.Initialized(), "engine=%s", k)
			require.Equal(t, int64(0), w.LastActivityNanos(), "engine=%s", k)
			w.Advance(10 * delta)
			require.Equal(t, Snapshot{}, w.Snapshot(), "engine=%s", k)

			require.Equal(t, AddOK, w.Add(1*delta+500, 7.0))
			require.True(t, w.Initialized(), "engine=%s", k)
			require.Equal(t, 1*delta+500, w.LastActivityNanos(), "engine=%s", k)
			w.Advance(4*delta + 500)
			require.Equal(t, uint64(1), w.Snapshot().Count, "engine=%s", k)
			w.Advance(6*delta + 500)
			snap := w.Snapshot()
			require.Equal(t, uint64(0), snap.Count, "engine=%s", k)
			require.False(t, snap.HasFirstSeen, "engine=%s", k)
			require.Equal(t, int64(0), snap.FirstSeenNanos, "engine=%s retained stale FirstSeenNanos", k)
		}
	})
}

func TestInvalidNewWindowConfigs(t *testing.T) {
	_, err := NewWindow(KindAlgebraicChan, 0, time.Second)
	require.Error(t, err)
	_, err = NewWindow(KindAlgebraicChan, time.Minute, 0)
	require.Error(t, err)
	_, err = NewWindow(KindAlgebraicChan, 10*time.Second, 3*time.Second)
	require.Error(t, err)
	_, err = NewWindow(Kind("unknown"), time.Minute, time.Second)
	require.Error(t, err)
	require.False(t, Kind("unknown").Valid())
	for _, k := range AllKinds() {
		require.True(t, k.Valid())
	}
}

func TestSubBucketDiscretizationError(t *testing.T) {
	// Quantify signed mid-bucket discretization error for B in {60, 300, 600}
	// over fixed W = 5m (300s) against continuous raw_rescan oracle (now-W, now].
	const windowDur = 5 * time.Minute
	const eventRatePerSec = 20 // 20 events/sec -> 6,000 events per 5m window
	stepNS := int64(time.Second) / eventRatePerSec

	for _, B := range []int{60, 300, 600} {
		bucketDur := windowDur / time.Duration(B)
		rawW, err := NewWindow(KindRawRescan, windowDur, bucketDur)
		require.NoError(t, err)
		chanW, err := NewWindow(KindAlgebraicChan, windowDur, bucketDur)
		require.NoError(t, err)

		totalEvents := 330 * eventRatePerSec // 330s total: full 300s window + 20s periodic sample (4 full 5s cycles)
		var sumSignedErr float64
		var maxAbsErr float64
		var minSignedErr float64
		var samples int

		for i := 0; i < totalEvents; i++ {
			ts := int64(i) * stepNS
			require.Equal(t, AddOK, rawW.Add(ts, 1.0))
			require.Equal(t, AddOK, chanW.Add(ts, 1.0))

			// Sample mid-bucket timestamps after window is full (t in [310s, 330s)).
			if ts >= int64(310*time.Second) {
				rs := rawW.Snapshot()
				cs := chanW.Snapshot()
				signedErr := (cs.Sum - rs.Sum) / rs.Sum
				sumSignedErr += signedErr
				if math.Abs(signedErr) > maxAbsErr {
					maxAbsErr = math.Abs(signedErr)
				}
				if samples == 0 || signedErr < minSignedErr {
					minSignedErr = signedErr
				}
				samples++
			}
		}

		meanSignedErr := sumSignedErr / float64(samples)
		expectedMeanBias := -1.0 / (2.0 * float64(B))
		expectedMaxBias := 1.0 / float64(B)
		t.Logf("B=%3d (Delta=%4v): mean_signed_err=%+.4f%% (theory %+.4f%%) | min=%+.4f%% | max_abs=%.4f%% (theory <= %.4f%%)",
			B, bucketDur, meanSignedErr*100, expectedMeanBias*100, minSignedErr*100, maxAbsErr*100, expectedMaxBias*100)

		// Verify B-slot design matches theoretical under-coverage [-1/B, 0], mean ~ -1/(2B).
		require.InDelta(t, expectedMeanBias, meanSignedErr, 0.0015, "B=%d mean bias mismatch", B)
		require.LessOrEqual(t, maxAbsErr, expectedMaxBias+0.0015, "B=%d max abs error exceeded 1/B", B)
	}
}

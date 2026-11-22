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
	"math/rand/v2"
	"testing"
	"time"
)

// Package-level sinks prevent the compiler from eliminating benchmark calls.
var (
	benchStatusSink AddStatus
	benchSnapSink   Snapshot
)

const benchBaseNanos int64 = 1700000000 * int64(time.Second)

func makeSteadyValues(n int) []float64 {
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = 100.0 + 15.0*math.Sin(float64(i)*0.07) + 3.0*math.Cos(float64(i)*0.19)
	}
	return vals
}

func BenchmarkAddInOrder(b *testing.B) {
	const win = 5 * time.Minute // 300s
	configs := []struct {
		buckets int
		bktDur  time.Duration
		eps     int // events/sec per series (10eps -> 100k/s across 10k series; 50eps -> 500k/s across 10k series)
	}{
		{buckets: 60, bktDur: 5 * time.Second, eps: 10},
		{buckets: 60, bktDur: 5 * time.Second, eps: 50},
		{buckets: 300, bktDur: time.Second, eps: 50},
	}

	vals := makeSteadyValues(1024)

	for _, tc := range configs {
		stepNanos := int64(time.Second) / int64(tc.eps)
		// Warm 360s so both ring capacity (4096 or 16384) and steady-state evictions are active.
		warmupEvents := 360 * tc.eps
		for _, kind := range AllKinds() {
			name := fmt.Sprintf("B=%d/rate=%deps/engine=%s", tc.buckets, tc.eps, kind)
			b.Run(name, func(b *testing.B) {
				w, err := NewWindow(kind, win, tc.bktDur)
				if err != nil {
					b.Fatal(err)
				}
				ts := benchBaseNanos
				for i := 0; i < warmupEvents; i++ {
					if st := w.Add(ts, vals[i&1023]); st != AddOK {
						b.Fatalf("warmup Add failed: status=%v", st)
					}
					ts += stepNanos
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchStatusSink = w.Add(ts, vals[i&1023])
					ts += stepNanos
				}
				b.StopTimer()

				diag := w.Diagnostics()
				if diag.DroppedLate != 0 || diag.DroppedNonFinite != 0 || diag.DroppedOverflow != 0 {
					b.Fatalf("unexpected drops during in-order bench: %+v", diag)
				}
			})
		}
	}
}

func BenchmarkSnapshot(b *testing.B) {
	const win = 5 * time.Minute // 300s
	const bktDur = 5 * time.Second
	configs := []int{10, 50} // 3,000 and 15,000 live events in the 300s window
	vals := makeSteadyValues(1024)

	for _, eps := range configs {
		stepNanos := int64(time.Second) / int64(eps)
		// Fill 330s (300s window + 30s steady-state slide) so:
		// 1) twostack_monoid is past its initial flip boundary (splitID < headBucket),
		//    forcing the steady-state 3-way front+back+head combine on every Snapshot().
		// 2) raw_rescan and raw_inc rings wrap around ring.head > 0.
		// All 5 engines still hold exact 300*eps live events in the active window.
		fillEvents := 330 * eps
		wantCount := uint64(300 * eps)
		for _, kind := range AllKinds() {
			name := fmt.Sprintf("B=60/rate=%deps/engine=%s", eps, kind)
			b.Run(name, func(b *testing.B) {
				w, err := NewWindow(kind, win, bktDur)
				if err != nil {
					b.Fatal(err)
				}
				ts := benchBaseNanos
				for i := 0; i < fillEvents; i++ {
					if st := w.Add(ts, vals[i&1023]); st != AddOK {
						b.Fatalf("fill Add failed: status=%v", st)
					}
					ts += stepNanos
				}
				if got := w.Snapshot().Count; got != wantCount {
					b.Fatalf("expected %d events in window before bench, got %d", wantCount, got)
				}
				if kind == KindTwoStackMonoid {
					tsm := w.(*twoStackMonoid)
					if tsm.splitID >= tsm.headBucket {
						b.Fatalf("twostack_monoid stopped on flip boundary (splitID=%d head=%d); backAgg would be skipped", tsm.splitID, tsm.headBucket)
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchSnapSink = w.Snapshot()
				}
			})
		}
	}
}

func BenchmarkAddOOO(b *testing.B) {
	const win = 5 * time.Minute // 300s
	const eps = 50              // 50 events/sec per series (15,000 live events in 300s window)
	stepNanos := int64(time.Second) / int64(eps)
	vals := makeSteadyValues(4096)

	configs := []struct {
		buckets int
		bktDur  time.Duration
		oooPct  int
	}{
		{buckets: 60, bktDur: 5 * time.Second, oooPct: 5},
		{buckets: 60, bktDur: 5 * time.Second, oooPct: 15},
		{buckets: 300, bktDur: time.Second, oooPct: 15},
	}

	for _, tc := range configs {
		const tableLen = 4096
		rng := rand.New(rand.NewPCG(42, uint64(tc.buckets*100+tc.oooPct)))
		lagNanos := make([]int64, tableLen)
		bktNanos := tc.bktDur.Nanoseconds()
		for i := 0; i < tableLen; i++ {
			if rng.IntN(100) < tc.oooPct {
				// Uniformly choose closed-bucket lag k in [1, B-1].
				// Guarantees k*Delta < W, so BOTH bucketed (closed range [head-(B-1), head-1])
				// and continuous raw_* ((maxTS-W, maxTS]) accept 100% of events with 0 late drops.
				k := 1 + rng.IntN(tc.buckets-1)
				lagNanos[i] = int64(k) * bktNanos
			}
		}

		// Warm 600s so raw_* ring AND OOO min-heap reach steady-state capacity,
		// preventing slice-grow allocations from polluting b.ReportAllocs().
		warmupEvents := 600 * eps

		for _, kind := range AllKinds() {
			name := fmt.Sprintf("B=%d/ooo=%dpct/engine=%s", tc.buckets, tc.oooPct, kind)
			b.Run(name, func(b *testing.B) {
				w, err := NewWindow(kind, win, tc.bktDur)
				if err != nil {
					b.Fatal(err)
				}
				// Anchor OOO lags strictly against lastInOrderTS (the highest ingested
				// timestamp / headBucket) so 100% of late events land inside closed buckets
				// [headBucket-(B-1), headBucket-1] AND inside continuous (maxTS-W, maxTS],
				// never landing in headBucket or triggering late drops.
				lastInOrderTS := benchBaseNanos
				for i := 0; i < warmupEvents; i++ {
					lag := lagNanos[i&(tableLen-1)]
					var evTS int64
					if i >= 300*eps && lag > 0 {
						evTS = lastInOrderTS - lag
					} else {
						lastInOrderTS += stepNanos
						evTS = lastInOrderTS
					}
					if st := w.Add(evTS, vals[i&(tableLen-1)]); st != AddOK {
						b.Fatalf("warmup OOO Add failed at i=%d: status=%v", i, st)
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := i & (tableLen - 1)
					lag := lagNanos[idx]
					var evTS int64
					if lag > 0 {
						evTS = lastInOrderTS - lag
					} else {
						lastInOrderTS += stepNanos
						evTS = lastInOrderTS
					}
					benchStatusSink = w.Add(evTS, vals[idx])
				}
				b.StopTimer()

				diag := w.Diagnostics()
				if diag.DroppedLate != 0 || diag.DroppedNonFinite != 0 || diag.DroppedOverflow != 0 {
					b.Fatalf("unexpected drops during OOO bench (%s): %+v", kind, diag)
				}
			})
		}
	}
}

func BenchmarkAddSpikeEviction(b *testing.B) {
	const win = 5 * time.Minute // 300s
	const bktDur = 5 * time.Second
	const buckets = 60
	const eps = 10 // 50 events/bucket, 3,000 events/window
	stepNanos := int64(time.Second) / int64(eps)
	spikePeriod := buckets * int(bktDur/time.Second) * eps // 1 spike of 1e12 every 300s (3,000 events)
	vals := makeSteadyValues(1024)

	for _, kind := range AllKinds() {
		name := fmt.Sprintf("B=%d/engine=%s", buckets, kind)
		b.Run(name, func(b *testing.B) {
			w, err := NewWindow(kind, win, bktDur)
			if err != nil {
				b.Fatal(err)
			}
			ts := benchBaseNanos
			for i := 0; i < 2*spikePeriod; i++ {
				v := vals[i&1023]
				if i%spikePeriod == 0 {
					v = 1e12
				}
				if st := w.Add(ts, v); st != AddOK {
					b.Fatalf("warmup spike Add failed: status=%v", st)
				}
				ts += stepNanos
			}

			// Snapshot CancelRefolds AFTER warmup so the post-loop assertion strictly
			// verifies spikes injected and evicted DURING the measured loop.
			beforeCancel := w.Diagnostics().CancelRefolds

			// Use a branch-friendly countdown instead of i%spikePeriod to avoid
			// hardware IDIVQ overhead and divider-unit contention inside the timed loop.
			rem := 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := vals[i&1023]
				if rem == 0 {
					v = 1e12
					rem = spikePeriod
				}
				rem--
				benchStatusSink = w.Add(ts, v)
				ts += stepNanos
			}
			b.StopTimer()

			diag := w.Diagnostics()
			if diag.DroppedLate != 0 || diag.DroppedNonFinite != 0 || diag.DroppedOverflow != 0 {
				b.Fatalf("unexpected drops during spike bench (%s): %+v", kind, diag)
			}
			if (kind == KindAlgebraicChan || kind == KindRawInc) && b.N >= 2*spikePeriod {
				deltaCancel := diag.CancelRefolds - beforeCancel
				wantMin := uint64(b.N / spikePeriod)
				if deltaCancel < wantMin {
					b.Fatalf("%s: expected >= %d measured cancellation refolds on 1e12 spike eviction, got %d (total=%d, before=%d)",
						kind, wantMin, deltaCancel, diag.CancelRefolds, beforeCancel)
				}
			}
		})
	}
}

func BenchmarkAddBipolar(b *testing.B) {
	const win = 5 * time.Minute // 300s
	const bktDur = 5 * time.Second
	const buckets = 60
	const eps = 10 // 50 events/bucket; alternating +1 / -1 per bucket
	stepNanos := int64(time.Second) / int64(eps)
	eventsPerBucket := int(bktDur/time.Second) * eps

	for _, kind := range AllKinds() {
		name := fmt.Sprintf("B=%d/engine=%s", buckets, kind)
		b.Run(name, func(b *testing.B) {
			w, err := NewWindow(kind, win, bktDur)
			if err != nil {
				b.Fatal(err)
			}
			ts := benchBaseNanos
			// Warmup runs 360*eps = 3,600 events = 72 buckets (even), so measured i=0 begins at +1.0.
			for i := 0; i < 360*eps; i++ {
				v := 1.0
				if (i/eventsPerBucket)%2 == 1 {
					v = -1.0
				}
				if st := w.Add(ts, v); st != AddOK {
					b.Fatalf("warmup bipolar Add failed: status=%v", st)
				}
				ts += stepNanos
			}

			// Use a countdown counter instead of (i/eventsPerBucket)%2 to avoid
			// hardware IDIVQ overhead and divider-unit contention inside the timed loop.
			rem := eventsPerBucket
			v := 1.0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if rem == 0 {
					v = -v
					rem = eventsPerBucket
				}
				rem--
				benchStatusSink = w.Add(ts, v)
				ts += stepNanos
			}
			b.StopTimer()

			diag := w.Diagnostics()
			if diag.DroppedLate != 0 || diag.DroppedNonFinite != 0 || diag.DroppedOverflow != 0 {
				b.Fatalf("unexpected drops during bipolar bench (%s): %+v", kind, diag)
			}
			if kind == KindAlgebraicChan && diag.CancelRefolds != 0 {
				b.Fatalf("expected 0 cancellation refolds on bipolar +/-1 stream (anti-thrash), got %d", diag.CancelRefolds)
			}
		})
	}
}

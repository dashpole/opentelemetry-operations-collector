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

// naiveBucket is a 24-byte bucket storing unshifted running totals.
type naiveBucket struct {
	count uint64
	sum   float64
	sumSq float64
}

// algebraicNaive implements Candidate 1: textbook DBSP bucketed ring with
// direct unshifted float64 running subtraction (S_new = S_old - b.sum,
// SumSq_new = SumSq_old - b.sumSq, M2 = SumSq - Sum^2/n) and NO re-fold.
// Intentionally preserves unguarded float64 subtraction to demonstrate
// catastrophic variance cancellation on high-offset series (1e9+eps, 1e12+eps)
// and permanent residual drift after large traffic spikes expire.
type algebraicNaive struct {
	buckets     []naiveBucket
	numBuckets  int
	bucketNanos int64

	headBucket  int64
	lastActTS   int64
	firstSeen   int64
	hasFirst    bool
	initialized bool

	count uint64
	sum   float64
	sumSq float64

	diag Diagnostics
}

func newAlgebraicNaive(numBuckets int, bucketNanos int64) *algebraicNaive {
	return &algebraicNaive{
		buckets:     make([]naiveBucket, numBuckets),
		numBuckets:  numBuckets,
		bucketNanos: bucketNanos,
	}
}

func (w *algebraicNaive) Add(tsNanos int64, val float64) AddStatus {
	if st := validateValue(val); st != AddOK {
		if st == AddDroppedNonFinite {
			w.diag.DroppedNonFinite++
		} else {
			w.diag.DroppedOverflow++
		}
		return st
	}
	bID := floorDiv(tsNanos, w.bucketNanos)
	if !w.initialized {
		w.initialized = true
		w.headBucket = bID
		w.lastActTS = tsNanos
		w.firstSeen = tsNanos
		w.hasFirst = true
		slot := posMod(bID, w.numBuckets)
		w.buckets[slot] = naiveBucket{count: 1, sum: val, sumSq: val * val}
		w.count = 1
		w.sum = val
		w.sumSq = val * val
		return AddOK
	}

	if bID < w.headBucket && distU64(w.headBucket, bID) >= uint64(w.numBuckets) {
		w.diag.DroppedLate++
		return AddDroppedLate
	}
	if tsNanos > w.lastActTS {
		w.lastActTS = tsNanos
	}
	if bID > w.headBucket {
		w.advanceToBucket(bID)
	}
	if !w.hasFirst {
		w.firstSeen = tsNanos
		w.hasFirst = true
	} else if tsNanos < w.firstSeen {
		w.firstSeen = tsNanos
	}

	slot := posMod(bID, w.numBuckets)
	sq := val * val
	w.buckets[slot].count++
	w.buckets[slot].sum += val
	w.buckets[slot].sumSq += sq

	w.count++
	w.sum += val
	w.sumSq += sq
	return AddOK
}

func (w *algebraicNaive) advanceToBucket(targetBucket int64) {
	if targetBucket <= w.headBucket {
		return
	}
	steps := distU64(targetBucket, w.headBucket)
	if steps >= uint64(w.numBuckets) {
		for i := range w.buckets {
			if w.buckets[i].count > 0 {
				w.diag.Evictions++
			}
			w.buckets[i] = naiveBucket{}
		}
		w.count = 0
		w.sum = 0
		w.sumSq = 0
		w.firstSeen = 0
		w.hasFirst = false
		w.headBucket = targetBucket
		return
	}
	headSlot := posMod(w.headBucket, w.numBuckets)
	for s := 1; s <= int(steps); s++ {
		// (oldHead + s - B) mod B == (headSlot + s) mod B, avoiding signed underflow near MinInt64.
		slot := (headSlot + s) % w.numBuckets
		eb := w.buckets[slot]
		if eb.count > 0 {
			w.diag.Evictions++
			w.count -= eb.count
			// Intentionally unguarded naive algebraic subtraction (no zero-reset
			// of sum/sumSq) so residual float64 drift after spike eviction is observable.
			w.sum -= eb.sum
			w.sumSq -= eb.sumSq
		}
		w.buckets[slot] = naiveBucket{}
	}
	w.headBucket = targetBucket
	if w.count == 0 {
		w.firstSeen = 0
		w.hasFirst = false
	}
}

func (w *algebraicNaive) Advance(nowNanos int64) {
	if !w.initialized {
		return
	}
	bID := floorDiv(nowNanos, w.bucketNanos)
	if bID > w.headBucket {
		w.advanceToBucket(bID)
	}
}

func (w *algebraicNaive) Query(nowNanos int64) Snapshot {
	w.Advance(nowNanos)
	return w.Snapshot()
}

func (w *algebraicNaive) Snapshot() Snapshot {
	if !w.initialized {
		return Snapshot{}
	}
	winStart := windowStartBucketNanos(w.headBucket, w.numBuckets, w.bucketNanos)
	if w.count == 0 {
		// Preserve residual sum/sumSq in Sum and M2 when empty to make naive
		// drift directly observable in baseline-vs-change tests.
		return Snapshot{
			Count:            0,
			Sum:              w.sum,
			M2:               w.sumSq,
			WindowStartNanos: winStart,
			FirstSeenNanos:   w.firstSeen,
			HasFirstSeen:     w.hasFirst,
		}
	}
	mean := w.sum / float64(w.count)
	// Textbook unshifted formula: M2 = sum(x^2) - (sum(x))^2 / n
	// (intentionally unclamped so negative catastrophic cancellation is observable
	// via RawVariance while Variance/StdDev clamp negative M2 to 0).
	m2 := w.sumSq - (w.sum*w.sum)/float64(w.count)
	return Snapshot{
		Count:            w.count,
		Sum:              w.sum,
		Mean:             mean,
		M2:               m2,
		WindowStartNanos: winStart,
		FirstSeenNanos:   w.firstSeen,
		HasFirstSeen:     w.hasFirst,
	}
}

func (w *algebraicNaive) Diagnostics() Diagnostics { return w.diag }
func (w *algebraicNaive) LastActivityNanos() int64 { return w.lastActTS }
func (w *algebraicNaive) Initialized() bool        { return w.initialized }

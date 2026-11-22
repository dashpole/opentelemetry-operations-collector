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

import "math"

// chanBucket is an exact 32-byte (half cache line) locally anchored Welford
// bucket. Per-bucket local anchoring eliminates intra-bucket quantization at
// large offsets (1e9, 1e12) via Sterbenz's Lemma while isolating spikes to
// their own bucket.
type chanBucket struct {
	count       uint64
	anchor      float64
	meanShifted float64
	m2          float64
}

func (b *chanBucket) addPoint(val float64) {
	if b.count == 0 {
		b.count = 1
		b.anchor = val
		b.meanShifted = 0
		b.m2 = 0
		return
	}
	b.count++
	xs := val - b.anchor
	d := xs - b.meanShifted
	b.meanShifted += d / float64(b.count)
	b.m2 += d * (xs - b.meanShifted)
}

// combineChan merges two locally anchored buckets using Chan's parallel
// variance formula with safe float64 division ordering (avoiding int64 overflow).
func combineChan(a, b chanBucket) chanBucket {
	if a.count == 0 {
		return b
	}
	if b.count == 0 {
		return a
	}
	nt := a.count + b.count
	bMeanRel := (b.anchor - a.anchor) + b.meanShifted
	delta := bMeanRel - a.meanShifted
	fnB := float64(b.count)
	fnT := float64(nt)
	meanS := a.meanShifted + delta*(fnB/fnT)
	m2 := a.m2 + b.m2 + delta*delta*(float64(a.count)/fnT)*fnB
	return chanBucket{
		count:       nt,
		anchor:      a.anchor,
		meanShifted: meanS,
		m2:          m2,
	}
}

// algebraicChan implements Candidate 2 (Recommended): O(B) sliding-window
// rollup with deferred-head locally anchored Welford buckets (32 B/bucket),
// O(1) evict_then_close Reverse Chan algebraic eviction, high-water-mark
// cancellation guards (peakM2 and scale-aware peakSumMag), and periodic
// drift-cap forward Chan re-fold.
type algebraicChan struct {
	buckets     []chanBucket
	numBuckets  int
	bucketNanos int64

	headBucket  int64
	lastActTS   int64
	firstSeen   int64
	hasFirst    bool
	initialized bool

	// closedAcc stores the exact union of closed buckets [headBucket-B+1, headBucket-1]
	// anchored at closedAcc.anchor.
	closedAcc          chanBucket
	peakM2             float64
	peakSumMag         float64
	evictionsSinceFold uint64
	periodicCap        uint64

	diag Diagnostics
}

func newAlgebraicChan(numBuckets int, bucketNanos int64) *algebraicChan {
	return &algebraicChan{
		buckets:     make([]chanBucket, numBuckets),
		numBuckets:  numBuckets,
		bucketNanos: bucketNanos,
		periodicCap: uint64(10 * numBuckets),
	}
}

func (w *algebraicChan) resetClosed() {
	w.closedAcc = chanBucket{}
	w.peakM2 = 0
	w.peakSumMag = 0
	w.evictionsSinceFold = 0
}

func (w *algebraicChan) updatePeaks() {
	if w.closedAcc.m2 > w.peakM2 {
		w.peakM2 = w.closedAcc.m2
	}
	sm := math.Abs(float64(w.closedAcc.count) * w.closedAcc.meanShifted)
	if sm > w.peakSumMag {
		w.peakSumMag = sm
	}
}

func (w *algebraicChan) Add(tsNanos int64, val float64) AddStatus {
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
		w.buckets[posMod(bID, w.numBuckets)].addPoint(val)
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
	w.buckets[slot].addPoint(val)

	// CRITICAL OOO guard: only update closedAcc when bID is strictly in the
	// closed range [headBucket-B+1, headBucket-1]. Out-of-order events with
	// ts < lastTs that still land inside headBucket MUST update head only to
	// avoid double counting.
	if bID < w.headBucket {
		w.closedAcc.addPoint(val)
		w.updatePeaks()
	}
	return AddOK
}

// evictFromClosed executes Reverse Chan algebraic downdate of bucket eb out of
// w.closedAcc. Returns (needsRefold, cancelTriggered, periodicTriggered).
func (w *algebraicChan) evictFromClosed(eb chanBucket) (bool, bool, bool) {
	if w.closedAcc.count <= eb.count {
		w.resetClosed()
		return false, false, false
	}

	na := w.closedAcc.count - eb.count
	fnB := float64(eb.count)
	fnA := float64(na)

	bMeanRel := (eb.anchor - w.closedAcc.anchor) + eb.meanShifted
	oldMean := w.closedAcc.meanShifted
	newMean := oldMean - (fnB/fnA)*(bMeanRel-oldMean)
	newM2 := w.closedAcc.m2 - eb.m2 - (bMeanRel-oldMean)*(bMeanRel-newMean)*fnB

	w.closedAcc.count = na
	w.closedAcc.meanShifted = newMean
	w.closedAcc.m2 = newM2
	w.evictionsSinceFold++

	if !isFinite(newMean) || !isFinite(newM2) || newM2 < 0 {
		return true, true, false
	}
	if w.peakM2 > 0 && !(newM2 >= refoldRatio*w.peakM2) {
		return true, true, false
	}
	scale := math.Abs(fnA * newMean)
	if disp := math.Sqrt(math.Max(0, fnA*newM2)); disp > scale {
		scale = disp
	}
	if w.peakSumMag > 0 && !(scale >= refoldRatio*w.peakSumMag) {
		return true, true, false
	}
	if w.evictionsSinceFold >= w.periodicCap {
		return true, false, true
	}
	return false, false, false
}

// refoldClosed rebuilds closedAcc by walking nSlots closed buckets backwards
// starting at startSlot (newest closed bucket = oldHead) so the fresh
// seriesAnchor is set to the newest non-empty surviving bucket's anchor.
// Returns true iff at least one non-empty bucket survived (avoiding inflating
// refold diagnostics when a multi-step jump empties the entire window).
func (w *algebraicChan) refoldClosed(startSlot, nSlots int) bool {
	w.resetClosed()
	var acc chanBucket
	slot := startSlot
	for i := 0; i < nSlots; i++ {
		b := w.buckets[slot]
		if b.count > 0 {
			if acc.count == 0 {
				acc = b
			} else {
				acc = combineChan(acc, b)
			}
		}
		slot--
		if slot < 0 {
			slot = w.numBuckets - 1
		}
	}
	if acc.count == 0 {
		return false
	}
	w.closedAcc = acc
	w.peakM2 = acc.m2
	w.peakSumMag = math.Abs(float64(acc.count) * acc.meanShifted)
	w.evictionsSinceFold = 0
	return true
}

func (w *algebraicChan) advanceToBucket(targetBucket int64) {
	if targetBucket <= w.headBucket {
		return
	}
	steps := distU64(targetBucket, w.headBucket)
	if steps >= uint64(w.numBuckets) {
		for i := range w.buckets {
			if w.buckets[i].count > 0 {
				w.diag.Evictions++
			}
			w.buckets[i] = chanBucket{}
		}
		w.resetClosed()
		w.firstSeen = 0
		w.hasFirst = false
		w.headBucket = targetBucket
		return
	}

	stepsInt := int(steps)
	headSlot := posMod(w.headBucket, w.numBuckets)
	needsRefold := false
	cancelTriggered := false
	periodicTriggered := false

	// evict_then_close ordering: evict expired slots first so exact zero-resets
	// trigger before folding oldHead into closedAcc. Once needsRefold trips,
	// short-circuit remaining Reverse Chan calls and still zero expired slots.
	for s := 1; s <= stepsInt; s++ {
		// (oldHead + s - B) mod B == (headSlot + s) mod B, avoiding signed underflow near MinInt64.
		slot := (headSlot + s) % w.numBuckets
		eb := w.buckets[slot]
		if eb.count > 0 {
			w.diag.Evictions++
			if !needsRefold {
				nr, ct, pt := w.evictFromClosed(eb)
				if nr {
					needsRefold = true
					cancelTriggered = ct
					periodicTriggered = pt
				}
			}
		}
		w.buckets[slot] = chanBucket{}
	}

	w.headBucket = targetBucket

	if needsRefold {
		// Surviving range [newOldest, oldHead] has exact length B - stepsInt.
		if w.refoldClosed(headSlot, w.numBuckets-stepsInt) {
			w.diag.Refolds++
			if cancelTriggered {
				w.diag.CancelRefolds++
			} else if periodicTriggered {
				w.diag.PeriodicRefolds++
			}
		}
	} else {
		oldHeadBucket := w.buckets[headSlot]
		if oldHeadBucket.count > 0 {
			w.closedAcc = combineChan(w.closedAcc, oldHeadBucket)
			w.updatePeaks()
		}
	}

	if w.closedAcc.count == 0 {
		w.firstSeen = 0
		w.hasFirst = false
	}
}

func (w *algebraicChan) Advance(nowNanos int64) {
	if !w.initialized {
		return
	}
	bID := floorDiv(nowNanos, w.bucketNanos)
	if bID > w.headBucket {
		w.advanceToBucket(bID)
	}
}

func (w *algebraicChan) Query(nowNanos int64) Snapshot {
	w.Advance(nowNanos)
	return w.Snapshot()
}

func (w *algebraicChan) Snapshot() Snapshot {
	if !w.initialized {
		return Snapshot{}
	}
	winStart := windowStartBucketNanos(w.headBucket, w.numBuckets, w.bucketNanos)
	head := w.buckets[posMod(w.headBucket, w.numBuckets)]
	total := combineChan(w.closedAcc, head)
	if total.count == 0 {
		return Snapshot{
			WindowStartNanos: winStart,
			FirstSeenNanos:   w.firstSeen,
			HasFirstSeen:     w.hasFirst,
		}
	}
	mean := total.anchor + total.meanShifted
	return Snapshot{
		Count:            total.count,
		Sum:              float64(total.count) * mean,
		Mean:             mean,
		M2:               total.m2,
		WindowStartNanos: winStart,
		FirstSeenNanos:   w.firstSeen,
		HasFirstSeen:     w.hasFirst,
	}
}

func (w *algebraicChan) Diagnostics() Diagnostics { return w.diag }
func (w *algebraicChan) LastActivityNanos() int64 { return w.lastActTS }
func (w *algebraicChan) Initialized() bool        { return w.initialized }

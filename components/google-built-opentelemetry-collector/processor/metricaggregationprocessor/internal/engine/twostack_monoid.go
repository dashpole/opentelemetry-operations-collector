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

// monoBucket is a 48-byte locally anchored associative monoid bucket combining
// Welford/Chan statistics with exact min and max.
type monoBucket struct {
	count       uint64
	anchor      float64
	meanShifted float64
	m2          float64
	min         float64
	max         float64
}

func (b *monoBucket) addPoint(val float64) {
	if b.count == 0 {
		b.count = 1
		b.anchor = val
		b.meanShifted = 0
		b.m2 = 0
		b.min = val
		b.max = val
		return
	}
	if val < b.min {
		b.min = val
	}
	if val > b.max {
		b.max = val
	}
	b.count++
	xs := val - b.anchor
	d := xs - b.meanShifted
	b.meanShifted += d / float64(b.count)
	b.m2 += d * (xs - b.meanShifted)
}

func combineMono(a, b monoBucket) monoBucket {
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
	minV := a.min
	if b.min < minV {
		minV = b.min
	}
	maxV := a.max
	if b.max > maxV {
		maxV = b.max
	}
	return monoBucket{
		count:       nt,
		anchor:      a.anchor,
		meanShifted: meanS,
		m2:          m2,
		min:         minV,
		max:         maxV,
	}
}

// twoStackMonoid implements Candidate 3: zero-subtraction Two-Stack (DABA /
// Tangwongsan et al., VLDB 2015) sliding-window monoid queue over B locally
// anchored buckets (96 B/bucket across buckets + frontAgg).
//
// Invariant for closed buckets [oldestID, headBucket-1] where oldestID = headBucket-B+1:
//   - splitID satisfies oldestID <= splitID <= headBucket.
//   - Front stack covers [oldestID, splitID-1]: for each id in that range,
//     frontAgg[posMod(id, B)] == combine(buckets[id], ..., buckets[splitID-1]).
//   - Back stack covers [splitID, headBucket-1]: backAgg == combine(buckets[splitID..headBucket-1]).
//   - Head bucket covers headBucket: buckets[posMod(headBucket, B)].
type twoStackMonoid struct {
	buckets     []monoBucket
	frontAgg    []monoBucket
	numBuckets  int
	bucketNanos int64

	headBucket  int64
	splitID     int64
	backAgg     monoBucket
	lastActTS   int64
	firstSeen   int64
	hasFirst    bool
	initialized bool

	diag Diagnostics
}

func newTwoStackMonoid(numBuckets int, bucketNanos int64) *twoStackMonoid {
	return &twoStackMonoid{
		buckets:     make([]monoBucket, numBuckets),
		frontAgg:    make([]monoBucket, numBuckets),
		numBuckets:  numBuckets,
		bucketNanos: bucketNanos,
	}
}

func (w *twoStackMonoid) Add(tsNanos int64, val float64) AddStatus {
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
		w.splitID = bID
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

	if bID == w.headBucket {
		return AddOK
	}
	if bID >= w.splitID {
		// Back-stack OOO hit: associative & commutative O(1) update into backAgg.
		w.backAgg.addPoint(val)
		w.diag.BackOOOHits++
		return AddOK
	}

	// Front-stack OOO hit: rebuild suffix aggregates [oldestID..bID] downwards
	// in O(B - k) where k = headBucket - bID (walking by count avoids MinInt64 wrap).
	w.diag.FrontOOORebuilds++
	var acc monoBucket
	if distU64(w.splitID, bID) > 1 {
		acc = w.frontAgg[(slot+1)%w.numBuckets]
	}
	nSlots := w.numBuckets - int(distU64(w.headBucket, bID))
	cur := slot
	for i := 0; i < nSlots; i++ {
		b := w.buckets[cur]
		if b.count > 0 {
			acc = combineMono(acc, b)
		}
		w.frontAgg[cur] = acc
		cur--
		if cur < 0 {
			cur = w.numBuckets - 1
		}
	}
	return AddOK
}

func (w *twoStackMonoid) advanceToBucket(targetBucket int64) {
	if targetBucket <= w.headBucket {
		return
	}
	steps := distU64(targetBucket, w.headBucket)
	if steps >= uint64(w.numBuckets) {
		for i := range w.buckets {
			if w.buckets[i].count > 0 {
				w.diag.Evictions++
			}
			w.buckets[i] = monoBucket{}
			w.frontAgg[i] = monoBucket{}
		}
		w.backAgg = monoBucket{}
		w.firstSeen = 0
		w.hasFirst = false
		w.headBucket = targetBucket
		w.splitID = targetBucket
		return
	}

	stepsInt := int(steps)
	headSlot := posMod(w.headBucket, w.numBuckets)
	for s := 1; s <= stepsInt; s++ {
		// (oldHead + s - B) mod B == (headSlot + s) mod B, avoiding signed underflow near MinInt64.
		slot := (headSlot + s) % w.numBuckets
		if w.buckets[slot].count > 0 {
			w.diag.Evictions++
		}
		w.buckets[slot] = monoBucket{}
		w.frontAgg[slot] = monoBucket{}
	}

	w.headBucket = targetBucket

	// newOldest >= splitID <=> targetBucket - splitID >= B - 1 (unsigned safe).
	if targetBucket > w.splitID && distU64(targetBucket, w.splitID) >= uint64(w.numBuckets-1) {
		// Front stack exhausted (or crossed into back stack): flip back stack
		// into front stack from targetBucket-1 backwards for B-1 slots.
		w.splitID = targetBucket
		w.backAgg = monoBucket{}
		w.diag.StackFlips++
		var acc monoBucket
		cur := (posMod(targetBucket, w.numBuckets) + w.numBuckets - 1) % w.numBuckets
		for i := 0; i < w.numBuckets-1; i++ {
			b := w.buckets[cur]
			if b.count > 0 {
				acc = combineMono(acc, b)
			}
			w.frontAgg[cur] = acc
			cur--
			if cur < 0 {
				cur = w.numBuckets - 1
			}
		}
	} else {
		// Front stack still non-empty; push oldHead onto back stack in O(1).
		oldHeadBucket := w.buckets[headSlot]
		if oldHeadBucket.count > 0 {
			w.backAgg = combineMono(w.backAgg, oldHeadBucket)
		}
	}

	if w.closedCount() == 0 {
		w.firstSeen = 0
		w.hasFirst = false
	}
}

func (w *twoStackMonoid) hasFront() bool {
	// oldestID < splitID <=> headBucket - splitID < B - 1 (always splitID <= headBucket).
	return w.numBuckets > 1 && distU64(w.headBucket, w.splitID) < uint64(w.numBuckets-1)
}

func (w *twoStackMonoid) oldestSlot() int {
	// (headBucket - B + 1) mod B == (posMod(headBucket, B) + 1) mod B.
	return (posMod(w.headBucket, w.numBuckets) + 1) % w.numBuckets
}

func (w *twoStackMonoid) closedCount() uint64 {
	var c uint64
	if w.hasFront() {
		c += w.frontAgg[w.oldestSlot()].count
	}
	if w.splitID < w.headBucket {
		c += w.backAgg.count
	}
	return c
}

func (w *twoStackMonoid) Advance(nowNanos int64) {
	if !w.initialized {
		return
	}
	bID := floorDiv(nowNanos, w.bucketNanos)
	if bID > w.headBucket {
		w.advanceToBucket(bID)
	}
}

func (w *twoStackMonoid) Query(nowNanos int64) Snapshot {
	w.Advance(nowNanos)
	return w.Snapshot()
}

func (w *twoStackMonoid) Snapshot() Snapshot {
	if !w.initialized {
		return Snapshot{}
	}
	winStart := windowStartBucketNanos(w.headBucket, w.numBuckets, w.bucketNanos)

	var total monoBucket
	if w.hasFront() {
		total = w.frontAgg[w.oldestSlot()]
	}
	if w.splitID < w.headBucket {
		total = combineMono(total, w.backAgg)
	}
	head := w.buckets[posMod(w.headBucket, w.numBuckets)]
	total = combineMono(total, head)

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
		HasMinMax:        true,
		Min:              total.min,
		Max:              total.max,
		WindowStartNanos: winStart,
		FirstSeenNanos:   w.firstSeen,
		HasFirstSeen:     w.hasFirst,
	}
}

func (w *twoStackMonoid) Diagnostics() Diagnostics { return w.diag }
func (w *twoStackMonoid) LastActivityNanos() int64 { return w.lastActTS }
func (w *twoStackMonoid) Initialized() bool        { return w.initialized }

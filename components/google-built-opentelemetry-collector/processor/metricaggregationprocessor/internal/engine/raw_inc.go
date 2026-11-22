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

// rawInc implements Baseline B: exact continuous sliding window (now-W, now]
// backed by a raw ring deque + OOO min-heap, maintaining an O(1) incremental
// seriesAnchor-shifted single-point Reverse Chan accumulator with
// high-water-mark guards and periodic re-fold. This isolates O(N*W) memory
// scaling from O(N*W) query rescan CPU cost while keeping full numerical parity.
type rawInc struct {
	rawBuffer

	count              uint64
	anchor             float64
	meanShifted        float64
	m2                 float64
	peakM2             float64
	peakSumMag         float64
	evictionsSinceFold uint64
}

func newRawInc(windowNanos int64) *rawInc {
	return &rawInc{
		rawBuffer: rawBuffer{
			windowNanos: windowNanos,
		},
	}
}

func (w *rawInc) resetAcc() {
	w.count = 0
	w.anchor = 0
	w.meanShifted = 0
	w.m2 = 0
	w.peakM2 = 0
	w.peakSumMag = 0
	w.evictionsSinceFold = 0
}

func (w *rawInc) updatePeaks() {
	if w.m2 > w.peakM2 {
		w.peakM2 = w.m2
	}
	sm := math.Abs(float64(w.count) * w.meanShifted)
	if sm > w.peakSumMag {
		w.peakSumMag = sm
	}
}

func (w *rawInc) addPoint(val float64) {
	if w.count == 0 {
		w.count = 1
		w.anchor = val
		w.meanShifted = 0
		w.m2 = 0
		return
	}
	w.count++
	xs := val - w.anchor
	d := xs - w.meanShifted
	w.meanShifted += d / float64(w.count)
	w.m2 += d * (xs - w.meanShifted)
	w.updatePeaks()
}

func (w *rawInc) refold() {
	w.resetAcc()
	newest, ok := w.newestSample()
	if !ok {
		return
	}
	w.anchor = newest.val
	for i := 0; i < w.ring.size; i++ {
		w.addRawUnchecked(w.ring.at(i).val)
	}
	for i := range w.ooo {
		w.addRawUnchecked(w.ooo[i].val)
	}
	w.peakM2 = w.m2
	w.peakSumMag = math.Abs(float64(w.count) * w.meanShifted)
	w.evictionsSinceFold = 0
	w.diag.Refolds++
}

func (w *rawInc) addRawUnchecked(val float64) {
	w.count++
	xs := val - w.anchor
	d := xs - w.meanShifted
	w.meanShifted += d / float64(w.count)
	w.m2 += d * (xs - w.meanShifted)
}

func (w *rawInc) evictExpired(maxTS int64) {
	needsRefold := false
	cancelTriggered := false
	periodicTriggered := false

	w.evictExpiredBefore(maxTS, func(s rawSample) {
		if needsRefold {
			return
		}
		if w.count <= 1 {
			w.resetAcc()
			return
		}
		na := w.count - 1
		xs := s.val - w.anchor
		oldMean := w.meanShifted
		newMean := oldMean - (xs-oldMean)/float64(na)
		newM2 := w.m2 - (xs-oldMean)*(xs-newMean)

		w.count = na
		w.meanShifted = newMean
		w.m2 = newM2
		w.evictionsSinceFold++

		if !isFinite(newMean) || !isFinite(newM2) || newM2 < 0 {
			needsRefold = true
			cancelTriggered = true
			return
		}
		if w.peakM2 > 0 && !(newM2 >= refoldRatio*w.peakM2) {
			needsRefold = true
			cancelTriggered = true
			return
		}
		scale := math.Abs(float64(na) * newMean)
		if disp := math.Sqrt(math.Max(0, float64(na)*newM2)); disp > scale {
			scale = disp
		}
		if w.peakSumMag > 0 && !(scale >= refoldRatio*w.peakSumMag) {
			needsRefold = true
			cancelTriggered = true
			return
		}
		if w.evictionsSinceFold >= rawPeriodicRefoldInterval {
			needsRefold = true
			periodicTriggered = true
		}
	})

	if needsRefold {
		if w.totalLen() == 0 {
			w.resetAcc()
			return
		}
		if cancelTriggered {
			w.diag.CancelRefolds++
		} else if periodicTriggered {
			w.diag.PeriodicRefolds++
		}
		w.refold()
	}
}

func (w *rawInc) Add(tsNanos int64, val float64) AddStatus {
	if st := validateValue(val); st != AddOK {
		if st == AddDroppedNonFinite {
			w.diag.DroppedNonFinite++
		} else {
			w.diag.DroppedOverflow++
		}
		return st
	}
	if !w.initialized {
		w.initialized = true
		w.maxTS = tsNanos
		w.lastActTS = tsNanos
		w.pushSample(tsNanos, val)
		w.addPoint(val)
		return AddOK
	}
	if tsNanos < w.maxTS && distU64(w.maxTS, tsNanos) >= uint64(w.windowNanos) {
		w.diag.DroppedLate++
		return AddDroppedLate
	}
	if tsNanos > w.lastActTS {
		w.lastActTS = tsNanos
	}
	// Evict BEFORE pushing so (a) exact zero-resets trigger cleanly, (b) refolds
	// never double-count val, and (c) ring capacity does not prematurely double.
	if tsNanos > w.maxTS {
		w.Advance(tsNanos)
	}
	w.pushSample(tsNanos, val)
	w.addPoint(val)
	return AddOK
}

func (w *rawInc) Advance(nowNanos int64) {
	if !w.initialized || nowNanos <= w.maxTS {
		return
	}
	if distU64(nowNanos, w.maxTS) >= uint64(w.windowNanos) && w.totalLen() > 0 {
		w.diag.Evictions += uint64(w.totalLen())
		w.ring.clear()
		w.ooo.clear()
		w.firstSeen = 0
		w.hasFirst = false
		w.resetAcc()
		w.maxTS = nowNanos
		return
	}
	w.maxTS = nowNanos
	w.evictExpired(w.maxTS)
	if w.totalLen() == 0 {
		w.firstSeen = 0
		w.hasFirst = false
		w.resetAcc()
	}
}

func (w *rawInc) Query(nowNanos int64) Snapshot {
	w.Advance(nowNanos)
	return w.Snapshot()
}

func (w *rawInc) Snapshot() Snapshot {
	if !w.initialized {
		return Snapshot{}
	}
	if w.count == 0 {
		return Snapshot{
			WindowStartNanos: subSaturate(w.maxTS, w.windowNanos),
			FirstSeenNanos:   w.firstSeen,
			HasFirstSeen:     w.hasFirst,
		}
	}
	mean := w.anchor + w.meanShifted
	return Snapshot{
		Count:            w.count,
		Sum:              float64(w.count) * mean,
		Mean:             mean,
		M2:               w.m2,
		WindowStartNanos: subSaturate(w.maxTS, w.windowNanos),
		FirstSeenNanos:   w.firstSeen,
		HasFirstSeen:     w.hasFirst,
	}
}

func (w *rawInc) Diagnostics() Diagnostics { return w.diag }
func (w *rawInc) LastActivityNanos() int64 { return w.lastActTS }
func (w *rawInc) Initialized() bool        { return w.initialized }

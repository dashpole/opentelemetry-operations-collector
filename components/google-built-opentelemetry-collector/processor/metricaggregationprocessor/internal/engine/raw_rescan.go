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

// rawRescan implements Baseline A: exact continuous sliding window (now-W, now]
// backed by a raw ring deque + OOO min-heap, rescanning with anchored Welford
// on every Query/Snapshot (exact ~1e-16 precision even at 1e12 offset).
type rawRescan struct {
	rawBuffer
}

func newRawRescan(windowNanos int64) *rawRescan {
	return &rawRescan{
		rawBuffer: rawBuffer{
			windowNanos: windowNanos,
		},
	}
}

func (w *rawRescan) Add(tsNanos int64, val float64) AddStatus {
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
		return AddOK
	}
	if tsNanos < w.maxTS && distU64(w.maxTS, tsNanos) >= uint64(w.windowNanos) {
		w.diag.DroppedLate++
		return AddDroppedLate
	}
	if tsNanos > w.lastActTS {
		w.lastActTS = tsNanos
	}
	if tsNanos > w.maxTS {
		w.Advance(tsNanos)
	}
	w.pushSample(tsNanos, val)
	return AddOK
}

func (w *rawRescan) Advance(nowNanos int64) {
	if !w.initialized || nowNanos <= w.maxTS {
		return
	}
	if distU64(nowNanos, w.maxTS) >= uint64(w.windowNanos) && w.totalLen() > 0 {
		w.diag.Evictions += uint64(w.totalLen())
		w.ring.clear()
		w.ooo.clear()
		w.firstSeen = 0
		w.hasFirst = false
		w.maxTS = nowNanos
		return
	}
	w.maxTS = nowNanos
	w.evictExpiredBefore(w.maxTS, nil)
	if w.totalLen() == 0 {
		w.firstSeen = 0
		w.hasFirst = false
	}
}

func (w *rawRescan) Query(nowNanos int64) Snapshot {
	w.Advance(nowNanos)
	return w.Snapshot()
}

func (w *rawRescan) Snapshot() Snapshot {
	if !w.initialized {
		return Snapshot{}
	}
	total := w.totalLen()
	if total == 0 {
		return Snapshot{
			WindowStartNanos: subSaturate(w.maxTS, w.windowNanos),
			FirstSeenNanos:   w.firstSeen,
			HasFirstSeen:     w.hasFirst,
		}
	}

	var anchor float64
	if w.ring.size > 0 {
		anchor = w.ring.back().val
	} else {
		anchor = w.ooo[0].val
	}

	var count uint64
	var meanS, m2 float64
	minV := anchor
	maxV := anchor

	for i := 0; i < w.ring.size; i++ {
		x := w.ring.at(i).val
		if x < minV {
			minV = x
		}
		if x > maxV {
			maxV = x
		}
		count++
		xs := x - anchor
		d := xs - meanS
		meanS += d / float64(count)
		m2 += d * (xs - meanS)
	}
	for i := range w.ooo {
		x := w.ooo[i].val
		if x < minV {
			minV = x
		}
		if x > maxV {
			maxV = x
		}
		count++
		xs := x - anchor
		d := xs - meanS
		meanS += d / float64(count)
		m2 += d * (xs - meanS)
	}

	mean := anchor + meanS
	return Snapshot{
		Count:            count,
		Sum:              float64(count) * mean,
		Mean:             mean,
		M2:               m2,
		HasMinMax:        true,
		Min:              minV,
		Max:              maxV,
		WindowStartNanos: subSaturate(w.maxTS, w.windowNanos),
		FirstSeenNanos:   w.firstSeen,
		HasFirstSeen:     w.hasFirst,
	}
}

func (w *rawRescan) Diagnostics() Diagnostics { return w.diag }
func (w *rawRescan) LastActivityNanos() int64 { return w.lastActTS }
func (w *rawRescan) Initialized() bool        { return w.initialized }

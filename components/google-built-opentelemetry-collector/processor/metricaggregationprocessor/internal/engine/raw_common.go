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

// rawSample is a compact 16-byte raw datapoint record.
type rawSample struct {
	ts  int64
	val float64
}

// rawRing is a power-of-2 circular deque storing monotonic in-order samples.
// Power-of-2 capacity enables fast bitmask indexing without modulo division.
type rawRing struct {
	buf  []rawSample
	head int
	size int
}

func (r *rawRing) pushBack(s rawSample) {
	if r.size == len(r.buf) {
		r.grow()
	}
	idx := (r.head + r.size) & (len(r.buf) - 1)
	r.buf[idx] = s
	r.size++
}

func (r *rawRing) grow() {
	newCap := len(r.buf) * 2
	if newCap == 0 {
		newCap = 16
	}
	nb := make([]rawSample, newCap)
	if r.size > 0 {
		first := len(r.buf) - r.head
		if first >= r.size {
			copy(nb, r.buf[r.head:r.head+r.size])
		} else {
			copy(nb, r.buf[r.head:])
			copy(nb[first:], r.buf[:r.size-first])
		}
	}
	r.buf = nb
	r.head = 0
}

func (r *rawRing) front() rawSample {
	return r.buf[r.head]
}

func (r *rawRing) back() rawSample {
	return r.buf[(r.head+r.size-1)&(len(r.buf)-1)]
}

func (r *rawRing) popFront() rawSample {
	s := r.buf[r.head]
	r.head = (r.head + 1) & (len(r.buf) - 1)
	r.size--
	return s
}

func (r *rawRing) at(i int) rawSample {
	return r.buf[(r.head+i)&(len(r.buf)-1)]
}

func (r *rawRing) clear() {
	r.head = 0
	r.size = 0
}

// sampleMinHeap is a zero-interface binary min-heap ordered by sample.ts so
// out-of-order late events support O(log(N*W)) insertion and eviction without
// container/heap boxing allocations.
type sampleMinHeap []rawSample

func (h *sampleMinHeap) push(s rawSample) {
	*h = append(*h, s)
	i := len(*h) - 1
	for i > 0 {
		p := (i - 1) >> 1
		if (*h)[p].ts <= (*h)[i].ts {
			break
		}
		(*h)[p], (*h)[i] = (*h)[i], (*h)[p]
		i = p
	}
}

func (h *sampleMinHeap) pop() rawSample {
	old := *h
	min := old[0]
	n := len(old) - 1
	old[0] = old[n]
	old = old[:n]
	*h = old
	i := 0
	for {
		l := (i << 1) + 1
		if l >= n {
			break
		}
		smallest := l
		if r := l + 1; r < n && old[r].ts < old[l].ts {
			smallest = r
		}
		if old[i].ts <= old[smallest].ts {
			break
		}
		old[i], old[smallest] = old[smallest], old[i]
		i = smallest
	}
	return min
}

func (h *sampleMinHeap) clear() {
	*h = (*h)[:0]
}

// rawBuffer combines a monotonic circular ring (O(1) amortized in-order) with
// a binary min-heap for out-of-order late samples (O(log(N*W))).
type rawBuffer struct {
	windowNanos int64
	ring        rawRing
	ooo         sampleMinHeap
	maxTS       int64
	lastActTS   int64
	firstSeen   int64
	hasFirst    bool
	initialized bool
	diag        Diagnostics
}

func (b *rawBuffer) totalLen() int {
	return b.ring.size + len(b.ooo)
}

func (b *rawBuffer) pushSample(ts int64, val float64) {
	if !b.hasFirst {
		b.firstSeen = ts
		b.hasFirst = true
	} else if ts < b.firstSeen {
		b.firstSeen = ts
	}
	s := rawSample{ts: ts, val: val}
	if b.ring.size == 0 || ts >= b.ring.back().ts {
		b.ring.pushBack(s)
	} else {
		b.ooo.push(s)
	}
}

// evictExpiredBefore removes every sample with maxTS - sample.ts >= windowNanos
// using unsigned distance arithmetic so signed int64 timestamps near MinInt64 or
// MaxInt64 never wrap.
func (b *rawBuffer) evictExpiredBefore(maxTS int64, onEvict func(rawSample)) {
	w := uint64(b.windowNanos)
	for b.ring.size > 0 {
		f := b.ring.front()
		if f.ts >= maxTS || distU64(maxTS, f.ts) < w {
			break
		}
		s := b.ring.popFront()
		b.diag.Evictions++
		if onEvict != nil {
			onEvict(s)
		}
	}
	for len(b.ooo) > 0 {
		f := b.ooo[0]
		if f.ts >= maxTS || distU64(maxTS, f.ts) < w {
			break
		}
		s := b.ooo.pop()
		b.diag.Evictions++
		if onEvict != nil {
			onEvict(s)
		}
	}
}

func (b *rawBuffer) newestSample() (rawSample, bool) {
	hasRing := b.ring.size > 0
	hasOOO := len(b.ooo) > 0
	if !hasRing && !hasOOO {
		return rawSample{}, false
	}
	var best rawSample
	if hasRing {
		best = b.ring.back()
	}
	if hasOOO {
		if !hasRing {
			best = b.ooo[0]
		}
		for i := range b.ooo {
			if b.ooo[i].ts > best.ts {
				best = b.ooo[i]
			}
		}
	}
	return best, true
}

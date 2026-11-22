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

// Package engine implements and compares five streaming sliding-window metric
// aggregation engines behind a shared interface.
package engine

import (
	"fmt"
	"math"
	"time"
)

// Kind identifies a sliding-window aggregation engine implementation.
type Kind string

const (
	// KindRawRescan is Baseline A: raw event ring buffer + OOO min-heap with
	// anchored Welford rescan on query (exact continuous window (now-W, now]).
	KindRawRescan Kind = "raw_rescan"

	// KindRawInc is Baseline B: raw event ring buffer + OOO min-heap with
	// anchored single-point Reverse Chan incremental maintenance.
	KindRawInc Kind = "raw_inc"

	// KindAlgebraicNaive is Candidate 1: bucketed ring with direct unshifted
	// float64 Sum / SumSq algebraic subtraction (no re-fold).
	KindAlgebraicNaive Kind = "algebraic_naive"

	// KindAlgebraicChan is Candidate 2 (Recommended): bucketed locally anchored
	// Welford + deferred-head Reverse Chan O(1) eviction + high-water-mark and
	// periodic forward Chan re-fold.
	KindAlgebraicChan Kind = "algebraic_chan"

	// KindTwoStackMonoid is Candidate 3: bucketed locally anchored Welford +
	// zero-subtraction Two-Stack (DABA) associative monoid queue supporting min/max.
	KindTwoStackMonoid Kind = "twostack_monoid"
)

// AllKinds returns every supported engine kind in canonical order.
func AllKinds() []Kind {
	return []Kind{
		KindRawRescan,
		KindRawInc,
		KindAlgebraicNaive,
		KindAlgebraicChan,
		KindTwoStackMonoid,
	}
}

// SupportsMinMax reports whether the engine tracks exact min and max values.
func (k Kind) SupportsMinMax() bool {
	return k == KindRawRescan || k == KindTwoStackMonoid
}

// Valid reports whether k is a known engine kind.
func (k Kind) Valid() bool {
	switch k {
	case KindRawRescan, KindRawInc, KindAlgebraicNaive, KindAlgebraicChan, KindTwoStackMonoid:
		return true
	default:
		return false
	}
}

// MaxAbsValue is the maximum accepted finite input magnitude. Rejecting finite
// values above 1e140 guarantees (2 * MaxAbsValue)^2 * 1e9 = 4e289 << math.MaxFloat64
// so squared deviations cannot overflow to +Inf and create Inf-Inf = NaN.
const MaxAbsValue = 1e140

// refoldRatio is the high-water-mark precision retention floor (1e-3 retains at
// least ~43 of 53 mantissa bits before triggering an O(B) forward Chan re-fold).
const refoldRatio = 1e-3

// rawPeriodicRefoldInterval caps cumulative floating-point drift on raw_inc.
const rawPeriodicRefoldInterval = 10000

// VarianceMode selects sample (n-1) vs population (n) variance denominator.
type VarianceMode string

const (
	VarianceSample     VarianceMode = "sample"
	VariancePopulation VarianceMode = "population"
)

// AddStatus reports the disposition of an Add call.
type AddStatus uint8

const (
	AddOK AddStatus = iota
	AddDroppedNonFinite
	AddDroppedOverflow
	AddDroppedLate
)

// Snapshot contains the aggregated window statistics at a query timestamp.
type Snapshot struct {
	Count     uint64
	Sum       float64
	Mean      float64
	M2        float64
	HasMinMax bool
	Min       float64
	Max       float64
	// WindowStartNanos is the inclusive/aligned start timestamp of the active window.
	WindowStartNanos int64
	// FirstSeenNanos is the timestamp of the first accepted event since last full clear.
	FirstSeenNanos int64
	HasFirstSeen   bool
}

// Variance computes sample or population variance from M2 and Count.
// For sample variance with Count < 2 (or empty windows), returns 0.
func (s Snapshot) Variance(mode VarianceMode) float64 {
	if s.Count == 0 {
		return 0
	}
	if mode == VariancePopulation {
		if s.M2 <= 0 {
			return 0
		}
		return s.M2 / float64(s.Count)
	}
	if s.Count < 2 || s.M2 <= 0 {
		return 0
	}
	return s.M2 / float64(s.Count-1)
}

// StdDev returns the standard deviation for the requested variance mode.
func (s Snapshot) StdDev(mode VarianceMode) float64 {
	v := s.Variance(mode)
	if v <= 0 {
		return 0
	}
	return math.Sqrt(v)
}

// RawVariance returns the unclamped M2/denom value (preserving negative values
// emitted by unguarded algebraic_naive to allow non-tautological cancellation tests).
func (s Snapshot) RawVariance(mode VarianceMode) float64 {
	if s.Count == 0 {
		return 0
	}
	if mode == VariancePopulation {
		return s.M2 / float64(s.Count)
	}
	if s.Count < 2 {
		return 0
	}
	return s.M2 / float64(s.Count-1)
}

// Diagnostics exposes internal engine counters for benchmarking and drift verification.
type Diagnostics struct {
	DroppedLate      uint64
	DroppedNonFinite uint64
	DroppedOverflow  uint64
	// Evictions counts evicted individual datapoints on raw_rescan/raw_inc,
	// and evicted non-empty buckets on bucketed engines (algebraic_naive,
	// algebraic_chan, twostack_monoid). Do not compare Refolds/Evictions
	// directly across raw vs bucketed engines without normalizing units.
	Evictions        uint64
	Refolds          uint64
	CancelRefolds    uint64
	PeriodicRefolds  uint64
	StackFlips       uint64
	FrontOOORebuilds uint64
	BackOOOHits      uint64
}

// Window maintains sliding-window statistics for a single metric time series.
// Implementations are NOT concurrency-safe; callers synchronize per shard.
type Window interface {
	// Add ingests a datapoint (tsNanos, value) into the sliding window.
	Add(tsNanos int64, val float64) AddStatus
	// Advance moves the window clock forward to nowNanos (monotonic; no-op if
	// nowNanos is behind the current watermark) and evicts expired state.
	Advance(nowNanos int64)
	// Query advances to nowNanos and returns the current window snapshot.
	Query(nowNanos int64) Snapshot
	// Snapshot returns the current snapshot without advancing the watermark.
	Snapshot() Snapshot
	// Diagnostics returns internal drop/eviction/refold counters.
	Diagnostics() Diagnostics
	// LastActivityNanos returns the highest ingested event timestamp (0 if never initialized).
	LastActivityNanos() int64
	// Initialized reports whether at least one valid datapoint has been accepted.
	Initialized() bool
}

// NewWindow constructs a Window of the requested engine kind for windowDur and bucketDur.
func NewWindow(kind Kind, windowDur, bucketDur time.Duration) (Window, error) {
	if windowDur <= 0 {
		return nil, fmt.Errorf("window duration must be > 0, got %v", windowDur)
	}
	if bucketDur <= 0 {
		return nil, fmt.Errorf("bucket duration must be > 0, got %v", bucketDur)
	}
	if windowDur%bucketDur != 0 {
		return nil, fmt.Errorf("window duration %v must be an exact multiple of bucket duration %v", windowDur, bucketDur)
	}
	buckets := int(windowDur / bucketDur)
	if buckets < 1 {
		return nil, fmt.Errorf("bucket count must be >= 1, got %d", buckets)
	}

	windowNanos := windowDur.Nanoseconds()
	bucketNanos := bucketDur.Nanoseconds()

	switch kind {
	case KindRawRescan:
		return newRawRescan(windowNanos), nil
	case KindRawInc:
		return newRawInc(windowNanos), nil
	case KindAlgebraicNaive:
		return newAlgebraicNaive(buckets, bucketNanos), nil
	case KindAlgebraicChan:
		return newAlgebraicChan(buckets, bucketNanos), nil
	case KindTwoStackMonoid:
		return newTwoStackMonoid(buckets, bucketNanos), nil
	default:
		return nil, fmt.Errorf("unknown engine kind %q", kind)
	}
}

func validateValue(val float64) AddStatus {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return AddDroppedNonFinite
	}
	if math.Abs(val) > MaxAbsValue {
		return AddDroppedOverflow
	}
	return AddOK
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// floorDiv computes Euclidean floor(a / b) for positive b, handling negative a.
func floorDiv(a, b int64) int64 {
	q := a / b
	r := a % b
	if r < 0 {
		q--
	}
	return q
}

// posMod returns a mod m in [0, m) for positive m, handling negative a.
func posMod(a int64, m int) int {
	r := int(a % int64(m))
	if r < 0 {
		r += m
	}
	return r
}

// distU64 computes uint64(hi) - uint64(lo) for hi >= lo without signed int64 overflow.
func distU64(hi, lo int64) uint64 {
	return uint64(hi) - uint64(lo)
}

// subSaturate computes a - d (for d >= 0), saturating at math.MinInt64 on underflow.
func subSaturate(a, d int64) int64 {
	if d <= 0 {
		return a
	}
	if a < math.MinInt64+d {
		return math.MinInt64
	}
	return a - d
}

// windowStartBucketNanos computes (headBucket - (numBuckets-1)) * bucketNanos,
// saturating at math.MinInt64 on underflow.
func windowStartBucketNanos(headBucket int64, numBuckets int, bucketNanos int64) int64 {
	oldest := subSaturate(headBucket, int64(numBuckets-1))
	if oldest < 0 && oldest < math.MinInt64/bucketNanos {
		return math.MinInt64
	}
	if oldest > 0 && oldest > math.MaxInt64/bucketNanos {
		return math.MaxInt64
	}
	return oldest * bucketNanos
}

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

package metricaggregationprocessor

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
)

const (
	numShards                = 64
	defaultLazySweepCooldown = time.Second
	minReaperInterval        = time.Second
	maxReaperInterval        = time.Minute
)

var _ processor.Metrics = (*metricAggregationProcessor)(nil)

// Stats captures processor-wide observability counters for verification and telemetry.
// Refolds, CancelRefolds, and PeriodicRefolds are strictly cumulative/monotonic
// across idle series evictions.
type Stats struct {
	ActiveSeries            int64
	DroppedCardinality      uint64
	DroppedNonFinite        uint64
	DroppedOverflow         uint64
	DroppedLate             uint64
	DroppedNoRecordedValue  uint64
	DroppedMissingTimestamp uint64
	DroppedFutureSkew       uint64
	IdleEvicted             uint64
	Refolds                 uint64
	CancelRefolds           uint64
	PeriodicRefolds         uint64
}

type seriesMetadata struct {
	resKey         string
	scopeKey       string
	resource       pcommon.Resource
	resourceSchema string
	scope          pcommon.InstrumentationScope
	scopeSchema    string
	metricName     string
	metricUnit     string
	isDeltaSum     bool
	dataPointAttrs pcommon.Map
}

type seriesEntry struct {
	key              string
	win              engine.Window
	meta             *seriesMetadata
	lastArrivalNanos int64
}

// seriesShard is explicitly padded to 64 bytes (one x86-64/ARM64 cache line)
// to prevent false sharing between adjacent shards under concurrent ingestion.
// Layout: mu (8B) + series map (8B) + 3 archived refold counters (24B) + lastSweepNanos (8B) + pad (16B) = 64B.
type seriesShard struct {
	mu                     sync.Mutex
	series                 map[string]*seriesEntry
	evictedRefolds         uint64
	evictedCancelRefolds   uint64
	evictedPeriodicRefolds uint64
	lastSweepNanos         int64
	_                      [16]byte
}

type emittedSample struct {
	meta   *seriesMetadata
	snap   engine.Snapshot
	emitTS int64
}

type metricAggregationProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics

	metricAllowlist map[string]struct{}
	emitsGauge      bool
	bucketNanos     int64
	clock           func() time.Time

	lazySweepCooldown time.Duration
	reaperInterval    time.Duration
	lastLazySweep     atomic.Int64

	shards [numShards]seriesShard

	activeSeries            atomic.Int64
	droppedCardinality      atomic.Uint64
	droppedNonFinite        atomic.Uint64
	droppedOverflow         atomic.Uint64
	droppedLate             atomic.Uint64
	droppedNoRecordedValue  atomic.Uint64
	droppedMissingTimestamp atomic.Uint64
	droppedFutureSkew       atomic.Uint64
	idleEvicted             atomic.Uint64

	lifecycleMu sync.Mutex
	started     bool
	cancel      context.CancelFunc
	done        chan struct{}
}

func newMetricAggregationProcessor(cfg *Config, logger *zap.Logger, nextConsumer consumer.Metrics) *metricAggregationProcessor {
	if logger == nil {
		logger = zap.NewNop()
	}
	var allow map[string]struct{}
	if len(cfg.MetricNames) > 0 {
		allow = make(map[string]struct{}, len(cfg.MetricNames))
		for _, n := range cfg.MetricNames {
			allow[n] = struct{}{}
		}
	}

	emitsGauge := cfg.AllowGaugeRate
	if !emitsGauge {
		for _, a := range cfg.Aggregations {
			if a != AggRate {
				emitsGauge = true
				break
			}
		}
	}

	reaperInterval := time.Duration(0)
	if cfg.IdleTTL > 0 {
		reaperInterval = cfg.IdleTTL / 4
		if reaperInterval < minReaperInterval {
			reaperInterval = minReaperInterval
		} else if reaperInterval > maxReaperInterval {
			reaperInterval = maxReaperInterval
		}
	}

	p := &metricAggregationProcessor{
		cfg:               cfg,
		logger:            logger,
		nextConsumer:      nextConsumer,
		metricAllowlist:   allow,
		emitsGauge:        emitsGauge,
		bucketNanos:       cfg.BucketDuration.Nanoseconds(),
		clock:             time.Now,
		lazySweepCooldown: defaultLazySweepCooldown,
		reaperInterval:    reaperInterval,
	}
	p.lastLazySweep.Store(math.MinInt64)
	for i := range p.shards {
		p.shards[i].series = make(map[string]*seriesEntry)
		p.shards[i].lastSweepNanos = math.MinInt64
	}
	return p
}

func (*metricAggregationProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *metricAggregationProcessor) Start(ctx context.Context, _ component.Host) error {
	for {
		p.lifecycleMu.Lock()
		if p.started {
			p.lifecycleMu.Unlock()
			return nil
		}
		if p.done != nil {
			// A prior Shutdown cancelled the background goroutine, which is still exiting.
			// Wait for it to finish so two background loops never overlap.
			prevDone := p.done
			p.lifecycleMu.Unlock()
			select {
			case <-prevDone:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		p.started = true
		if p.cfg.EmitInterval > 0 {
			loopCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			p.cancel = cancel
			p.done = done
			go p.emitLoop(loopCtx, done)
		} else if p.cfg.IdleTTL > 0 && p.reaperInterval > 0 {
			loopCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			p.cancel = cancel
			p.done = done
			go p.idleReaperLoop(loopCtx, done)
		}
		p.lifecycleMu.Unlock()
		return nil
	}
}

func (p *metricAggregationProcessor) Shutdown(ctx context.Context) error {
	p.lifecycleMu.Lock()
	cancel := p.cancel
	done := p.done
	p.started = false
	p.cancel = nil
	if cancel != nil {
		// Cancel under lock so concurrent Start immediately sees cancellation in flight.
		cancel()
	}
	p.lifecycleMu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *metricAggregationProcessor) finishLoop(done chan struct{}) {
	p.lifecycleMu.Lock()
	if p.done == done {
		p.done = nil
	}
	p.lifecycleMu.Unlock()
	close(done)
}

func (p *metricAggregationProcessor) emitLoop(loopCtx context.Context, done chan struct{}) {
	defer p.finishLoop(done)
	ticker := time.NewTicker(p.cfg.EmitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			if err := p.Flush(loopCtx, p.clock()); err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error("Failed to emit periodic sliding-window rollups", zap.Error(err))
			}
		}
	}
}

func (p *metricAggregationProcessor) idleReaperLoop(loopCtx context.Context, done chan struct{}) {
	defer p.finishLoop(done)
	ticker := time.NewTicker(p.reaperInterval)
	defer ticker.Stop()

	idleNanos := uint64(p.cfg.IdleTTL.Nanoseconds())
	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			p.evictIdleAllShards(p.clock().UnixNano(), idleNanos)
		}
	}
}

// evictIdleLocked deletes series idle >= idleNanos inside an ALREADY-LOCKED shard,
// records the shard sweep watermark monotonically, and archives cumulative refold
// counters so Stats() remains monotonic.
func (p *metricAggregationProcessor) evictIdleLocked(sh *seriesShard, nowNanos int64, idleNanos uint64) int {
	if nowNanos > sh.lastSweepNanos {
		sh.lastSweepNanos = nowNanos
	}
	evicted := 0
	for k, entry := range sh.series {
		if nowNanos > entry.lastArrivalNanos && uint64(nowNanos)-uint64(entry.lastArrivalNanos) >= idleNanos {
			d := entry.win.Diagnostics()
			sh.evictedRefolds += d.Refolds
			sh.evictedCancelRefolds += d.CancelRefolds
			sh.evictedPeriodicRefolds += d.PeriodicRefolds
			delete(sh.series, k)
			p.activeSeries.Add(-1)
			p.idleEvicted.Add(1)
			evicted++
		}
	}
	return evicted
}

func (p *metricAggregationProcessor) shouldSweepShardLocked(sh *seriesShard, nowNanos int64) bool {
	cooldown := p.lazySweepCooldown.Nanoseconds()
	if cooldown <= 0 {
		return true
	}
	if sh.lastSweepNanos == math.MinInt64 {
		return true
	}
	if nowNanos <= sh.lastSweepNanos {
		return false
	}
	return uint64(nowNanos)-uint64(sh.lastSweepNanos) >= uint64(cooldown)
}

func (p *metricAggregationProcessor) evictIdleAllShards(nowNanos int64, idleNanos uint64) int {
	total := 0
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		total += p.evictIdleLocked(sh, nowNanos, idleNanos)
		sh.mu.Unlock()
	}
	return total
}

func (p *metricAggregationProcessor) evictIdleOtherShards(skipIdx uint64, nowNanos int64, idleNanos uint64) int {
	total := 0
	for i := range p.shards {
		if uint64(i) == skipIdx {
			continue
		}
		sh := &p.shards[i]
		sh.mu.Lock()
		total += p.evictIdleLocked(sh, nowNanos, idleNanos)
		sh.mu.Unlock()
	}
	return total
}

func (p *metricAggregationProcessor) shouldSweepOtherShards(nowNanos int64) bool {
	cooldown := p.lazySweepCooldown.Nanoseconds()
	if cooldown <= 0 {
		return true
	}
	for {
		last := p.lastLazySweep.Load()
		if last != math.MinInt64 {
			if nowNanos <= last || uint64(nowNanos)-uint64(last) < uint64(cooldown) {
				return false
			}
		}
		if p.lastLazySweep.CompareAndSwap(last, nowNanos) {
			return true
		}
	}
}

// tryReserveSeries enforces activeSeries <= MaxSeries strictly across all 64
// concurrent shards via lock-free atomic CAS reservation.
func (p *metricAggregationProcessor) tryReserveSeries() bool {
	limit := int64(p.cfg.MaxSeries)
	for {
		cur := p.activeSeries.Load()
		if cur >= limit {
			return false
		}
		if p.activeSeries.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (p *metricAggregationProcessor) isMetricAllowed(name string) bool {
	if p.metricAllowlist == nil {
		return true
	}
	_, ok := p.metricAllowlist[name]
	return ok
}

func isValidInputValue(val float64) engine.AddStatus {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return engine.AddDroppedNonFinite
	}
	if math.Abs(val) > engine.MaxAbsValue {
		return engine.AddDroppedOverflow
	}
	return engine.AddOK
}

func (p *metricAggregationProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	nowClockNanos := p.clock().UnixNano()
	kb := acquireKeyBuilder()
	defer releaseKeyBuilder(kb)

	checkIdle := p.cfg.IdleTTL > 0
	var idleNanos uint64
	if checkIdle {
		idleNanos = uint64(p.cfg.IdleTTL.Nanoseconds())
	}

	var touchedKeys map[string]int
	var touched []emittedSample
	syncEmit := p.cfg.EmitInterval == 0
	if syncEmit {
		touchedKeys = make(map[string]int)
	}

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		kb.buf = kb.buf[:0]
		kb.appendResourcePrefix(rm.Resource(), rm.SchemaUrl())
		resLen := len(kb.buf)
		var resKey string

		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			kb.buf = kb.buf[:resLen]
			kb.appendScopePrefix(sm.Scope(), sm.SchemaUrl())
			scopeLen := len(kb.buf)
			var scopeKey string

			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				if !p.isMetricAllowed(m.Name()) {
					return false
				}

				var dps pmetric.NumberDataPointSlice
				var temporality pmetric.AggregationTemporality
				var isMonotonic bool
				var isDeltaSum bool

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					// If only AggRate is configured and allow_gauge_rate is false,
					// pass Gauge metrics through untouched instead of deleting them
					// and burning max_series slots.
					if !p.emitsGauge {
						return false
					}
					dps = m.Gauge().DataPoints()
					temporality = pmetric.AggregationTemporalityUnspecified
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					temporality = sum.AggregationTemporality()
					// Only Delta Sum inputs are additive increments in a sliding window.
					// Cumulative and Unspecified temporality MUST pass through untouched
					// (chain after cumulativetodeltaprocessor when cumulative rate is needed).
					if temporality != pmetric.AggregationTemporalityDelta {
						return false
					}
					isDeltaSum = true
					isMonotonic = sum.IsMonotonic()
					dps = sum.DataPoints()
				default:
					// Histograms, ExponentialHistograms, and Summaries pass through untouched.
					return false
				}

				kb.buf = kb.buf[:scopeLen]
				kb.appendMetricPrefix(m, temporality, isMonotonic)
				metricLen := len(kb.buf)

				for i := 0; i < dps.Len(); i++ {
					dp := dps.At(i)
					if dp.Flags().NoRecordedValue() {
						p.droppedNoRecordedValue.Add(1)
						continue
					}

					var val float64
					switch dp.ValueType() {
					case pmetric.NumberDataPointValueTypeDouble:
						val = dp.DoubleValue()
					case pmetric.NumberDataPointValueTypeInt:
						val = float64(dp.IntValue())
					default:
						p.droppedNoRecordedValue.Add(1)
						continue
					}

					// Validate value BEFORE reserving a cardinality slot so NaN/Inf/overflow
					// spam with random labels cannot exhaust max_series.
					if st := isValidInputValue(val); st != engine.AddOK {
						if st == engine.AddDroppedNonFinite {
							p.droppedNonFinite.Add(1)
						} else {
							p.droppedOverflow.Add(1)
						}
						continue
					}

					var tsNanos int64
					rawTS := dp.Timestamp()
					if rawTS == 0 {
						if !p.cfg.FallbackToArrivalTime {
							p.droppedMissingTimestamp.Add(1)
							continue
						}
						tsNanos = nowClockNanos
					} else if uint64(rawTS) > math.MaxInt64 {
						p.droppedFutureSkew.Add(1)
						continue
					} else {
						tsNanos = int64(rawTS)
					}

					if p.cfg.MaxFutureSkew > 0 && tsNanos > nowClockNanos {
						if uint64(tsNanos)-uint64(nowClockNanos) > uint64(p.cfg.MaxFutureSkew.Nanoseconds()) {
							p.droppedFutureSkew.Add(1)
							continue
						}
					}

					kb.buf = kb.buf[:metricLen]
					kb.appendDataPointAttrs(dp.Attributes())
					h := fnv1a128(kb.buf)
					shardIdx := (h[0] ^ h[1]) & (numShards - 1)
					sh := &p.shards[shardIdx]

					sh.mu.Lock()
					entry, ok := sh.series[string(kb.buf)]
					if !ok {
						reserved := p.tryReserveSeries()
						if !reserved && checkIdle {
							// Lazily evict expired entries in this shard first (cooldown-bounded so
							// excess-cardinality saturation stays O(1) per dropped datapoint).
							if p.shouldSweepShardLocked(sh, nowClockNanos) && p.evictIdleLocked(sh, nowClockNanos, idleNanos) > 0 {
								reserved = p.tryReserveSeries()
							}
							// If still full, release sh.mu and sweep other shards (bounded by cooldown)
							// so sync mode (EmitInterval==0) never permanently exhausts max_series.
							if !reserved && p.shouldSweepOtherShards(nowClockNanos) {
								sh.mu.Unlock()
								p.evictIdleOtherShards(shardIdx, nowClockNanos, idleNanos)
								sh.mu.Lock()
								if existing, found := sh.series[string(kb.buf)]; found {
									entry = existing
									ok = true
								} else {
									reserved = p.tryReserveSeries()
								}
							}
						}
						if !ok {
							if !reserved {
								sh.mu.Unlock()
								p.droppedCardinality.Add(1)
								continue
							}
							win, err := engine.NewWindow(p.cfg.Engine, p.cfg.WindowSize, p.cfg.BucketDuration)
							if err != nil {
								p.activeSeries.Add(-1)
								sh.mu.Unlock()
								p.logger.Error("Failed to create sliding window", zap.Error(err))
								continue
							}
							if resKey == "" {
								resKey = string(kb.buf[:resLen])
							}
							if scopeKey == "" {
								scopeKey = string(kb.buf[:scopeLen])
							}
							resClone := pcommon.NewResource()
							rm.Resource().CopyTo(resClone)
							scopeClone := pcommon.NewInstrumentationScope()
							sm.Scope().CopyTo(scopeClone)
							attrsClone := pcommon.NewMap()
							dp.Attributes().CopyTo(attrsClone)
							keyStr := string(kb.buf)
							entry = &seriesEntry{
								key:              keyStr,
								win:              win,
								lastArrivalNanos: nowClockNanos,
								meta: &seriesMetadata{
									resKey:         resKey,
									scopeKey:       scopeKey,
									resource:       resClone,
									resourceSchema: rm.SchemaUrl(),
									scope:          scopeClone,
									scopeSchema:    sm.SchemaUrl(),
									metricName:     m.Name(),
									metricUnit:     m.Unit(),
									isDeltaSum:     isDeltaSum,
									dataPointAttrs: attrsClone,
								},
							}
							sh.series[keyStr] = entry
						}
					}

					st := entry.win.Add(tsNanos, val)
					switch st {
					case engine.AddOK:
						if nowClockNanos > entry.lastArrivalNanos {
							entry.lastArrivalNanos = nowClockNanos
						}
						if syncEmit {
							emitTS := entry.win.LastActivityNanos()
							snap := entry.win.Snapshot()
							if idx, exists := touchedKeys[entry.key]; exists {
								touched[idx].snap = snap
								touched[idx].emitTS = emitTS
							} else {
								touchedKeys[entry.key] = len(touched)
								touched = append(touched, emittedSample{
									meta:   entry.meta,
									snap:   snap,
									emitTS: emitTS,
								})
							}
						}
					case engine.AddDroppedLate:
						p.droppedLate.Add(1)
					case engine.AddDroppedNonFinite:
						p.droppedNonFinite.Add(1)
					case engine.AddDroppedOverflow:
						p.droppedOverflow.Add(1)
					}
					sh.mu.Unlock()
				}

				return !p.cfg.PassthroughInput
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	if syncEmit && len(touched) > 0 {
		p.appendSamples(md, touched)
	}

	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.nextConsumer.ConsumeMetrics(ctx, md)
}

// Flush advances all active series to now (evicting idle series past idle_ttl),
// emits sliding-window Gauge snapshots, and forwards them to nextConsumer without
// holding any shard locks across downstream calls.
// On exact bucket-aligned timestamps (now.UnixNano() % BucketDuration == 0),
// Flush queries at now - 1ns so completed buckets [k-B, k-1] match exact width W.
func (p *metricAggregationProcessor) Flush(ctx context.Context, now time.Time) error {
	nowNanos := now.UnixNano()
	queryTS := nowNanos
	if nowNanos%p.bucketNanos == 0 && nowNanos > math.MinInt64 {
		queryTS = nowNanos - 1
	}

	var idleNanos uint64
	checkIdle := p.cfg.IdleTTL > 0
	if checkIdle {
		idleNanos = uint64(p.cfg.IdleTTL.Nanoseconds())
	}

	var samples []emittedSample
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		if checkIdle {
			p.evictIdleLocked(sh, nowNanos, idleNanos)
		}
		for _, entry := range sh.series {
			snap := entry.win.Query(queryTS)
			emitTS := nowNanos
			if entry.win.Initialized() {
				// Prevent invalid StartTimestamp > Timestamp and under-clamped elapsed_window
				// rate when a host has positive clock skew, NTP steps backward, or concurrent
				// Flush calls advance a silent window past an earlier Flush's timestamp.
				if lastAct := entry.win.LastActivityNanos(); lastAct > emitTS {
					emitTS = lastAct
				}
				if snap.WindowStartNanos > emitTS {
					emitTS = snap.WindowStartNanos
				}
			}
			samples = append(samples, emittedSample{
				meta:   entry.meta,
				snap:   snap,
				emitTS: emitTS,
			})
		}
		sh.mu.Unlock()
	}

	if len(samples) == 0 {
		return nil
	}

	out := pmetric.NewMetrics()
	p.appendSamples(out, samples)
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

type metricKey struct {
	name string
	unit string
}

type scopeMetricGroup struct {
	sm      pmetric.ScopeMetrics
	metrics map[metricKey]pmetric.Metric
}

func (p *metricAggregationProcessor) appendSamples(dst pmetric.Metrics, samples []emittedSample) {
	rmByKey := make(map[string]pmetric.ResourceMetrics)
	smByKey := make(map[string]*scopeMetricGroup)

	for _, s := range samples {
		m := s.meta
		grp, ok := smByKey[m.scopeKey]
		if !ok {
			rm, rOK := rmByKey[m.resKey]
			if !rOK {
				rm = dst.ResourceMetrics().AppendEmpty()
				m.resource.CopyTo(rm.Resource())
				rm.SetSchemaUrl(m.resourceSchema)
				rmByKey[m.resKey] = rm
			}
			sm := rm.ScopeMetrics().AppendEmpty()
			m.scope.CopyTo(sm.Scope())
			sm.SetSchemaUrl(m.scopeSchema)
			grp = &scopeMetricGroup{
				sm:      sm,
				metrics: make(map[metricKey]pmetric.Metric),
			}
			smByKey[m.scopeKey] = grp
		}

		startTS := toTimestamp(s.snap.WindowStartNanos)
		emitTS := toTimestamp(s.emitTS)

		for _, agg := range p.cfg.Aggregations {
			switch agg {
			case AggSum:
				sumVal := s.snap.Sum
				if s.snap.Count == 0 {
					// Enforce Collector telemetry contract across all engines (including
					// algebraic_naive, whose raw Snapshot intentionally exposes drift).
					sumVal = 0
				}
				p.addGaugeDouble(grp, m.metricName+".window.sum", m.metricUnit, m.dataPointAttrs, startTS, emitTS, sumVal)
			case AggCount:
				p.addGaugeInt(grp, m.metricName+".window.count", "1", m.dataPointAttrs, startTS, emitTS, int64(s.snap.Count))
			case AggRate:
				if !m.isDeltaSum && !p.cfg.AllowGaugeRate {
					continue
				}
				unit := "1/s"
				if m.metricUnit != "" {
					unit = m.metricUnit + "/s"
				}
				p.addGaugeDouble(grp, m.metricName+".window.rate", unit, m.dataPointAttrs, startTS, emitTS, p.computeRate(s.snap, s.emitTS))
			case AggMean:
				if s.snap.Count == 0 {
					continue
				}
				p.addGaugeDouble(grp, m.metricName+".window.mean", m.metricUnit, m.dataPointAttrs, startTS, emitTS, s.snap.Mean)
			case AggStdDev:
				if s.snap.Count == 0 {
					continue
				}
				p.addGaugeDouble(grp, m.metricName+".window.stddev", m.metricUnit, m.dataPointAttrs, startTS, emitTS, s.snap.StdDev(p.cfg.VarianceMode))
			case AggMin:
				if s.snap.Count == 0 || !s.snap.HasMinMax {
					continue
				}
				p.addGaugeDouble(grp, m.metricName+".window.min", m.metricUnit, m.dataPointAttrs, startTS, emitTS, s.snap.Min)
			case AggMax:
				if s.snap.Count == 0 || !s.snap.HasMinMax {
					continue
				}
				p.addGaugeDouble(grp, m.metricName+".window.max", m.metricUnit, m.dataPointAttrs, startTS, emitTS, s.snap.Max)
			}
		}
	}

	// Prune empty scopes/resources (e.g., when count==0 omits mean/stddev/min/max
	// and no count/sum/rate aggregations were configured).
	dst.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
}

func (p *metricAggregationProcessor) computeRate(snap engine.Snapshot, emitTS int64) float64 {
	if snap.Count == 0 {
		return 0
	}
	if p.cfg.RateMode == RateModeElapsedWindow {
		denom := p.cfg.BucketDuration
		if snap.HasFirstSeen && emitTS > snap.FirstSeenNanos {
			d := uint64(emitTS) - uint64(snap.FirstSeenNanos)
			winU := uint64(p.cfg.WindowSize.Nanoseconds())
			bktU := uint64(p.cfg.BucketDuration.Nanoseconds())
			if d > winU {
				d = winU
			} else if d < bktU {
				d = bktU
			}
			denom = time.Duration(d)
		}
		return snap.Sum / denom.Seconds()
	}
	return snap.Sum / p.cfg.WindowSize.Seconds()
}

func (*metricAggregationProcessor) getOrCreateGauge(grp *scopeMetricGroup, name, unit string) pmetric.Metric {
	k := metricKey{name: name, unit: unit}
	m, ok := grp.metrics[k]
	if ok {
		return m
	}
	m = grp.sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	m.SetEmptyGauge()
	grp.metrics[k] = m
	return m
}

func (p *metricAggregationProcessor) addGaugeDouble(grp *scopeMetricGroup, name, unit string, attrs pcommon.Map, startTS, emitTS pcommon.Timestamp, val float64) {
	m := p.getOrCreateGauge(grp, name, unit)
	dp := m.Gauge().DataPoints().AppendEmpty()
	attrs.CopyTo(dp.Attributes())
	dp.SetStartTimestamp(startTS)
	dp.SetTimestamp(emitTS)
	dp.SetDoubleValue(val)
}

func (p *metricAggregationProcessor) addGaugeInt(grp *scopeMetricGroup, name, unit string, attrs pcommon.Map, startTS, emitTS pcommon.Timestamp, val int64) {
	m := p.getOrCreateGauge(grp, name, unit)
	dp := m.Gauge().DataPoints().AppendEmpty()
	attrs.CopyTo(dp.Attributes())
	dp.SetStartTimestamp(startTS)
	dp.SetTimestamp(emitTS)
	dp.SetIntValue(val)
}

func toTimestamp(nanos int64) pcommon.Timestamp {
	if nanos <= 0 {
		return 0
	}
	return pcommon.Timestamp(nanos)
}

// Stats aggregates atomic processor counters and strictly monotonic cumulative
// per-series + archived-evicted engine refold totals.
func (p *metricAggregationProcessor) Stats() Stats {
	s := Stats{
		ActiveSeries:            p.activeSeries.Load(),
		DroppedCardinality:      p.droppedCardinality.Load(),
		DroppedNonFinite:        p.droppedNonFinite.Load(),
		DroppedOverflow:         p.droppedOverflow.Load(),
		DroppedLate:             p.droppedLate.Load(),
		DroppedNoRecordedValue:  p.droppedNoRecordedValue.Load(),
		DroppedMissingTimestamp: p.droppedMissingTimestamp.Load(),
		DroppedFutureSkew:       p.droppedFutureSkew.Load(),
		IdleEvicted:             p.idleEvicted.Load(),
	}
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		s.Refolds += sh.evictedRefolds
		s.CancelRefolds += sh.evictedCancelRefolds
		s.PeriodicRefolds += sh.evictedPeriodicRefolds
		for _, entry := range sh.series {
			d := entry.win.Diagnostics()
			s.Refolds += d.Refolds
			s.CancelRefolds += d.CancelRefolds
			s.PeriodicRefolds += d.PeriodicRefolds
		}
		sh.mu.Unlock()
	}
	return s
}

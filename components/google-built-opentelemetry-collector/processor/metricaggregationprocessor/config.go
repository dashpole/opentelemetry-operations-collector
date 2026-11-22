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
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
)

// AggregationType selects a rolled-up sliding-window output statistic.
type AggregationType string

const (
	AggSum    AggregationType = "sum"
	AggCount  AggregationType = "count"
	AggMean   AggregationType = "mean"
	AggStdDev AggregationType = "stddev"
	AggRate   AggregationType = "rate"
	AggMin    AggregationType = "min"
	AggMax    AggregationType = "max"
)

// RateMode controls the denominator used for sliding-window rate calculations.
type RateMode string

const (
	// RateModeFixedWindow divides window sum by the full configured WindowSize.
	RateModeFixedWindow RateMode = "fixed_window"
	// RateModeElapsedWindow divides window sum by clamp(now - firstSeen, BucketDuration, WindowSize).
	RateModeElapsedWindow RateMode = "elapsed_window"
)

// Default configuration constants.
const (
	DefaultWindowSize     = 5 * time.Minute
	DefaultBucketDuration = 5 * time.Second
	DefaultMaxBuckets     = 10000
	DefaultMaxSeries      = 100000
	DefaultIdleTTL        = 10 * time.Minute
)

// Config defines configuration for the metricaggregation processor.
type Config struct {
	// WindowSize is the sliding window duration W (must be an exact multiple of BucketDuration).
	WindowSize time.Duration `mapstructure:"window_size"`
	// BucketDuration is the discrete sub-window width Delta = W / B.
	BucketDuration time.Duration `mapstructure:"bucket_duration"`
	// MaxBuckets caps B = WindowSize / BucketDuration to prevent accidental OOM from tiny buckets.
	MaxBuckets int `mapstructure:"max_buckets"`
	// Engine selects the underlying sliding-window engine implementation.
	Engine engine.Kind `mapstructure:"engine"`
	// Aggregations lists the output rollup gauge metrics to emit per matched series.
	Aggregations []AggregationType `mapstructure:"aggregations"`
	// VarianceMode selects sample (n-1, default) or population (n) variance/stddev.
	VarianceMode engine.VarianceMode `mapstructure:"variance_mode"`
	// RateMode selects fixed_window (default) or elapsed_window denominator for rate.
	RateMode RateMode `mapstructure:"rate_mode"`
	// AllowGaugeRate enables emitting rate = sum / W for Gauge inputs (default false: Delta Sum only).
	AllowGaugeRate bool `mapstructure:"allow_gauge_rate"`
	// MaxSeries strictly bounds the maximum number of active series tracked across all shards.
	MaxSeries int `mapstructure:"max_series"`
	// IdleTTL evicts series that have received no datapoints for at least IdleTTL (0 disables).
	// When > 0, must be >= WindowSize so live windows are not evicted prematurely.
	IdleTTL time.Duration `mapstructure:"idle_ttl"`
	// MaxFutureSkew drops datapoints with ts > clock.Now() + MaxFutureSkew (0 disables).
	MaxFutureSkew time.Duration `mapstructure:"max_future_skew"`
	// FallbackToArrivalTime stamps datapoints with Timestamp == 0 using clock.Now() instead of dropping.
	FallbackToArrivalTime bool `mapstructure:"fallback_to_arrival_time"`
	// EmitInterval controls periodic background rollup emission. When 0 (default), rollups
	// are emitted synchronously inside ConsumeMetrics for each touched series.
	EmitInterval time.Duration `mapstructure:"emit_interval"`
	// PassthroughInput keeps matched input metrics alongside emitted rollup metrics (default false).
	PassthroughInput bool `mapstructure:"passthrough_input"`
	// MetricNames optionally restricts aggregation to the named metrics (empty matches all eligible).
	MetricNames []string `mapstructure:"metric_names"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	return &Config{
		WindowSize:     DefaultWindowSize,
		BucketDuration: DefaultBucketDuration,
		MaxBuckets:     DefaultMaxBuckets,
		Engine:         engine.KindAlgebraicChan,
		Aggregations:   []AggregationType{AggSum, AggCount, AggMean, AggStdDev, AggRate},
		VarianceMode:   engine.VarianceSample,
		RateMode:       RateModeFixedWindow,
		MaxSeries:      DefaultMaxSeries,
		IdleTTL:        DefaultIdleTTL,
	}
}

// Validate checks that the processor configuration is internally consistent and safe.
func (cfg *Config) Validate() error {
	if cfg == nil {
		return errors.New("config must not be nil")
	}

	var errs []error

	if cfg.WindowSize <= 0 {
		errs = append(errs, fmt.Errorf("window_size must be > 0, got %v", cfg.WindowSize))
	}
	if cfg.BucketDuration <= 0 {
		errs = append(errs, fmt.Errorf("bucket_duration must be > 0, got %v", cfg.BucketDuration))
	}
	if cfg.MaxBuckets <= 0 {
		errs = append(errs, fmt.Errorf("max_buckets must be > 0, got %d", cfg.MaxBuckets))
	}
	if cfg.WindowSize > 0 && cfg.BucketDuration > 0 {
		if cfg.WindowSize < cfg.BucketDuration {
			errs = append(errs, fmt.Errorf("window_size (%v) must be >= bucket_duration (%v)", cfg.WindowSize, cfg.BucketDuration))
		} else if cfg.WindowSize%cfg.BucketDuration != 0 {
			errs = append(errs, fmt.Errorf("window_size (%v) must be an exact multiple of bucket_duration (%v)", cfg.WindowSize, cfg.BucketDuration))
		} else {
			buckets := int64(cfg.WindowSize / cfg.BucketDuration)
			if cfg.MaxBuckets > 0 && buckets > int64(cfg.MaxBuckets) {
				errs = append(errs, fmt.Errorf("bucket count %d (window_size/bucket_duration) exceeds max_buckets (%d)", buckets, cfg.MaxBuckets))
			}
		}
	}

	if !cfg.Engine.Valid() {
		errs = append(errs, fmt.Errorf("unknown engine %q (valid: raw_rescan, raw_inc, algebraic_naive, algebraic_chan, twostack_monoid)", cfg.Engine))
	}

	if len(cfg.Aggregations) == 0 {
		errs = append(errs, errors.New("aggregations must specify at least one aggregation"))
	} else {
		seen := make(map[AggregationType]struct{}, len(cfg.Aggregations))
		for _, a := range cfg.Aggregations {
			switch a {
			case AggSum, AggCount, AggMean, AggStdDev, AggRate:
			case AggMin, AggMax:
				if cfg.Engine.Valid() && !cfg.Engine.SupportsMinMax() {
					errs = append(errs, fmt.Errorf("aggregation %q is not supported by engine %q (supported by: raw_rescan, twostack_monoid)", a, cfg.Engine))
				}
			default:
				errs = append(errs, fmt.Errorf("unknown aggregation %q", a))
			}
			if _, dup := seen[a]; dup {
				errs = append(errs, fmt.Errorf("duplicate aggregation %q", a))
			}
			seen[a] = struct{}{}
		}
	}

	switch cfg.VarianceMode {
	case engine.VarianceSample, engine.VariancePopulation:
	default:
		errs = append(errs, fmt.Errorf("unknown variance_mode %q (valid: sample, population)", cfg.VarianceMode))
	}

	switch cfg.RateMode {
	case RateModeFixedWindow, RateModeElapsedWindow:
	default:
		errs = append(errs, fmt.Errorf("unknown rate_mode %q (valid: fixed_window, elapsed_window)", cfg.RateMode))
	}

	if cfg.MaxSeries <= 0 {
		errs = append(errs, fmt.Errorf("max_series must be > 0, got %d", cfg.MaxSeries))
	}
	if cfg.IdleTTL < 0 {
		errs = append(errs, fmt.Errorf("idle_ttl must be >= 0, got %v", cfg.IdleTTL))
	} else if cfg.IdleTTL > 0 && cfg.WindowSize > 0 && cfg.IdleTTL < cfg.WindowSize {
		errs = append(errs, fmt.Errorf("idle_ttl (%v) must be >= window_size (%v) when enabled", cfg.IdleTTL, cfg.WindowSize))
	}
	if cfg.MaxFutureSkew < 0 {
		errs = append(errs, fmt.Errorf("max_future_skew must be >= 0, got %v", cfg.MaxFutureSkew))
	}
	if cfg.EmitInterval < 0 {
		errs = append(errs, fmt.Errorf("emit_interval must be >= 0, got %v", cfg.EmitInterval))
	}

	for _, name := range cfg.MetricNames {
		if name == "" {
			errs = append(errs, errors.New("metric_names must not contain empty strings"))
			break
		}
	}

	return errors.Join(errs...)
}

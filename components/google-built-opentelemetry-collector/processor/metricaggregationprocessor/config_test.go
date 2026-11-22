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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/engine"
)

func TestConfigStructAndLoadYAML(t *testing.T) {
	require.NoError(t, componenttest.CheckConfigStruct(createDefaultConfig()))

	// Unmarshal testdata/config.yaml into a zero-value Config so every mapstructure
	// tag is actively exercised (not masked by default values).
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	cfg := &Config{}
	require.NoError(t, cm.Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	require.Equal(t, 5*time.Minute, cfg.WindowSize)
	require.Equal(t, 5*time.Second, cfg.BucketDuration)
	require.Equal(t, 10000, cfg.MaxBuckets)
	require.Equal(t, engine.KindAlgebraicChan, cfg.Engine)
	require.Equal(t, []AggregationType{AggSum, AggCount, AggMean, AggStdDev, AggRate}, cfg.Aggregations)
	require.Equal(t, engine.VarianceSample, cfg.VarianceMode)
	require.Equal(t, RateModeFixedWindow, cfg.RateMode)
	require.False(t, cfg.AllowGaugeRate)
	require.Equal(t, 100000, cfg.MaxSeries)
	require.Equal(t, 10*time.Minute, cfg.IdleTTL)
	require.Equal(t, time.Minute, cfg.MaxFutureSkew)
	require.False(t, cfg.FallbackToArrivalTime)
	require.Equal(t, time.Duration(0), cfg.EmitInterval)
	require.False(t, cfg.PassthroughInput)
	require.Equal(t, []string{"http.server.request.duration"}, cfg.MetricNames)

	// Verify non-default booleans and enums unmarshal correctly via confmap.
	nonDefaultYAML := map[string]any{
		"window_size":              "2m",
		"bucket_duration":          "10s",
		"max_buckets":              500,
		"engine":                   "twostack_monoid",
		"aggregations":             []any{"sum", "min", "max"},
		"variance_mode":            "population",
		"rate_mode":                "elapsed_window",
		"allow_gauge_rate":         true,
		"max_series":               42,
		"idle_ttl":                 "4m",
		"max_future_skew":          "15s",
		"fallback_to_arrival_time": true,
		"emit_interval":            "2s",
		"passthrough_input":        true,
		"metric_names":             []any{"a", "b"},
	}
	cm2 := confmap.NewFromStringMap(nonDefaultYAML)
	cfg2 := &Config{}
	require.NoError(t, cm2.Unmarshal(cfg2))
	require.NoError(t, cfg2.Validate())
	require.Equal(t, 2*time.Minute, cfg2.WindowSize)
	require.Equal(t, 10*time.Second, cfg2.BucketDuration)
	require.Equal(t, 500, cfg2.MaxBuckets)
	require.Equal(t, engine.KindTwoStackMonoid, cfg2.Engine)
	require.Equal(t, []AggregationType{AggSum, AggMin, AggMax}, cfg2.Aggregations)
	require.Equal(t, engine.VariancePopulation, cfg2.VarianceMode)
	require.Equal(t, RateModeElapsedWindow, cfg2.RateMode)
	require.True(t, cfg2.AllowGaugeRate)
	require.Equal(t, 42, cfg2.MaxSeries)
	require.Equal(t, 4*time.Minute, cfg2.IdleTTL)
	require.Equal(t, 15*time.Second, cfg2.MaxFutureSkew)
	require.True(t, cfg2.FallbackToArrivalTime)
	require.Equal(t, 2*time.Second, cfg2.EmitInterval)
	require.True(t, cfg2.PassthroughInput)
	require.Equal(t, []string{"a", "b"}, cfg2.MetricNames)

	// Typed-nil Config.Validate() must return an error without panicking.
	var nilCfg *Config
	require.ErrorContains(t, nilCfg.Validate(), "config must not be nil")
}

func TestConfigValidate(t *testing.T) {
	validBase := func() *Config {
		return createDefaultConfig().(*Config)
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:   "default_valid",
			mutate: func(*Config) {},
		},
		{
			name: "twostack_with_min_max_valid",
			mutate: func(c *Config) {
				c.Engine = engine.KindTwoStackMonoid
				c.Aggregations = []AggregationType{AggSum, AggCount, AggMean, AggStdDev, AggMin, AggMax}
			},
		},
		{
			name: "raw_rescan_with_min_max_valid",
			mutate: func(c *Config) {
				c.Engine = engine.KindRawRescan
				c.Aggregations = []AggregationType{AggMin, AggMax}
			},
		},
		{
			name: "idle_ttl_zero_disabled_valid",
			mutate: func(c *Config) {
				c.IdleTTL = 0
			},
		},
		{
			name: "window_size_zero",
			mutate: func(c *Config) {
				c.WindowSize = 0
			},
			wantErr: "window_size must be > 0",
		},
		{
			name: "bucket_duration_zero",
			mutate: func(c *Config) {
				c.BucketDuration = 0
			},
			wantErr: "bucket_duration must be > 0",
		},
		{
			name: "window_shorter_than_bucket",
			mutate: func(c *Config) {
				c.WindowSize = time.Second
				c.BucketDuration = 5 * time.Second
			},
			wantErr: "must be >= bucket_duration",
		},
		{
			name: "window_not_multiple_of_bucket",
			mutate: func(c *Config) {
				c.WindowSize = 10 * time.Second
				c.BucketDuration = 3 * time.Second
			},
			wantErr: "exact multiple",
		},
		{
			name: "buckets_exceed_max_buckets",
			mutate: func(c *Config) {
				c.WindowSize = 24 * time.Hour
				c.BucketDuration = time.Millisecond
				c.MaxBuckets = 10000
				c.IdleTTL = 24 * time.Hour
			},
			wantErr: "exceeds max_buckets",
		},
		{
			name: "max_buckets_zero",
			mutate: func(c *Config) {
				c.MaxBuckets = 0
			},
			wantErr: "max_buckets must be > 0",
		},
		{
			name: "unknown_engine",
			mutate: func(c *Config) {
				c.Engine = engine.Kind("invalid_engine")
			},
			wantErr: "unknown engine",
		},
		{
			name: "empty_aggregations",
			mutate: func(c *Config) {
				c.Aggregations = nil
			},
			wantErr: "at least one aggregation",
		},
		{
			name: "duplicate_aggregation",
			mutate: func(c *Config) {
				c.Aggregations = []AggregationType{AggSum, AggSum}
			},
			wantErr: "duplicate aggregation",
		},
		{
			name: "unknown_aggregation",
			mutate: func(c *Config) {
				c.Aggregations = []AggregationType{"p99"}
			},
			wantErr: "unknown aggregation",
		},
		{
			name: "min_rejected_on_algebraic_chan",
			mutate: func(c *Config) {
				c.Engine = engine.KindAlgebraicChan
				c.Aggregations = []AggregationType{AggSum, AggMin}
			},
			wantErr: "not supported by engine",
		},
		{
			name: "max_rejected_on_algebraic_naive",
			mutate: func(c *Config) {
				c.Engine = engine.KindAlgebraicNaive
				c.Aggregations = []AggregationType{AggMax}
			},
			wantErr: "not supported by engine",
		},
		{
			name: "min_rejected_on_raw_inc",
			mutate: func(c *Config) {
				c.Engine = engine.KindRawInc
				c.Aggregations = []AggregationType{AggMin}
			},
			wantErr: "not supported by engine",
		},
		{
			name: "invalid_variance_mode",
			mutate: func(c *Config) {
				c.VarianceMode = "unbiased"
			},
			wantErr: "unknown variance_mode",
		},
		{
			name: "invalid_rate_mode",
			mutate: func(c *Config) {
				c.RateMode = "dynamic"
			},
			wantErr: "unknown rate_mode",
		},
		{
			name: "max_series_zero",
			mutate: func(c *Config) {
				c.MaxSeries = 0
			},
			wantErr: "max_series must be > 0",
		},
		{
			name: "idle_ttl_negative",
			mutate: func(c *Config) {
				c.IdleTTL = -time.Second
			},
			wantErr: "idle_ttl must be >= 0",
		},
		{
			name: "idle_ttl_shorter_than_window",
			mutate: func(c *Config) {
				c.WindowSize = 5 * time.Minute
				c.IdleTTL = time.Minute
			},
			wantErr: "idle_ttl (1m0s) must be >= window_size (5m0s)",
		},
		{
			name: "negative_max_future_skew",
			mutate: func(c *Config) {
				c.MaxFutureSkew = -time.Second
			},
			wantErr: "max_future_skew must be >= 0",
		},
		{
			name: "negative_emit_interval",
			mutate: func(c *Config) {
				c.EmitInterval = -time.Second
			},
			wantErr: "emit_interval must be >= 0",
		},
		{
			name: "empty_metric_name",
			mutate: func(c *Config) {
				c.MetricNames = []string{"valid.metric", ""}
			},
			wantErr: "must not contain empty strings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validBase()
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/processor/metricaggregationprocessor/internal/metadata"
)

func TestFactoryAndLifecycle(t *testing.T) {
	f := NewFactory()
	require.Equal(t, metadata.Type, f.Type())
	require.Equal(t, component.StabilityLevelDevelopment, f.MetricsStability())

	ctx := context.Background()
	set := processortest.NewNopSettings(metadata.Type)
	sink := &consumertest.MetricsSink{}

	// Reject wrong config type and typed-nil (*Config)(nil) without panicking.
	type badCfg struct{}
	_, err := f.CreateMetrics(ctx, set, &badCfg{}, sink)
	require.Error(t, err)

	var typedNil *Config
	_, err = f.CreateMetrics(ctx, set, typedNil, sink)
	require.Error(t, err)

	// Reject nil consumer.
	_, err = f.CreateMetrics(ctx, set, f.CreateDefaultConfig(), nil)
	require.Error(t, err)

	// Reject invalid config.
	bad := f.CreateDefaultConfig().(*Config)
	bad.WindowSize = 0
	_, err = f.CreateMetrics(ctx, set, bad, sink)
	require.Error(t, err)

	// Valid sync mode lifecycle: Shutdown without Start is safe.
	p, err := f.CreateMetrics(ctx, set, f.CreateDefaultConfig(), sink)
	require.NoError(t, err)
	require.True(t, p.Capabilities().MutatesData)
	require.NoError(t, p.Shutdown(ctx))

	// Valid periodic mode lifecycle: Start twice, Shutdown twice, restart, Shutdown.
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.EmitInterval = 10 * time.Millisecond
	p2, err := f.CreateMetrics(ctx, set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p2.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, p2.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, p2.Shutdown(ctx))
	require.NoError(t, p2.Shutdown(ctx))
	require.NoError(t, p2.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, p2.Shutdown(ctx))
}

func TestConcurrentStartShutdownNoRaceOrPanic(t *testing.T) {
	f := NewFactory()
	set := processortest.NewNopSettings(metadata.Type)
	sink := &consumertest.MetricsSink{}
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.EmitInterval = 2 * time.Millisecond
	p, err := f.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := 0; iter < 15; iter++ {
				if (id+iter)%2 == 0 {
					assert.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
				} else {
					assert.NoError(t, p.Shutdown(context.Background()))
				}
			}
		}(i)
	}
	wg.Wait()
	require.NoError(t, p.Shutdown(context.Background()))
}

type blockingConsumer struct {
	entered chan struct{}
	unblock chan struct{}
}

func (*blockingConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (b *blockingConsumer) ConsumeMetrics(ctx context.Context, _ pmetric.Metrics) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.unblock:
		return nil
	}
}

func TestShutdownCancelsBlockedFlush(t *testing.T) {
	bc := &blockingConsumer{
		entered: make(chan struct{}, 1),
		unblock: make(chan struct{}),
	}
	defer close(bc.unblock)

	cfg := createDefaultConfig().(*Config)
	cfg.WindowSize = 10 * time.Second
	cfg.BucketDuration = time.Second
	cfg.IdleTTL = 20 * time.Second
	cfg.EmitInterval = 5 * time.Millisecond

	p := newMetricAggregationProcessor(cfg, nil, bc)
	now := time.Now()
	require.NoError(t, p.ConsumeMetrics(context.Background(), makeDeltaSumMetric("h", "s", "m", "", now, 1, nil, false)))
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))

	select {
	case <-bc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for emitLoop to enter blocked consumer")
	}

	// Shutdown MUST cancel loopCtx and return promptly (NOT hang forever).
	shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(shutCtx))
}

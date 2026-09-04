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

package googlecontrolplane

import (
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/confmap/provider/googlecontrolplane/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func validBaseTopology() map[string]any {
	return map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"endpoint": "0.0.0.0:4317",
					},
				},
			},
		},
		"processors": map[string]any{
			"batch": map[string]any{
				"send_batch_size": 200,
				"timeout":         "1s",
			},
		},
		"exporters": map[string]any{
			"otlp": map[string]any{
				"endpoint": "telemetry.googleapis.com:443",
			},
		},
		"extensions": map[string]any{
			"googleclientauth": map[string]any{},
		},
		"service": map[string]any{
			"extensions": []any{"googleclientauth"},
			"pipelines": map[string]any{
				"metrics": map[string]any{
					"receivers":  []any{"otlp"},
					"processors": []any{"batch"},
					"exporters":  []any{"otlp"},
				},
			},
		},
	}
}

func TestPreValidator(t *testing.T) {
	reg := driver.NewDefaultRegistry()

	t.Run("Valid configuration succeeds", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		err := pv.Validate(conf)
		assert.NoError(t, err)
		assert.Empty(t, observedLogs.All())
	})

	t.Run("Nil configuration fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		err := pv.Validate(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configuration must not be nil")
		require.Len(t, observedLogs.All(), 1)
		assert.Equal(t, EventPolicyCompilationFailed, observedLogs.All()[0].ContextMap()["event.name"])
	})

	t.Run("Undeclared extension in service.extensions fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["extensions"] = []any{"googleclientauth", "undeclared_extension"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service references undeclared extension: undeclared_extension")

		logs := observedLogs.All()
		require.Len(t, logs, 1)
		assert.Equal(t, EventPolicyCompilationFailed, logs[0].ContextMap()["event.name"])
		assert.Equal(t, "undeclared_extension", logs[0].ContextMap()["component_id"])
	})

	t.Run("Empty pipelines allowed for composable provider", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"] = map[string]any{}

		err := pv.Validate(conf)
		require.NoError(t, err)
		assert.Empty(t, observedLogs.All())
	})

	t.Run("Empty receivers in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["receivers"] = []any{}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics must have at least one receiver")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Undeclared receiver in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["receivers"] = []any{"ghost_receiver"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics references undeclared receiver: ghost_receiver")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Empty exporters in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["exporters"] = []any{}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics must have at least one exporter")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Undeclared exporter in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["exporters"] = []any{"ghost_exporter"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics references undeclared exporter: ghost_exporter")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Duplicate processor in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["processors"] = []any{"batch", "batch"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "references duplicate processor \"batch\"")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Undeclared processor in pipeline fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["processors"] = []any{"ghost_processor"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics references undeclared processor: ghost_processor")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Unregistered component type fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["receivers"].(map[string]any)["unknown_receiver_type/custom"] = map[string]any{}
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"].(map[string]any)["receivers"] = []any{"unknown_receiver_type/custom"}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown receiver type \"unknown_receiver_type\" for component \"unknown_receiver_type/custom\"")

		logs := observedLogs.All()
		require.Len(t, logs, 1)
		assert.Equal(t, "unknown_receiver_type/custom", logs[0].ContextMap()["component_id"])
	})

	t.Run("Component config Validate() failure fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		// Invalid batch config: negative timeout or send_batch_max_size < send_batch_size
		conf["processors"].(map[string]any)["batch"] = map[string]any{
			"send_batch_size":     1000,
			"send_batch_max_size": 100, // Invalid: max_size must be >= send_batch_size
		}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid config for processor \"batch\"")

		logs := observedLogs.All()
		require.Len(t, logs, 1)
		assert.Equal(t, "batch", logs[0].ContextMap()["component_id"])
	})

	t.Run("Nil-map safety: empty or nil component config does not panic", func(t *testing.T) {
		pv := NewPreValidator(reg, zap.NewNop())

		conf := validBaseTopology()
		// Setting nil map like `batch:` in YAML
		conf["processors"].(map[string]any)["batch"] = nil

		assert.NotPanics(t, func() {
			_ = pv.Validate(conf)
		})
	})

	t.Run("Component config is not a valid map fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["receivers"].(map[string]any)["otlp"] = 123

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "receiver \"otlp\" configuration is not a valid map")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Pipeline configuration is not a valid map fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"].(map[string]any)["pipelines"].(map[string]any)["metrics"] = "invalid"

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline metrics configuration is invalid")
		require.Len(t, observedLogs.All(), 1)
	})

	t.Run("Service section is nil or not a map fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		conf["service"] = "not_a_map"

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service configuration is invalid or missing")

		conf["service"] = nil
		err = pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service configuration is invalid or missing")
		require.NotEmpty(t, observedLogs.All())
	})

	t.Run("Unmarshal error in component config fails", func(t *testing.T) {
		observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
		pv := NewPreValidator(reg, zap.New(observedZapCore))

		conf := validBaseTopology()
		// batch processor send_batch_size expects int, provide incompatible nested map
		conf["processors"].(map[string]any)["batch"] = map[string]any{
			"send_batch_size": map[string]any{"incompatible": "type"},
		}

		err := pv.Validate(conf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal config for processor \"batch\"")
		require.Len(t, observedLogs.All(), 1)
	})
}

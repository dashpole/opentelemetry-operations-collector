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

package driver

import (
	"google.golang.org/protobuf/proto"
)

const (
	TypeURLGcpDestination      = "type.googleapis.com/google.telemetry.policy.v1alpha1.GcpDestinationPolicy"
	TypeURLGcpDestinationAlias = "gcp_destination@v1"
)

// GcpDestinationDriver generates collector configuration for exporting to Google Cloud.
type GcpDestinationDriver struct{}

// NewGcpDestinationDriver creates a new GcpDestinationDriver.
func NewGcpDestinationDriver() *GcpDestinationDriver {
	return &GcpDestinationDriver{}
}

func (d *GcpDestinationDriver) TypeURL() string {
	return TypeURLGcpDestination
}

func (d *GcpDestinationDriver) Class() PolicyClass {
	return PolicyClassDestination
}

func (d *GcpDestinationDriver) Validate(policy proto.Message) error {
	// GcpDestinationPolicy has sensible defaults for all fields; any proto payload is valid.
	return nil
}

func (d *GcpDestinationDriver) GenerateConfig(policy proto.Message, ctx *CompilationContext) (*ConfigFragment, error) {
	frag := NewConfigFragment()

	exporterName := "otlp/gcp_destination"
	metricBatchName := "batch/gcp_destination_metrics"
	logBatchName := "batch/gcp_destination_logs"
	traceBatchName := "batch/gcp_destination_traces"

	frag.Exporters[exporterName] = map[string]any{
		"endpoint": "telemetry.googleapis.com:443",
		"auth": map[string]any{
			"authenticator": "googleclientauth",
		},
	}

	frag.Extensions["googleclientauth"] = map[string]any{}
	frag.ServiceExts = append(frag.ServiceExts, "googleclientauth")

	// Tuned batch sizes per UTP sizing guidelines
	frag.Processors[metricBatchName] = map[string]any{
		"send_batch_size":     200,
		"send_batch_max_size": 200,
		"timeout":             "5s",
	}
	frag.Processors[logBatchName] = map[string]any{
		"send_batch_size":     8192,
		"send_batch_max_size": 8192,
		"timeout":             "1s",
	}
	frag.Processors[traceBatchName] = map[string]any{
		"send_batch_size":     25000,
		"send_batch_max_size": 25000,
		"timeout":             "5s",
	}

	if ctx != nil && ctx.ResolvedTokens != nil {
		ctx.ResolvedTokens["active_destination_exporter"] = exporterName
		ctx.ResolvedTokens["active_destination_metric_preprocess"] = metricBatchName
	}

	return frag, nil
}

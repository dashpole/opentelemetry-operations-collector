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

package ingestor

import (
	"context"

	xdsv1alpha1 "github.com/GoogleCloudPlatform/opentelemetry-operations-collector/gen/go/xds/v1alpha1"
	status "google.golang.org/genproto/googleapis/rpc/status"
)

// PolicyUpdate represents an atomic update of telemetry policies received from a control plane or local file.
type PolicyUpdate struct {
	Revision       string
	RevisionNumber int64
	Nonce          string
	Collector      *xdsv1alpha1.TelemetryCollector
}

// PolicyAck represents an acknowledgment (ACK) or negative acknowledgment (NACK) sent back to the transport.
// ErrorDetail is nil on ACK, or populated with gRPC status error details on NACK.
type PolicyAck struct {
	Revision    string
	Nonce       string
	ErrorDetail *status.Status
}

// PolicyIngestor is the transport-agnostic interface for ingesting telemetry policy sets and acknowledging them.
type PolicyIngestor interface {
	// Start begins streaming or watching policy updates. The onUpdate callback is invoked whenever a new policy set arrives.
	Start(ctx context.Context, onUpdate func(PolicyUpdate) error) error

	// Stop terminates the ingestor and releases all associated background resources.
	Stop(ctx context.Context) error

	// Acknowledge communicates the processing outcome (ACK or NACK) back to the control plane.
	Acknowledge(ctx context.Context, ack PolicyAck) error
}

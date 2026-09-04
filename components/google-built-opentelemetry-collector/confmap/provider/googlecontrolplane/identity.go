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
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// NamespaceUUID is the canonical RFC 4122 DNS namespace UUID used for deterministic v5 generation.
var NamespaceUUID = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

// IdentityResolver deterministically resolves the collector instance ID.
type IdentityResolver struct {
	fleetID        string
	envLookup      func(string) string
	hostIDFunc     func() (string, error)
	fileReader     func(string) ([]byte, error)
	metadataGetter func(string) (string, error)
}

// IdentityOption configures an IdentityResolver.
type IdentityOption func(*IdentityResolver)

// WithEnvLookup sets the environment variable lookup function.
func WithEnvLookup(f func(string) string) IdentityOption {
	return func(r *IdentityResolver) {
		if f != nil {
			r.envLookup = f
		}
	}
}

// WithHostIDFunc sets the host ID detection function.
func WithHostIDFunc(f func() (string, error)) IdentityOption {
	return func(r *IdentityResolver) {
		if f != nil {
			r.hostIDFunc = f
		}
	}
}

// WithFileReader sets the file reader function for platform resource detection.
func WithFileReader(f func(string) ([]byte, error)) IdentityOption {
	return func(r *IdentityResolver) {
		if f != nil {
			r.fileReader = f
		}
	}
}

// WithMetadataGetter sets the GCE metadata getter function.
func WithMetadataGetter(f func(string) (string, error)) IdentityOption {
	return func(r *IdentityResolver) {
		if f != nil {
			r.metadataGetter = f
		}
	}
}

// NewIdentityResolver creates a new IdentityResolver for the specified fleet ID.
func NewIdentityResolver(fleetID string, opts ...IdentityOption) *IdentityResolver {
	r := &IdentityResolver{
		fleetID:    fleetID,
		envLookup:  os.Getenv,
		fileReader: os.ReadFile,
		metadataGetter: func(url string) (string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return "", err
			}
			req.Header.Set("Metadata-Flavor", "Google")
			client := &http.Client{Timeout: 200 * time.Millisecond}
			resp, err := client.Do(req)
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("metadata server returned status %d", resp.StatusCode)
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(string(data)), nil
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// FleetID returns the configured fleet ID.
func (r *IdentityResolver) FleetID() string {
	return r.fleetID
}

// ResolveID deterministically resolves the collector instance ID following the 3-tier precedence:
// 1. COLLECTOR_ID environment variable (if valid UUID)
// 2. COLLECTOR_NAME environment variable (UUID v5 with NamespaceUUID)
// 3. Platform resource detection fallback (Pod UID -> GCE Instance ID -> machine-id -> hostname + fleetID)
func (r *IdentityResolver) ResolveID() (string, error) {
	// Tier 1: Explicit UUID via COLLECTOR_ID
	if explicitID := strings.TrimSpace(r.envLookup("COLLECTOR_ID")); explicitID != "" {
		if _, err := uuid.Parse(explicitID); err == nil {
			return explicitID, nil
		}
	}

	// Tier 2: Deterministic Seed String via COLLECTOR_NAME
	if seedName := strings.TrimSpace(r.envLookup("COLLECTOR_NAME")); seedName != "" {
		return uuid.NewSHA1(NamespaceUUID, []byte(seedName)).String(), nil
	}

	// Tier 3: Platform Resource Detection Fallback
	platformID := r.detectPlatformID()
	seed := platformID
	if r.fleetID != "" {
		seed = platformID + ":" + r.fleetID
	}
	if seed == "" {
		seed = "default-collector"
	}
	return uuid.NewSHA1(NamespaceUUID, []byte(seed)).String(), nil
}

// Resolve is a convenience method that returns the resolved ID, falling back to a default UUID on any unexpected error.
func (r *IdentityResolver) Resolve() string {
	id, err := r.ResolveID()
	if err != nil {
		return uuid.NewSHA1(NamespaceUUID, []byte("fallback:"+r.fleetID)).String()
	}
	return id
}

func (r *IdentityResolver) detectPlatformID() string {
	if r.hostIDFunc != nil {
		if id, err := r.hostIDFunc(); err == nil && strings.TrimSpace(id) != "" {
			return strings.TrimSpace(id)
		}
	}

	// 1. Kubernetes Pod UID via env var POD_UID
	if podUID := strings.TrimSpace(r.envLookup("POD_UID")); podUID != "" {
		return podUID
	}

	// 2. Kubernetes Pod UID via serviceaccount file
	if data, err := r.fileReader("/var/run/secrets/kubernetes.io/serviceaccount/pod.uid"); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id
		}
	}

	// 3. GCE Instance ID via metadata server
	if r.metadataGetter != nil {
		if id, err := r.metadataGetter("http://metadata.google.internal/computeMetadata/v1/instance/id"); err == nil && id != "" {
			return id
		}
	}

	// 4. Linux machine-id
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id", "/sys/class/dmi/id/product_uuid"} {
		if data, err := r.fileReader(path); err == nil {
			if id := strings.TrimSpace(string(data)); id != "" {
				return id
			}
		}
	}

	// 5. Hostname fallback
	if hostname, err := os.Hostname(); err == nil && strings.TrimSpace(hostname) != "" {
		return strings.TrimSpace(hostname)
	}

	return "unknown-host"
}

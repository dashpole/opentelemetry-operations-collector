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
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityResolver(t *testing.T) {
	t.Run("Tier 1: Explicit valid UUID", func(t *testing.T) {
		validUUID := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
		resolver := NewIdentityResolver("fleet-123",
			WithEnvLookup(func(key string) string {
				if key == "COLLECTOR_ID" {
					return validUUID
				}
				return ""
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, validUUID, id)
	})

	t.Run("Tier 1: Invalid UUID falls back to Tier 2", func(t *testing.T) {
		invalidUUID := "not-a-valid-uuid"
		seedName := "my-collector-instance-1"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(seedName)).String()

		resolver := NewIdentityResolver("fleet-123",
			WithEnvLookup(func(key string) string {
				switch key {
				case "COLLECTOR_ID":
					return invalidUUID
				case "COLLECTOR_NAME":
					return seedName
				default:
					return ""
				}
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Tier 2: Seed string COLLECTOR_NAME", func(t *testing.T) {
		seedName := "test-agent-alpha"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(seedName)).String()

		resolver := NewIdentityResolver("fleet-123",
			WithEnvLookup(func(key string) string {
				if key == "COLLECTOR_NAME" {
					return seedName
				}
				return ""
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)

		// Determinism check: running multiple times yields same UUID
		id2, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, id, id2)
	})

	t.Run("Tier 3: Platform HostID function with fleetID", func(t *testing.T) {
		fleetID := "fleet-xyz"
		platformID := "node-hardware-999"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(platformID+":"+fleetID)).String()

		resolver := NewIdentityResolver(fleetID,
			WithEnvLookup(func(string) string { return "" }),
			WithHostIDFunc(func() (string, error) {
				return platformID, nil
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Tier 3: Pod UID from environment", func(t *testing.T) {
		podUID := "6ec96582-74b8-4d56-b072-23f2081d6f55"
		fleetID := "fleet-k8s"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(podUID+":"+fleetID)).String()

		resolver := NewIdentityResolver(fleetID,
			WithEnvLookup(func(key string) string {
				if key == "POD_UID" {
					return podUID
				}
				return ""
			}),
			WithHostIDFunc(func() (string, error) {
				return "", errors.New("not available")
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Tier 3: Pod UID from serviceaccount file", func(t *testing.T) {
		podUID := "pod-file-uuid-777"
		fleetID := "fleet-file"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(podUID+":"+fleetID)).String()

		resolver := NewIdentityResolver(fleetID,
			WithEnvLookup(func(string) string { return "" }),
			WithFileReader(func(path string) ([]byte, error) {
				if path == "/var/run/secrets/kubernetes.io/serviceaccount/pod.uid" {
					return []byte(podUID + "\n"), nil
				}
				return nil, errors.New("not found")
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Tier 3: GCE Instance ID from metadata", func(t *testing.T) {
		instanceID := "gce-instance-5544332211"
		fleetID := "fleet-gce"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(instanceID+":"+fleetID)).String()

		resolver := NewIdentityResolver(fleetID,
			WithEnvLookup(func(string) string { return "" }),
			WithFileReader(func(string) ([]byte, error) {
				return nil, errors.New("not found")
			}),
			WithMetadataGetter(func(url string) (string, error) {
				if url == "http://metadata.google.internal/computeMetadata/v1/instance/id" {
					return instanceID, nil
				}
				return "", errors.New("not found")
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Tier 3: machine-id file", func(t *testing.T) {
		machineID := "b59a6bc80d324d6bb0e9e1c31278f9f2"
		fleetID := "fleet-linux"
		expectedUUID := uuid.NewSHA1(NamespaceUUID, []byte(machineID+":"+fleetID)).String()

		resolver := NewIdentityResolver(fleetID,
			WithEnvLookup(func(string) string { return "" }),
			WithMetadataGetter(func(string) (string, error) {
				return "", errors.New("not found")
			}),
			WithFileReader(func(path string) ([]byte, error) {
				if path == "/etc/machine-id" {
					return []byte(machineID + "\n"), nil
				}
				return nil, errors.New("not found")
			}),
		)

		id, err := resolver.ResolveID()
		require.NoError(t, err)
		assert.Equal(t, expectedUUID, id)
	})

	t.Run("Determinism across instances", func(t *testing.T) {
		r1 := NewIdentityResolver("fleet-det",
			WithEnvLookup(func(key string) string {
				if key == "COLLECTOR_NAME" {
					return "deterministic-agent"
				}
				return ""
			}),
		)
		r2 := NewIdentityResolver("fleet-det",
			WithEnvLookup(func(key string) string {
				if key == "COLLECTOR_NAME" {
					return "deterministic-agent"
				}
				return ""
			}),
		)

		id1 := r1.Resolve()
		id2 := r2.Resolve()
		assert.Equal(t, id1, id2)
		assert.Equal(t, "fleet-det", r1.FleetID())
	})
}

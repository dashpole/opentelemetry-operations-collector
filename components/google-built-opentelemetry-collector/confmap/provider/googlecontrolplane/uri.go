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
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Scheme is the URI scheme identifier for the googlecontrolplane provider.
const Scheme = "googlecontrolplane"

// Supported transport schemes for the googlecontrolplane provider.
const (
	TransportXDS  = "xds"
	TransportFile = "file"
)

// ParsedURI encapsulates the parsed configuration, target transport, and options from a
// googlecontrolplane connection string.
type ParsedURI struct {
	Raw            string
	Transport      string        // TransportXDS ("xds") or TransportFile ("file")
	Endpoint       string        // Remote host:port address for xDS transport
	Path           string        // Local filesystem path for file transport
	FleetID        string        // Query parameter: gcp.fleet_id
	Project        string        // Query parameter: project
	BaseConfigURI  string        // Query parameter: base_config
	StartupTimeout time.Duration // Query parameter: startup_timeout
	QueryParams    url.Values    // Raw query parameters
}

// ParseURI parses and validates a raw googlecontrolplane URI.
//
// Supported URI formats:
//   - googlecontrolplane:xds://<endpoint>[?<query>]
//   - googlecontrolplane://xds://<endpoint>[?<query>]
//   - googlecontrolplane:file://<path>[?<query>]
//   - googlecontrolplane://file://<path>[?<query>]
//   - googlecontrolplane://<endpoint>[?<query>] (implicit xds)
//   - googlecontrolplane:///<path>[?<query>]    (implicit file)
//   - googlecontrolplane:/<path>[?<query>]      (implicit file)
//
// Recognized query parameters:
//   - gcp.fleet_id: Identifier of the collector fleet (maps to xDS Node.Cluster)
//   - project: GCP project ID/number
//   - base_config: Path or URI to a fallback/base configuration
//   - startup_timeout: Initial connection & policy receipt grace period (e.g. 50ms, 15s)
func ParseURI(rawURI string) (*ParsedURI, error) {
	if !strings.HasPrefix(rawURI, Scheme+":") {
		return nil, fmt.Errorf("invalid scheme: expected URI to start with %q, got %q", Scheme+":", rawURI)
	}

	rest := strings.TrimPrefix(rawURI, Scheme+":")

	// Extract query string if present
	pathPart, queryString, hasQuery := strings.Cut(rest, "?")

	var queryVals url.Values
	if hasQuery {
		var err error
		queryVals, err = url.ParseQuery(queryString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse query string in URI %q: %w", rawURI, err)
		}
	} else {
		queryVals = make(url.Values)
	}

	fleetID := queryVals.Get("gcp.fleet_id")
	project := queryVals.Get("project")
	baseConfig := queryVals.Get("base_config")

	var startupTimeout time.Duration
	if timeoutStr := queryVals.Get("startup_timeout"); timeoutStr != "" {
		d, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid startup_timeout duration %q: %w", timeoutStr, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("startup_timeout duration must be non-negative, got %v", d)
		}
		startupTimeout = d
	}

	var transport string
	var endpoint string
	var path string

	// Handle explicit sub-schemes or implicit paths
	work := strings.TrimPrefix(pathPart, "//")

	if strings.HasPrefix(work, "xds:") {
		transport = TransportXDS
		sub := strings.TrimPrefix(work, "xds:")
		endpoint = strings.TrimLeft(sub, "/")
	} else if strings.HasPrefix(work, "file:") {
		transport = TransportFile
		sub := strings.TrimPrefix(work, "file:")
		if strings.HasPrefix(sub, "///") {
			path = strings.TrimPrefix(sub, "//")
		} else if strings.HasPrefix(sub, "//") {
			trimmedSub := strings.TrimPrefix(sub, "//")
			if trimmedSub != "" && !strings.HasPrefix(trimmedSub, "/") {
				path = "/" + trimmedSub
			} else {
				path = trimmedSub
			}
		} else {
			path = sub
		}
	} else if strings.HasPrefix(pathPart, "///") {
		// e.g. googlecontrolplane:///path/to/dir -> file transport
		transport = TransportFile
		path = strings.TrimPrefix(pathPart, "//")
	} else if strings.HasPrefix(pathPart, "//") {
		// e.g. googlecontrolplane://localhost:50051 -> host:port for xDS
		if strings.Contains(work, "://") {
			idx := strings.Index(work, "://")
			subScheme := work[:idx]
			return nil, fmt.Errorf("unsupported transport scheme %q in URI: %s", subScheme, rawURI)
		}
		transport = TransportXDS
		endpoint = work
	} else if strings.HasPrefix(pathPart, "/") {
		// e.g. googlecontrolplane:/path/to/dir -> file transport
		transport = TransportFile
		path = pathPart
	} else if strings.Contains(work, "://") {
		// Unsupported scheme like ftp:// or http://
		idx := strings.Index(work, "://")
		subScheme := work[:idx]
		return nil, fmt.Errorf("unsupported transport scheme %q in URI: %s", subScheme, rawURI)
	} else {
		// Implicit xDS endpoint if host/port provided without scheme
		if work != "" {
			transport = TransportXDS
			endpoint = work
		}
	}

	switch transport {
	case TransportXDS:
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return nil, fmt.Errorf("xds endpoint cannot be empty in URI: %s", rawURI)
		}
	case TransportFile:
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, fmt.Errorf("file path cannot be empty in URI: %s", rawURI)
		}
	default:
		return nil, fmt.Errorf("could not determine transport from URI: %s", rawURI)
	}

	return &ParsedURI{
		Raw:            rawURI,
		Transport:      transport,
		Endpoint:       endpoint,
		Path:           path,
		FleetID:        fleetID,
		Project:        project,
		BaseConfigURI:  baseConfig,
		StartupTimeout: startupTimeout,
		QueryParams:    queryVals,
	}, nil
}

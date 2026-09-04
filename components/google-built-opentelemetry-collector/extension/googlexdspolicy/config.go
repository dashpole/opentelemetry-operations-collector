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

package googlexdspolicy

import (
	"errors"
	"strings"
	"time"
)

// DefaultStartupTimeout is the default duration to wait for xDS client initialization.
const DefaultStartupTimeout = 5 * time.Second

// Config defines configuration parameters for googlexdspolicy extension.
type Config struct {
	Endpoint          string        `mapstructure:"endpoint"`
	FleetID           string        `mapstructure:"fleet_id"`
	ProjectID         string        `mapstructure:"project_id"`
	CollectorID       string        `mapstructure:"collector_id"`
	ServerAuthority   string        `mapstructure:"server_authority"`
	CACertPath        string        `mapstructure:"ca_cert_path"`
	ClientCertPath    string        `mapstructure:"client_cert_path"`
	ClientKeyPath     string        `mapstructure:"client_key_path"`
	Insecure          bool          `mapstructure:"insecure"`
	StartupTimeout    time.Duration `mapstructure:"startup_timeout"`
	SupportedTypeURLs []string      `mapstructure:"supported_type_urls"`
}

// Validate checks whether the configuration is valid.
func (cfg *Config) Validate() error {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return errors.New("endpoint must not be empty")
	}
	if cfg.StartupTimeout < 0 {
		return errors.New("startup_timeout must not be negative")
	}
	hasCert := strings.TrimSpace(cfg.ClientCertPath) != ""
	hasKey := strings.TrimSpace(cfg.ClientKeyPath) != ""
	if (hasCert && !hasKey) || (!hasCert && hasKey) {
		return errors.New("client_cert_path and client_key_path must both be set or both be empty")
	}
	return nil
}

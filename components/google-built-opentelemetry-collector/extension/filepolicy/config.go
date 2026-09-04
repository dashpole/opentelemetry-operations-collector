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

package filepolicy

import (
	"errors"
	"strings"
	"time"
)

// DefaultDebounceInterval is the default filesystem debounce interval.
const DefaultDebounceInterval = 50 * time.Millisecond

// Config defines configuration parameters for filepolicy extension.
type Config struct {
	Path             string        `mapstructure:"path"`
	DebounceInterval time.Duration `mapstructure:"debounce_interval"`
}

// Validate checks whether the configuration is valid.
func (cfg *Config) Validate() error {
	if strings.TrimSpace(cfg.Path) == "" {
		return errors.New("path must not be empty")
	}
	if cfg.DebounceInterval < 0 {
		return errors.New("debounce_interval must not be negative")
	}
	return nil
}

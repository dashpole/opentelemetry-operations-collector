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
	"strings"
	"sync"

	"go.opentelemetry.io/collector/confmap"
)

// EscapeHatchResolver manages resolution of dynamic component identifiers referenced via
// ${googlecontrolplane:component//...} or ${googlecontrolplane:component://...}.
type EscapeHatchResolver struct {
	mu             sync.RWMutex
	resolvedTokens map[string]string
}

// NewEscapeHatchResolver creates a new EscapeHatchResolver.
func NewEscapeHatchResolver() *EscapeHatchResolver {
	return &EscapeHatchResolver{
		resolvedTokens: make(map[string]string),
	}
}

// UpdateTokens updates the internal mapping of token names to component IDs.
func (r *EscapeHatchResolver) UpdateTokens(tokens map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resolvedTokens = make(map[string]string, len(tokens))
	for k, v := range tokens {
		r.resolvedTokens[k] = v
	}
}

// ResolveToken resolves a single token name to a *confmap.Retrieved wrapping a scalar string.
func (r *EscapeHatchResolver) ResolveToken(token string) (*confmap.Retrieved, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Normalize token by trimming leading slashes
	token = strings.TrimPrefix(token, "/")

	name, ok := r.resolvedTokens[token]
	if !ok {
		return nil, fmt.Errorf("unknown component token: %q", token)
	}

	// Return scalar string so retrieved.AsString() returns the component name cleanly
	return confmap.NewRetrieved(name)
}

// ResolveURI inspects a URI and, if it matches a component token pattern, resolves it.
// Returns (retrieved, isToken, error).
func (r *EscapeHatchResolver) ResolveURI(uri string) (*confmap.Retrieved, bool, error) {
	cleanURI := strings.TrimPrefix(uri, "googlecontrolplane:")
	cleanURI = strings.TrimPrefix(cleanURI, "//")

	var token string
	if strings.HasPrefix(cleanURI, "component//") {
		token = strings.TrimPrefix(cleanURI, "component//")
	} else if strings.HasPrefix(cleanURI, "component://") {
		token = strings.TrimPrefix(cleanURI, "component://")
	} else {
		return nil, false, nil
	}

	retrieved, err := r.ResolveToken(token)
	return retrieved, true, err
}

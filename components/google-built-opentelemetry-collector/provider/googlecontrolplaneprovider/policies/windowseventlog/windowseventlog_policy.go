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

package windowseventlog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-collector/pkg/googlepolicy"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/pipelines"
)

func init() {
	if err := googlepolicy.RegisterPolicyDriver(PolicyType, &googlepolicy.GenericDriver[*WindowsEventLogSourcePolicy]{}); err != nil {
		panic(err)
	}
}

// PolicyType is the registered policy type identifier for Windows Event Log source policies.
const PolicyType = "windows_event_log"

const (
	receiverTypeStr = "windowseventlog"

	// DefaultSuppressRenderingInfo disables synchronous EvtFormatMessage IPC calls
	// (3.56 ms/event -> 77.9 us/event via EvtRenderEventXML), raising the single-channel
	// throughput ceiling from ~280 events/sec to ~12,830 events/sec while preserving
	// all <System> and <EventData>/<UserData> fields.
	DefaultSuppressRenderingInfo = true

	// DefaultEventDrivenScraping enables Win32 WaitForMultipleObjects push signaling
	// on EvtSubscribe instead of 1-second timer polling.
	DefaultEventDrivenScraping = true

	// DefaultMaxReads increases the per-cycle EvtNext batch size and file_storage
	// (bbolt) bookmark persistence interval from 100 to 500 events.
	DefaultMaxReads = 500

	// DefaultIgnoreChannelErrors implements fail-open behavior when a configured
	// channel does not exist on a subset of fleet VMs.
	DefaultIgnoreChannelErrors = true
)

var (
	// ErrMissingPolicyName is returned when neither id nor name is set on the policy.
	ErrMissingPolicyName = errors.New("policy must be named (missing id or name)")
	// ErrConflictingPolicyNames is returned when both id and name are set to different values.
	ErrConflictingPolicyNames = errors.New("policy id and name cannot be set to different values")
	// ErrMissingChannels is returned when the channels slice is empty.
	ErrMissingChannels = errors.New("windows_event_log policy must specify at least one channel")
	// ErrEmptyChannel is returned when any channel entry is empty or whitespace-only.
	ErrEmptyChannel = errors.New("channel name cannot be empty")
	// ErrInvalidChannelName is returned when a channel name contains the confmap key delimiter "::".
	ErrInvalidChannelName = errors.New("channel name cannot contain confmap delimiter '::'")
)

type seenChannelsContextKey struct{}

// SeenChannels tracks canonical lowercase channel keys that have already been
// claimed by an earlier WindowsEventLogSourcePolicy in the same PolicySet evaluation,
// mapping lowercase channel key -> first-seen trimmed channel name.
type SeenChannels struct {
	claimed map[string]string
}

// NewSeenChannels creates a new SeenChannels tracker for a PolicySet evaluation.
func NewSeenChannels() *SeenChannels {
	return &SeenChannels{
		claimed: make(map[string]string),
	}
}

// WithSeenChannels returns a child context carrying the given SeenChannels tracker
// (or a fresh one if seen is nil) so multiple WindowsEventLogSourcePolicy instances
// evaluated in a single PolicySet deduplicate channels case-insensitively and
// preserve first-seen channel casing.
func WithSeenChannels(ctx context.Context, seen *SeenChannels) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if seen == nil {
		seen = NewSeenChannels()
	}
	return context.WithValue(ctx, seenChannelsContextKey{}, seen)
}

func seenChannelsFromContext(ctx context.Context) *SeenChannels {
	if ctx == nil {
		return nil
	}
	seen, _ := ctx.Value(seenChannelsContextKey{}).(*SeenChannels)
	return seen
}

// WindowsEventLogSourcePolicy compiles a declarative list of Windows Event Log
// channels into high-throughput windowseventlog/<channel> receiver configurations.
type WindowsEventLogSourcePolicy struct {
	ID       string   `mapstructure:"id"`
	Name     string   `mapstructure:"name"`
	Channels []string `mapstructure:"channels"`
}

var _ googlepolicy.SourcePolicy = (*WindowsEventLogSourcePolicy)(nil)

// PolicyName returns the unique identifier of this policy instance, preferring
// the protobuf field "id" and falling back to "name".
func (p *WindowsEventLogSourcePolicy) PolicyName() string {
	if p.ID != "" {
		return p.ID
	}
	return p.Name
}

// PolicyType returns the registered policy type ("windows_event_log").
func (p *WindowsEventLogSourcePolicy) PolicyType() string {
	return PolicyType
}

// PolicyClass returns PolicyClassSource.
func (p *WindowsEventLogSourcePolicy) PolicyClass() googlepolicy.PolicyClass {
	return googlepolicy.PolicyClassSource
}

// Validate checks that the policy has a valid identifier and at least one
// non-empty channel name free of confmap key delimiters.
func (p *WindowsEventLogSourcePolicy) Validate() error {
	if p.ID != "" && p.Name != "" && p.ID != p.Name {
		return fmt.Errorf("%w: id=%q, name=%q", ErrConflictingPolicyNames, p.ID, p.Name)
	}
	if strings.TrimSpace(p.PolicyName()) == "" {
		return ErrMissingPolicyName
	}
	if len(p.Channels) == 0 {
		return ErrMissingChannels
	}
	for i, ch := range p.Channels {
		trimmed := strings.TrimSpace(ch)
		if trimmed == "" {
			return fmt.Errorf("channels[%d]: %w", i, ErrEmptyChannel)
		}
		if strings.Contains(trimmed, "::") {
			return fmt.Errorf("channels[%d]: %w: %q", i, ErrInvalidChannelName, ch)
		}
	}
	return nil
}

// DeduplicatedChannels returns the policy's trimmed channels deduplicated
// case-insensitively in first-seen order, preserving first-seen casing.
func (p *WindowsEventLogSourcePolicy) DeduplicatedChannels() []string {
	return DeduplicateChannels(p.Channels)
}

// DeduplicateChannels deduplicates a slice of channel names case-insensitively,
// trimming leading/trailing whitespace and preserving first-seen casing and order.
func DeduplicateChannels(channels []string) []string {
	seen := make(map[string]struct{}, len(channels))
	out := make([]string, 0, len(channels))
	for _, ch := range channels {
		trimmed := strings.TrimSpace(ch)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// Evaluate synthesizes windowseventlog/<lowercase_channel> receiver configurations
// with GBOC's high-throughput defaults. When ctx carries WithSeenChannels(ctx),
// channels already claimed by an earlier policy in the same PolicySet retain
// their first-seen channel casing so confmap.Merge never overwrites it.
func (p *WindowsEventLogSourcePolicy) Evaluate(ctx context.Context) (*confmap.Conf, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	receiverType, err := component.NewType(receiverTypeStr)
	if err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: invalid receiver type %q: %w", p.PolicyName(), receiverTypeStr, err)
	}

	seen := seenChannelsFromContext(ctx)
	receivers := make(map[component.ID]component.Config)

	for _, ch := range p.DeduplicatedChannels() {
		key := strings.ToLower(ch)
		channelName := ch
		if seen != nil {
			if firstSeen, exists := seen.claimed[key]; exists {
				channelName = firstSeen
			} else {
				seen.claimed[key] = ch
			}
		}

		recID := component.NewIDWithName(receiverType, key)
		receivers[recID] = component.Config(map[string]any{
			"channel":                 channelName,
			"suppress_rendering_info": DefaultSuppressRenderingInfo,
			"event_driven_scraping":   DefaultEventDrivenScraping,
			"max_reads":               DefaultMaxReads,
			"ignore_channel_errors":   DefaultIgnoreChannelErrors,
		})
	}

	conf := &otelcol.Config{
		Receivers: receivers,
	}

	cm := confmap.New()
	if err := cm.Marshal(conf); err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: marshaling config got error '%w'", p.PolicyName(), err)
	}
	return cm, nil
}

// LogsPipelines wires all deduplicated windowseventlog/<lowercase_channel>
// receivers into the shared logs/windows_event_log pipeline. When multiple
// WindowsEventLogSourcePolicy instances are active, confmap.Merge (with
// confmap.enableMergeAppendOption enabled by the provider) unions and
// deduplicates the pipeline's receivers slice.
func (p *WindowsEventLogSourcePolicy) LogsPipelines(preExportProcessors []component.ID, exporters []component.ID, _ []component.ID) (*confmap.Conf, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	receiverType, err := component.NewType(receiverTypeStr)
	if err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: invalid receiver type %q: %w", p.PolicyName(), receiverTypeStr, err)
	}

	deduped := p.DeduplicatedChannels()
	receiverIDs := make([]component.ID, 0, len(deduped))
	for _, ch := range deduped {
		receiverIDs = append(receiverIDs, component.NewIDWithName(receiverType, strings.ToLower(ch)))
	}

	pipeID := pipeline.NewIDWithName(pipeline.SignalLogs, PolicyType)
	conf := &otelcol.Config{
		Service: service.Config{
			Pipelines: pipelines.Config{
				pipeID: &pipelines.PipelineConfig{
					Receivers:  receiverIDs,
					Processors: preExportProcessors,
					Exporters:  exporters,
				},
			},
		},
	}

	cm := confmap.New()
	if err := cm.Marshal(conf); err != nil {
		return nil, fmt.Errorf("policy implementation failure for %s: marshaling logs pipeline got error '%w'", p.PolicyName(), err)
	}
	return cm, nil
}

// MetricsPipelines returns nil because WindowsEventLogSourcePolicy only produces logs.
func (p *WindowsEventLogSourcePolicy) MetricsPipelines(_ []component.ID, _ []component.ID, _ []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

// TracesPipelines returns nil because WindowsEventLogSourcePolicy only produces logs.
func (p *WindowsEventLogSourcePolicy) TracesPipelines(_ []component.ID, _ []component.ID, _ []component.ID) (*confmap.Conf, error) {
	return nil, nil
}

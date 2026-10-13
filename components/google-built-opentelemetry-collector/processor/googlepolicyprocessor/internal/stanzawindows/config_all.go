// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package stanzawindows contains the verbatim cross-platform Windows Event XML
// unmarshaling (xml.go, security.go, config_all.go) and pcommon.Value conversion
// (converter.go) from upstream github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza@v0.160.0.
package stanzawindows

// EventDataFormat controls the structure of the event_data field in the log body.
type EventDataFormat string

const (
	// EventDataFormatMap emits event_data as a flat map with named Data elements
	// as direct keys and anonymous Data elements numbered as param1, param2, etc.
	EventDataFormatMap EventDataFormat = "map"
	// EventDataFormatArray emits event_data with a nested "data" array of
	// single-key maps, preserving the original format.
	EventDataFormatArray EventDataFormat = "array"
)

// Severity mirrors github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry.Severity.
type Severity int

const (
	Default Severity = 0
	Info    Severity = 9
	Warn    Severity = 13
	Error   Severity = 17
	Fatal   Severity = 21
)

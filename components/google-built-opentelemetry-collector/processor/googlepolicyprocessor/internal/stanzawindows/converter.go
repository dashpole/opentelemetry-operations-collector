// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stanzawindows

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// UpsertToAttributeVal converts a Stanza entry body/attribute value (including
// []string slices produced for keywords and details["Additional Context"]) into
// a pcommon.Value, matching pkg/stanza/adapter/converter.go.
func UpsertToAttributeVal(value any, dest pcommon.Value) {
	upsertToAttributeVal(value, dest)
}

// PopulateLogRecord reproduces the exact sendEvent (pkg/stanza/operator/input/windows/input.go:434-456)
// and convertInto (pkg/stanza/adapter/converter.go:58-92) pipeline for a parsed Windows Event XML.
func PopulateLogRecord(dest plog.LogRecord, xmlBytes []byte, raw bool, format EventDataFormat) error {
	var (
		event ParsedEvent
		err   error
	)
	if raw {
		event, err = unmarshalRawEventXML(xmlBytes)
	} else {
		event, err = unmarshalEventXML(xmlBytes)
	}
	if err != nil {
		return err
	}

	var body any = event.getOriginal()
	if !raw {
		body = formattedBody(event.toEventXML(), format)
	}

	ts := parseTimestamp(event.getSystemTime())
	if !ts.IsZero() {
		dest.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	}
	dest.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts))
	sev := parseSeverity(event.getRenderedLevel(), event.getLevel())
	dest.SetSeverityNumber(sevMap[sev])
	dest.SetSeverityText(defaultSevTextMap[sev])
	upsertToAttributeVal(body, dest.Body())
	return nil
}

func upsertToAttributeVal(value any, dest pcommon.Value) {
	switch t := value.(type) {
	case bool:
		dest.SetBool(t)
	case string:
		dest.SetStr(t)
	case []string:
		upsertStringsToSlice(t, dest.SetEmptySlice())
	case []byte:
		dest.SetEmptyBytes().FromRaw(t)
	case int64:
		dest.SetInt(t)
	case int32:
		dest.SetInt(int64(t))
	case int16:
		dest.SetInt(int64(t))
	case int8:
		dest.SetInt(int64(t))
	case int:
		dest.SetInt(int64(t))
	case uint64:
		dest.SetInt(int64(t))
	case uint32:
		dest.SetInt(int64(t))
	case uint16:
		dest.SetInt(int64(t))
	case uint8:
		dest.SetInt(int64(t))
	case uint:
		dest.SetInt(int64(t))
	case float64:
		dest.SetDouble(t)
	case float32:
		dest.SetDouble(float64(t))
	case map[string]any:
		upsertToMap(t, dest.SetEmptyMap())
	case []any:
		upsertToSlice(t, dest.SetEmptySlice())
	case nil:
	default:
		dest.SetStr(fmt.Sprintf("%v", t))
	}
}

func upsertToMap(obsMap map[string]any, dest pcommon.Map) {
	dest.EnsureCapacity(len(obsMap))
	for k, v := range obsMap {
		upsertToAttributeVal(v, dest.PutEmpty(k))
	}
}

func upsertToSlice(obsArr []any, dest pcommon.Slice) {
	dest.EnsureCapacity(len(obsArr))
	for _, v := range obsArr {
		upsertToAttributeVal(v, dest.AppendEmpty())
	}
}

func upsertStringsToSlice(obsArr []string, dest pcommon.Slice) {
	dest.EnsureCapacity(len(obsArr))
	for _, v := range obsArr {
		dest.AppendEmpty().SetStr(v)
	}
}

var sevMap = map[Severity]plog.SeverityNumber{
	Default: plog.SeverityNumberUnspecified,
	Info:    plog.SeverityNumberInfo,
	Warn:    plog.SeverityNumberWarn,
	Error:   plog.SeverityNumberError,
	Fatal:   plog.SeverityNumberFatal,
}

var defaultSevTextMap = map[Severity]string{
	Default: "",
	Info:    "INFO",
	Warn:    "WARN",
	Error:   "ERROR",
	Fatal:   "FATAL",
}

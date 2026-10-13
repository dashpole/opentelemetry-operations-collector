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

package selfmetrics

import (
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Normative metric names emitted by upstream exporterhelper/internal/queue/obs_queue.go
// (with the "otelcol_" prefix added by the Collector's internal telemetry MeterProvider).
const (
	MetricExporterQueueSize                 = "otelcol_exporter_queue_size"
	MetricExporterQueueCapacity             = "otelcol_exporter_queue_capacity"
	MetricExporterEnqueueFailedLogRecords   = "otelcol_exporter_enqueue_failed_log_records"
	MetricExporterEnqueueFailedMetricPoints = "otelcol_exporter_enqueue_failed_metric_points"
	MetricExporterEnqueueFailedSpans        = "otelcol_exporter_enqueue_failed_spans"

	// Baseline 24-metric catalog error counters for comparison.
	MetricExporterSendFailedLogRecords   = "otelcol_exporter_send_failed_log_records"
	MetricExporterSendFailedMetricPoints = "otelcol_exporter_send_failed_metric_points"
	MetricExporterSendFailedSpans        = "otelcol_exporter_send_failed_spans"
	MetricExporterSentLogRecords         = "otelcol_exporter_sent_log_records"
	MetricReceiverRefusedLogRecords      = "otelcol_receiver_refused_log_records"
)

// ObsQueueMetricSpec describes the expected OTLP metric schema emitted by obsQueue in v0.162.0.
type ObsQueueMetricSpec struct {
	Name             string
	Type             pmetric.MetricType
	Unit             string
	IsMonotonic      bool
	RequiredAttrKeys []string
	ForbiddenAttrs   []string
}

// NormativeObsQueueSpecs returns the 5 queue observability metrics proposed for
// registration under agent.googleapis.com/agent/* in go/tango-gboc-self-o11y-hld.
// Note that upstream exporter/exporterhelper/metadata.yaml in v0.162.0 uses singular
// units "{record}", "{datapoint}", and "{span}" for enqueue_failed_* counters.
func NormativeObsQueueSpecs() []ObsQueueMetricSpec {
	return []ObsQueueMetricSpec{
		{
			Name:             MetricExporterQueueSize,
			Type:             pmetric.MetricTypeGauge,
			Unit:             "{batch}",
			RequiredAttrKeys: []string{"exporter", "data_type"},
		},
		{
			Name:             MetricExporterQueueCapacity,
			Type:             pmetric.MetricTypeGauge,
			Unit:             "{batch}",
			RequiredAttrKeys: []string{"exporter", "data_type"},
		},
		{
			Name:             MetricExporterEnqueueFailedLogRecords,
			Type:             pmetric.MetricTypeSum,
			Unit:             "{record}",
			IsMonotonic:      true,
			RequiredAttrKeys: []string{"exporter"},
			ForbiddenAttrs:   []string{"data_type"},
		},
		{
			Name:             MetricExporterEnqueueFailedMetricPoints,
			Type:             pmetric.MetricTypeSum,
			Unit:             "{datapoint}",
			IsMonotonic:      true,
			RequiredAttrKeys: []string{"exporter"},
			ForbiddenAttrs:   []string{"data_type"},
		},
		{
			Name:             MetricExporterEnqueueFailedSpans,
			Type:             pmetric.MetricTypeSum,
			Unit:             "{span}",
			IsMonotonic:      true,
			RequiredAttrKeys: []string{"exporter"},
			ForbiddenAttrs:   []string{"data_type"},
		},
	}
}

// ObservedMetricPoint captures a single observed OTLP metric data point and its schema metadata.
type ObservedMetricPoint struct {
	Name        string
	Description string
	Unit        string
	Type        pmetric.MetricType
	IsMonotonic bool
	Temporality pmetric.AggregationTemporality
	IntValue    int64
	Attributes  map[string]string
	Resource    map[string]string
}

// CatalogHealthView summarizes what an operator or alerting policy sees under
// the Baseline 24-Metric Catalog vs. the Proposed 29-Metric Catalog.
type CatalogHealthView struct {
	// Baseline catalog fields (send_failed_* and receiver_refused_* only)
	BaselineSendFailedLogs      int64
	BaselineReceiverRefusedLogs int64
	BaselineDetectsIssue        bool

	// Proposed catalog fields (adds queue_size, queue_capacity, enqueue_failed_*)
	ProposedQueueSize         int64
	ProposedQueueCapacity     int64
	ProposedQueueUtilization  float64
	ProposedEnqueueFailedLogs int64
	ProposedDetectsSaturation bool
	ProposedDetectsDrops      bool
}

// ComputeQueueUtilization returns the dimensionless queue utilization ratio
// (queue_size / queue_capacity) in [0.0, 1.0+], or NaN if capacity <= 0.
func ComputeQueueUtilization(queueSize, queueCapacity int64) float64 {
	if queueCapacity <= 0 {
		return math.NaN()
	}
	return float64(queueSize) / float64(queueCapacity)
}

// EvaluateCatalogHealth computes both the Baseline (24-metric catalog) and Proposed
// (with obsQueue metrics) health views for a target queue exporter ID and signal.
func EvaluateCatalogHealth(points []ObservedMetricPoint, queueExporterID, otlpExporterID, dataType string) CatalogHealthView {
	var view CatalogHealthView
	for _, pt := range points {
		switch pt.Name {
		case MetricExporterSendFailedLogRecords:
			if otlpExporterID == "" || pt.Attributes["exporter"] == otlpExporterID {
				if pt.IntValue > view.BaselineSendFailedLogs {
					view.BaselineSendFailedLogs = pt.IntValue
				}
			}
		case MetricReceiverRefusedLogRecords:
			if pt.IntValue > view.BaselineReceiverRefusedLogs {
				view.BaselineReceiverRefusedLogs = pt.IntValue
			}
		case MetricExporterQueueSize:
			if pt.Attributes["exporter"] == queueExporterID && pt.Attributes["data_type"] == dataType {
				view.ProposedQueueSize = pt.IntValue
			}
		case MetricExporterQueueCapacity:
			if pt.Attributes["exporter"] == queueExporterID && pt.Attributes["data_type"] == dataType {
				view.ProposedQueueCapacity = pt.IntValue
			}
		case MetricExporterEnqueueFailedLogRecords:
			if pt.Attributes["exporter"] == queueExporterID {
				if pt.IntValue > view.ProposedEnqueueFailedLogs {
					view.ProposedEnqueueFailedLogs = pt.IntValue
				}
			}
		}
	}

	view.BaselineDetectsIssue = (view.BaselineSendFailedLogs + view.BaselineReceiverRefusedLogs) > 0
	if view.ProposedQueueCapacity > 0 {
		view.ProposedQueueUtilization = ComputeQueueUtilization(view.ProposedQueueSize, view.ProposedQueueCapacity)
		view.ProposedDetectsSaturation = view.ProposedQueueUtilization >= 0.80
	}
	view.ProposedDetectsDrops = (view.BaselineSendFailedLogs + view.BaselineReceiverRefusedLogs + view.ProposedEnqueueFailedLogs) > 0
	return view
}

// ExtractObservedMetricPoints flattens a pmetric.Metrics payload into ObservedMetricPoint records.
func ExtractObservedMetricPoints(md pmetric.Metrics) []ObservedMetricPoint {
	var out []ObservedMetricPoint
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resAttrs := attrMapToStrings(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						out = append(out, ObservedMetricPoint{
							Name:        m.Name(),
							Description: m.Description(),
							Unit:        m.Unit(),
							Type:        m.Type(),
							IntValue:    dp.IntValue(),
							Attributes:  attrMapToStrings(dp.Attributes()),
							Resource:    resAttrs,
						})
					}
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					dps := sum.DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						out = append(out, ObservedMetricPoint{
							Name:        m.Name(),
							Description: m.Description(),
							Unit:        m.Unit(),
							Type:        m.Type(),
							IsMonotonic: sum.IsMonotonic(),
							Temporality: sum.AggregationTemporality(),
							IntValue:    dp.IntValue(),
							Attributes:  attrMapToStrings(dp.Attributes()),
							Resource:    resAttrs,
						})
					}
				}
			}
		}
	}
	return out
}

func attrMapToStrings(m pcommon.Map) map[string]string {
	res := make(map[string]string, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		res[k] = v.AsString()
		return true
	})
	return res
}

func (v CatalogHealthView) String() string {
	return fmt.Sprintf(
		"Baseline[send_failed=%d, receiver_refused=%d, detects_issue=%v] | Proposed[queue=%d/%d (util=%.2f), enqueue_failed=%d, detects_saturation=%v, detects_drops=%v]",
		v.BaselineSendFailedLogs,
		v.BaselineReceiverRefusedLogs,
		v.BaselineDetectsIssue,
		v.ProposedQueueSize,
		v.ProposedQueueCapacity,
		v.ProposedQueueUtilization,
		v.ProposedEnqueueFailedLogs,
		v.ProposedDetectsSaturation,
		v.ProposedDetectsDrops,
	)
}

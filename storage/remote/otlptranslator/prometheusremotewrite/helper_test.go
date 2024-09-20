// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/debbf30360b8d3a0ded8db09c4419d2a9c99b94a/pkg/translator/prometheusremotewrite/helper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package prometheusremotewrite

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/prompb"
)

func TestCreateAttributes(t *testing.T) {
	resourceAttrs := map[string]string{
		"service.name":        "service name",
		"service.instance.id": "service ID",
		"existent-attr":       "resource value",
		// This one is for testing conflict with metric attribute.
		"metric-attr": "resource value",
		// This one is for testing conflict with auto-generated job attribute.
		"job": "resource value",
		// This one is for testing conflict with auto-generated instance attribute.
		"instance": "resource value",
	}

	resource := pcommon.NewResource()
	for k, v := range resourceAttrs {
		resource.Attributes().PutStr(k, v)
	}
	attrs := pcommon.NewMap()
	attrs.PutStr("__name__", "test_metric")
	attrs.PutStr("metric-attr", "metric value")

	testCases := []struct {
		name                      string
		promoteResourceAttributes []string
		expectedLabels            []prompb.Label
	}{
		{
			name:                      "Successful conversion without resource attribute promotion",
			promoteResourceAttributes: nil,
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion",
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion, conflicting resource attributes are ignored",
			promoteResourceAttributes: []string{"non-existent-attr", "existent-attr", "metric-attr", "job", "instance"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
		{
			name:                      "Successful conversion with resource attribute promotion, attributes are only promoted once",
			promoteResourceAttributes: []string{"existent-attr", "existent-attr"},
			expectedLabels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "test_metric",
				},
				{
					Name:  "instance",
					Value: "service ID",
				},
				{
					Name:  "job",
					Value: "service name",
				},
				{
					Name:  "existent_attr",
					Value: "resource value",
				},
				{
					Name:  "metric_attr",
					Value: "metric value",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			settings := Settings{
				PromoteResourceAttributes: tc.promoteResourceAttributes,
			}
			lbls := createAttributes(resource, attrs, settings, nil, false)

			assert.ElementsMatch(t, lbls, tc.expectedLabels)
		})
	}
}

func Test_convertTimeStamp(t *testing.T) {
	tests := []struct {
		name string
		arg  pcommon.Timestamp
		want int64
	}{
		{"zero", 0, 0},
		{"1ms", 1_000_000, 1},
		{"1s", pcommon.Timestamp(time.Unix(1, 0).UnixNano()), 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertTimeStamp(tt.arg)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrometheusConverter_AddSummaryDataPoints(t *testing.T) {
	now := time.Now()
	nowUnixNano := pcommon.Timestamp(now.UnixNano())
	nowMinus20s := pcommon.Timestamp(now.Add(-20 * time.Second).UnixNano())
	nowMinus1h := pcommon.Timestamp(now.Add(-1 * time.Hour).UnixNano())
	tests := []struct {
		name   string
		metric func() pmetric.Metric
		want   func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "summary with start time equal to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowUnixNano)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowUnixNano)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time within two minutes to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowMinus20s)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowMinus20s)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowMinus20s)},
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowMinus20s)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary with start time older than two minutes to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)
				dp.SetStartTimestamp(nowMinus1h)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + createdSuffix},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(nowMinus1h)), Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
		{
			name: "summary without start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_summary")
				metric.SetEmptySummary()

				dp := metric.Summary().DataPoints().AppendEmpty()
				dp.SetTimestamp(nowUnixNano)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + countStr},
				}
				sumLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_summary" + sumStr},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
					timeSeriesSignature(sumLabels): {
						Labels: sumLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(nowUnixNano)},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			err := converter.addSummaryDataPoints(
				context.Background(),
				metric.Summary().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric:                 true,
					EnableCreatedTimestampZeroIngestion: true,
				},
				metric.Name(),
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			assert.Equal(t, tt.want(), converter.unique)
			assert.Empty(t, converter.conflicts)
		})
	}
}

func TestPrometheusConverter_AddHistogramDataPoints(t *testing.T) {
	ts := pcommon.Timestamp(time.Now().UnixNano())
	tests := []struct {
		name   string
		metric func() pmetric.Metric
		want   func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "histogram with start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_hist")
				metric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.Histogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(ts)
				pt.SetStartTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + countStr},
				}
				createdLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + createdSuffix},
				}
				infLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist_bucket"},
					{Name: model.BucketLabel, Value: "+Inf"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(infLabels): {
						Labels: infLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(createdLabels): {
						Labels: createdLabels,
						Samples: []prompb.Sample{
							{Value: float64(convertTimeStamp(ts)), Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
		{
			name: "histogram without start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_hist")
				metric.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.Histogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(ts)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist" + countStr},
				}
				infLabels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_hist_bucket"},
					{Name: model.BucketLabel, Value: "+Inf"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(infLabels): {
						Labels: infLabels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
					timeSeriesSignature(labels): {
						Labels: labels,
						Samples: []prompb.Sample{
							{Value: 0, Timestamp: convertTimeStamp(ts)},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			err := converter.addHistogramDataPoints(
				context.Background(),
				metric.Histogram().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric:                 true,
					EnableCreatedTimestampZeroIngestion: true,
				},
				metric.Name(),
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			assert.Equal(t, tt.want(), converter.unique)
			assert.Empty(t, converter.conflicts)
		})
	}
}

func TestPrometheusConverter_AddExponentialHistogramDataPoints(t *testing.T) {
	now := time.Now()
	nowUnixNano := pcommon.Timestamp(now.UnixNano())
	nowMinus20s := pcommon.Timestamp(now.Add(-20 * time.Second).UnixNano())
	nowMinus1h := pcommon.Timestamp(now.Add(-1 * time.Hour).UnixNano())
	tests := []struct {
		name   string
		metric func() pmetric.Metric
		want   func() map[uint64]*prompb.TimeSeries
	}{
		{
			name: "histogram with start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_exponential_hist")
				metric.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.ExponentialHistogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(nowUnixNano)
				pt.SetStartTimestamp(nowUnixNano)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_exponential_hist"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Histograms: []prompb.Histogram{
							{
								Timestamp: convertTimeStamp(nowUnixNano),
								Count: &prompb.Histogram_CountInt{
									CountInt: 0,
								},
								ZeroCount: &prompb.Histogram_ZeroCountInt{
									ZeroCountInt: 0,
								},
								ZeroThreshold: defaultZeroThreshold,
							},
						},
					},
				}
			},
		},
		{
			name: "histogram without start time",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_exponential_hist")
				metric.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.ExponentialHistogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(nowUnixNano)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_exponential_hist"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Histograms: []prompb.Histogram{
							{
								Timestamp: convertTimeStamp(nowUnixNano),
								Count: &prompb.Histogram_CountInt{
									CountInt: 0,
								},
								ZeroCount: &prompb.Histogram_ZeroCountInt{
									ZeroCountInt: 0,
								},
								ZeroThreshold: defaultZeroThreshold,
							},
						},
					},
				}
			},
		},
		{
			name: "histogram with start time within two minutes to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_exponential_hist")
				metric.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.ExponentialHistogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(nowUnixNano)
				pt.SetStartTimestamp(nowMinus20s)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_exponential_hist"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Histograms: []prompb.Histogram{
							{
								Timestamp: convertTimeStamp(nowMinus20s),
							},
							{
								Timestamp: convertTimeStamp(nowUnixNano),
								Count: &prompb.Histogram_CountInt{
									CountInt: 0,
								},
								ZeroCount: &prompb.Histogram_ZeroCountInt{
									ZeroCountInt: 0,
								},
								ZeroThreshold: defaultZeroThreshold,
							},
						},
					},
				}
			},
		},
		{
			name: "histogram with start time older than two minutes to sample timestamp",
			metric: func() pmetric.Metric {
				metric := pmetric.NewMetric()
				metric.SetName("test_exponential_hist")
				metric.SetEmptyExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

				pt := metric.ExponentialHistogram().DataPoints().AppendEmpty()
				pt.SetTimestamp(nowUnixNano)
				pt.SetStartTimestamp(nowMinus1h)

				return metric
			},
			want: func() map[uint64]*prompb.TimeSeries {
				labels := []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_exponential_hist"},
				}
				return map[uint64]*prompb.TimeSeries{
					timeSeriesSignature(labels): {
						Labels: labels,
						Histograms: []prompb.Histogram{
							{
								Timestamp: convertTimeStamp(nowUnixNano),
								Count: &prompb.Histogram_CountInt{
									CountInt: 0,
								},
								ZeroCount: &prompb.Histogram_ZeroCountInt{
									ZeroCountInt: 0,
								},
								ZeroThreshold: defaultZeroThreshold,
							},
						},
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := tt.metric()
			converter := NewPrometheusConverter()

			annot, err := converter.addExponentialHistogramDataPoints(
				context.Background(),
				metric.ExponentialHistogram().DataPoints(),
				pcommon.NewResource(),
				Settings{
					ExportCreatedMetric:                 true,
					EnableCreatedTimestampZeroIngestion: true,
				},
				metric.Name(),
			)
			require.NoError(t, err)
			require.Empty(t, annot)

			assert.Equal(t, tt.want(), converter.unique)
			assert.Empty(t, converter.conflicts)
		})
	}
}

// Copyright 2013 The Prometheus Authors
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

package rules

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// A RecordingRule records its vector expression into new timeseries.
type RecordingRule struct {
	name   string
	vector parser.Expr
	labels labels.Labels
	// The health of the recording rule.
	health *atomic.String
	// Timestamp of last evaluation of the recording rule.
	evaluationTimestamp *atomic.Time
	// The last error seen by the recording rule.
	lastError *atomic.Error
	// Duration of how long it took to evaluate the recording rule.
	evaluationDuration *atomic.Duration
}

// NewRecordingRule returns a new recording rule.
func NewRecordingRule(name string, vector parser.Expr, lset labels.Labels) *RecordingRule {
	return &RecordingRule{
		name:                name,
		vector:              vector,
		labels:              lset,
		health:              atomic.NewString(string(HealthUnknown)),
		evaluationTimestamp: atomic.NewTime(time.Time{}),
		evaluationDuration:  atomic.NewDuration(0),
		lastError:           atomic.NewError(nil),
	}
}

// Name returns the rule name.
func (rule *RecordingRule) Name() string {
	return rule.name
}

// Query returns the rule query expression.
func (rule *RecordingRule) Query() parser.Expr {
	return rule.vector
}

// Labels returns the rule labels.
func (rule *RecordingRule) Labels() labels.Labels {
	return rule.labels
}

// Eval evaluates the rule.
func (rule *RecordingRule) Eval(ctx context.Context, ts time.Time, query QueryFunc, _ *url.URL, limit int) (promql.Vector, error) {
	vector, err := rule.eval(ctx, ts, query)
	if err != nil {
		return nil, err
	}

	if err = rule.applyVectorLabels(vector); err != nil {
		return nil, err
	}

	if err = rule.limit(vector, limit); err != nil {
		return nil, err
	}

	rule.SetHealth(HealthGood)
	rule.SetLastError(err)
	return vector, nil
}

// EvalWithExemplars evaluates the rule along with emitting exemplars.
func (rule *RecordingRule) EvalWithExemplars(ctx context.Context, ts time.Time, interval time.Duration,
	query QueryFunc, eq ExemplarQueryFunc, _ *url.URL, limit int) (promql.Vector, []exemplar.QueryResult, error) {
	vector, err := rule.eval(ctx, ts, query)
	if err != nil {
		return nil, nil, err
	}

	selectors := parser.ExtractSelectors(rule.vector)
	if len(selectors) < 1 {
		return nil, nil, err
	}

	// Query all the raw exemplars that match the query
	ex, err := eq(ctx, rule.vector, ts, interval)
	if err != nil {
		return nil, nil, err
	}

	// For each vector received, we create matchers for all labels of the vector. If it matches
	// we replace the exemplar's series labels with the vector's labels. We do this to ensure that we are able to append
	// the exemplars to the same series ref the recorded metric went into.
	// If none of the selectors match, then drop the exemplars on the floor.
	var exemplars []exemplar.QueryResult
	for _, v := range vector {
		matchers := make([]*labels.Matcher, 0, len(v.Metric))
		for _, l := range v.Metric {
			matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
		}

		for i := range ex {
			if ok := matches(ex[i].SeriesLabels, matchers); ok {
				ex[i].SeriesLabels = v.Metric
				exemplars = append(exemplars, ex[i])
			}
		}
	}

	if err = rule.applyVectorLabels(vector); err != nil {
		return nil, nil, err
	}

	if err = rule.limit(vector, limit); err != nil {
		return nil, nil, err
	}

	rule.applyExemplarLabels(exemplars)
	rule.SetHealth(HealthGood)
	rule.SetLastError(err)
	return vector, exemplars, nil
}

// eval executes the query and returns a promql.Vector.
func (rule *RecordingRule) eval(ctx context.Context, ts time.Time, query QueryFunc) (promql.Vector, error) {
	ctx = NewOriginContext(ctx, NewRuleDetail(rule))
	vector, err := query(ctx, rule.vector.String(), ts)
	if err != nil {
		return nil, err
	}

	return vector, nil
}

// applyVectorLabels applies labels from the rule and sets the metric name for the vector.
func (rule *RecordingRule) applyVectorLabels(vector promql.Vector) error {
	lb := labels.NewBuilder(labels.EmptyLabels())

	for i := range vector {
		sample := &vector[i]
		sample.Metric = rule.applyLabels(lb, sample.Metric)
	}

	// Check that the rule does not produce identical metrics after applying
	// labels.
	if vector.ContainsSameLabelset() {
		return fmt.Errorf("vector contains metrics with the same labelset after applying rule labels")
	}

	return nil
}

// applyExemplarLabels applies labels from the rule and sets the metric name for the exemplar result.
func (rule *RecordingRule) applyExemplarLabels(ex []exemplar.QueryResult) {
	// Override the metric name and labels.
	lb := labels.NewBuilder(labels.EmptyLabels())

	for i := range ex {
		e := &ex[i]
		e.SeriesLabels = rule.applyLabels(lb, e.SeriesLabels)
	}
}

func (rule *RecordingRule) applyLabels(lb *labels.Builder, ls labels.Labels) labels.Labels {
	lb.Reset(ls)
	lb.Set(labels.MetricName, rule.name)

	rule.labels.Range(func(l labels.Label) {
		lb.Set(l.Name, l.Value)
	})
	return lb.Labels()
}

// limit ensures that any limits being set on the rules for series limit are enforced.
func (rule *RecordingRule) limit(vector promql.Vector, limit int) error {
	numSeries := len(vector)
	if limit > 0 && numSeries > limit {
		return fmt.Errorf("exceeded limit of %d with %d series", limit, numSeries)
	}

	return nil
}

func (rule *RecordingRule) String() string {
	r := rulefmt.Rule{
		Record: rule.name,
		Expr:   rule.vector.String(),
		Labels: rule.labels.Map(),
	}

	byt, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Sprintf("error marshaling recording rule: %q", err.Error())
	}

	return string(byt)
}

// SetEvaluationDuration updates evaluationDuration to the time in seconds it took to evaluate the rule on its last evaluation.
func (rule *RecordingRule) SetEvaluationDuration(dur time.Duration) {
	rule.evaluationDuration.Store(dur)
}

// SetLastError sets the current error seen by the recording rule.
func (rule *RecordingRule) SetLastError(err error) {
	rule.lastError.Store(err)
}

// LastError returns the last error seen by the recording rule.
func (rule *RecordingRule) LastError() error {
	return rule.lastError.Load()
}

// SetHealth sets the current health of the recording rule.
func (rule *RecordingRule) SetHealth(health RuleHealth) {
	rule.health.Store(string(health))
}

// Health returns the current health of the recording rule.
func (rule *RecordingRule) Health() RuleHealth {
	return RuleHealth(rule.health.Load())
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the recording rule.
func (rule *RecordingRule) GetEvaluationDuration() time.Duration {
	return rule.evaluationDuration.Load()
}

// SetEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule was last evaluated.
func (rule *RecordingRule) SetEvaluationTimestamp(ts time.Time) {
	rule.evaluationTimestamp.Store(ts)
}

// GetEvaluationTimestamp returns the time the evaluation took place.
func (rule *RecordingRule) GetEvaluationTimestamp() time.Time {
	return rule.evaluationTimestamp.Load()
}

func matches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

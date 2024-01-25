// Copyright 2015 The Prometheus Authors
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

package scaleway

import (
	"github.com/prometheus/prometheus/discovery"
)

var _ discovery.DiscovererMetrics = (*scalewayMetrics)(nil)

type scalewayMetrics struct {
	refreshMetrics discovery.RefreshMetricsInstantiator
}

// Register implements discovery.DiscovererMetrics.
func (m *scalewayMetrics) Register() error {
	return nil
}

// Unregister implements discovery.DiscovererMetrics.
func (m *scalewayMetrics) Unregister() {}

// Copyright The Prometheus Authors
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

package inhibit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/alert"
)

// cache contains the runtime state of the inhibit rule.
type cache struct {
	equal    map[model.LabelName]struct{}
	ruleName string
	logger   *slog.Logger

	mtx sync.RWMutex
	// alerts is the map of alerts that match the source matchers of the inhibit rule.
	alerts map[model.Fingerprint]*alert.Alert
	// index is a map of equal label fingerprint to the set of source alert
	// fingerprints stored in the cache. The size of a set is the inhibition
	// count of that equal group: a target stays inhibited while at least one
	// source remains, and the inhibition is released only when the last
	// source of the group disappears.
	index map[model.Fingerprint]model.FingerprintSet
}

func newCache(equal map[model.LabelName]struct{}, ruleName string, logger *slog.Logger) *cache {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &cache{
		equal:    equal,
		ruleName: ruleName,
		logger:   logger,
		alerts:   make(map[model.Fingerprint]*alert.Alert),
		index:    make(map[model.Fingerprint]model.FingerprintSet),
	}
}

// fingerprintEquals returns the fingerprint of the equal labels of the given label set.
func (c *cache) fingerprintEquals(lset model.LabelSet) model.Fingerprint {
	equalSet := make(model.LabelSet, len(c.equal))
	for n := range c.equal {
		equalSet[n] = lset[n]
	}
	return equalSet.Fingerprint()
}

// set adds or replaces the given source alert. A resolved alert can no longer
// act as an inhibition source and is removed synchronously instead of waiting
// for the next GC cycle. This also covers alerts that used to inhibit as
// sources and have now recovered themselves.
func (c *cache) set(a *alert.Alert) {
	fp := a.Fingerprint()
	eq := c.fingerprintEquals(a.Labels)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if a.Resolved() {
		c.removeAlertLocked(fp, eq)
		return
	}
	if _, ok := c.alerts[fp]; !ok {
		if c.addLocked(eq, fp) == 1 {
			c.logger.Debug("inhibition established", "rule", c.ruleName, "equal", eq, "source", fp)
		}
	}
	c.alerts[fp] = a
}

// addLocked inserts fp into the equal group eq and returns the new inhibition
// count of the group. It is idempotent: inserting the same fingerprint twice
// does not change the count.
func (c *cache) addLocked(eq, fp model.Fingerprint) int {
	set, ok := c.index[eq]
	if !ok {
		set = model.FingerprintSet{}
		c.index[eq] = set
	}
	set[fp] = struct{}{}
	return len(set)
}

// removeAlertLocked drops the alert fp from the cache and from the equal
// group eq, logging the release of the inhibition when the last source of the
// group disappears. It is a no-op for alerts that are not cached.
func (c *cache) removeAlertLocked(fp, eq model.Fingerprint) {
	if _, ok := c.alerts[fp]; !ok {
		return
	}
	delete(c.alerts, fp)
	if c.removeLocked(eq, fp) == 0 {
		c.logger.Debug("inhibition released", "rule", c.ruleName, "equal", eq, "source", fp)
	}
}

// removeLocked deletes fp from the equal group eq and returns the new
// inhibition count of the group. It is idempotent: removing a fingerprint
// that is not part of the group is a no-op, so concurrent updates can never
// drive the count below zero.
func (c *cache) removeLocked(eq, fp model.Fingerprint) int {
	set, ok := c.index[eq]
	if !ok {
		return 0
	}
	delete(set, fp)
	if len(set) == 0 {
		delete(c.index, eq)
		return 0
	}
	return len(set)
}

// sourceCount returns the inhibition count of the equal group that lset
// belongs to, i.e. the number of cached source alerts sharing its equal
// labels.
func (c *cache) sourceCount(lset model.LabelSet) int {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return len(c.index[c.fingerprintEquals(lset)])
}

// find returns the fingerprint of a cached source alert that shares the equal
// labels of lset, is active at now, and satisfies match. Sources that have
// expired without an update are pruned synchronously, so the inhibition count
// reflects their expiry without waiting for the next GC cycle.
func (c *cache) find(lset model.LabelSet, now time.Time, match func(*alert.Alert) bool) (model.Fingerprint, bool) {
	eq := c.fingerprintEquals(lset)

	c.mtx.RLock()
	for fp := range c.index[eq] {
		a := c.alerts[fp]
		if a.ResolvedAt(now) {
			continue
		}
		if !match(a) {
			continue
		}
		c.mtx.RUnlock()
		return fp, true
	}
	empty := len(c.index[eq]) == 0
	c.mtx.RUnlock()

	if empty {
		return model.Fingerprint(0), false
	}

	// No active source matched but the group still holds expired sources.
	// Prune them under the write lock, re-checking first in case a concurrent
	// update added a fresh source in the meantime.
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for fp := range c.index[eq] {
		a := c.alerts[fp]
		if a.ResolvedAt(now) {
			c.removeAlertLocked(fp, eq)
			continue
		}
		if !match(a) {
			continue
		}
		return fp, true
	}
	return model.Fingerprint(0), false
}

func (c *cache) run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.gc()
		}
	}
}

func (c *cache) gc() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for fp, a := range c.alerts {
		if !a.Resolved() {
			continue
		}
		c.removeAlertLocked(fp, c.fingerprintEquals(a.Labels))
	}
}

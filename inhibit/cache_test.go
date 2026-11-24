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
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/alertmanager/alert"
)

// matchAll is a find predicate that accepts every source alert.
func matchAll(*alert.Alert) bool { return true }

// newSourceAlert builds a source alert for the equal group e=1 with the given
// id and end time.
func newSourceAlert(id string, startsAt, endsAt time.Time) *alert.Alert {
	return alert.New(model.Alert{
		Labels:   model.LabelSet{"s": "1", "e": "1", "id": model.LabelValue(id)},
		StartsAt: startsAt,
		EndsAt:   endsAt,
	}, time.Time{}, false)
}

func TestCacheMultiSourceReleaseOnlyWhenAllSourcesGone(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "multi-source", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(time.Hour)))
	c.set(newSourceAlert("src-2", now.Add(-time.Hour), now.Add(time.Hour)))

	require.Equal(t, 2, c.sourceCount(target))
	_, ok := c.find(target, now, matchAll)
	require.True(t, ok, "target must be inhibited while two sources are active")

	// The first source resolves: the inhibition count drops to one and the
	// target stays inhibited by the remaining source.
	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(-time.Second)))
	require.Equal(t, 1, c.sourceCount(target))
	_, ok = c.find(target, now, matchAll)
	require.True(t, ok, "target must stay inhibited while one source remains")

	// The last source resolves: the inhibition is released.
	c.set(newSourceAlert("src-2", now.Add(-time.Hour), now.Add(-time.Second)))
	require.Equal(t, 0, c.sourceCount(target))
	_, ok = c.find(target, now, matchAll)
	require.False(t, ok, "target must not be inhibited once all sources are gone")
	require.Empty(t, c.alerts)
	require.Empty(t, c.index)
}

func TestCacheResolvedSourceRemovedSynchronously(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "source-expiry", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(time.Hour)))
	require.Equal(t, 1, c.sourceCount(target))

	// A resolved update for the source must update the count synchronously,
	// without waiting for the next GC cycle.
	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(-time.Second)))
	require.Equal(t, 0, c.sourceCount(target))
	require.Empty(t, c.alerts)
	_, ok := c.find(target, now, matchAll)
	require.False(t, ok)
}

func TestCacheExpiredSourcePrunedOnFind(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "passive-expiry", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	// The source expires passively: no resolved update is ever pushed.
	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(time.Minute)))
	require.Equal(t, 1, c.sourceCount(target))

	// A lookup after the expiry time must not inhibit and must prune the
	// expired source synchronously instead of relying on the next GC cycle.
	later := now.Add(2 * time.Minute)
	_, ok := c.find(target, later, matchAll)
	require.False(t, ok)
	require.Equal(t, 0, c.sourceCount(target), "expired source must be pruned synchronously")
	require.Empty(t, c.alerts)
	require.Empty(t, c.index)
}

func TestCacheGCUpdatesCountAndLogsRelease(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "gc-rule", logger)
	target := model.LabelSet{"t": "1", "e": "1"}

	c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(time.Hour)))
	require.Contains(t, buf.String(), "inhibition established")
	require.Contains(t, buf.String(), "rule=gc-rule")

	// Expire the cached alert in place (as a refresh with a past EndsAt would
	// do) and run GC: the count must drop to zero and the release must be
	// logged.
	expired := newSourceAlert("src-1", now.Add(-time.Hour), now.Add(-time.Second))
	c.alerts[expired.Fingerprint()] = expired
	c.gc()

	require.Equal(t, 0, c.sourceCount(target))
	require.Empty(t, c.alerts)
	require.Empty(t, c.index)
	require.Contains(t, buf.String(), "inhibition released")
}

func TestCacheTargetRecoveryCleansUpSourceRecord(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "two-sided", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	// A two-sided alert matches both the source and the target side. While
	// active it inhibits pure targets of its equal group.
	twoSided := alert.New(model.Alert{
		Labels:   model.LabelSet{"s": "1", "t": "1", "e": "1"},
		StartsAt: now.Add(-time.Hour),
		EndsAt:   now.Add(time.Hour),
	}, time.Time{}, false)
	c.set(twoSided)
	require.Equal(t, 1, c.sourceCount(target))
	_, ok := c.find(target, now, matchAll)
	require.True(t, ok)

	// When the two-sided alert recovers, its source record must be cleaned up
	// so the inhibition of the remaining targets is released.
	recovered := alert.New(model.Alert{
		Labels:   model.LabelSet{"s": "1", "t": "1", "e": "1"},
		StartsAt: now.Add(-time.Hour),
		EndsAt:   now.Add(-time.Second),
	}, time.Time{}, false)
	c.set(recovered)
	require.Equal(t, 0, c.sourceCount(target))
	require.Empty(t, c.alerts)
	_, ok = c.find(target, now, matchAll)
	require.False(t, ok, "recovered target must not keep inhibiting as a source")
}

func TestCacheConcurrentUpdatesNeverNegative(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "concurrent", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	const workers = 8
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			id := fmt.Sprintf("src-%d", i)
			active := newSourceAlert(id, now.Add(-time.Hour), now.Add(time.Hour))
			resolved := newSourceAlert(id, now.Add(-time.Hour), now.Add(-time.Second))
			for range 100 {
				c.set(active)
				c.set(resolved)
				// Duplicate removals of the same source must not decrement
				// the count twice.
				c.set(resolved)
				c.find(target, now, matchAll)
			}
		})
	}
	wg.Wait()

	require.Equal(t, 0, c.sourceCount(target), "inhibition count must not go negative")
	require.Empty(t, c.alerts)
	require.Empty(t, c.index)
}

func TestCacheRefreshedSourceKeepsCount(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := newCache(map[model.LabelName]struct{}{"e": {}}, "refresh", nil)
	target := model.LabelSet{"t": "1", "e": "1"}

	// Repeated refreshes of the same source alert must keep the count at one.
	for i := range 3 {
		c.set(newSourceAlert("src-1", now.Add(-time.Hour), now.Add(time.Duration(i+1)*time.Hour)))
		require.Equal(t, 1, c.sourceCount(target))
	}
	_, ok := c.find(target, now, matchAll)
	require.True(t, ok)
}

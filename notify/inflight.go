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

package notify

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/alertmanager/alert"
)

// inflightCall is a notification delivery in progress. Concurrent executions
// carrying the same dedup key join the same call so that the receiver gets the
// notification once and every caller observes its result.
type inflightCall struct {
	// done closes once the leader finished, successfully or not.
	done chan struct{}
	// err is the leader's result; read it only after done is closed.
	err error
}

// inflightRegistry deduplicates notification deliveries that are in progress
// at the same time. A notification already persisted in the notification log is
// filtered by DedupStage; this registry covers the window in which the first
// delivery has not finished yet and a second flush of the same aggregation
// group reaches the pipeline.
type inflightRegistry struct {
	mtx   sync.Mutex
	calls map[string]*inflightCall
}

// newInflightRegistry returns a ready inflightRegistry.
func newInflightRegistry() *inflightRegistry {
	return &inflightRegistry{calls: map[string]*inflightCall{}}
}

// joinOrStart returns leader=true when no delivery for key is running yet; the
// caller then owns the delivery and must call the returned finish function
// with its result. When a delivery is already running it returns the call to
// wait on and leader=false; the caller blocks on call.wait until the leader
// reports a result or its own context is canceled.
func (r *inflightRegistry) joinOrStart(key string) (leader bool, call *inflightCall, finish func(error)) {
	r.mtx.Lock()
	if existing, ok := r.calls[key]; ok {
		r.mtx.Unlock()
		return false, existing, nil
	}

	started := &inflightCall{done: make(chan struct{})}
	r.calls[key] = started
	r.mtx.Unlock()

	finish = func(deliveryErr error) {
		started.err = deliveryErr
		close(started.done)

		r.mtx.Lock()
		// Only delete the entry we own; a re-delivery starting the instant the
		// leader finishes must not be removed by the leader's cleanup.
		if r.calls[key] == started {
			delete(r.calls, key)
		}
		r.mtx.Unlock()
	}
	return true, started, finish
}

// wait blocks until the leader's delivery finishes or ctx ends. It returns the
// leader's error in the first case and the context's error in the second.
func (c *inflightCall) wait(ctx context.Context) error {
	select {
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// inflightKey builds the dedup key for one delivery attempt. It combines the
// aggregation group, the integration (a group can fan out to several
// integrations) and the fingerprints of the alerts in the batch, so a changed
// alert set is a different notification and is not coalesced away.
func inflightKey(gkey string, i Integration, alerts []*alert.Alert) string {
	hashes := make([]uint64, 0, len(alerts))
	for _, a := range alerts {
		hashes = append(hashes, hashAlert(a))
	}
	slices.Sort(hashes)

	var b strings.Builder
	b.WriteString(gkey)
	b.WriteByte(0)
	b.WriteString(i.name)
	b.WriteByte(0)
	for _, h := range hashes {
		b.WriteString(itoa(h))
		b.WriteByte(',')
	}
	return b.String()
}

// itoa appends the decimal representation of h without the allocation of
// strconv.FormatUint on this hot path.
func itoa(h uint64) string {
	if h == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for h > 0 {
		pos--
		buf[pos] = byte('0' + h%10)
		h /= 10
	}
	return string(buf[pos:])
}

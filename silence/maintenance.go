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

package silence

import (
	"errors"
	"fmt"
	"time"
)

// MaintenanceFunc represents the function to run as part of the periodic maintenance for silences.
// It returns the size of the snapshot taken or an error if it failed.
type MaintenanceFunc func() (int64, error)

// Maintenance garbage collects the silence state at the given interval. If the snapshot
// file is set, a snapshot is written to it afterwards.
// Terminates on receiving from stopc.
// If not nil, the last argument is an override for what to do as part of the maintenance - for advanced usage.
func (s *Silences) Maintenance(interval time.Duration, snapf string, stopc <-chan struct{}, override MaintenanceFunc) {
	if interval == 0 || stopc == nil {
		s.logger.Error("interval or stop signal are missing - not running maintenance")
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	var doMaintenance MaintenanceFunc
	doMaintenance = func() (int64, error) {
		var size int64

		if _, err := s.GC(); err != nil {
			return size, err
		}
		if snapf == "" {
			return size, nil
		}
		f, err := openReplace(snapf)
		if err != nil {
			return size, err
		}
		if size, err = s.Snapshot(f); err != nil {
			f.Close()
			return size, err
		}
		return size, f.Close()
	}

	if override != nil {
		doMaintenance = override
	}

	runMaintenance := func(do MaintenanceFunc) error {
		s.metrics.maintenanceTotal.Inc()
		s.logger.Debug("Running maintenance")
		start := s.nowUTC()
		size, err := do()
		s.metrics.snapshotSize.Set(float64(size))
		if err != nil {
			s.metrics.maintenanceErrorsTotal.Inc()
			return err
		}
		s.logger.Debug("Maintenance done", "duration", time.Since(start), "size", size)
		return nil
	}

Loop:
	for {
		select {
		case <-stopc:
			break Loop
		case <-t.C:
			if err := runMaintenance(doMaintenance); err != nil {
				s.logger.Error("Running maintenance failed", "err", err)
			}
		}
	}

	// No need for final maintenance if we don't want to snapshot.
	if snapf == "" {
		return
	}
	if err := runMaintenance(doMaintenance); err != nil {
		s.logger.Error("Creating shutdown snapshot failed", "err", err)
	}
}

// GC runs a garbage collection that removes silences that have ended longer
// than the configured retention time ago.
//
// The collection is atomic: it first determines all expired entries and only
// applies the deletions once the whole state has been validated. If any entry
// is invalid, an error is returned and the state is left completely untouched,
// so no unexpired silence can ever be lost through a failed maintenance run.
func (s *Silences) GC() (int, error) {
	start := time.Now()
	defer func() { s.metrics.gcDuration.Observe(time.Since(start).Seconds()) }()

	now := s.nowUTC()
	var errs error

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Phase 1: validate the whole state and collect the silences eligible for
	// garbage collection without mutating anything. A dangling version index
	// entry or a zero expiration timestamp aborts the whole collection.
	var expiredIDs []string
	for _, sv := range s.vi {
		sil, ok := s.st[sv.id]
		if !ok {
			s.metrics.gcErrorsTotal.Inc()
			errs = errors.Join(errs, fmt.Errorf("silence %s in version index missing from state", sv.id))
			continue
		}
		if sil.ExpiresAt == nil || sil.ExpiresAt.AsTime().IsZero() {
			s.metrics.gcErrorsTotal.Inc()
			errs = errors.Join(errs, fmt.Errorf("silence %s has zero expiration timestamp", sil.Silence.Id))
			continue
		}
		if !sil.ExpiresAt.AsTime().After(now) {
			expiredIDs = append(expiredIDs, sil.Silence.Id)
		}
	}
	if errs != nil {
		// Do not apply any deletion: callers must keep every unexpired entry.
		return 0, errs
	}
	if len(expiredIDs) == 0 {
		return 0, nil
	}

	// Phase 2: apply all deletions against the validated state.
	expired := make(map[string]struct{}, len(expiredIDs))
	for _, id := range expiredIDs {
		expired[id] = struct{}{}
		delete(s.st, id)
		delete(s.mi, id)
	}

	// During GC we delete expired silences from the state map and the indices.
	// If after the deletion we have more than 50% spare capacity, reallocate
	// the version index to a smaller slice while leaving some growth buffer.
	needsRealloc := cap(s.vi) > 1024 && len(s.vi)-len(expired) < cap(s.vi)/2
	var targetVi versionIndex
	if needsRealloc {
		newCap := max((len(s.vi)-len(expired))*5/4, 1024)
		targetVi = make(versionIndex, 0, newCap)
	} else {
		targetVi = s.vi[:0]
	}
	for _, sv := range s.vi {
		if _, ok := expired[sv.id]; ok {
			continue
		}
		targetVi = append(targetVi, sv)
	}
	if !needsRealloc {
		// Clear tail to prevent string pointer leaks.
		clear(s.vi[len(targetVi):])
	}
	s.vi = targetVi
	s.bumpVersion()
	s.updateSizeMetrics()

	return len(expiredIDs), nil
}

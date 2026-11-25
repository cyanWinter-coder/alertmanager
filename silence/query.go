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
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/pkg/labels"
	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// QueryParam expresses parameters along which silences are queried.
type QueryParam func(*query) error

type query struct {
	ids     []string
	since   *int
	filters []silenceFilter
	// matchSet is set by QMatches. When present, the label index narrows the
	// candidate silences before the full matcher evaluation runs.
	matchSet model.LabelSet
	// matchSetSet distinguishes "QMatches was used" from a nil label set.
	matchSetSet bool
}

// silenceFilter is a function that returns true if a silence
// should be dropped from a result set for a given time.
type silenceFilter func(*pb.Silence, *Silences, time.Time) (bool, error)

// QIDs configures a query to select the given silence IDs.
func QIDs(ids ...string) QueryParam {
	return func(q *query) error {
		if len(ids) == 0 {
			return errors.New("QIDs filter must have at least one id")
		}
		if q.since != nil {
			return fmt.Errorf("QSince cannot be used with QIDs")
		}
		q.ids = append(q.ids, ids...)
		return nil
	}
}

// QSince filters silences to those created after the provided version. This can be used to
// scan all silences which have been added after the provided version to incrementally update
// a cache.
func QSince(version int) QueryParam {
	return func(q *query) error {
		if len(q.ids) != 0 {
			return fmt.Errorf("QSince cannot be used with QIDs")
		}
		q.since = &version
		return nil
	}
}

// QMatches returns silences that match the given label set.
//
// The label index first narrows the candidate silences to those whose equality
// matchers can possibly match set; the full matcher set (including regex and
// negation matchers) is then evaluated only against those candidates.
// Expired silences are never returned.
func QMatches(set model.LabelSet) QueryParam {
	return func(q *query) error {
		q.matchSet = set
		q.matchSetSet = true
		f := func(sil *pb.Silence, s *Silences, at time.Time) (bool, error) {
			// Expired silences never match, regardless of their labels.
			if StateFromProtoAt(sil, at) == SilenceStateExpired {
				return false, nil
			}
			m, err := s.mi.get(sil)
			if err != nil {
				return true, err
			}
			return m.Matches(set), nil
		}
		q.filters = append(q.filters, f)
		return nil
	}
}

// HasMatchers reports whether one stored matcher set contains every matcher
// from one of the requested sets. Matchers must have the same name, type, and pattern.
// No requested sets means every silence matches.
// A nil or empty requested set also matches every silence.
// Nil matchers do not match.
func HasMatchers(sil *pb.Silence, sets ...*pb.MatcherSet) bool {
	if len(sets) == 0 {
		return true
	}
	for _, requested := range sets {
		if len(requested.GetMatchers()) == 0 {
			return true
		}
		for _, stored := range sil.GetMatcherSets() {
			if containsMatchers(stored.GetMatchers(), requested.GetMatchers(), protoMatcherEqual) {
				return true
			}
		}
	}
	return false
}

// HasLabelMatchers reports whether one stored matcher set contains every requested
// label matcher with the same name, type, and value. It compares definitions
// without evaluating label values. A silence with no matcher sets does not match.
// Nil matchers do not match.
func HasLabelMatchers(sil *pb.Silence, matchers []*labels.Matcher) bool {
	for _, stored := range sil.GetMatcherSets() {
		if containsMatchers(stored.GetMatchers(), matchers, labelMatcherEqual) {
			return true
		}
	}
	return false
}

func containsMatchers[T any](stored []*pb.Matcher, requested []T, equal func(T, *pb.Matcher) bool) bool {
	for _, matcher := range requested {
		if !slices.ContainsFunc(stored, func(candidate *pb.Matcher) bool {
			return equal(matcher, candidate)
		}) {
			return false
		}
	}
	return true
}

func protoMatcherEqual(requested, stored *pb.Matcher) bool {
	return requested != nil && stored != nil &&
		requested.Name == stored.Name && requested.Type == stored.Type && requested.Pattern == stored.Pattern
}

func labelMatcherEqual(requested *labels.Matcher, stored *pb.Matcher) bool {
	if requested == nil || stored == nil {
		return false
	}
	if requested.Name != stored.Name || requested.Value != stored.Pattern {
		return false
	}
	switch requested.Type {
	case labels.MatchEqual:
		return stored.Type == pb.Matcher_EQUAL
	case labels.MatchNotEqual:
		return stored.Type == pb.Matcher_NOT_EQUAL
	case labels.MatchRegexp:
		return stored.Type == pb.Matcher_REGEXP
	case labels.MatchNotRegexp:
		return stored.Type == pb.Matcher_NOT_REGEXP
	default:
		return false
	}
}

// QHasMatchers filters silences by matcher definitions using HasMatchers.
// It compares names, types, and patterns without evaluating label values.
func QHasMatchers(sets ...*pb.MatcherSet) QueryParam {
	return func(q *query) error {
		q.filters = append(q.filters, func(sil *pb.Silence, _ *Silences, _ time.Time) (bool, error) {
			return HasMatchers(sil, sets...), nil
		})
		return nil
	}
}

// QState filters queried silences by the given states.
func QState(states ...SilenceState) QueryParam {
	return func(q *query) error {
		f := func(sil *pb.Silence, _ *Silences, at time.Time) (bool, error) {
			s := StateFromProtoAt(sil, at)

			if slices.Contains(states, s) {
				return true, nil
			}
			return false, nil
		}
		q.filters = append(q.filters, f)
		return nil
	}
}

// QueryOne returns the first matching silence.
// It returns ErrNotFound if no silence matches.
// Callers must not modify the returned silence or any data it references.
func (s *Silences) QueryOne(ctx context.Context, params ...QueryParam) (*pb.Silence, error) {
	_, span := tracer.Start(ctx, "silence.Silences.QueryOne")
	defer span.End()
	res, _, err := s.Query(ctx, params...)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, ErrNotFound
	}
	return res[0], nil
}

// Query returns matching silences and the state version used by the query.
// Callers must not modify the returned silences or any data they reference.
func (s *Silences) Query(ctx context.Context, params ...QueryParam) ([]*pb.Silence, int, error) {
	return s.QueryAt(ctx, s.nowUTC(), params...)
}

// QueryAt evaluates query filters at the given time.
// Callers must not modify the returned silences or any data they reference.
// Clone a silence before modifying it.
// The records remain valid after the store replaces or removes them.
// It reads the current stored records, not a historical snapshot.
func (s *Silences) QueryAt(ctx context.Context, at time.Time, params ...QueryParam) ([]*pb.Silence, int, error) {
	_, span := tracer.Start(ctx, "silence.Silences.Query")
	defer span.End()
	s.metrics.queriesTotal.Inc()
	defer prometheus.NewTimer(s.metrics.queryDuration).ObserveDuration()

	if err := ctx.Err(); err != nil {
		s.metrics.queryErrorsTotal.Inc()
		return nil, 0, err
	}
	q := &query{}
	for _, p := range params {
		if err := p(q); err != nil {
			s.metrics.queryErrorsTotal.Inc()
			return nil, 0, err
		}
	}
	sils, version, err := s.query(ctx, q, at)
	if err != nil {
		s.metrics.queryErrorsTotal.Inc()
	}
	return sils, version, err
}

// Version of the silence state.
func (s *Silences) Version() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.version
}

// CountState counts silences by state.
func (s *Silences) CountState(ctx context.Context, states ...SilenceState) (int, error) {
	_, span := tracer.Start(ctx, "silence.Silences.CountState")
	defer span.End()

	now := s.nowUTC()
	count := 0

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for _, sv := range s.vi {
		if slices.Contains(states, StateFromProtoAt(s.st[sv.id].Silence, now)) {
			count++
		}
	}
	return count, nil
}

// snapshot is a point-in-time, immutable view of the silence state and its
// indices. It is captured under the read lock at the start of a query and
// referenced for the query's whole duration, so silences added or deleted
// while the query runs do not affect its result. The backing maps are mutated
// in place by writers (who hold the write lock), so the snapshot may only be
// read while its read lock is held; the query keeps that lock until done.
type snapshot struct {
	st      state
	vi      versionIndex
	version int
	// candidates is the label-index narrowed candidate set for a QMatches
	// query, nil when no label matching was requested.
	candidates idSet
}

// query executes the given query and returns the resulting silences.
func (s *Silences) query(ctx context.Context, q *query, now time.Time) ([]*pb.Silence, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	var res []*pb.Silence
	var err error

	scannedCount := 0
	defer func() {
		s.metrics.queryScannedTotal.Add(float64(scannedCount))
	}()

	// appendIfFiltersMatch appends the given silence to the result set
	// if it matches all filters in the query. In case of a filter error, the error is returned.
	appendIfFiltersMatch := func(res []*pb.Silence, sil *pb.Silence) ([]*pb.Silence, error) {
		for _, f := range q.filters {
			matches, err := f(sil, s, now)
			// In case of error return it immediately and don't process further filters.
			if err != nil {
				return res, err
			}
			// If one filter doesn't match, return the result unchanged, immediately.
			if !matches {
				return res, nil
			}
		}
		// All filters matched, append the silence to the result.
		return append(res, sil), nil
	}

	// Take a read lock on Silences and capture a snapshot of everything the
	// query will read. The lock is held for the whole evaluation because the
	// snapshot shares the live backing maps: writers mutate those maps under
	// the write lock, and Go maps are not safe for concurrent read/write.
	// Holding the read lock gives each query a stable point-in-time view while
	// still allowing many readers to run concurrently.
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	snap := snapshot{
		st:      s.st,
		vi:      s.vi,
		version: s.version,
	}
	if q.matchSetSet {
		snap.candidates = s.mi.candidates(q.matchSet)
	}

	// considerCandidate reports whether a silence survives the label-index
	// narrowing. Without QMatches every silence is considered.
	considerCandidate := func(id string) bool {
		if snap.candidates == nil {
			return true
		}
		return snap.candidates.contains(id)
	}

	if err := ctx.Err(); err != nil {
		return nil, snap.version, err
	}

	// If we have IDs, only consider the silences with the given IDs, if they exist.
	if q.ids != nil {
		res = make([]*pb.Silence, 0, len(q.ids))
		for _, id := range q.ids {
			if err := ctx.Err(); err != nil {
				return nil, snap.version, err
			}
			if !considerCandidate(id) {
				continue
			}
			if sil, ok := snap.st[id]; ok {
				scannedCount++
				// append the silence to the results if it satisfies the query.
				res, err = appendIfFiltersMatch(res, sil.Silence)
				if err != nil {
					return nil, snap.version, err
				}
			}
		}
		return res, snap.version, nil
	}

	start := 0
	if q.since != nil {
		var found bool
		start, found = snap.vi.findVersionGreaterThan(*q.since)
		// no new silences, nothing to do
		if !found {
			return res, snap.version, nil
		}
		// Track how many silences we skipped using the version index.
		s.metrics.querySkippedTotal.Add(float64(start))
	}
	// Unfiltered queries return every scanned silence.
	// Limit initial capacity for filtered queries.
	capacity := len(snap.vi) - start
	if len(q.filters) > 0 {
		capacity = min(64, capacity)
	}
	res = make([]*pb.Silence, 0, capacity)
	for _, sv := range snap.vi[start:] {
		if err := ctx.Err(); err != nil {
			return nil, snap.version, err
		}
		if !considerCandidate(sv.id) {
			continue
		}
		sil, ok := snap.st[sv.id]
		if !ok {
			// A version-index entry with no state would be a dangling reference.
			// Deletions synchronize both structures, so this should not happen;
			// skip it defensively rather than panicking on a nil dereference.
			continue
		}
		scannedCount++
		// append the silence to the results if it satisfies the query.
		res, err = appendIfFiltersMatch(res, sil.Silence)
		if err != nil {
			return nil, snap.version, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, snap.version, err
	}
	return res, snap.version, nil
}

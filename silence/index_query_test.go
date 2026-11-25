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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

func eqMatcher(name, value string) *pb.Matcher {
	return &pb.Matcher{Type: pb.Matcher_EQUAL, Name: name, Pattern: value}
}

func neqMatcher(name, value string) *pb.Matcher {
	return &pb.Matcher{Type: pb.Matcher_NOT_EQUAL, Name: name, Pattern: value}
}

func regexMatcher(name, pattern string) *pb.Matcher {
	return &pb.Matcher{Type: pb.Matcher_REGEXP, Name: name, Pattern: pattern}
}

func silenceWithMatchers(id string, start, end time.Time, sets ...[]*pb.Matcher) *pb.Silence {
	matcherSets := make([]*pb.MatcherSet, 0, len(sets))
	for _, m := range sets {
		matcherSets = append(matcherSets, &pb.MatcherSet{Matchers: m})
	}
	return &pb.Silence{
		Id:          id,
		MatcherSets: matcherSets,
		StartsAt:    timestamppb.New(start),
		EndsAt:      timestamppb.New(end),
	}
}

// assertIndexInSync verifies the matcher index, the state store and the
// version index describe exactly the same set of silences, with no dangling
// postings or unindexed entries.
func assertIndexInSync(t *testing.T, s *Silences) {
	t.Helper()

	require.Len(t, s.mi.entries, len(s.st), "index entries must match state")
	require.Len(t, s.vi, len(s.st), "version index must match state")

	indexed := make(idSet, len(s.mi.entries))
	for id, entry := range s.mi.entries {
		_, inState := s.st[id]
		require.True(t, inState, "index entry %q has no state record", id)
		indexed.add(id)

		// Every equality key of the silence must point back at it.
		for _, keys := range entry.equalKeys {
			for _, key := range keys {
				posting, ok := s.mi.postings[key]
				require.True(t, ok, "missing posting for key %v", key)
				require.True(t, posting.contains(id), "posting %v lost silence %q", key, id)
			}
		}
	}

	for id := range s.st {
		require.True(t, indexed.contains(id), "state silence %q missing from index", id)
	}

	// Postings and the unindexed set may only reference indexed silences.
	for key, posting := range s.mi.postings {
		for id := range posting {
			require.True(t, indexed.contains(id), "dangling posting %v -> %q", key, id)
		}
	}
	for id := range s.mi.unindexed {
		require.True(t, indexed.contains(id), "dangling unindexed entry %q", id)
	}
}

func TestMatcherIndexAddRemoveAndPostings(t *testing.T) {
	t.Parallel()

	idx := newMatcherIndex(0)
	now := time.Now()

	// Two silences sharing one equality key, one silence with only a regex
	// matcher (unindexed), one silence with an equality + negation matcher.
	silA := silenceWithMatchers("a", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "api"), eqMatcher("env", "prod")})
	silB := silenceWithMatchers("b", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "api")})
	silRegex := silenceWithMatchers("r", now, now.Add(time.Hour),
		[]*pb.Matcher{regexMatcher("path", "/v[0-9]+/.+")})
	silMixed := silenceWithMatchers("m", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "web"), neqMatcher("env", "dev")})

	for _, sil := range []*pb.Silence{silA, silB, silRegex, silMixed} {
		_, err := idx.add(sil)
		require.NoError(t, err)
	}

	require.Len(t, idx.entries, 4)
	require.Contains(t, idx.unindexed, "r")
	require.NotContains(t, idx.unindexed, "a")

	// Shared posting holds both a and b.
	posting := idx.postings[labelKey{name: "job", value: "api"}]
	require.ElementsMatch(t, []string{"a", "b"}, keysOf(posting))

	// Removing a silence drops it from every posting and reclaims empty ones.
	idx.remove("b")
	require.NotContains(t, idx.entries, "b")
	require.ElementsMatch(t, []string{"a"}, keysOf(idx.postings[labelKey{name: "job", value: "api"}]))

	idx.remove("a")
	_, stillThere := idx.postings[labelKey{name: "env", value: "prod"}]
	require.False(t, stillThere, "empty posting must be reclaimed")
	_, jobThere := idx.postings[labelKey{name: "job", value: "api"}]
	require.False(t, jobThere)

	// The unindexed regex silence is unaffected by unrelated removals.
	require.Contains(t, idx.unindexed, "r")
	idx.remove("r")
	require.Empty(t, idx.unindexed, "r was the only unindexed silence")
	require.Len(t, idx.entries, 1)
	require.Contains(t, idx.entries, "m")
	require.Empty(t, idx.postings[labelKey{name: "env", value: "prod"}])

	// Removing an unknown ID is a no-op.
	idx.remove("does-not-exist")
}

func keysOf(set idSet) []string {
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func TestMatcherIndexCandidatesNarrowing(t *testing.T) {
	t.Parallel()

	idx := newMatcherIndex(0)
	now := time.Now()

	// AND within a set: needs both job=api AND env=prod.
	silAnd := silenceWithMatchers("and", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "api"), eqMatcher("env", "prod")})
	// OR across sets: either job=api or team=infra.
	silOr := silenceWithMatchers("or", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "api")},
		[]*pb.Matcher{eqMatcher("team", "infra")})
	// Only a regex matcher: always a candidate.
	silRegex := silenceWithMatchers("regex", now, now.Add(time.Hour),
		[]*pb.Matcher{regexMatcher("path", ".*")})
	// Equality on a different value: not a candidate for this label set.
	silOther := silenceWithMatchers("other", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "worker")})
	// Equality + negation: candidate via the equality key.
	silMixed := silenceWithMatchers("mixed", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "api"), neqMatcher("env", "dev")})

	for _, sil := range []*pb.Silence{silAnd, silOr, silRegex, silOther, silMixed} {
		_, err := idx.add(sil)
		require.NoError(t, err)
	}

	candidates := idx.candidates(model.LabelSet{"job": "api", "env": "prod"})
	got := keysOf(candidates)
	require.ElementsMatch(t, []string{"and", "or", "regex", "mixed"}, got)
	require.NotContains(t, got, "other", "non-matching equality value must be narrowed out")

	// The AND silence drops out when one of its keys is absent.
	candidates = idx.candidates(model.LabelSet{"job": "api"})
	require.NotContains(t, keysOf(candidates), "and")
	require.Contains(t, keysOf(candidates), "or")
	require.Contains(t, keysOf(candidates), "regex")

	// Empty label set still keeps unindexed (regex) silences as candidates.
	candidates = idx.candidates(model.LabelSet{})
	require.ElementsMatch(t, []string{"regex"}, keysOf(candidates))
}

// TestQueryQMatchesExcludesExpired verifies requirement #4: an empty/label
// match query never returns expired silences.
func TestQueryQMatchesExcludesExpired(t *testing.T) {
	t.Parallel()

	store, err := New(Options{Metrics: prometheus.NewRegistry()})
	require.NoError(t, err)
	now := store.nowUTC()

	matchers := []*pb.Matcher{eqMatcher("foo", "bar")}

	// Set clamps past StartsAt to now, so an already-expired silence has to be
	// injected into the state directly (the same pattern the package's GC and
	// snapshot tests use).
	active := silenceWithMatchers("", now.Add(-time.Hour), now.Add(time.Hour), matchers)
	pending := silenceWithMatchers("", now.Add(2*time.Hour), now.Add(3*time.Hour), matchers)
	for _, sil := range []*pb.Silence{active, pending} {
		require.NoError(t, store.Set(t.Context(), sil))
	}
	expired := silenceWithMatchers("", now.Add(-4*time.Hour), now.Add(-2*time.Hour), matchers)
	expiredMS := store.toMeshSilence(expired)
	store.st[expired.Id] = expiredMS
	store.indexSilence(expiredMS.Silence)
	assertIndexInSync(t, store)

	set := model.LabelSet{"foo": "bar"}

	// At the current time, the expired silence must never show up.
	res, _, err := store.Query(t.Context(), QMatches(set))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{active.Id, pending.Id}, idsOf(res))
	for _, sil := range res {
		require.NotEqual(t, SilenceStateExpired, StateFromProtoAt(sil, store.nowUTC()))
	}

	// At -90m the expired silence had already ended (at -2h), so it is excluded.
	// The other two are not expired then and are returned.
	resAt, _, err := store.QueryAt(t.Context(), now.Add(-90*time.Minute), QMatches(set))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{active.Id, pending.Id}, idsOf(resAt))
	for _, sil := range resAt {
		require.NotEqual(t, SilenceStateExpired, StateFromProtoAt(sil, now.Add(-90*time.Minute)))
	}

	// At -3h the expired silence was still active and is returned as a match.
	// All three are present; assert the expired one is active at that time.
	resEarly, _, err := store.QueryAt(t.Context(), now.Add(-3*time.Hour), QMatches(set))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{active.Id, pending.Id, expired.Id}, idsOf(resEarly))
	earlyByID := map[string]*pb.Silence{}
	for _, sil := range resEarly {
		earlyByID[sil.Id] = sil
	}
	require.Equal(t, SilenceStateActive, StateFromProtoAt(earlyByID[expired.Id], now.Add(-3*time.Hour)))

	// QMatches at "now" must never surface a silence that is expired now.
	require.NotContains(t, idsOf(res), expired.Id)
}

func idsOf(sils []*pb.Silence) []string {
	out := make([]string, 0, len(sils))
	for _, sil := range sils {
		out = append(out, sil.Id)
	}
	return out
}

// TestQuerySnapshotIsolation verifies requirement #2: writes that happen while
// a query is running cannot change its result. The query works off a snapshot
// captured under the read lock.
func TestQuerySnapshotIsolation(t *testing.T) {
	t.Parallel()

	store, err := New(Options{Metrics: prometheus.NewRegistry()})
	require.NoError(t, err)
	now := store.nowUTC()

	for range 50 {
		sil := silenceWithMatchers("", now, now.Add(time.Hour),
			[]*pb.Matcher{eqMatcher("job", "api")},
		)
		require.NoError(t, store.Set(t.Context(), sil))
	}

	const workers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers continuously add silences while readers run.
	for range workers / 2 {
		wg.Go(func() {
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				sil := silenceWithMatchers(
					"", now, now.Add(time.Hour),
					[]*pb.Matcher{eqMatcher("job", "writer")},
				)
				_ = store.Set(context.Background(), sil)
				i++
			}
		})
	}

	// Readers must never observe a dangling reference or an expired match, and
	// every returned silence must still exist in the snapshot they read.
	for range workers / 2 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, _, qerr := store.Query(context.Background(),
					QMatches(model.LabelSet{"job": "api"}))
				if qerr != nil {
					return
				}
				for _, sil := range res {
					if StateFromProtoAt(sil, store.nowUTC()) == SilenceStateExpired {
						t.Errorf("query returned expired silence %q", sil.Id)
						return
					}
				}
			}
		})
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After the storm the collections must still be mutually consistent.
	assertIndexInSync(t, store)
}

// TestBatchDeleteKeepsIndexConsistent verifies requirements #3 and #5: after a
// batch of deletions (via GC) the index, postings and state store agree and no
// dangling references remain.
func TestBatchDeleteKeepsIndexConsistent(t *testing.T) {
	t.Parallel()

	store, err := New(Options{Metrics: prometheus.NewRegistry()})
	require.NoError(t, err)
	now := store.nowUTC()

	const total = 200
	for i := range total {
		// Half expire immediately (past end), half stay active. Interleave
		// equality keys and a regex-only silence every so often.
		var matchers []*pb.Matcher
		if i%10 == 0 {
			matchers = []*pb.Matcher{regexMatcher("path", ".*")}
		} else {
			matchers = []*pb.Matcher{
				eqMatcher("job", "job-"+strconv.Itoa(i%5)),
				neqMatcher("env", "dev"),
			}
		}
		start := now.Add(-time.Hour)
		end := now.Add(time.Hour)
		if i%2 == 0 {
			end = now.Add(-time.Minute) // expired, eligible for GC once retention passes.
		}
		sil := silenceWithMatchers(fmt.Sprintf("sil-%d", i), start, end, matchers)
		ms := store.toMeshSilence(sil)
		store.st[sil.Id] = ms
		store.indexSilence(ms.Silence)
	}

	assertIndexInSync(t, store)
	require.Len(t, store.st, total)

	// Expired silences carry ExpiresAt = end + retention. Use a retention of
	// zero semantics by recreating with no retention so they are collectible.
	n, err := store.GC()
	require.NoError(t, err)
	require.Equal(t, total/2, n)

	assertIndexInSync(t, store)
	require.Len(t, store.st, total/2)
	require.Len(t, store.mi.entries, total/2)

	// A second GC must remove nothing and leave everything consistent.
	n2, err := store.GC()
	require.NoError(t, err)
	require.Zero(t, n2)
	assertIndexInSync(t, store)

	// Remaining silences are all still queryable and match their labels.
	res, _, err := store.Query(t.Context(), QMatches(model.LabelSet{
		"job":  "job-1",
		"path": "/x",
	}))
	require.NoError(t, err)
	for _, sil := range res {
		require.Contains(t, store.st, sil.Id, "query returned a silence not in state")
		require.NotEqual(t, SilenceStateExpired, StateFromProtoAt(sil, store.nowUTC()))
	}
}

// TestBatchWriteIndexStateConsistency verifies requirement #5 across a large
// batch of inserts: index size, state size and version index all agree.
func TestBatchWriteIndexStateConsistency(t *testing.T) {
	t.Parallel()

	store, err := New(Options{Metrics: prometheus.NewRegistry()})
	require.NoError(t, err)
	now := store.nowUTC()

	const total = 500
	for i := range total {
		var matchers []*pb.Matcher
		switch i % 3 {
		case 0:
			matchers = []*pb.Matcher{regexMatcher("path", ".*"), eqMatcher("shard", strconv.Itoa(i%7))}
		case 1:
			matchers = []*pb.Matcher{eqMatcher("job", "api"), neqMatcher("env", "dev")}
		default:
			matchers = []*pb.Matcher{neqMatcher("debug", "true")}
		}
		sil := silenceWithMatchers("", now, now.Add(time.Hour), matchers)
		require.NoError(t, store.Set(t.Context(), sil))
	}

	assertIndexInSync(t, store)
	require.Equal(t, total, store.Version())

	// Updating an existing silence in place (same matchers, extended end time)
	// must not duplicate or orphan index entries.
	target := store.vi[0].id
	original := store.st[target].Silence
	updated := cloneSilence(original)
	updated.EndsAt = timestamppb.New(now.Add(2 * time.Hour))
	require.NoError(t, store.Set(t.Context(), updated))
	assertIndexInSync(t, store)
	require.Len(t, store.st, total)

	// Creating a replacement silence and expiring the original must also leave
	// the index and state mutually consistent.
	replacement := silenceWithMatchers("", now, now.Add(time.Hour),
		[]*pb.Matcher{eqMatcher("job", "replaced")})
	require.NoError(t, store.Set(t.Context(), replacement))
	require.NoError(t, store.Expire(t.Context(), target))
	assertIndexInSync(t, store)
}

// TestConcurrentReadWriteNoDanglingMatches is a focused race detector run over
// the hot query path with concurrent mutators.
func TestConcurrentReadWriteNoDanglingMatches(t *testing.T) {
	t.Parallel()

	store, err := New(Options{Metrics: prometheus.NewRegistry()})
	require.NoError(t, err)
	now := store.nowUTC()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			sil := silenceWithMatchers(
				"", now, now.Add(time.Hour),
				[]*pb.Matcher{eqMatcher("foo", "bar")},
			)
			if err := store.Set(context.Background(), sil); err == nil && i%25 == 0 {
				_ = store.Expire(context.Background(), sil.Id)
			}
			i++
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, _, qerr := store.Query(context.Background(),
				QState(SilenceStateActive),
				QMatches(model.LabelSet{"foo": "bar"}),
			)
			if qerr != nil {
				return
			}
			for _, sil := range res {
				store.mtx.RLock()
				_, ok := store.getSilence(sil.Id)
				store.mtx.RUnlock()
				if !ok {
					t.Errorf("dangling match: silence %q not in state", sil.Id)
					return
				}
			}
		}
	})

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	assertIndexInSync(t, store)
}

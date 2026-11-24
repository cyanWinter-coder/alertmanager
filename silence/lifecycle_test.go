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
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/prometheus/alertmanager/eventrecorder"
	"github.com/prometheus/alertmanager/labelset"
	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// newLifecycleSilence builds a silence matching foo=bar that is active now and
// ends after the given duration.
func newLifecycleSilence(now time.Time, end time.Duration) *pb.Silence {
	return &pb.Silence{
		MatcherSets: []*pb.MatcherSet{{
			Matchers: []*pb.Matcher{{
				Type:    pb.Matcher_EQUAL,
				Name:    "foo",
				Pattern: "bar",
			}},
		}},
		StartsAt: timestamppb.New(now),
		EndsAt:   timestamppb.New(now.Add(end)),
	}
}

// TestSilenceCreatedImmediatelyQueryable covers requirement 1: a silence must be
// queryable as soon as Set returns, and once its end time has passed it must
// neither be reported as active nor mute any alert.
func TestSilenceCreatedImmediatelyQueryable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)
		silencer := NewSilencer(s, promslog.NewNopLogger(), eventrecorder.NopRecorder())
		lset := labelset.FromModel(model.LabelSet{"foo": "bar"})

		now := s.nowUTC()
		sil := newLifecycleSilence(now, time.Minute)
		require.NoError(t, s.Set(t.Context(), sil))
		require.NotEmpty(t, sil.Id)

		// Immediately queryable by ID.
		got, err := s.QueryOne(t.Context(), QIDs(sil.Id))
		require.NoError(t, err)
		require.Equal(t, sil.Id, got.Id)

		// Immediately matches alerts while active.
		require.True(t, silencer.Mutes(t.Context(), lset))

		// Advance past the end time. The end is exclusive, so at EndsAt the
		// silence is expired and must stop muting without any GC running.
		time.Sleep(time.Minute)
		expired, _, err := s.Query(t.Context(), QIDs(sil.Id), QState(SilenceStateActive))
		require.NoError(t, err)
		require.Empty(t, expired, "expired silence must not match an active-state query")

		state, err := s.QueryOne(t.Context(), QIDs(sil.Id))
		require.NoError(t, err)
		require.Equal(t, SilenceStateExpired, StateFromProtoAt(state, s.nowUTC()))
		require.False(t, silencer.Mutes(t.Context(), lset), "expired silence must not mute alerts")
	})
}

// TestSilenceExpiryBumpsVersion covers requirement 3: expiring a silence,
// letting it time out, and garbage collecting it all advance the state version
// so query-side caches never hold on to a stale muting decision.
func TestSilenceExpiryBumpsVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: 0})
		require.NoError(t, err)

		now := s.nowUTC()
		sil := newLifecycleSilence(now, time.Minute)
		require.NoError(t, s.Set(t.Context(), sil))
		versionAfterCreate := s.Version()
		require.Positive(t, versionAfterCreate)

		// Explicit expiry changes the stored silence and bumps the version.
		// Advance the clock so the new UpdatedAt supersedes the stored one.
		time.Sleep(time.Millisecond)
		require.NoError(t, s.Expire(t.Context(), sil.Id))
		require.Greater(t, s.Version(), versionAfterCreate)

		// Idempotent expiry of an already expired silence is a no-op and must
		// not move the version.
		versionBeforeIdempotent := s.Version()
		require.NoError(t, s.Expire(t.Context(), sil.Id))
		require.Equal(t, versionBeforeIdempotent, s.Version())

		// With zero retention the expired silence is immediately collectable.
		// GC removes it and bumps the version again.
		n, err := s.GC()
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Greater(t, s.Version(), versionBeforeIdempotent)

		// A no-op GC leaves the version untouched.
		versionAfterGC := s.Version()
		n, err = s.GC()
		require.NoError(t, err)
		require.Zero(t, n)
		require.Equal(t, versionAfterGC, s.Version())
	})
}

// TestSilenceUpdateBumpsVersion covers requirement 3 for in-place edits: an
// update that actually changes the stored silence advances the version, while
// a no-op rewrite keeps it stable.
func TestSilenceUpdateBumpsVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)

		now := s.nowUTC()
		sil := newLifecycleSilence(now, time.Hour)
		require.NoError(t, s.Set(t.Context(), sil))

		// A no-op rewrite does not change the stored state.
		versionBefore := s.Version()
		require.NoError(t, s.Set(t.Context(), cloneSilence(sil)))
		require.Equal(t, versionBefore, s.Version())

		// A real edit advances the version even though the ID and version index
		// entry stay the same.
		time.Sleep(time.Millisecond)
		updated := cloneSilence(sil)
		updated.Comment = "extended"
		updated.EndsAt = timestamppb.New(s.nowUTC().Add(2 * time.Hour))
		require.NoError(t, s.Set(t.Context(), updated))
		require.Equal(t, sil.Id, updated.Id)
		require.Greater(t, s.Version(), versionBefore)

		// The version index still holds a single entry for this silence, but the
		// overall state version reports the newer generation.
		require.Len(t, s.vi, 1)
	})
}

// TestGCFailurePreservesAllSilences covers requirement 2: when maintenance
// encounters an invalid entry it must not partially delete state. Every
// silence, including the invalid one, stays in place and the version is
// unchanged; a later GC after the invalid entry was repaired succeeds.
func TestGCFailurePreservesAllSilences(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry()})
		require.NoError(t, err)
		now := s.nowUTC()

		// An unexpired, valid silence that must survive the failed GC.
		valid := &pb.MeshSilence{
			Silence: &pb.Silence{
				Id: "valid",
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "b"}},
				}},
				StartsAt:  timestamppb.New(now),
				EndsAt:    timestamppb.New(now.Add(time.Hour)),
				UpdatedAt: timestamppb.New(now),
			},
			ExpiresAt: timestamppb.New(now.Add(time.Hour)),
		}
		// A silence that is genuinely due for GC but is malformed.
		invalid := &pb.MeshSilence{
			Silence: &pb.Silence{
				Id: "invalid",
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "c"}},
				}},
				StartsAt:  timestamppb.New(now),
				EndsAt:    timestamppb.New(now),
				UpdatedAt: timestamppb.New(now),
			},
			ExpiresAt: nil,
		}
		for _, msil := range []*pb.MeshSilence{valid, invalid} {
			s.st[msil.Silence.Id] = msil
			s.indexSilence(msil.Silence)
		}
		// A dangling version index entry adds a second, independent error.
		s.vi.add(s.version+1, "missing")
		s.version++

		versionBefore := s.Version()

		n, gerr := s.GC()
		require.Error(t, gerr)
		require.Contains(t, gerr.Error(), "zero expiration timestamp")
		require.Contains(t, gerr.Error(), "missing from state")
		require.Zero(t, n)

		// Everything is preserved: no half-updated state.
		require.Len(t, s.st, 2)
		require.Len(t, s.mi, 2)
		require.Len(t, s.vi, 3)
		require.Contains(t, s.st, "valid")
		require.Contains(t, s.st, "invalid")
		require.Equal(t, versionBefore, s.Version())

		// Repair both problems and confirm GC now cleans up atomically.
		s.st["invalid"].ExpiresAt = timestamppb.New(now.Add(-time.Second))
		// Drop the dangling index entry.
		repairedVi := make(versionIndex, 0, len(s.vi)-1)
		for _, sv := range s.vi {
			if sv.id != "missing" {
				repairedVi = append(repairedVi, sv)
			}
		}
		s.vi = repairedVi

		n, gerr = s.GC()
		require.NoError(t, gerr)
		require.Equal(t, 1, n)
		require.Len(t, s.st, 1)
		require.Contains(t, s.st, "valid")
		require.NotContains(t, s.st, "invalid")
		require.Greater(t, s.Version(), versionBefore)
	})
}

// TestSnapshotRestoresOnlyUnexpired covers requirement 4: after a restart only
// silences whose mesh expiration is still in the future are restored, and the
// restored version index carries monotonically increasing per-entry versions.
func TestSnapshotRestoresOnlyUnexpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)
		now := s.nowUTC()

		entries := []*pb.MeshSilence{
			{
				// Unexpired: ends in the future, retention pushes ExpiresAt even further.
				Silence: &pb.Silence{
					Id: "active",
					MatcherSets: []*pb.MatcherSet{{
						Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "b"}},
					}},
					StartsAt:  timestamppb.New(now),
					EndsAt:    timestamppb.New(now.Add(time.Hour)),
					UpdatedAt: timestamppb.New(now),
				},
				ExpiresAt: timestamppb.New(now.Add(2 * time.Hour)),
			},
			{
				// Expired at the mesh level (retention elapsed) even though an
				// expired silence itself may still be interesting historically.
				Silence: &pb.Silence{
					Id: "gone",
					MatcherSets: []*pb.MatcherSet{{
						Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "c"}},
					}},
					StartsAt:  timestamppb.New(now.Add(-2 * time.Hour)),
					EndsAt:    timestamppb.New(now.Add(-time.Hour)),
					UpdatedAt: timestamppb.New(now.Add(-2 * time.Hour)),
				},
				ExpiresAt: timestamppb.New(now.Add(-time.Minute)),
			},
			{
				// Zero ExpiresAt must be treated as expired on restore.
				Silence: &pb.Silence{
					Id: "zero",
					MatcherSets: []*pb.MatcherSet{{
						Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "d"}},
					}},
					StartsAt:  timestamppb.New(now),
					EndsAt:    timestamppb.New(now.Add(time.Hour)),
					UpdatedAt: timestamppb.New(now),
				},
				ExpiresAt: nil,
			},
		}
		for _, e := range entries {
			s.st[e.Silence.Id] = e
		}

		f, err := os.CreateTemp(t.TempDir(), "silences")
		require.NoError(t, err)
		n, err := s.Snapshot(f)
		require.NoError(t, err)
		require.Positive(t, n)
		require.NoError(t, f.Close())

		// Simulate a restart by constructing a fresh Silences from the snapshot.
		restored, err := New(Options{
			Metrics:      prometheus.NewRegistry(),
			Retention:    time.Hour,
			SnapshotFile: f.Name(),
			Logger:       promslog.NewNopLogger(),
		})
		require.NoError(t, err)

		require.Len(t, restored.st, 1)
		require.Contains(t, restored.st, "active")
		require.NotContains(t, restored.st, "gone")
		require.NotContains(t, restored.st, "zero")
		require.Len(t, restored.mi, 1)
		require.Len(t, restored.vi, 1)
		require.Equal(t, 1, restored.Version(), "the single restored silence must own version 1")
		require.Equal(t, 1, restored.vi[0].version)
		require.Equal(t, "active", restored.vi[0].id)

		// The restored silence is queryable and still active.
		got, err := restored.QueryOne(t.Context(), QIDs("active"), QState(SilenceStateActive))
		require.NoError(t, err)
		require.Equal(t, "active", got.Id)
	})
}

// TestSnapshotRestoreEmptyKeepsZeroVersion asserts that a snapshot containing
// only expired silences restores an empty store at version zero, rather than
// inventing a version bump for nothing.
func TestSnapshotRestoreEmptyKeepsZeroVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry()})
		require.NoError(t, err)
		now := s.nowUTC()
		s.st["gone"] = &pb.MeshSilence{
			Silence: &pb.Silence{
				Id: "gone",
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "b"}},
				}},
				StartsAt:  timestamppb.New(now.Add(-time.Hour)),
				EndsAt:    timestamppb.New(now.Add(-time.Minute)),
				UpdatedAt: timestamppb.New(now.Add(-time.Hour)),
			},
			ExpiresAt: timestamppb.New(now.Add(-time.Second)),
		}

		var buf bytes.Buffer
		_, err = s.Snapshot(&buf)
		require.NoError(t, err)

		restored, err := New(Options{
			Metrics:        prometheus.NewRegistry(),
			SnapshotReader: bytes.NewReader(buf.Bytes()),
			Logger:         promslog.NewNopLogger(),
		})
		require.NoError(t, err)
		require.Empty(t, restored.st)
		require.Empty(t, restored.vi)
		require.Zero(t, restored.Version())
	})
}

// TestSilenceConcurrentLifecycle covers requirement 5: concurrent creation,
// expiry, GC and queries must never expose a half-updated state. While writers
// mutate the store, readers continuously verify that the state map, matcher
// index and version index stay mutually consistent and that the state version
// never moves backwards.
func TestSilenceConcurrentLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Millisecond})
		require.NoError(t, err)

		const writers = 8
		const iterations = 25
		const readerIterations = writers * iterations * 4

		var wg sync.WaitGroup

		// Reader goroutines continuously validate the internal invariants: the
		// three indices must reference exactly the same IDs, the version index
		// must stay ordered by version, and a query's version must never
		// regress. A half-applied mutation (for example a GC that deleted from
		// the state map but not the version index) would be caught here.
		for range 4 {
			wg.Go(func() {
				lastVersion := 0
				for range readerIterations {
					sils, version, qerr := s.Query(t.Context())
					if qerr != nil {
						t.Errorf("unexpected query error: %v", qerr)
						return
					}
					// The state version is monotonic.
					require.GreaterOrEqual(t, version, lastVersion)
					lastVersion = version

					// Query results are internally consistent: no duplicate IDs.
					seen := make(map[string]struct{}, len(sils))
					for _, sil := range sils {
						require.NotEmpty(t, sil.Id)
						_, dup := seen[sil.Id]
						require.False(t, dup, "query returned duplicate silence %s", sil.Id)
						seen[sil.Id] = struct{}{}
					}

					s.mtx.RLock()
					require.Len(t, s.mi, len(s.st), "state and matcher index diverged")
					require.Len(t, s.vi, len(s.st), "state and version index diverged")
					for i, sv := range s.vi {
						_, ok := s.st[sv.id]
						require.True(t, ok, "version index references missing silence %s", sv.id)
						_, ok = s.mi[sv.id]
						require.True(t, ok, "matcher index missing silence %s", sv.id)
						if i > 0 {
							require.GreaterOrEqual(t, sv.version, s.vi[i-1].version,
								"version index is not ordered by version")
						}
					}
					s.mtx.RUnlock()
					// Yield so other goroutines make progress and the synthetic
					// clock can advance under testing/synctest.
					time.Sleep(time.Millisecond)
				}
			})
		}

		for w := range writers {
			wg.Go(func() {
				for range iterations {
					now := s.nowUTC()
					sil := newLifecycleSilence(now, time.Millisecond)
					sil.CreatedBy = "writer"
					sil.Comment = string(rune('a' + w))
					require.NoError(t, s.Set(t.Context(), sil))

					// Immediately queryable under concurrency.
					if _, err := s.QueryOne(t.Context(), QIDs(sil.Id)); err != nil {
						t.Errorf("newly created silence %s not queryable: %v", sil.Id, err)
						return
					}
					// Expiry is idempotent and safe to race with GC.
					require.NoError(t, s.Expire(t.Context(), sil.Id))
					// Let the (1ms) retention elapse so the next GC actually
					// collects this silence, then collect it.
					time.Sleep(2 * time.Millisecond)
					if _, err := s.GC(); err != nil {
						t.Errorf("unexpected GC error: %v", err)
						return
					}
				}
			})
		}

		wg.Wait()
		synctest.Wait()

		// All silences were expired and collected, so the store and every index
		// must have converged to empty.
		n, err := s.GC()
		require.NoError(t, err)
		require.Zero(t, n)
		require.Empty(t, s.st)
		require.Empty(t, s.mi)
		require.Empty(t, s.vi)
	})
}

// TestMaintenanceFailurePreservesState drives the full Maintenance loop: a
// maintenance run whose GC fails must leave the state untouched and must not
// overwrite the snapshot; after the invalid entry is repaired the next run
// garbage collects normally and persists the resulting snapshot.
func TestMaintenanceFailurePreservesState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapFile := filepath.Join(t.TempDir(), "silences")
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Millisecond})
		require.NoError(t, err)
		now := s.nowUTC()

		valid := &pb.MeshSilence{
			Silence: &pb.Silence{
				Id: "valid",
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "b"}},
				}},
				StartsAt:  timestamppb.New(now),
				EndsAt:    timestamppb.New(now.Add(time.Hour)),
				UpdatedAt: timestamppb.New(now),
			},
			ExpiresAt: timestamppb.New(now.Add(time.Hour)),
		}
		invalid := &pb.MeshSilence{
			Silence: &pb.Silence{
				Id: "invalid",
				MatcherSets: []*pb.MatcherSet{{
					Matchers: []*pb.Matcher{{Type: pb.Matcher_EQUAL, Name: "a", Pattern: "c"}},
				}},
				StartsAt:  timestamppb.New(now),
				EndsAt:    timestamppb.New(now),
				UpdatedAt: timestamppb.New(now),
			},
			ExpiresAt: nil,
		}
		for _, msil := range []*pb.MeshSilence{valid, invalid} {
			s.st[msil.Silence.Id] = msil
			s.indexSilence(msil.Silence)
		}
		stateBefore := len(s.st)
		versionBefore := s.Version()

		stopc := make(chan struct{})
		done := make(chan struct{})
		go func() {
			s.Maintenance(time.Minute, snapFile, stopc, nil)
			close(done)
		}()

		// First tick: GC fails on the invalid silence. The failed run must not
		// touch the state and must not write a snapshot file.
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Len(t, s.st, stateBefore)
		require.Equal(t, versionBefore, s.Version())
		_, statErr := os.Stat(snapFile)
		require.True(t, os.IsNotExist(statErr), "failed maintenance must not write a snapshot")

		// Repair the invalid entry (make it genuinely expired) and tick again.
		s.mtx.Lock()
		s.st["invalid"].ExpiresAt = timestamppb.New(now.Add(-time.Second))
		s.mtx.Unlock()
		time.Sleep(time.Minute)
		synctest.Wait()

		close(stopc)
		<-done

		// The successful run collected the expired silence and persisted state.
		require.Len(t, s.st, 1)
		require.Contains(t, s.st, "valid")
		_, statErr = os.Stat(snapFile)
		require.NoError(t, statErr, "successful maintenance must write a snapshot")

		// A fresh instance restored from that snapshot sees only the survivor.
		restored, err := New(Options{
			Metrics:      prometheus.NewRegistry(),
			SnapshotFile: snapFile,
			Logger:       promslog.NewNopLogger(),
		})
		require.NoError(t, err)
		require.Len(t, restored.st, 1)
		require.Contains(t, restored.st, "valid")
	})
}

// TestSilencerCacheInvalidatedOnExpiry is the end-to-end reproduction of the
// on-call report: a silence that muted an alert must stop muting it once it
// expires, even when no newer silence is ever created (so the only signal the
// silencer cache gets is the version bump driven by expiry/GC).
func TestSilencerCacheInvalidatedOnExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Options{Metrics: prometheus.NewRegistry(), Retention: time.Hour})
		require.NoError(t, err)
		silencer := NewSilencer(s, promslog.NewNopLogger(), eventrecorder.NopRecorder())
		lset := labelset.FromModel(model.LabelSet{"foo": "bar"})

		now := s.nowUTC()
		sil := newLifecycleSilence(now, time.Minute)
		require.NoError(t, s.Set(t.Context(), sil))

		// Populate the silencer cache while the silence is active.
		require.True(t, silencer.Mutes(t.Context(), lset))
		cached := silencer.cache.get(lset.Fingerprint())
		require.Equal(t, []string{sil.Id}, cached.silenceIDs)

		// Time out the silence without creating any other silence. The cache
		// entry is only refreshed because the state version moves.
		time.Sleep(time.Minute)
		require.False(t, silencer.Mutes(t.Context(), lset))
		refreshed := silencer.cache.get(lset.Fingerprint())
		require.Empty(t, refreshed.silenceIDs)
		require.Equal(t, s.Version(), refreshed.version)
	})
}

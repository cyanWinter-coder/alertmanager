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
	"fmt"
	"sort"

	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/pkg/labels"
	pb "github.com/prometheus/alertmanager/silence/silencepb"
)

// idSet is an unordered set of silence IDs.
type idSet map[string]struct{}

func (s idSet) add(id string) {
	s[id] = struct{}{}
}

func (s idSet) contains(id string) bool {
	_, ok := s[id]
	return ok
}

// labelKey identifies one concrete label name/value pair that an equality
// matcher looks for.
type labelKey struct {
	name  string
	value string
}

// indexedSilence holds the index metadata for one silence alongside its
// compiled matcher sets:
//
//   - equalKeys records the positive equality matcher keys of each matcher
//     set (AND within a set, OR across sets). It drives candidate narrowing.
//   - unindexed is true when no matcher set contains a positive equality
//     matcher, so the silence can only be ruled out by a full evaluation and
//     must be a candidate of every query.
type indexedSilence struct {
	matchers  labels.MatcherSet
	equalKeys [][]labelKey
	unindexed bool
}

// matcherIndex backs silence matching lookups. It keeps three structures in
// sync, all keyed by silence ID:
//
//   - entries: compiled matcher sets plus the equality-key metadata used for
//     narrowing.
//   - postings: a label-based inverted index. Each positive equality matcher
//     contributes the silence ID to a posting keyed by its label name and
//     value, allowing candidate sets to be narrowed with postings lookups
//     instead of scanning every silence.
//   - unindexed: IDs of silences that contain no usable equality matcher in
//     any of their matcher sets. They must be considered candidates for every
//     query.
//
// A silence appears either in at least one posting or in unindexed, never in
// neither and never in both. Remove deletes every reference so no dangling ID
// survives a deletion.
type matcherIndex struct {
	entries   map[string]*indexedSilence
	postings  map[labelKey]idSet
	unindexed idSet
}

func newMatcherIndex(hint int) matcherIndex {
	return matcherIndex{
		entries:   make(map[string]*indexedSilence, hint),
		postings:  make(map[labelKey]idSet, hint),
		unindexed: make(idSet, hint),
	}
}

// get retrieves the compiled matcher set for a given silence.
func (c matcherIndex) get(s *pb.Silence) (labels.MatcherSet, error) {
	if e, ok := c.entries[s.Id]; ok {
		return e.matchers, nil
	}
	return nil, ErrNotFound
}

// add compiles a silence's matchers and indexes the result. It is idempotent:
// re-adding a silence first removes its previous entries, so replacing a
// silence cannot leave stale postings behind. It returns the compiled matcher
// set.
func (c matcherIndex) add(s *pb.Silence) (labels.MatcherSet, error) {
	// Drop any previous indexing for this ID so an update cannot leave stale
	// label postings pointing at the silence.
	c.remove(s.Id)

	matcherSet := make(labels.MatcherSet, 0, len(s.MatcherSets))
	equalKeys := make([][]labelKey, 0, len(s.MatcherSets))
	indexed := false

	for _, ms := range s.MatcherSets {
		matchers := make(labels.Matchers, len(ms.Matchers))
		keys := make([]labelKey, 0, len(ms.Matchers))

		for i, m := range ms.Matchers {
			var mt labels.MatchType
			switch m.Type {
			case pb.Matcher_EQUAL:
				mt = labels.MatchEqual
			case pb.Matcher_NOT_EQUAL:
				mt = labels.MatchNotEqual
			case pb.Matcher_REGEXP:
				mt = labels.MatchRegexp
			case pb.Matcher_NOT_REGEXP:
				mt = labels.MatchNotRegexp
			default:
				return nil, fmt.Errorf("unknown matcher type %q", m.Type)
			}
			matcher, err := labels.NewMatcher(mt, m.Name, m.Pattern)
			if err != nil {
				return nil, err
			}

			matchers[i] = matcher

			// Only positive equality matchers can narrow a candidate set:
			// everything else must be evaluated against each candidate.
			if m.Type == pb.Matcher_EQUAL {
				key := labelKey{name: m.Name, value: m.Pattern}
				keys = append(keys, key)
				posting, ok := c.postings[key]
				if !ok {
					posting = make(idSet)
					c.postings[key] = posting
				}
				posting.add(s.Id)
				indexed = true
			}
		}

		matcherSet = append(matcherSet, &matchers)
		equalKeys = append(equalKeys, keys)
	}

	c.entries[s.Id] = &indexedSilence{
		matchers:  matcherSet,
		equalKeys: equalKeys,
		unindexed: !indexed,
	}
	if !indexed {
		c.unindexed.add(s.Id)
	}
	return matcherSet, nil
}

// remove deletes every index reference to the silence ID, including its
// compiled matchers and metadata, all label postings and the unindexed
// fallback entry. Empty postings are reclaimed so the inverted index does not
// grow without bound as silences are deleted. It is a no-op for unknown IDs.
func (c matcherIndex) remove(id string) {
	if entry, ok := c.entries[id]; ok {
		for _, keys := range entry.equalKeys {
			for _, key := range keys {
				if posting, ok := c.postings[key]; ok {
					delete(posting, id)
					if len(posting) == 0 {
						delete(c.postings, key)
					}
				}
			}
		}
	}
	delete(c.entries, id)
	delete(c.unindexed, id)
}

// candidates returns the IDs of silences that could possibly match the given
// label set. It narrows the candidate set with postings lookups instead of
// scanning every silence; the full matcher evaluation (regex and negation
// matchers) still runs against every candidate afterwards.
//
// For each queried label, the matching posting gives the silences carrying an
// equality matcher for that exact name/value pair. A silence qualifies when
// all equality keys of at least one of its matcher sets are satisfied (AND
// within a set, OR across sets). Silences without any equality matcher are
// always included because their regex/negation matchers can only be ruled out
// by the full evaluation.
func (c matcherIndex) candidates(set model.LabelSet) idSet {
	candidates := make(idSet, len(c.unindexed))
	for id := range c.unindexed {
		candidates.add(id)
	}

	// Resolve the postings for every (name, value) pair present in the queried
	// label set. Only silences in these postings can match an equality key.
	present := make(map[labelKey]idSet, len(set))
	for name, value := range set {
		key := labelKey{name: string(name), value: string(value)}
		if posting, ok := c.postings[key]; ok {
			present[key] = posting
		}
	}

	// Walk every posting that the query can hit. Each silence found there is
	// checked against the equality keys of its matcher sets; a single fully
	// satisfied set makes it a candidate.
	seen := make(idSet)
	for _, posting := range present {
		for id := range posting {
			if seen.contains(id) {
				continue
			}
			seen.add(id)

			entry := c.entries[id]
			for _, keys := range entry.equalKeys {
				if len(keys) == 0 {
					// Matcher set with no equality matchers cannot narrow; the
					// full evaluation decides, so keep the silence as a candidate.
					candidates.add(id)
					break
				}
				allPresent := true
				for _, key := range keys {
					p, ok := present[key]
					if !ok || !p.contains(id) {
						allPresent = false
						break
					}
				}
				if allPresent {
					candidates.add(id)
					break
				}
			}
		}
	}
	return candidates
}

// silenceVersion associates a silence with the Silences version when it was created.
type silenceVersion struct {
	id      string
	version int
}

// versionIndex is a index into Silences ordered by the version of Silences when the
// silence was created. The index is always sorted from lowest to highest version.
//
// The versionIndex allows clients of Silences.Query to incrementally update local caches
// of query results. Instead of a new version requiring the client to scan  everything
// again to get an up-to-date picture of Silences, they can use the versionIndex to figure
// out which silences have been added since the last version they saw. This means they can
// just scan the NEW silences, rather than all of them.
type versionIndex []silenceVersion

// add pushes a new silenceVersionMapping to the back of the silenceVersionIndex. It does not
// validate the input.
func (s *versionIndex) add(version int, sil string) {
	*s = append(*s, silenceVersion{version: version, id: sil})
}

// findVersionGreaterThan uses a log(n) search to find the first index of the versionIndex
// which has a version higher than version. If any entries with a higher version exist,
// it returns true and the starting index (which is guaranteed to be a valid index into
// the slice). Otherwise it returns false.
func (s versionIndex) findVersionGreaterThan(version int) (index int, found bool) {
	startIdx := sort.Search(len(s), func(i int) bool {
		return s[i].version > version
	})
	return startIdx, startIdx < len(s)
}

func (s *Silences) indexSilence(sil *pb.Silence) {
	s.version++
	s.vi.add(s.version, sil.Id)
	if _, err := s.mi.add(sil); err != nil {
		s.metrics.matcherCompileIndexSilenceErrorsTotal.Inc()
		s.logger.Error("Failed to compile silence matchers", "silence_id", sil.Id, "err", err)
	}
}

// deindexSilence removes every index reference to the silence so no dangling
// entry remains after it is deleted from the state store. It must be called
// while holding s.mtx.
func (s *Silences) deindexSilence(id string) {
	s.mi.remove(id)
}

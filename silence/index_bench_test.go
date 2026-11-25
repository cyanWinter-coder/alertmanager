package silence

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/prometheus/alertmanager/silence/silencepb"
)

// BenchmarkQueryNarrowing measures a low-selectivity equality query where only
// a handful of silences can match, isolating the label-index narrowing benefit.
func BenchmarkQueryNarrowing(b *testing.B) {
	for _, total := range []int{1000, 10000, 50000} {
		b.Run(strconv.Itoa(total), func(b *testing.B) {
			s, err := New(Options{Metrics: prometheus.NewRegistry()})
			require.NoError(b, err)
			now := time.Now()
			for i := range total {
				sil := &silencepb.Silence{
					Matchers: []*silencepb.Matcher{{
						Type: silencepb.Matcher_EQUAL, Name: "job", Pattern: "job-" + strconv.Itoa(i),
					}},
					StartsAt: timestamppb.New(now.Add(-time.Minute)),
					EndsAt:   timestamppb.New(now.Add(time.Hour)),
				}
				require.NoError(b, s.Set(b.Context(), sil))
			}
			// One needle that actually matches.
			needle := &silencepb.Silence{
				Matchers: []*silencepb.Matcher{{
					Type: silencepb.Matcher_EQUAL, Name: "job", Pattern: "needle",
				}},
				StartsAt: timestamppb.New(now.Add(-time.Minute)),
				EndsAt:   timestamppb.New(now.Add(time.Hour)),
			}
			require.NoError(b, s.Set(b.Context(), needle))
			lset := model.LabelSet{"job": "needle"}
			sils, _, err := s.Query(b.Context(), QState(SilenceStateActive), QMatches(lset))
			require.NoError(b, err)
			require.Len(b, sils, 1)
			b.ResetTimer()
			for b.Loop() {
				_, _, err := s.Query(context.Background(), QState(SilenceStateActive), QMatches(lset))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

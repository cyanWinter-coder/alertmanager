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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/alertmanager/alert"
	"github.com/prometheus/alertmanager/eventrecorder"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/nflog/nflogpb"
)

func newRetryTestStage(t *testing.T, notifier Notifier) (*RetryStage, *inflightRegistry) {
	t.Helper()
	integration := NewIntegration(notifier, sendResolved(false), "test", 0, "test-receiver")
	registry := newInflightRegistry()
	return NewRetryStage(integration, "test", NewMetrics(prometheus.NewRegistry(), featurecontrol.NoopFlags{}), eventrecorder.NopRecorder(), registry), registry
}

// errIntegrationDown is a shared sentinel so tests can errors.Is across
// different notifier closures.
var errIntegrationDown = errors.New("integration down")

// errUpstreamUnavailable is the last integration error a retry loop returns
// when its context is canceled while waiting for the next attempt.
var errUpstreamUnavailable = errors.New("upstream unavailable")

func retryTestAlerts() []*alert.Alert {
	return []*alert.Alert{
		alert.New(model.Alert{
			Labels: model.LabelSet{"alertname": "test", "instance": "api-1"},
			EndsAt: time.Now().Add(time.Hour),
		}, time.Time{}, false),
	}
}

func retryTestContext() context.Context {
	ctx := context.Background()
	ctx = WithGroupKey(ctx, "testkey")
	ctx = WithFiringAlerts(ctx, []uint64{0})
	ctx = WithRepeatInterval(ctx, time.Hour)
	return ctx
}

// TestRetryStageCanceledContextStopsImmediately checks requirement 1: when the
// pipeline context is canceled while a backoff wait is in progress, the stage
// returns promptly and no further attempt is scheduled.
func TestRetryStageCanceledContextStopsImmediately(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
			attempts.Add(1)
			return Retry(0, errUpstreamUnavailable, ServerErrorReason)
		}))

		ctx, cancel := context.WithCancel(retryTestContext())
		done := make(chan error, 1)
		go func() {
			_, _, err := stage.Exec(ctx, promslog.NewNopLogger(), retryTestAlerts()...)
			done <- err
		}()
		synctest.Wait()

		// First attempt happens immediately and fails; the second attempt is
		// parked behind the exponential backoff.
		require.Equal(t, int32(1), attempts.Load())
		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			require.Error(t, err)
			// Canceled during a backoff wait: the loop returns the last error
			// reported by the integration rather than scheduling another try.
			require.ErrorIs(t, err, errUpstreamUnavailable)
		case <-time.After(time.Second):
			t.Fatal("retry stage did not return after the context was canceled")
		}

		// Give any wrongly scheduled attempt a chance to run, then prove none
		// happened: cancellation must not schedule another try.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(1), attempts.Load(), "no attempt may run after cancellation")
		require.Equal(t, 1.0, prom_testutil.ToFloat64(stage.metrics.numTotalFailedNotifications.WithLabelValues("test", ServerErrorReason.String())))
	})
}

// TestRetryStageCanceledWhileNotifyInFlight checks requirement 1 from the other
// side: an attempt whose own context gets canceled returns that error instead
// of being retried.
func TestRetryStageCanceledWhileNotifyInFlight(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(retryTestContext())
	attempted := make(chan struct{})
	stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
		close(attempted)
		<-ctx.Done()
		return Retry(0, ctx.Err(), RequestErrorReason)
	}))

	done := make(chan error, 1)
	go func() {
		_, _, err := stage.Exec(ctx, promslog.NewNopLogger(), retryTestAlerts()...)
		done <- err
	}()
	<-attempted
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("retry stage did not return after the context was canceled")
	}
}

// TestRetryStageBackoffGrowsAndIsCapped checks requirement 2: the delays
// between attempts grow exponentially, stay within the backoff's randomized
// MaxInterval bound, and once the total retry budget is spent the stage
// returns the last error.
func TestRetryStageBackoffGrowsAndIsCapped(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("upstream permanently down")
		var (
			attempts    atomic.Int32
			attemptTime []time.Time
			delays      []time.Duration
			mu          sync.Mutex
		)
		stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
			mu.Lock()
			now := time.Now()
			if len(attemptTime) > 0 {
				delays = append(delays, now.Sub(attemptTime[len(attemptTime)-1]))
			}
			attemptTime = append(attemptTime, now)
			mu.Unlock()
			attempts.Add(1)
			return Retry(0, boom, ServerErrorReason)
		}))

		start := time.Now()
		_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		elapsed := time.Since(start)

		require.Error(t, err)
		require.ErrorIs(t, err, boom, "the last integration error must be wrapped in the returned error")
		require.Contains(t, err.Error(), "retry budget exhausted")
		require.Equal(t, retryMaxElapsedTime, elapsed)
		require.Greater(t, attempts.Load(), int32(10), "exponential retries should run many times within the budget")

		mu.Lock()
		gotDelays := append([]time.Duration(nil), delays...)
		mu.Unlock()
		require.NotEmpty(t, gotDelays)

		// MaxInterval caps the backoff's base interval; with the default
		// randomization factor of 0.5 the randomized wait reaches at most
		// 1.5 * MaxInterval. Jitter can invert two adjacent samples, so the
		// growth trend is checked statistically: later waits must average
		// longer than early ones.
		maxRandomized := time.Duration(float64(backoff.DefaultMaxInterval) * (1 + backoff.DefaultRandomizationFactor))
		for _, d := range gotDelays {
			require.LessOrEqual(t, d, maxRandomized+time.Millisecond, "backoff must stay within the randomized MaxInterval")
		}
		firstHalf := average(gotDelays[:len(gotDelays)/2])
		secondHalf := average(gotDelays[len(gotDelays)/2:])
		require.Greater(t, secondHalf, firstHalf, "backoff delays must trend upward toward the cap")
	})
}

// TestRetryBackoffScheduleDeterministic pins the unjittered growth and cap
// behavior with RandomizationFactor disabled, so the exponential progression
// and MaxInterval cap are checked without depending on random samples.
func TestRetryBackoffScheduleDeterministic(t *testing.T) {
	t.Parallel()

	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.Reset()

	var schedule []time.Duration
	for range 20 {
		schedule = append(schedule, b.NextBackOff())
	}

	// Base intervals grow by Multiplier until they reach MaxInterval, after
	// which every wait is exactly the cap.
	for i := 1; i < len(schedule); i++ {
		if schedule[i-1] < backoff.DefaultMaxInterval {
			require.Greater(t, schedule[i], schedule[i-1], "unjittered backoff must grow strictly until capped")
		}
		require.LessOrEqual(t, schedule[i], backoff.DefaultMaxInterval, "base interval must not exceed MaxInterval")
	}
	require.Equal(t, backoff.DefaultMaxInterval, schedule[len(schedule)-1], "the schedule must settle at MaxInterval")
	require.Equal(t, backoff.DefaultMaxInterval, schedule[len(schedule)-2], "the cap holds once reached")
}

func average(ds []time.Duration) time.Duration {
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return sum / time.Duration(len(ds))
}

// TestRetryStageBackoffBudgetLeavesNoSuccessRecord checks requirement 4: when
// every attempt fails and the budget is exhausted, the failure must not be
// recorded as a delivery. In the production pipeline SetNotifiesStage only
// runs after a successful retry, so assert the stage returns an error and the
// notification log is never written.
func TestRetryStageBackoffBudgetLeavesNoSuccessRecord(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var logged atomic.Bool
		tnflog := &testNflog{
			qerr: nflog.ErrNotFound,
			logFunc: func(_ *nflogpb.Receiver, _ string, _, _, _ []uint64, _ *nflog.Store, _ time.Duration) error {
				logged.Store(true)
				return nil
			},
		}
		recv := &nflogpb.Receiver{GroupName: "test"}
		integration := NewIntegration(
			notifierFunc(func(context.Context, ...*alert.Alert) NotifyVerdict {
				return Retry(0, errors.New("permanent failure"), ServerErrorReason)
			}),
			sendResolved(false), "test", 0, "test-receiver",
		)
		registry := newInflightRegistry()
		metrics := NewMetrics(prometheus.NewRegistry(), featurecontrol.NoopFlags{})
		stages := MultiStage{
			NewDedupStage(&integration, tnflog, recv),
			NewRetryStage(integration, "test", metrics, eventrecorder.NopRecorder(), registry),
			NewSetNotifiesStage(tnflog, recv, featurecontrol.NoopFlags{}),
		}

		_, _, err := stages.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		require.Error(t, err)
		require.False(t, logged.Load(), "a failed notification must never be written to the notification log")
	})
}

// TestRetryStageRetrySuccessAfterFailuresUpdatesRecord checks requirement 4's
// happy path: a delivery that fails first and then succeeds is recorded once,
// with exactly one notification log write carrying the alert fingerprint.
func TestRetryStageRetrySuccessAfterFailuresUpdatesRecord(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var (
			attempts atomic.Int32
			logCalls atomic.Int32
			lastLog  struct {
				firing []uint64
				mu     sync.Mutex
			}
		)
		tnflog := &testNflog{
			qerr: nflog.ErrNotFound,
			logFunc: func(_ *nflogpb.Receiver, _ string, firingAlerts, _, _ []uint64, _ *nflog.Store, _ time.Duration) error {
				logCalls.Add(1)
				lastLog.mu.Lock()
				lastLog.firing = append([]uint64(nil), firingAlerts...)
				lastLog.mu.Unlock()
				return nil
			},
		}
		recv := &nflogpb.Receiver{GroupName: "test"}
		integration := NewIntegration(
			notifierFunc(func(context.Context, ...*alert.Alert) NotifyVerdict {
				if attempts.Add(1) <= 2 {
					return Retry(0, errors.New("temporary failure"), ServerErrorReason)
				}
				return Success()
			}),
			sendResolved(false), "test", 0, "test-receiver",
		)
		registry := newInflightRegistry()
		metrics := NewMetrics(prometheus.NewRegistry(), featurecontrol.NoopFlags{})
		stages := MultiStage{
			NewDedupStage(&integration, tnflog, recv),
			NewRetryStage(integration, "test", metrics, eventrecorder.NopRecorder(), registry),
			NewSetNotifiesStage(tnflog, recv, featurecontrol.NoopFlags{}),
		}

		alerts := retryTestAlerts()
		_, res, err := stages.Exec(retryTestContext(), promslog.NewNopLogger(), alerts...)
		require.NoError(t, err)
		require.Equal(t, alerts, res)
		require.Equal(t, int32(3), attempts.Load(), "the two failed attempts must be retried before success")
		require.Equal(t, int32(1), logCalls.Load(), "only the successful delivery updates the notification log")
		lastLog.mu.Lock()
		require.Equal(t, []uint64{hashAlert(alerts[0])}, lastLog.firing)
		lastLog.mu.Unlock()
	})
}

// TestRetryStageConcurrentSameBatchCoalesces checks requirements 3 and 5:
// concurrent executions for the same aggregation group and the same batch of
// alerts result in a single physical delivery whose result every caller
// observes.
func TestRetryStageConcurrentSameBatchCoalesces(t *testing.T) {
	t.Parallel()

	var deliveries atomic.Int32
	stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
		deliveries.Add(1)
		// Stay in flight long enough for the other goroutines to join.
		time.Sleep(50 * time.Millisecond)
		return Success()
	}))

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			_, alerts, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
			require.Len(t, alerts, 1)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), deliveries.Load(), "the receiver must be notified exactly once for the batch")
	require.Equal(t, 1.0, prom_testutil.ToFloat64(stage.metrics.numNotificationRequestsTotal.WithLabelValues("test")))
}

// TestRetryStageConcurrentDifferentBatchesDoNotCoalesce checks requirement 3's
// boundary: a different alert fingerprint set is a different notification and
// must be delivered independently.
func TestRetryStageConcurrentDifferentBatchesDoNotCoalesce(t *testing.T) {
	t.Parallel()

	var deliveries atomic.Int32
	stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
		deliveries.Add(1)
		time.Sleep(50 * time.Millisecond)
		return Success()
	}))

	other := alert.New(model.Alert{
		Labels: model.LabelSet{"alertname": "test", "instance": "api-2"},
		EndsAt: time.Now().Add(time.Hour),
	}, time.Time{}, false)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		require.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), other)
		require.NoError(t, err)
	}()
	wg.Wait()

	require.Equal(t, int32(2), deliveries.Load(), "different alert batches must each be delivered")
}

// TestRetryStageConcurrentRetrySharesFailure checks requirement 5 for the
// failing path: while the leader keeps retrying, followers wait on the same
// delivery and get the leader's final error without triggering extra attempts.
func TestRetryStageConcurrentRetrySharesFailure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// First run: how many attempts does one leader make over the whole
		// retry budget?
		var soloAttempts atomic.Int32
		soloStage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
			soloAttempts.Add(1)
			return Retry(0, errIntegrationDown, ServerErrorReason)
		}))
		_, _, soloErr := soloStage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		require.Error(t, soloErr)
		require.ErrorIs(t, soloErr, errIntegrationDown)

		// Second run: several callers racing for the same batch must not
		// multiply the number of physical attempts.
		var sharedAttempts atomic.Int32
		stage, _ := newRetryTestStage(t, notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
			sharedAttempts.Add(1)
			return Retry(0, errIntegrationDown, ServerErrorReason)
		}))

		const callers = 4
		var wg sync.WaitGroup
		errs := make(chan error, callers)
		wg.Add(callers)
		for range callers {
			go func() {
				defer wg.Done()
				_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			require.Error(t, err)
			require.ErrorIs(t, err, errIntegrationDown)
		}
		// Jitter can move the exact attempt at which the budget deadline lands
		// by one, so compare against a tight window around the solo run rather
		// than an exact count. The property under test is that four callers do
		// not multiply the physical attempts: without coalescing the count
		// would be roughly callers times the solo count.
		require.InDelta(t, soloAttempts.Load(), sharedAttempts.Load(), 2,
			"coalesced callers must share one leader's retry loop")
		require.Less(t, sharedAttempts.Load(), soloAttempts.Load()*int32(callers))
	})
}

// TestRetryStageFollowerCanceledWhileLeaderRetries checks requirement 1 for
// coalesced calls: canceling a follower's context unblocks that follower
// without canceling the leader's delivery.
func TestRetryStageFollowerCanceledWhileLeaderRetries(t *testing.T) {
	t.Parallel()

	leaderInAttempt := make(chan struct{})
	releaseLeader := make(chan struct{})
	var deliveries atomic.Int32
	integration := NewIntegration(
		notifierFunc(func(ctx context.Context, _ ...*alert.Alert) NotifyVerdict {
			if deliveries.Add(1) == 1 {
				close(leaderInAttempt)
				<-releaseLeader
			}
			return Success()
		}),
		sendResolved(false), "test", 0, "test-receiver",
	)
	registry := newInflightRegistry()
	stage := NewRetryStage(integration, "test", NewMetrics(prometheus.NewRegistry(), featurecontrol.NoopFlags{}), eventrecorder.NopRecorder(), registry)

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		require.NoError(t, err)
	}()
	<-leaderInAttempt

	followerCtx, cancel := context.WithCancel(retryTestContext())
	followerDone := make(chan error, 1)
	go func() {
		_, _, err := stage.Exec(followerCtx, promslog.NewNopLogger(), retryTestAlerts()...)
		followerDone <- err
	}()

	cancel()
	select {
	case err := <-followerDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("follower did not return after its context was canceled")
	}

	close(releaseLeader)
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("leader delivery was aborted by the follower's cancellation")
	}
	require.Equal(t, int32(1), deliveries.Load(), "canceling a follower must not trigger another delivery")
}

// TestInflightRegistryReuseAfterLeaderFinishes makes sure a completed delivery
// unregisters its key so the next notification batch for the same group is
// delivered again rather than waiting on a stale call.
func TestInflightRegistryReuseAfterLeaderFinishes(t *testing.T) {
	t.Parallel()

	registry := newInflightRegistry()
	ctx := context.Background()
	key := "k"

	leader, call, finish := registry.joinOrStart(key)
	require.True(t, leader)
	require.NotNil(t, call)

	leader2, call2, _ := registry.joinOrStart(key)
	require.False(t, leader2)
	require.Same(t, call, call2)

	finish(nil)
	require.NoError(t, call2.wait(ctx))

	leader3, _, _ := registry.joinOrStart(key)
	require.True(t, leader3, "the key must be available again after the leader finished")
}

// TestInflightKeyContainsGroupIntegrationAndFingerprints pins requirement 3's
// key composition: group, integration and alert fingerprints all participate.
func TestInflightKeyContainsGroupIntegrationAndFingerprints(t *testing.T) {
	t.Parallel()

	alerts := retryTestAlerts()
	otherGroup := alert.New(model.Alert{
		Labels: model.LabelSet{"alertname": "other"},
		EndsAt: time.Now().Add(time.Hour),
	}, time.Time{}, false)

	base := NewIntegration(notifierFunc(func(context.Context, ...*alert.Alert) NotifyVerdict { return Success() }), sendResolved(false), "webhook", 0, "recv")
	sameIdxDifferentName := NewIntegration(notifierFunc(func(context.Context, ...*alert.Alert) NotifyVerdict { return Success() }), sendResolved(false), "email", 0, "recv")

	cases := []struct {
		name string
		a    string
		b    string
		eq   bool
	}{
		{"same group, integration and alerts", inflightKey("g", base, alerts), inflightKey("g", base, alerts), true},
		{"alert order does not matter", inflightKey("g", base, alerts), inflightKey("g", base, slicesReversed(alerts)), true},
		{"different group", inflightKey("g", base, alerts), inflightKey("h", base, alerts), false},
		{"different integration", inflightKey("g", base, alerts), inflightKey("g", sameIdxDifferentName, alerts), false},
		{"different alerts", inflightKey("g", base, alerts), inflightKey("g", base, []*alert.Alert{otherGroup}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.eq {
				require.Equal(t, tc.a, tc.b)
			} else {
				require.NotEqual(t, tc.a, tc.b)
			}
		})
	}
}

func slicesReversed(alerts []*alert.Alert) []*alert.Alert {
	out := append([]*alert.Alert(nil), alerts...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// TestRetryStageFailureReasonForExhaustedBudget pins the metrics reason used
// when the retry budget is spent.
func TestRetryStageFailureReasonForExhaustedBudget(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		stage, _ := newRetryTestStage(t, notifierFunc(func(context.Context, ...*alert.Alert) NotifyVerdict {
			return Retry(0, errors.New("nope"), ServerErrorReason)
		}))

		_, _, err := stage.Exec(retryTestContext(), promslog.NewNopLogger(), retryTestAlerts()...)
		require.Error(t, err)
		require.Equal(t, 1.0, prom_testutil.ToFloat64(stage.metrics.numTotalFailedNotifications.WithLabelValues("test", RetryBudgetExhaustedReason.String())))
	})
}

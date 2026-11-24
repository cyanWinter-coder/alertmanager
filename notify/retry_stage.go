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
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/alertmanager/alert"
	"github.com/prometheus/alertmanager/eventrecorder"
)

// retryMaxElapsedTime bounds the total time spent retrying a notification.
// Exponential backoff alone only caps the delay between two attempts, so a
// permanently failing integration would otherwise retry until the pipeline
// context ended; with this cap the stage gives up and returns the last error,
// leaving the notification unlogged so a later flush can try again.
const retryMaxElapsedTime = 5 * time.Minute

// RetryStage notifies via passed integration with exponential backoff until it
// succeeds, the retry budget is exhausted or the context is canceled.
// Concurrent executions for the same aggregation group and the same batch of
// alerts share a single delivery, so a receiver is never notified twice for
// one notification batch.
type RetryStage struct {
	integration Integration
	groupName   string
	metrics     *Metrics
	labelValues []string
	recorder    eventrecorder.Recorder
	inflight    *inflightRegistry
}

// NewRetryStage returns a new instance of a RetryStage. The inflight registry
// must be shared by the stages built for one pipeline so that concurrent
// deliveries of the same notification batch coalesce.
func NewRetryStage(i Integration, groupName string, metrics *Metrics, recorder eventrecorder.Recorder, inflight *inflightRegistry) *RetryStage {
	labelValues := []string{i.Name()}

	if metrics.ff.EnableReceiverNamesInMetrics() {
		labelValues = append(labelValues, i.receiverName)
	}

	return &RetryStage{
		integration: i,
		groupName:   groupName,
		metrics:     metrics,
		labelValues: labelValues,
		recorder:    recorder,
		inflight:    inflight,
	}
}

func (r RetryStage) Exec(ctx context.Context, l *slog.Logger, alerts ...*alert.Alert) (context.Context, []*alert.Alert, error) {
	// Every alert in the group was muted, so there is nothing to deliver. The
	// stages after this one still run, to record the state of the group.
	if len(alerts) == 0 {
		return ctx, alerts, nil
	}

	r.metrics.numNotifications.WithLabelValues(r.labelValues...).Inc()

	ctx, span := tracer.StartRetryStage(ctx, r.groupName, r.integration.name, r.labelValues, len(alerts))
	defer span.End()

	l = l.With("receiver", r.groupName, "integration", r.integration.String())
	gkey, hasGroupKey := GroupKey(ctx)
	if hasGroupKey {
		l = l.With("aggrGroup", gkey)
	}

	// Coalesce with a delivery of the same batch already in progress. Without a
	// group key (e.g. unit tests calling the stage directly) dedup is skipped.
	if hasGroupKey && r.inflight != nil {
		key := inflightKey(gkey, r.integration, alerts)
		leader, call, finish := r.inflight.joinOrStart(key)
		if !leader {
			err := call.wait(ctx)
			if err != nil {
				r.recordFailure(ctx, span, l, r.coalescedFailureReason(err), err)
			}
			return ctx, alerts, err
		}

		ctx, alerts, err := r.deliver(ctx, span, l, alerts...)
		finish(err)
		if err != nil {
			return ctx, nil, err
		}
		return ctx, alerts, nil
	}

	ctx, alerts, failureReason, err := r.exec(ctx, l, alerts...)
	if err != nil {
		r.recordFailure(ctx, span, l, failureReason, err)
	}
	return ctx, alerts, err
}

// deliver runs the retry loop and records the leader's outcome on its own span
// and failure metrics. Followers reuse the returned error without recording it
// a second time.
func (r RetryStage) deliver(ctx context.Context, span trace.Span, l *slog.Logger, alerts ...*alert.Alert) (context.Context, []*alert.Alert, error) {
	ctx, _, failureReason, err := r.exec(ctx, l, alerts...)
	if err != nil {
		r.recordFailure(ctx, span, l, failureReason, err)
	}
	return ctx, alerts, err
}

// coalescedFailureReason maps the error a follower observed while waiting on
// the leader to the reason recorded on the follower's own span and metrics.
func (r RetryStage) coalescedFailureReason(err error) Reason {
	if errors.Is(err, context.Canceled) {
		return ContextCanceledReason
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ContextDeadlineExceededReason
	}
	return DefaultReason
}

func (r RetryStage) recordFailure(ctx context.Context, span trace.Span, l *slog.Logger, failureReason Reason, err error) {
	r.integration.recordFailure(span, failureReason, err)
	r.metrics.numTotalFailedNotifications.WithLabelValues(append(r.labelValues, failureReason.String())...).Inc()
	if errors.Is(ctx.Err(), context.Canceled) {
		l.Debug("Notify failed", "reason", failureReason.String(), "err", err)
	} else {
		l.Error("Notify failed", "reason", failureReason.String(), "err", err)
	}
}

func (r RetryStage) exec(ctx context.Context, l *slog.Logger, alerts ...*alert.Alert) (context.Context, []*alert.Alert, Reason, error) {
	var sent alert.AlertSlice

	// If we shouldn't send notifications for resolved alerts, but there are only
	// resolved alerts, report them all as successfully notified (we still want the
	// notification log to log them for the next run of DedupStage).
	if !r.integration.SendResolved() {
		firing, ok := FiringAlerts(ctx)
		if !ok {
			return ctx, nil, DefaultReason, errors.New("firing alerts missing")
		}
		if len(firing) == 0 {
			return ctx, alerts, DefaultReason, nil
		}
		for _, a := range alerts {
			if a.Status() != model.AlertResolved {
				sent = append(sent, a)
			}
		}
	} else {
		sent = alerts
	}

	// Exponential backoff grows the delay between attempts up to MaxInterval.
	// The total retry time is bounded separately by the deadline below, since
	// ExponentialBackOff itself never stops retrying.
	b := backoff.NewExponentialBackOff()
	// Redundant for a freshly constructed ExponentialBackOff, but the BackOff
	// docs require Reset before first use.
	b.Reset()

	deadline := time.Now().Add(retryMaxElapsedTime)

	// Zero delay so the first attempt happens immediately.
	tick := time.NewTimer(0)
	defer tick.Stop()

	var (
		i          = 0
		iErr       error
		iReason    Reason
		retryAfter time.Duration
	)

	for {
		// Always check the context first: a canceled pipeline returns
		// immediately and never schedules another attempt.
		select {
		case <-ctx.Done():
			if iErr == nil {
				iErr = ctx.Err()
				if errors.Is(iErr, context.Canceled) {
					iReason = ContextCanceledReason
				} else if errors.Is(iErr, context.DeadlineExceeded) {
					iReason = ContextDeadlineExceededReason
				}
			}

			if iErr != nil {
				return ctx, nil, iReason, fmt.Errorf("%s/%s: notify retry canceled after %d attempts: %w", r.groupName, r.integration.String(), i, iErr)
			}
			return ctx, nil, DefaultReason, nil
		default:
		}

		// The retry budget is spent. Give up and return the last error instead
		// of scheduling another attempt; nothing is written to the
		// notification log, so a later flush retries the group.
		if i > 0 && !time.Now().Before(deadline) {
			if iErr == nil {
				iErr = errors.New("no error recorded")
			}
			return ctx, nil, RetryBudgetExhaustedReason, fmt.Errorf("%s/%s: notify retry budget exhausted after %d attempts over %s, last error: %w", r.groupName, r.integration.String(), i, retryMaxElapsedTime, iErr)
		}

		select {
		case <-tick.C:
			now := time.Now()
			verdict := r.integration.Notify(ctx, sent...)
			i++
			dur := time.Since(now)
			// Back off from the end of the attempt, not from its start, so a
			// slow failure doesn't eat into the wait.
			retryAfter = verdict.Delay()
			r.metrics.notificationLatencySeconds.WithLabelValues(r.labelValues...).Observe(dur.Seconds())
			r.metrics.numNotificationRequestsTotal.WithLabelValues(r.labelValues...).Inc()
			if err := verdict.Err(); err != nil {
				r.metrics.numNotificationRequestsFailedTotal.WithLabelValues(r.labelValues...).Inc()
				if !verdict.ShouldRetry() {
					return ctx, alerts, verdict.Reason(), fmt.Errorf("%s/%s: notify retry canceled due to unrecoverable error after %d attempts: %w", r.groupName, r.integration.String(), i, err)
				}
				if ctx.Err() == nil {
					if iErr == nil || err.Error() != iErr.Error() || verdict.Reason() != iReason {
						// Log the error if the context isn't done and the error isn't the same as before.
						l.Warn("Notify attempt failed, will retry later", "attempts", i, "reason", verdict.Reason().String(), "err", err)
					}
					// Save this error to be able to return the last seen error by an
					// integration upon context timeout.
					iErr, iReason = err, verdict.Reason()
				} else if iErr == nil && verdict.Reason() != DefaultReason {
					iErr, iReason = err, verdict.Reason()
				}
			} else {
				l := l.With(
					"attempts", i,
					"duration", dur,
					"numAlerts", len(sent),
				)
				if i <= 1 {
					l.Debug("Notify success", "alerts", sent)
				} else {
					l.Info("Notify success")
				}

				r.recorder.RecordEvent(ctx, func() eventrecorder.EventData {
					return newNotificationEvent(ctx, alerts, r.integration)
				})
				return ctx, alerts, DefaultReason, nil
			}
		case <-ctx.Done():
			// Loop back to the top so the context error is returned by a
			// single code path and no further attempt is scheduled.
			continue
		}

		delay := max(b.NextBackOff(), retryAfter)
		// Never wait past the retry budget: the next loop iteration detects
		// the exhausted budget and returns the last error. Cancellation stays
		// responsive regardless, since the timer is selected alongside
		// ctx.Done() above.
		if remaining := time.Until(deadline); delay > remaining {
			delay = remaining
		}
		tick.Reset(delay)
	}
}

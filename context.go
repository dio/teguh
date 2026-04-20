// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package teguh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrSuspended is returned by TaskContext methods (SleepFor, SleepUntil,
// AwaitEvent) when the task has been suspended. Handlers must propagate this
// error up to the Worker without further processing.
var ErrSuspended = errors.New("teguh: task suspended")

// ErrCancelled is returned by Heartbeat when the task has been cancelled by
// an external actor. Handlers must propagate this error up to the Worker.
var ErrCancelled = errors.New("teguh: task cancelled")

// TaskContext is passed to every task handler. It provides durable execution
// primitives: exactly-once steps, timer-based sleep, and event coordination.
//
// Checkpoint state is pre-loaded from the database when the run is claimed, so
// step cache hits require no extra round-trips.
type TaskContext struct {
	client      *Client
	queue       string
	run         Run
	checkpoints map[string]json.RawMessage // in-memory cache, keyed by step name
}

// RunID returns the current execution's run ID.
func (tc *TaskContext) RunID() string { return tc.run.RunID }

// TaskID returns the task ID.
func (tc *TaskContext) TaskID() string { return tc.run.TaskID }

// Attempt returns the current attempt number (1-based).
func (tc *TaskContext) Attempt() int { return tc.run.Attempt }

// WakeEvent returns the event name that caused this resumption, or nil.
func (tc *TaskContext) WakeEvent() *string { return tc.run.WakeEvent }

// EventPayload returns the payload of the wake event, or nil.
func (tc *TaskContext) EventPayload() json.RawMessage { return tc.run.EventPayload }

// Params returns the raw JSON params passed at spawn time.
func (tc *TaskContext) Params() json.RawMessage { return tc.run.Params }

// Step executes fn and durably persists the result under name. On retry the
// cached result is returned immediately without calling fn again (exactly-once
// per step name).
//
// T must be JSON-marshalable. Use the package-level generic Step[T] function
// for typed convenience:
//
//	result, err := teguh.Step(ctx, tc, "fetch-user", func(ctx context.Context) (*User, error) {
//	    return fetchUser(ctx, userID)
//	})
func Step[T any](ctx context.Context, tc *TaskContext, name string, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	// Cache hit: step was completed on a prior attempt.
	if raw, ok := tc.checkpoints[name]; ok {
		var result T
		if err := json.Unmarshal(raw, &result); err != nil {
			return zero, fmt.Errorf("teguh: unmarshal checkpoint %q: %w", name, err)
		}
		return result, nil
	}

	// Execute the step.
	result, err := fn(ctx)
	if err != nil {
		return zero, err
	}

	// Persist to database.
	raw, err := json.Marshal(result)
	if err != nil {
		return zero, fmt.Errorf("teguh: marshal checkpoint %q: %w", name, err)
	}
	if err := tc.client.SetCheckpoint(ctx, tc.queue, tc.run.TaskID, name, raw, tc.run.RunID, 0); err != nil {
		return zero, err
	}

	// Update in-memory cache.
	tc.checkpoints[name] = raw
	return result, nil
}

// Heartbeat extends the run's lease by the worker's claim timeout. Returns
// ErrCancelled if the task has been cancelled (workers should stop immediately).
func (tc *TaskContext) Heartbeat(ctx context.Context, extendBySecs int) error {
	if err := tc.client.ExtendClaim(ctx, tc.queue, tc.run.RunID, extendBySecs); err != nil {
		if isCancelledError(err) {
			return ErrCancelled
		}
		return err
	}
	return nil
}

// SleepFor suspends the task for duration d. The run will be re-queued after
// d has elapsed (by the ticker or claim_task's inline recovery sweep).
//
// Always returns ErrSuspended; the handler must return this error immediately.
func (tc *TaskContext) SleepFor(ctx context.Context, d time.Duration) error {
	return tc.SleepUntil(ctx, time.Now().Add(d))
}

// SleepUntil suspends the task until t. Always returns ErrSuspended.
func (tc *TaskContext) SleepUntil(ctx context.Context, t time.Time) error {
	if err := tc.client.ScheduleRun(ctx, tc.queue, tc.run.RunID, t); err != nil {
		return fmt.Errorf("teguh: sleep_until: %w", err)
	}
	return ErrSuspended
}

// AwaitEvent suspends the task waiting for eventName to be emitted. If the
// event was already emitted before this call, returns immediately with the
// payload (no suspension).
//
// stepName is used to persist the received payload as a checkpoint so that
// on replay the handler skips the wait and receives the cached payload.
//
// timeout optionally caps the wait; when it elapses the task resumes with a
// nil payload.
//
// Returns (payload, ErrSuspended) when suspended; (payload, nil) when the
// event was already available or on timeout resume.
func (tc *TaskContext) AwaitEvent(ctx context.Context, stepName, eventName string, timeout ...time.Duration) (json.RawMessage, error) {
	// Cache hit: step was completed on a prior attempt.
	// A cached value of "null" means the event timed out; return nil payload.
	if raw, ok := tc.checkpoints[stepName]; ok {
		if string(raw) == "null" {
			return nil, nil
		}
		return raw, nil
	}

	var timeoutSecs *int
	if len(timeout) > 0 && timeout[0] > 0 {
		s := int(timeout[0].Seconds())
		timeoutSecs = &s
	}

	shouldSuspend, payload, err := tc.client.AwaitEvent(
		ctx, tc.queue, tc.run.TaskID, tc.run.RunID, stepName, eventName, timeoutSecs,
	)
	if err != nil {
		return nil, fmt.Errorf("teguh: await_event %q: %w", eventName, err)
	}

	if shouldSuspend {
		return nil, ErrSuspended
	}

	// Normalize the SQL 'null'::jsonb timeout sentinel to Go nil so the return
	// value is consistent with the cache-hit fast path above, which also returns
	// nil for timeouts. Cache the sentinel so replay skips the DB call.
	if len(payload) == 0 || string(payload) == "null" {
		tc.checkpoints[stepName] = json.RawMessage("null")
		return nil, nil
	}
	tc.checkpoints[stepName] = payload
	return payload, nil
}

// EmitEvent emits eventName with payload. First-write-wins: later calls for
// the same event name in the same queue are ignored.
// Wakes any tasks suspended on this event and fires pg_notify.
func (tc *TaskContext) EmitEvent(ctx context.Context, eventName string, payload any) error {
	return tc.client.EmitEvent(ctx, tc.queue, eventName, payload)
}

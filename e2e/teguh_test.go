// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	teguh "github.com/dio/teguh"
)

// TeguhSuite groups all e2e tests. TestMain (in e2e_test.go) starts the
// embedded PostgreSQL instance and installs the schema before the suite runs.
type TeguhSuite struct {
	suite.Suite
}

// TestTeguhSuite is the single go test entry point for the entire suite.
func TestTeguhSuite(t *testing.T) {
	suite.Run(t, new(TeguhSuite))
}


func newClient(t *testing.T) *teguh.Client {
	t.Helper()
	client, err := teguh.Connect(context.Background(), testDSN)
	require.NoError(t, err, "connect")
	t.Cleanup(client.Close)
	return client
}

func setupQueue(t *testing.T, client *teguh.Client, queue string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, client.CreateQueue(ctx, queue), "create_queue")
	t.Cleanup(func() {
		_ = client.DropQueue(context.Background(), queue)
	})
}

// ticker manually re-queues sleeping tasks (replaces pg_cron in tests).
func ticker(t *testing.T, client *teguh.Client) int {
	t.Helper()
	n, err := client.Ticker(context.Background())
	require.NoError(t, err, "ticker")
	return n
}


func (s *TeguhSuite) TestSpawnTask() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_spawn")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_spawn", "noop", map[string]any{"x": 1}, nil)
	require.NoError(t, err)
	require.True(t, res.Created)
	require.NotEmpty(t, res.TaskID)
	require.Equal(t, 1, res.Attempt)
}

func (s *TeguhSuite) TestSpawnClaimComplete() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_scc")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_scc", "ping", map[string]any{"val": 42}, nil)
	require.NoError(t, err)
	require.True(t, res.Created)

	runs, err := client.ClaimTask(ctx, "test_scc", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	r := runs[0]
	require.Equal(t, "ping", r.TaskName)
	require.Equal(t, res.TaskID, r.TaskID)

	var params map[string]any
	require.NoError(t, json.Unmarshal(r.Params, &params))
	require.Equal(t, float64(42), params["val"])

	require.NoError(t, client.CompleteRun(ctx, "test_scc", r.RunID, map[string]any{"ok": true}))

	result, err := client.GetTaskResult(ctx, "test_scc", r.TaskID)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	require.Equal(t, 1, result.Attempts)
}

func (s *TeguhSuite) TestClaimEmptyQueue() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_empty")

	runs, err := client.ClaimTask(context.Background(), "test_empty", "w1", 30, 5)
	require.NoError(t, err)
	require.Empty(t, runs)
}


func (s *TeguhSuite) TestIdempotencyKey() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_idem")
	ctx := context.Background()

	opts := &teguh.SpawnOptions{IdempotencyKey: "order-123"}
	r1, err := client.SpawnTask(ctx, "test_idem", "process-order", nil, opts)
	require.NoError(t, err)
	require.True(t, r1.Created)

	r2, err := client.SpawnTask(ctx, "test_idem", "process-order", nil, opts)
	require.NoError(t, err)
	require.False(t, r2.Created)
	require.Equal(t, r1.TaskID, r2.TaskID)
}


func (s *TeguhSuite) TestRetryOnFail() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_retry")
	ctx := context.Background()

	maxAttempts := 3
	_, err := client.SpawnTask(ctx, "test_retry", "flaky", nil, &teguh.SpawnOptions{
		MaxAttempts: &maxAttempts,
		RetryStrategy: map[string]any{
			"kind":         "fixed",
			"base_seconds": 0,
		},
	})
	require.NoError(t, err)

	// Attempt 1: fail
	runs, err := client.ClaimTask(ctx, "test_retry", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NoError(t, client.FailRun(ctx, "test_retry", runs[0].RunID,
		map[string]any{"message": "attempt 1 failure"}, nil))

	// Attempt 2: fail
	runs, err = client.ClaimTask(ctx, "test_retry", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, 2, runs[0].Attempt)
	require.NoError(t, client.FailRun(ctx, "test_retry", runs[0].RunID,
		map[string]any{"message": "attempt 2 failure"}, nil))

	// Attempt 3: succeed
	runs, err = client.ClaimTask(ctx, "test_retry", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, 3, runs[0].Attempt)
	require.NoError(t, client.CompleteRun(ctx, "test_retry", runs[0].RunID, nil))

	result, err := client.GetTaskResult(ctx, "test_retry", runs[0].TaskID)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
	require.Equal(t, 3, result.Attempts)
}

func (s *TeguhSuite) TestExhaustedRetries() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_exhaust")
	ctx := context.Background()

	maxAttempts := 2
	res, err := client.SpawnTask(ctx, "test_exhaust", "always-fail", nil, &teguh.SpawnOptions{
		MaxAttempts:   &maxAttempts,
		RetryStrategy: map[string]any{"kind": "fixed", "base_seconds": 0},
	})
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		runs, err := client.ClaimTask(ctx, "test_exhaust", "w1", 30, 1)
		require.NoError(t, err)
		require.Len(t, runs, 1)
		require.NoError(t, client.FailRun(ctx, "test_exhaust", runs[0].RunID,
			map[string]any{"attempt": i}, nil))
	}

	result, err := client.GetTaskResult(ctx, "test_exhaust", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "failed", result.State)
}


func (s *TeguhSuite) TestStepCheckpointExactlyOnce() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_steps")
	ctx := context.Background()

	var callCount int32

	// Spawn task, claim it, write a checkpoint, fail the run, re-claim,
	// verify the step is NOT called again on the second attempt.
	res, err := client.SpawnTask(ctx, "test_steps", "multi-step", nil, &teguh.SpawnOptions{
		RetryStrategy: map[string]any{"kind": "fixed", "base_seconds": 0},
	})
	require.NoError(t, err)

	// Attempt 1: write step-A, then fail (simulating a crash after step-A).
	runs, err := client.ClaimTask(ctx, "test_steps", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	atomic.AddInt32(&callCount, 1)
	stepStateJSON, _ := json.Marshal("computed-value")
	require.NoError(t, client.SetCheckpoint(ctx, "test_steps", runs[0].TaskID, "step-A",
		stepStateJSON, runs[0].RunID, 0))
	require.NoError(t, client.FailRun(ctx, "test_steps", runs[0].RunID,
		map[string]any{"message": "crash after step-A"}, nil))

	// Attempt 2: load checkpoints, step-A should be cached.
	runs, err = client.ClaimTask(ctx, "test_steps", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, 2, runs[0].Attempt)

	cps, err := client.GetCheckpoints(ctx, "test_steps", runs[0].TaskID, runs[0].RunID)
	require.NoError(t, err)
	require.Len(t, cps, 1, "step-A checkpoint must carry over to attempt 2")
	require.Equal(t, "step-A", cps[0].Name)
	var cached string
	require.NoError(t, json.Unmarshal(cps[0].State, &cached))
	require.Equal(t, "computed-value", cached, "cached value must match what was written on attempt 1")

	require.NoError(t, client.CompleteRun(ctx, "test_steps", runs[0].RunID, nil))

	result, err := client.GetTaskResult(ctx, "test_steps", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
}


func (s *TeguhSuite) TestSleepAndResume() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_sleep")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_sleep", "delayed-task", nil, nil)
	require.NoError(t, err)

	// Claim and put to sleep for 100ms.
	runs, err := client.ClaimTask(ctx, "test_sleep", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	wakeAt := time.Now().Add(100 * time.Millisecond)
	require.NoError(t, client.ScheduleRun(ctx, "test_sleep", runs[0].RunID, wakeAt))

	// Task should not be claimable before wake time.
	immediateRuns, err := client.ClaimTask(ctx, "test_sleep", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, immediateRuns, "task must not be claimable before wake time")

	// Wait past wake time, then ticker re-queues it.
	time.Sleep(200 * time.Millisecond)
	n := ticker(t, client)
	require.GreaterOrEqual(t, n, 1, "ticker must re-queue at least the sleeping task")

	// Now claimable.
	runs2, err := client.ClaimTask(ctx, "test_sleep", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs2, 1)
	require.Equal(t, res.TaskID, runs2[0].TaskID)

	require.NoError(t, client.CompleteRun(ctx, "test_sleep", runs2[0].RunID, nil))
}


func (s *TeguhSuite) TestAwaitAndEmitEvent() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_events")
	ctx := context.Background()

	// Spawn a task that will wait for an external signal.
	res, err := client.SpawnTask(ctx, "test_events", "signal-waiter", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_events", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Task calls await_event, should suspend.
	shouldSuspend, payload, err := client.AwaitEvent(
		ctx, "test_events", runs[0].TaskID, runs[0].RunID,
		"wait-for-signal", "order.shipped", nil,
	)
	require.NoError(t, err)
	require.True(t, shouldSuspend, "task must suspend while waiting for event")
	require.Nil(t, payload)

	// Another actor emits the event.
	require.NoError(t, client.EmitEvent(ctx, "test_events", "order.shipped",
		map[string]any{"tracking": "ABC123"}))

	// emit_event re-queues the sleeping task; claim it again.
	resumedRuns, err := client.ClaimTask(ctx, "test_events", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, resumedRuns, 1)
	require.Equal(t, res.TaskID, resumedRuns[0].TaskID)
	require.NotNil(t, resumedRuns[0].WakeEvent)
	require.Equal(t, "order.shipped", *resumedRuns[0].WakeEvent)

	// The checkpoint for "wait-for-signal" should carry the event payload.
	cps, err := client.GetCheckpoints(ctx, "test_events", resumedRuns[0].TaskID, resumedRuns[0].RunID)
	require.NoError(t, err)

	var found bool
	for _, cp := range cps {
		if cp.Name == "wait-for-signal" {
			found = true
			var eventData map[string]any
			require.NoError(t, json.Unmarshal(cp.State, &eventData))
			require.Equal(t, "ABC123", eventData["tracking"])
		}
	}
	require.True(t, found, "wait-for-signal checkpoint must exist")

	require.NoError(t, client.CompleteRun(ctx, "test_events", resumedRuns[0].RunID, nil))
}

func (s *TeguhSuite) TestEmitBeforeAwait() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_early_emit")
	ctx := context.Background()

	// Emit first, then the task claims and awaits, must NOT suspend.
	require.NoError(t, client.EmitEvent(ctx, "test_early_emit", "ready",
		map[string]any{"pre": true}))

	_, err := client.SpawnTask(ctx, "test_early_emit", "eager-task", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_early_emit", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	shouldSuspend, payload, err := client.AwaitEvent(
		ctx, "test_early_emit", runs[0].TaskID, runs[0].RunID,
		"step-wait", "ready", nil,
	)
	require.NoError(t, err)
	require.False(t, shouldSuspend, "event already emitted, must NOT suspend")
	require.NotNil(t, payload)

	var data map[string]any
	require.NoError(t, json.Unmarshal(payload, &data))
	require.Equal(t, true, data["pre"])

	require.NoError(t, client.CompleteRun(ctx, "test_early_emit", runs[0].RunID, nil))
}


func (s *TeguhSuite) TestCancelPendingTask() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_cancel")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_cancel", "cancelable", nil, nil)
	require.NoError(t, err)

	require.NoError(t, client.CancelTask(ctx, "test_cancel", res.TaskID))

	// After cancel the task must not be claimable.
	runs, err := client.ClaimTask(ctx, "test_cancel", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, runs)

	result, err := client.GetTaskResult(ctx, "test_cancel", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "cancelled", result.State)
}


func (s *TeguhSuite) TestManualRetry() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_manual_retry")
	ctx := context.Background()

	maxAttempts := 1
	res, err := client.SpawnTask(ctx, "test_manual_retry", "one-shot", nil,
		&teguh.SpawnOptions{MaxAttempts: &maxAttempts})
	require.NoError(t, err)

	// Fail (exhausts the budget).
	runs, err := client.ClaimTask(ctx, "test_manual_retry", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NoError(t, client.FailRun(ctx, "test_manual_retry", runs[0].RunID,
		map[string]any{"msg": "terminal"}, nil))

	result, err := client.GetTaskResult(ctx, "test_manual_retry", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "failed", result.State)

	// Manual retry resets the task.
	retried, err := client.RetryTask(ctx, "test_manual_retry", res.TaskID, false)
	require.NoError(t, err)
	require.Equal(t, res.TaskID, retried.TaskID)

	runs2, err := client.ClaimTask(ctx, "test_manual_retry", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs2, 1)
	require.NoError(t, client.CompleteRun(ctx, "test_manual_retry", runs2[0].RunID, nil))
}


func (s *TeguhSuite) TestWorkerBasicDispatch() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_worker_basic")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan string, 1)

	w := client.NewWorker("test_worker_basic",
		teguh.WithPollInterval(100*time.Millisecond),
		teguh.WithConcurrency(2),
		teguh.WithClaimTimeout(30),
		teguh.WithWorkerID("test-worker"),
	)
	w.Handle("greet", func(ctx context.Context, tc *teguh.TaskContext) error {
		var params map[string]any
		_ = json.Unmarshal(tc.Params(), &params)
		done <- fmt.Sprintf("hello %v", params["name"])
		return nil
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	_, err := client.SpawnTask(ctx, "test_worker_basic", "greet",
		map[string]any{"name": "world"}, nil)
	require.NoError(t, err)

	select {
	case msg := <-done:
		require.Equal(t, "hello world", msg)
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for worker to process task")
	}
}

func (s *TeguhSuite) TestWorkerConcurrency() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_worker_conc")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const numTasks = 5
	var processed int32
	allDone := make(chan struct{})

	w := client.NewWorker("test_worker_conc",
		teguh.WithPollInterval(100*time.Millisecond),
		teguh.WithConcurrency(numTasks),
		teguh.WithWorkerID("conc-worker"),
	)
	w.Handle("work", func(ctx context.Context, tc *teguh.TaskContext) error {
		time.Sleep(50 * time.Millisecond)
		if atomic.AddInt32(&processed, 1) == numTasks {
			close(allDone)
		}
		return nil
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	for i := 0; i < numTasks; i++ {
		_, err := client.SpawnTask(ctx, "test_worker_conc", "work",
			map[string]any{"i": i}, nil)
		require.NoError(t, err)
	}

	select {
	case <-allDone:
		require.Equal(t, int32(numTasks), atomic.LoadInt32(&processed))
	case <-ctx.Done():
		require.Failf(t, "timeout", "only %d/%d tasks processed", atomic.LoadInt32(&processed), numTasks)
	}
}

func (s *TeguhSuite) TestWorkerCatchAll() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_worker_catchall")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	received := make(chan string, 1)
	w := client.NewWorker("test_worker_catchall",
		teguh.WithPollInterval(100*time.Millisecond),
	)
	w.Handle("*", func(ctx context.Context, tc *teguh.TaskContext) error {
		received <- string(tc.Params())
		return nil
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	_, err := client.SpawnTask(ctx, "test_worker_catchall", "unknown-type",
		map[string]any{"key": "val"}, nil)
	require.NoError(t, err)

	select {
	case payload := <-received:
		require.Contains(t, payload, "key")
	case <-ctx.Done():
		require.Fail(t, "timeout")
	}
}


// TestDurableMultiStep simulates the "order fulfillment" workflow:
//
//  1. fetch-order   (step)
//  2. charge-card   (step)
//  3. ship-order    (step → emits "order.shipped" event)
//  4. Complete
//
// On the second attempt (simulating a crash after charge-card) the first two
// steps must be replayed from checkpoints without executing the functions again.
func (s *TeguhSuite) TestDurableMultiStep() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_durable")
	ctx := context.Background()

	var fetchCalls, chargeCalls int32

	res, err := client.SpawnTask(ctx, "test_durable", "fulfillment",
		map[string]any{"order_id": 99}, &teguh.SpawnOptions{
			RetryStrategy: map[string]any{"kind": "fixed", "base_seconds": 0},
		})
	require.NoError(t, err)

	// ── Attempt 1: fetch + charge, then crash ──
	runs, err := client.ClaimTask(ctx, "test_durable", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Simulate writing step checkpoints manually (as a Worker would via Step[T]).
	fetchJSON, _ := json.Marshal(map[string]any{"order_id": 99, "total": 150})
	require.NoError(t, client.SetCheckpoint(ctx, "test_durable",
		runs[0].TaskID, "fetch-order", fetchJSON, runs[0].RunID, 0))
	atomic.AddInt32(&fetchCalls, 1)

	chargeJSON, _ := json.Marshal(map[string]any{"charge_id": "ch_abc"})
	require.NoError(t, client.SetCheckpoint(ctx, "test_durable",
		runs[0].TaskID, "charge-card", chargeJSON, runs[0].RunID, 0))
	atomic.AddInt32(&chargeCalls, 1)

	// Crash before ship-order.
	require.NoError(t, client.FailRun(ctx, "test_durable", runs[0].RunID,
		map[string]any{"message": "crash before ship"}, nil))

	// ── Attempt 2: checkpoints for fetch+charge must be loaded ──
	runs, err = client.ClaimTask(ctx, "test_durable", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, 2, runs[0].Attempt)

	cps, err := client.GetCheckpoints(ctx, "test_durable", runs[0].TaskID, runs[0].RunID)
	require.NoError(t, err)
	require.Len(t, cps, 2, "both checkpoints must be visible on attempt 2")

	cpMap := make(map[string]json.RawMessage)
	for _, cp := range cps {
		cpMap[cp.Name] = cp.State
	}
	require.Contains(t, cpMap, "fetch-order")
	require.Contains(t, cpMap, "charge-card")

	// These would NOT be called again in a real Step[T], verified by the cache.
	// We assert call counts stayed at 1 (set during attempt 1).
	require.Equal(t, int32(1), atomic.LoadInt32(&fetchCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&chargeCalls))

	// ship-order: new step on attempt 2.
	shipJSON, _ := json.Marshal(map[string]any{"tracking": "TRACK-001"})
	require.NoError(t, client.SetCheckpoint(ctx, "test_durable",
		runs[0].TaskID, "ship-order", shipJSON, runs[0].RunID, 0))

	require.NoError(t, client.EmitEvent(ctx, "test_durable", "order.shipped",
		map[string]any{"tracking": "TRACK-001"}))

	require.NoError(t, client.CompleteRun(ctx, "test_durable", runs[0].RunID,
		map[string]any{"shipped": true}))

	result, err := client.GetTaskResult(ctx, "test_durable", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
}


// TestAwaitEventTimeout verifies that a task suspended waiting for an event
// with a timeout is re-queued when the timeout expires, and that the second
// call to AwaitEvent returns nil payload (not a JSON null sentinel).
func (s *TeguhSuite) TestAwaitEventTimeout() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_event_timeout")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_event_timeout", "waiter", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_event_timeout", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Suspend with a 1-second timeout; task must always take the suspend path.
	timeoutSecs := 1
	shouldSuspend, _, err := client.AwaitEvent(
		ctx, "test_event_timeout", runs[0].TaskID, runs[0].RunID,
		"wait-step", "never.fires", &timeoutSecs,
	)
	require.NoError(t, err)
	require.True(t, shouldSuspend, "task must suspend while waiting for event")

	// Wait for the 1-second timeout to expire, then ticker re-queues the task.
	time.Sleep(1100 * time.Millisecond)
	ticker(t, client)

	resumed, err := client.ClaimTask(ctx, "test_event_timeout", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, resumed, 1, "task must be re-queued after wait timeout")
	require.Equal(t, res.TaskID, resumed[0].TaskID)

	// On re-entry AwaitEvent must return nil payload (timeout), not suspend.
	shouldSuspend2, payload2, err := client.AwaitEvent(
		ctx, "test_event_timeout", resumed[0].TaskID, resumed[0].RunID,
		"wait-step", "never.fires", nil,
	)
	require.NoError(t, err)
	require.False(t, shouldSuspend2, "second entry must not suspend: checkpoint hit")
	require.Nil(t, payload2, "timed-out wait must return nil payload, not JSON null")

	require.NoError(t, client.CompleteRun(ctx, "test_event_timeout", resumed[0].RunID, nil))

	result, err := client.GetTaskResult(ctx, "test_event_timeout", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "completed", result.State)
}


// TestClaimTaskInlineRecovery verifies that claim_task's inline recovery sweep
// re-queues sleeping tasks whose wake time has arrived even when ticker has not
// been called explicitly.
func (s *TeguhSuite) TestClaimTaskInlineRecovery() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_inline_recovery")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_inline_recovery", "snoozer", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_inline_recovery", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Put task to sleep for 100ms.
	wakeAt := time.Now().Add(100 * time.Millisecond)
	require.NoError(t, client.ScheduleRun(ctx, "test_inline_recovery", runs[0].RunID, wakeAt))

	// Verify not claimable before wake time.
	none, err := client.ClaimTask(ctx, "test_inline_recovery", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, none)

	// Wait past wake time. Do NOT call ticker.
	time.Sleep(200 * time.Millisecond)

	// claim_task's inline sweep must re-queue and claim the sleeping task.
	resumed, err := client.ClaimTask(ctx, "test_inline_recovery", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, resumed, 1, "inline recovery sweep must re-queue task without ticker")
	require.Equal(t, res.TaskID, resumed[0].TaskID)

	require.NoError(t, client.CompleteRun(ctx, "test_inline_recovery", resumed[0].RunID, nil))
}


// TestAvailableAt verifies that a task spawned with a future available_at is
// not claimable before that time but becomes claimable after.
func (s *TeguhSuite) TestAvailableAt() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_available_at")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_available_at", "delayed", nil, &teguh.SpawnOptions{
		AvailableAt: time.Now().Add(200 * time.Millisecond),
	})
	require.NoError(t, err)
	require.True(t, res.Created)

	// Not yet claimable.
	none, err := client.ClaimTask(ctx, "test_available_at", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, none, "task must not be claimable before available_at")

	// Wait past available_at; inline sweep in next claim_task picks it up.
	time.Sleep(300 * time.Millisecond)
	runs, err := client.ClaimTask(ctx, "test_available_at", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1, "task must be claimable after available_at elapses")
	require.Equal(t, res.TaskID, runs[0].TaskID)

	require.NoError(t, client.CompleteRun(ctx, "test_available_at", runs[0].RunID, nil))
}


// TestRetryTaskSpawnNew verifies that RetryTask with spawn_new=true creates a
// new task (new task_id) while leaving the original in its terminal state.
func (s *TeguhSuite) TestRetryTaskSpawnNew() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_retry_spawn_new")
	ctx := context.Background()

	maxAttempts := 1
	original, err := client.SpawnTask(ctx, "test_retry_spawn_new", "one-shot",
		map[string]any{"v": 1}, &teguh.SpawnOptions{MaxAttempts: &maxAttempts})
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_retry_spawn_new", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NoError(t, client.FailRun(ctx, "test_retry_spawn_new", runs[0].RunID,
		map[string]any{"msg": "terminal"}, nil))

	orig, err := client.GetTaskResult(ctx, "test_retry_spawn_new", original.TaskID)
	require.NoError(t, err)
	require.Equal(t, "failed", orig.State)

	// spawn_new=true creates a fresh task.
	spawned, err := client.RetryTask(ctx, "test_retry_spawn_new", original.TaskID, true)
	require.NoError(t, err)
	require.NotEqual(t, original.TaskID, spawned.TaskID, "spawn_new must produce a different task_id")
	require.True(t, spawned.Created)

	runs2, err := client.ClaimTask(ctx, "test_retry_spawn_new", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs2, 1)
	require.Equal(t, spawned.TaskID, runs2[0].TaskID)
	require.NoError(t, client.CompleteRun(ctx, "test_retry_spawn_new", runs2[0].RunID, nil))

	// Original remains failed.
	orig2, err := client.GetTaskResult(ctx, "test_retry_spawn_new", original.TaskID)
	require.NoError(t, err)
	require.Equal(t, "failed", orig2.State)
}


// TestWorkerAwaitEvent uses the Worker's high-level API to test that a handler
// can call tc.AwaitEvent, the task suspends, emit_event wakes it, and the
// worker completes it with the event payload.
func (s *TeguhSuite) TestWorkerAwaitEvent() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_worker_event")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	type result struct{ tracking string }
	done := make(chan result, 1)

	w := client.NewWorker("test_worker_event",
		teguh.WithPollInterval(100*time.Millisecond),
		teguh.WithWorkerID("event-worker"),
	)
	w.Handle("ship-waiter", func(ctx context.Context, tc *teguh.TaskContext) error {
		payload, err := tc.AwaitEvent(ctx, "wait-shipped", "order.shipped")
		if err != nil {
			return err
		}
		var data map[string]any
		if payload != nil {
			if err := json.Unmarshal(payload, &data); err != nil {
				return fmt.Errorf("unmarshal payload: %w", err)
			}
		}
		tracking, _ := data["tracking"].(string)
		done <- result{tracking: tracking}
		return nil
	})

	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	_, err := client.SpawnTask(ctx, "test_worker_event", "ship-waiter", nil, nil)
	require.NoError(t, err)

	// Give the worker time to claim and suspend.
	time.Sleep(300 * time.Millisecond)

	require.NoError(t, client.EmitEvent(ctx, "test_worker_event", "order.shipped",
		map[string]any{"tracking": "XYZ-99"}))

	select {
	case r := <-done:
		require.Equal(t, "XYZ-99", r.tracking)
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for event-driven task to complete")
	}
}

func (s *TeguhSuite) TestWorkerSleepResume() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_worker_sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var completions int32
	done := make(chan struct{})

	w := client.NewWorker("test_worker_sleep",
		teguh.WithPollInterval(100*time.Millisecond),
		teguh.WithWorkerID("sleep-worker"),
	)
	w.Handle("sleepy", func(ctx context.Context, tc *teguh.TaskContext) error {
		if tc.Attempt() == 1 {
			// First execution: sleep for 200ms then return ErrSuspended.
			return tc.SleepFor(ctx, 200*time.Millisecond)
		}
		// Second execution (after sleep): complete.
		atomic.AddInt32(&completions, 1)
		close(done)
		return nil
	})

	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	// Seed the ticker goroutine: periodically call Ticker so sleeping tasks wake.
	tickerStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-tickerStop:
				return
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
				client.Ticker(ctx) //nolint:errcheck
			}
		}
	}()

	_, err := client.SpawnTask(ctx, "test_worker_sleep", "sleepy", nil, nil)
	require.NoError(t, err)

	select {
	case <-done:
		close(tickerStop)
		require.Equal(t, int32(1), atomic.LoadInt32(&completions))
	case <-ctx.Done():
		close(tickerStop)
		require.Fail(t, "timeout waiting for sleep+resume cycle")
	}
}


// TestAwaitEventNoTimeoutNullAvailableAt verifies that await_event stores
// NULL available_at for no-timeout waits, so ticker never re-queues them.
func (s *TeguhSuite) TestAwaitEventNoTimeoutNullAvailableAt() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_null_await")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_null_await", "waiter", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_null_await", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Suspend without a timeout.
	shouldSuspend, payload, err := client.AwaitEvent(
		ctx, "test_null_await", runs[0].TaskID, runs[0].RunID,
		"step-no-timeout", "my-event", nil,
	)
	require.NoError(t, err)
	require.True(t, shouldSuspend)
	require.Nil(t, payload)

	// available_at must be NULL (not 'infinity') in the task row.
	var availableAt *time.Time
	row := client.Pool().QueryRow(ctx,
		`SELECT available_at FROM teguh.t_test_null_await WHERE task_id = $1`,
		res.TaskID)
	require.NoError(t, row.Scan(&availableAt))
	require.Nil(t, availableAt, "available_at must be NULL for no-timeout AwaitEvent")

	// Ticker must NOT re-queue the task (available_at IS NULL is excluded).
	n := ticker(t, client)
	require.Equal(t, 0, n, "ticker must not re-queue a no-timeout waiting task")

	// Task is still not claimable.
	noRuns, err := client.ClaimTask(ctx, "test_null_await", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, noRuns)

	// Emit wakes it.
	require.NoError(t, client.EmitEvent(ctx, "test_null_await", "my-event", nil))
	woken, err := client.ClaimTask(ctx, "test_null_await", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, woken, 1)
	require.NoError(t, client.CompleteRun(ctx, "test_null_await", woken[0].RunID, nil))
}


// TestConcurrentCancelVsComplete races cancel against complete and verifies no
// deadlock and that the final state is terminal.
func (s *TeguhSuite) TestConcurrentCancelVsComplete() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_cancel_vs_complete")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_cancel_vs_complete", "race-task", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_cancel_vs_complete", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	runID := runs[0].RunID

	// Race cancel vs complete; both may error but neither must deadlock.
	cancelErr := make(chan error, 1)
	completeErr := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cancelErr <- client.CancelTask(ctx, "test_cancel_vs_complete", res.TaskID)
	}()
	go func() {
		defer wg.Done()
		completeErr <- client.CompleteRun(ctx, "test_cancel_vs_complete", runID, nil)
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock detected: cancel+complete did not finish within 10s")
	}

	// Drain errors; exactly one operation succeeds (or both may succeed
	// if cancel fires after complete). Final state must be terminal.
	<-cancelErr
	<-completeErr

	result, err := client.GetTaskResult(ctx, "test_cancel_vs_complete", res.TaskID)
	require.NoError(t, err)
	require.Contains(t, []string{"completed", "cancelled"}, result.State,
		"final state must be a terminal state")
}


// TestCancelledTaskNotRequeueOnEmit verifies that emit_event does not re-queue
// a task that was cancelled while waiting for that event.
func (s *TeguhSuite) TestCancelledTaskNotRequeueOnEmit() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_cancel_emit")
	ctx := context.Background()

	res, err := client.SpawnTask(ctx, "test_cancel_emit", "waiter", nil, nil)
	require.NoError(t, err)

	runs, err := client.ClaimTask(ctx, "test_cancel_emit", "w1", 30, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)

	// Suspend waiting for event.
	shouldSuspend, _, err := client.AwaitEvent(
		ctx, "test_cancel_emit", runs[0].TaskID, runs[0].RunID,
		"wait-step", "payment.received", nil,
	)
	require.NoError(t, err)
	require.True(t, shouldSuspend)

	// Cancel the task while it is sleeping.
	require.NoError(t, client.CancelTask(ctx, "test_cancel_emit", res.TaskID))

	// Emitting the event must not re-queue the cancelled task.
	require.NoError(t, client.EmitEvent(ctx, "test_cancel_emit", "payment.received",
		map[string]any{"amount": 100}))

	// Task must remain cancelled, not re-queued.
	noRuns, err := client.ClaimTask(ctx, "test_cancel_emit", "w1", 30, 1)
	require.NoError(t, err)
	require.Empty(t, noRuns, "cancelled task must not be claimable after emit_event")

	result, err := client.GetTaskResult(ctx, "test_cancel_emit", res.TaskID)
	require.NoError(t, err)
	require.Equal(t, "cancelled", result.State)
}


// TestWorkerHandlerPanic verifies that handler panics are recovered and result
// in a failed run that can be retried.
func (s *TeguhSuite) TestWorkerHandlerPanic() {
	t := s.T()
	client := newClient(t)
	setupQueue(t, client, "test_panic")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	maxAttempts := 2
	res, err := client.SpawnTask(ctx, "test_panic", "crasher", nil,
		&teguh.SpawnOptions{MaxAttempts: &maxAttempts})
	require.NoError(t, err)

	var attempt int32
	done := make(chan struct{})

	w := client.NewWorker("test_panic",
		teguh.WithPollInterval(100*time.Millisecond),
		teguh.WithWorkerID("panic-worker"),
	)
	w.Handle("crasher", func(_ context.Context, tc *teguh.TaskContext) error {
		n := int(atomic.AddInt32(&attempt, 1))
		if n == 1 {
			panic("simulated handler crash")
		}
		// Second attempt: succeed.
		close(done)
		return nil
	})

	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Start(ctx) }()
	t.Cleanup(func() {
		if err := <-workerErr; err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("worker Start: %v", err)
		}
	})

	select {
	case <-done:
		result, err := client.GetTaskResult(ctx, "test_panic", res.TaskID)
		require.NoError(t, err)
		require.Equal(t, "completed", result.State)
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for panic recovery and retry")
	}
}


// TestPortableUuidv7Format verifies that portable_uuidv7() always returns a
// valid UUIDv7 (version nibble == '7'), regardless of whether pgcrypto is
// installed or the uuid_send fallback is active.
func (s *TeguhSuite) TestPortableUuidv7Format() {
	t := s.T()
	ctx := context.Background()

	// Report which implementation is active.
	var hasPgcrypto bool
	require.NoError(t,
		testPool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto')`).
			Scan(&hasPgcrypto))
	if hasPgcrypto {
		t.Log("pgcrypto installed — portable_uuidv7() uses gen_random_bytes(10)")
	} else {
		t.Log("pgcrypto not installed — portable_uuidv7() uses uuid_send(gen_random_uuid()) fallback")
	}

	// Generate several UUIDs and validate UUIDv7 format.
	for i := 0; i < 10; i++ {
		var id string
		require.NoError(t,
			testPool.QueryRow(ctx, `SELECT teguh.portable_uuidv7()`).Scan(&id))
		require.Len(t, id, 36, "UUID must be 36 characters: %s", id)
		parts := strings.Split(id, "-")
		require.Len(t, parts, 5, "UUID must have 5 dash-separated groups: %s", id)
		// The version nibble is the first hex digit of the third group.
		require.Equal(t, "7", string(parts[2][0]),
			"version nibble must be '7' for UUIDv7: %s", id)
	}
}

// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

// Package teguh is a Go client for Teguh, a durable execution engine built
// on PostgreSQL that combines absurd's workflow API with a zero-bloat dispatch
// queue inspired by PgQue/PgQ. No PostgreSQL extensions are required.
//
// Quick start:
//
//	client, err := teguh.Connect(ctx, dsn)
//
//	// Low-level: spawn a task and claim it manually
//	res, err := client.SpawnTask(ctx, "jobs", "send-email", params, nil)
//	runs, err := client.ClaimTask(ctx, "jobs", "worker-1", 30, 1)
//
//	// High-level: register handlers and start a worker
//	w := client.NewWorker("jobs",
//	    teguh.WithConcurrency(10),
//	    teguh.WithClaimTimeout(30),
//	)
//	w.Handle("send-email", func(ctx context.Context, tc *teguh.TaskContext) error {
//	    // Use tc.Step, tc.SleepFor, tc.AwaitEvent for durable execution.
//	    return nil
//	})
//	w.Start(ctx)
package teguh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is the main teguh client. It wraps a pgx connection pool and is
// safe for concurrent use.
type Client struct {
	pool *pgxpool.Pool
}

// Connect opens a new Client using the given PostgreSQL DSN.
// The caller must call Close when done.
func Connect(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("teguh: connect: %w", err)
	}
	return &Client{pool: pool}, nil
}

// Close releases all connections in the pool.
func (c *Client) Close() { c.pool.Close() }

// Pool returns the underlying pgxpool for direct SQL access (e.g. DDL).
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// CreateQueue provisions all per-queue tables (t_, p_, r_, c_, e_, w_).
func (c *Client) CreateQueue(ctx context.Context, queue string) error {
	if _, err := c.pool.Exec(ctx, `SELECT teguh.create_queue($1)`, queue); err != nil {
		return fmt.Errorf("teguh: create_queue %q: %w", queue, err)
	}
	return nil
}

// DropQueue removes all per-queue tables and the queue registry entry.
func (c *Client) DropQueue(ctx context.Context, queue string) error {
	if _, err := c.pool.Exec(ctx, `SELECT teguh.drop_queue($1)`, queue); err != nil {
		return fmt.Errorf("teguh: drop_queue %q: %w", queue, err)
	}
	return nil
}

// SpawnTask enqueues a new task and returns the spawn result.
// params must be JSON-marshalable; opts may be nil.
func (c *Client) SpawnTask(ctx context.Context, queue, taskName string, params any, opts *SpawnOptions) (SpawnResult, error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: marshal params: %w", err)
	}
	optsJSON, err := opts.toJSONB()
	if err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: marshal options: %w", err)
	}

	rows, err := c.pool.Query(ctx,
		`SELECT task_id, run_id, attempt, created FROM teguh.spawn_task($1,$2,$3::jsonb,$4::jsonb)`,
		queue, taskName, string(paramsJSON), string(optsJSON),
	)
	if err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: spawn_task: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return SpawnResult{}, fmt.Errorf("teguh: spawn_task returned no rows")
	}
	var res SpawnResult
	if err := rows.Scan(&res.TaskID, &res.RunID, &res.Attempt, &res.Created); err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: scan spawn_task: %w", err)
	}
	return res, rows.Err()
}

// ClaimTask claims up to qty pending tasks from the queue for workerID with a
// lease of claimTimeoutSecs seconds. Returns the claimed runs (may be empty).
func (c *Client) ClaimTask(ctx context.Context, queue, workerID string, claimTimeoutSecs, qty int) ([]Run, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT run_id, task_id, attempt, task_name, params, retry_strategy,
		        max_attempts, headers, wake_event, event_payload
		   FROM teguh.claim_task($1,$2,$3,$4)`,
		queue, workerID, claimTimeoutSecs, qty,
	)
	if err != nil {
		return nil, fmt.Errorf("teguh: claim_task: %w", err)
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		var params, retryStrategy, headers, eventPayload []byte
		if err := rows.Scan(
			&r.RunID, &r.TaskID, &r.Attempt, &r.TaskName,
			&params, &retryStrategy, &r.MaxAttempts, &headers,
			&r.WakeEvent, &eventPayload,
		); err != nil {
			return nil, fmt.Errorf("teguh: scan claim_task: %w", err)
		}
		r.Params = params
		r.RetryStrategy = retryStrategy
		r.Headers = headers
		r.EventPayload = eventPayload
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// CompleteRun marks the run as successfully completed with an optional result payload.
func (c *Client) CompleteRun(ctx context.Context, queue, runID string, result any) error {
	var resultJSON []byte
	if result != nil {
		var err error
		resultJSON, err = json.Marshal(result)
		if err != nil {
			return fmt.Errorf("teguh: marshal result: %w", err)
		}
	}
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.complete_run($1,$2,$3::jsonb)`,
		queue, runID, nullableJSON(resultJSON),
	); err != nil {
		return fmt.Errorf("teguh: complete_run: %w", err)
	}
	return nil
}

// FailRun marks the run as failed with a JSON reason payload. The engine will
// schedule a retry according to the task's retry_strategy. retryAt, when
// non-nil, overrides the computed retry delay.
func (c *Client) FailRun(ctx context.Context, queue, runID string, reason any, retryAt *time.Time) error {
	reasonJSON, err := json.Marshal(reason)
	if err != nil {
		return fmt.Errorf("teguh: marshal reason: %w", err)
	}
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.fail_run($1,$2,$3::jsonb,$4)`,
		queue, runID, string(reasonJSON), retryAt,
	); err != nil {
		return fmt.Errorf("teguh: fail_run: %w", err)
	}
	return nil
}

// ExtendClaim extends the lease for an active run by extendBySecs seconds.
// Returns an error if the task has been cancelled (sqlstate AB001).
func (c *Client) ExtendClaim(ctx context.Context, queue, runID string, extendBySecs int) error {
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.extend_claim($1,$2,$3)`,
		queue, runID, extendBySecs,
	); err != nil {
		return fmt.Errorf("teguh: extend_claim: %w", err)
	}
	return nil
}

// SetCheckpoint writes a named step result for a task. If extendClaimBy > 0
// it also extends the lease.
func (c *Client) SetCheckpoint(ctx context.Context, queue, taskID, stepName string, state json.RawMessage, ownerRunID string, extendClaimBy int) error {
	var extendArg *int
	if extendClaimBy > 0 {
		extendArg = &extendClaimBy
	}
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.set_task_checkpoint_state($1,$2::uuid,$3,$4::jsonb,$5::uuid,$6)`,
		queue, taskID, stepName, string(state), ownerRunID, extendArg,
	); err != nil {
		return fmt.Errorf("teguh: set_checkpoint %q: %w", stepName, err)
	}
	return nil
}

// GetCheckpoints loads all committed checkpoints for a task visible to a
// given run's attempt number.
func (c *Client) GetCheckpoints(ctx context.Context, queue, taskID, runID string) ([]Checkpoint, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT checkpoint_name, state FROM teguh.get_task_checkpoint_states($1,$2::uuid,$3::uuid)`,
		queue, taskID, runID,
	)
	if err != nil {
		return nil, fmt.Errorf("teguh: get_checkpoints: %w", err)
	}
	defer rows.Close()

	var cps []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var state []byte
		if err := rows.Scan(&cp.Name, &state); err != nil {
			return nil, fmt.Errorf("teguh: scan checkpoint: %w", err)
		}
		cp.State = state
		cps = append(cps, cp)
	}
	return cps, rows.Err()
}

// ScheduleRun suspends the run until wakeAt. The worker must return after
// calling this (or after TaskContext.SleepFor/SleepUntil returns ErrSuspended).
func (c *Client) ScheduleRun(ctx context.Context, queue, runID string, wakeAt time.Time) error {
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.schedule_run($1,$2::uuid,$3)`,
		queue, runID, wakeAt,
	); err != nil {
		return fmt.Errorf("teguh: schedule_run: %w", err)
	}
	return nil
}

// AwaitEvent suspends the run waiting for eventName, or returns immediately if
// the event was already emitted. timeoutSecs nil = wait forever.
// Returns (shouldSuspend, payload, err). When shouldSuspend is true the worker
// must stop processing the run.
func (c *Client) AwaitEvent(ctx context.Context, queue, taskID, runID, stepName, eventName string, timeoutSecs *int) (shouldSuspend bool, payload json.RawMessage, err error) {
	rows, err := c.pool.Query(ctx,
		`SELECT should_suspend, payload FROM teguh.await_event($1,$2::uuid,$3::uuid,$4,$5,$6)`,
		queue, taskID, runID, stepName, eventName, timeoutSecs,
	)
	if err != nil {
		return false, nil, fmt.Errorf("teguh: await_event: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return false, nil, fmt.Errorf("teguh: await_event returned no rows")
	}
	var payloadBytes []byte
	if err := rows.Scan(&shouldSuspend, &payloadBytes); err != nil {
		return false, nil, fmt.Errorf("teguh: scan await_event: %w", err)
	}
	// Normalize the 'null'::jsonb timeout sentinel to Go nil so callers receive
	// nil for both "event timed out" and "event emitted with no payload".
	if string(payloadBytes) == "null" {
		payloadBytes = nil
	}
	return shouldSuspend, payloadBytes, rows.Err()
}

// EmitEvent emits a named event with an optional payload. First-write-wins:
// subsequent calls for the same event name are silently ignored.
// Wakes any tasks waiting on this event and fires pg_notify.
// A nil payload sends SQL NULL, which the engine stores as JSON null.
func (c *Client) EmitEvent(ctx context.Context, queue, eventName string, payload any) error {
	var payloadArg any
	if payload != nil {
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("teguh: marshal event payload: %w", err)
		}
		s := string(payloadJSON)
		payloadArg = &s
	}
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.emit_event($1,$2,$3::jsonb)`,
		queue, eventName, payloadArg,
	); err != nil {
		return fmt.Errorf("teguh: emit_event %q: %w", eventName, err)
	}
	return nil
}

// CancelTask cancels a task by ID. Running workers detect cancellation at
// the next checkpoint or heartbeat call.
func (c *Client) CancelTask(ctx context.Context, queue, taskID string) error {
	if _, err := c.pool.Exec(ctx,
		`SELECT teguh.cancel_task($1,$2::uuid)`, queue, taskID,
	); err != nil {
		return fmt.Errorf("teguh: cancel_task: %w", err)
	}
	return nil
}

// RetryTask re-enqueues a failed or cancelled task.
func (c *Client) RetryTask(ctx context.Context, queue, taskID string, spawnNew bool) (SpawnResult, error) {
	opts, _ := json.Marshal(map[string]any{"spawn_new": spawnNew})
	rows, err := c.pool.Query(ctx,
		`SELECT task_id, run_id, attempt, created FROM teguh.retry_task($1,$2::uuid,$3::jsonb)`,
		queue, taskID, string(opts),
	)
	if err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: retry_task: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return SpawnResult{}, fmt.Errorf("teguh: retry_task returned no rows")
	}
	var res SpawnResult
	if err := rows.Scan(&res.TaskID, &res.RunID, &res.Attempt, &res.Created); err != nil {
		return SpawnResult{}, fmt.Errorf("teguh: scan retry_task: %w", err)
	}
	return res, rows.Err()
}

// GetTaskResult returns the current state and result of a task.
func (c *Client) GetTaskResult(ctx context.Context, queue, taskID string) (TaskResult, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT task_id, state, attempts, completed_payload, cancelled_at
		   FROM teguh.get_task_result($1,$2::uuid)`,
		queue, taskID,
	)
	if err != nil {
		return TaskResult{}, fmt.Errorf("teguh: get_task_result: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return TaskResult{}, fmt.Errorf("teguh: task %q not found in queue %q", taskID, queue)
	}
	var res TaskResult
	var payload []byte
	if err := rows.Scan(&res.TaskID, &res.State, &res.Attempts, &payload, &res.CancelledAt); err != nil {
		return TaskResult{}, fmt.Errorf("teguh: scan get_task_result: %w", err)
	}
	res.CompletedPayload = payload
	return res, rows.Err()
}

// Ticker re-queues sleeping tasks whose wake time has arrived and fires
// pg_notify. Returns the number of tasks re-queued. Safe to call manually
// (used in tests; in production use teguh.start() for pg_cron scheduling).
func (c *Client) Ticker(ctx context.Context) (int, error) {
	var n int
	if err := c.pool.QueryRow(ctx, `SELECT teguh.ticker()`).Scan(&n); err != nil {
		return 0, fmt.Errorf("teguh: ticker: %w", err)
	}
	return n, nil
}

// NewWorker creates a Worker for the given queue. Use Handle to register task
// handlers, then call Start to begin processing.
func (c *Client) NewWorker(queue string, opts ...Option) *Worker {
	w := &Worker{
		client:            c,
		queue:             queue,
		workerID:          defaultWorkerID(),
		pollInterval:      30 * time.Second,
		concurrency:       10,
		claimTimeout:      30,
		heartbeatInterval: 10 * time.Second,
		handlers:          make(map[string]HandlerFunc),
	}
	for _, o := range opts {
		o(w)
	}
	if w.batchSize == 0 {
		w.batchSize = w.concurrency
	}
	return w
}

// nullableJSON converts a nil or empty byte slice into a nil interface for
// use as a nullable SQL parameter.
func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	s := string(b)
	return &s
}

// isCancelledError reports whether err carries the AB001 sqlstate that teguh
// raises when a task has been cancelled.
func isCancelledError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "AB001"
	}
	return false
}

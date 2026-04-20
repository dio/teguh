// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package teguh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// HandlerFunc processes a single task execution. It receives a TaskContext
// that exposes durable execution primitives (Step, SleepFor, AwaitEvent, …).
//
//   - Return nil to mark the run as completed.
//   - Return ErrSuspended (propagated from tc.SleepFor / tc.AwaitEvent) to
//     leave the run suspended, the Worker will not call complete_run or fail_run.
//   - Return ErrCancelled (propagated from tc.Heartbeat) when the task has been
//     cancelled; the Worker will not call complete_run or fail_run.
//   - Return any other error to fail the run (with retry if configured).
type HandlerFunc func(ctx context.Context, tc *TaskContext) error

// Worker polls a teguh queue and dispatches claimed tasks to registered
// handlers. It uses LISTEN/NOTIFY (channel: teguh_<queue>) to wake
// immediately when new tasks are available, falling back to a poll interval.
type Worker struct {
	client            *Client
	queue             string
	workerID          string
	pollInterval      time.Duration
	concurrency       int
	claimTimeout      int
	heartbeatInterval time.Duration
	batchSize         int
	handlers          map[string]HandlerFunc
	catchAll          HandlerFunc
}

// Handle registers a handler for the named task. Use "*" to register a
// catch-all handler for any task name without a specific handler.
func (w *Worker) Handle(taskName string, fn HandlerFunc) {
	if taskName == "*" {
		w.catchAll = fn
	} else {
		w.handlers[taskName] = fn
	}
}

// Start begins the worker loop, blocking until ctx is cancelled.
// It opens a dedicated LISTEN connection (separate from the pool) for
// immediate pg_notify wakeup. The fallback poll interval fires when no
// notification arrives within WithPollInterval.
func (w *Worker) Start(ctx context.Context) error {
	listenConn, err := pgx.ConnectConfig(ctx, w.client.pool.Config().ConnConfig)
	if err != nil {
		return fmt.Errorf("teguh: open listen conn: %w", err)
	}
	defer listenConn.Close(context.Background()) //nolint:contextcheck,errcheck

	channel := pgx.Identifier{"teguh_" + w.queue}.Sanitize()
	if _, err := listenConn.Exec(ctx, "LISTEN "+channel); err != nil {
		return fmt.Errorf("teguh: LISTEN %s: %w", channel, err)
	}
	slog.InfoContext(ctx, "teguh: worker started",
		"queue", w.queue,
		"worker_id", w.workerID,
		"concurrency", w.concurrency,
		"poll_interval", w.pollInterval,
	)

	// sem limits in-flight runs to w.concurrency.
	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup

	for {
		if err := ctx.Err(); err != nil {
			// Wait for all in-flight runs to finish before returning.
			wg.Wait()
			return err
		}

		available := w.concurrency - len(sem)
		if available == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		batch := available
		if w.batchSize > 0 && w.batchSize < batch {
			batch = w.batchSize
		}

		runs, err := w.client.ClaimTask(ctx, w.queue, w.workerID, w.claimTimeout, batch)
		if err != nil {
			slog.ErrorContext(ctx, "teguh: claim_task error",
				"queue", w.queue, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(w.pollInterval):
			}
			continue
		}

		if len(runs) == 0 {
			// Queue is empty, wait for NOTIFY or the poll interval.
			waitCtx, cancel := context.WithTimeout(ctx, w.pollInterval)
			_, _ = listenConn.WaitForNotification(waitCtx)
			cancel()
			continue
		}

		for _, run := range runs {
			sem <- struct{}{}
			wg.Add(1)
			go func(r Run) {
				defer wg.Done()
				defer func() { <-sem }()
				w.executeRun(ctx, r)
			}(run)
		}
	}
}

// executeRun loads checkpoints, invokes the handler, then completes or fails
// the run depending on the returned error.
func (w *Worker) executeRun(ctx context.Context, run Run) {
	log := slog.With(
		"queue", w.queue,
		"task_id", run.TaskID,
		"run_id", run.RunID,
		"task_name", run.TaskName,
		"attempt", run.Attempt,
	)
	log.InfoContext(ctx, "teguh: run started")

	// Pre-load committed checkpoints so Step cache hits require no DB round-trip.
	cps, err := w.client.GetCheckpoints(ctx, w.queue, run.TaskID, run.RunID)
	if err != nil {
		log.ErrorContext(ctx, "teguh: get_checkpoints failed", "error", err)
		_ = w.client.FailRun(ctx, w.queue, run.RunID,
			map[string]any{"name": "$InternalError", "message": err.Error()},
			nil)
		return
	}

	cache := make(map[string]json.RawMessage, len(cps))
	for _, cp := range cps {
		cache[cp.Name] = cp.State
	}

	tc := &TaskContext{
		client:      w.client,
		queue:       w.queue,
		run:         run,
		checkpoints: cache,
	}

	handler := w.handlers[run.TaskName]
	if handler == nil {
		handler = w.catchAll
	}
	if handler == nil {
		log.WarnContext(ctx, "teguh: no handler registered", "task_name", run.TaskName)
		_ = w.client.FailRun(ctx, w.queue, run.RunID,
			map[string]any{"name": "$NoHandler", "message": "no handler for task " + run.TaskName},
			nil)
		return
	}

	// Background heartbeat: keep the lease alive while the run executes.
	stopHB := w.startHeartbeat(ctx, run, log)
	defer stopHB() // safety net for early returns via panic

	herr := handler(ctx, tc)

	// Stop heartbeat before any state-changing DB call to avoid extending
	// a claim that is about to be released.
	stopHB()

	switch {
	case herr == nil:
		if err := w.client.CompleteRun(ctx, w.queue, run.RunID, nil); err != nil {
			log.ErrorContext(ctx, "teguh: complete_run failed", "error", err)
		} else {
			log.InfoContext(ctx, "teguh: run completed")
		}

	case errors.Is(herr, ErrSuspended):
		// Task is sleeping or waiting for an event, leave it alone.
		log.InfoContext(ctx, "teguh: run suspended")

	case errors.Is(herr, ErrCancelled):
		// Task was cancelled by an external actor during execution.
		log.InfoContext(ctx, "teguh: task cancelled during execution")

	default:
		log.ErrorContext(ctx, "teguh: handler error", "error", herr)
		if err := w.client.FailRun(ctx, w.queue, run.RunID,
			map[string]any{"name": "$HandlerError", "message": herr.Error()},
			nil,
		); err != nil {
			log.ErrorContext(ctx, "teguh: fail_run error", "error", err)
		}
	}
}

// startHeartbeat launches a background goroutine that periodically extends the
// run's lease. The returned stop function is safe to call multiple times.
func (w *Worker) startHeartbeat(ctx context.Context, run Run, log *slog.Logger) func() {
	stop := make(chan struct{})
	var once sync.Once
	stopFn := func() { once.Do(func() { close(stop) }) }
	go func() {
		t := time.NewTicker(w.heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := w.client.ExtendClaim(ctx, w.queue, run.RunID, w.claimTimeout); err != nil {
					if isCancelledError(err) {
						log.InfoContext(ctx, "teguh: task cancelled during heartbeat")
					} else {
						log.WarnContext(ctx, "teguh: heartbeat failed", "error", err)
					}
				}
			}
		}
	}()
	return stopFn
}

// defaultWorkerID returns "hostname:pid" or "worker" on error.
func defaultWorkerID() string {
	host, _ := os.Hostname()
	if host == "" {
		return "worker"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

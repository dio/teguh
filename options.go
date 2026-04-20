// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package teguh

import "time"

// Option configures a Worker.
type Option func(*Worker)

// WithPollInterval sets the fallback sleep between claim_task calls when the
// queue is empty and no NOTIFY arrives. Defaults to 30s.
// Workers with LISTEN/NOTIFY only hit this fallback on missed notifications.
func WithPollInterval(d time.Duration) Option {
	return func(w *Worker) { w.pollInterval = d }
}

// WithConcurrency sets the maximum number of tasks executed concurrently by
// this worker process. Defaults to 10.
func WithConcurrency(n int) Option {
	return func(w *Worker) { w.concurrency = n }
}

// WithClaimTimeout sets the lease duration in seconds. Workers must heartbeat
// before this deadline or the task is automatically failed and re-queued.
// Defaults to 30s.
func WithClaimTimeout(secs int) Option {
	return func(w *Worker) { w.claimTimeout = secs }
}

// WithHeartbeatInterval sets how often the worker extends its lease.
// Must be less than WithClaimTimeout. Defaults to 10s.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(w *Worker) { w.heartbeatInterval = d }
}

// WithBatchSize sets how many tasks to claim per claim_task call.
// Defaults to the worker's concurrency setting.
func WithBatchSize(n int) Option {
	return func(w *Worker) { w.batchSize = n }
}

// WithWorkerID sets an explicit worker identifier (used in lease metadata and
// logs). Defaults to hostname:pid.
func WithWorkerID(id string) Option {
	return func(w *Worker) { w.workerID = id }
}

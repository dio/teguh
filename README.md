# teguh

Teguh is a durable execution engine built on PostgreSQL. It combines the workflow API from [absurd](https://github.com/nicholasgasior/absurd) with a zero-bloat dispatch queue inspired by PgQ, adding LISTEN/NOTIFY so workers wake immediately instead of polling on a fixed interval.

## Features

- Pull-based `claim_task` API, fully compatible with the absurd workflow model.
- LISTEN/NOTIFY wakeup via `pg_notify` on `spawn_task` and `emit_event`, with a configurable fallback poll interval.
- Zero dead-tuple dispatch path: `p_<queue>` rows are inserted on spawn and deleted on claim, never updated.
- Active leases tracked in `r_<queue>` only while a run is in-flight, keeping the table small and bounded.
- Exactly-once step checkpoints via `teguh.Step[T]`, with in-memory cache pre-loaded at claim time.
- Timer-based sleep and resume (`SleepFor`, `SleepUntil`) without a run row while sleeping.
- Event coordination (`AwaitEvent`, `EmitEvent`) with first-write-wins semantics and timeout support.
- Configurable retry strategies: fixed delay, exponential backoff, or none.
- Task cancellation, manual retry, and result inspection.
- Optional pg_cron integration via `teguh.start()` and `teguh.stop()`, gracefully skipped when pg_cron is absent.
- Single-file SQL install, no external dependencies beyond PostgreSQL and pgcrypto.

## Requirements

- PostgreSQL 14 or later.
- The `pgcrypto` extension (used for `gen_random_bytes` in the UUIDv7 generator).
- Go 1.21 or later (for generic `Step[T]`).

## Installation

Install the schema once per database:

```sql
\i sql/teguh.sql
```

Or apply it programmatically:

```go
schema, _ := os.ReadFile("sql/teguh.sql")
_, err = pool.Exec(ctx, string(schema))
```

## Quick start

```go
import "github.com/dio/teguh"

client, err := teguh.Connect(ctx, dsn)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Create a queue.
if err := client.CreateQueue(ctx, "jobs"); err != nil {
    log.Fatal(err)
}

// Register handlers and start a worker.
w := client.NewWorker("jobs",
    teguh.WithConcurrency(10),
    teguh.WithClaimTimeout(30),
    teguh.WithPollInterval(30*time.Second),
)

w.Handle("send-email", func(ctx context.Context, tc *teguh.TaskContext) error {
    var params struct {
        To      string `json:"to"`
        Subject string `json:"subject"`
    }
    if err := json.Unmarshal(tc.Params(), &params); err != nil {
        return err
    }
    return sendEmail(ctx, params.To, params.Subject)
})

// Start blocks until ctx is cancelled.
if err := w.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
    log.Fatal(err)
}
```

Spawn a task from anywhere:

```go
res, err := client.SpawnTask(ctx, "jobs", "send-email",
    map[string]any{"to": "user@example.com", "subject": "Hello"},
    nil,
)
```

## Durable execution

Use `teguh.Step[T]` for exactly-once sub-steps. On retry, the cached result is returned immediately without re-executing the function.

```go
w.Handle("order-fulfillment", func(ctx context.Context, tc *teguh.TaskContext) error {
    // Step 1: charge the card. Runs exactly once, even across retries.
    charge, err := teguh.Step(ctx, tc, "charge", func(ctx context.Context) (*ChargeResult, error) {
        return chargeCard(ctx, orderID)
    })
    if err != nil {
        return err
    }

    // Step 2: ship the order.
    _, err = teguh.Step(ctx, tc, "ship", func(ctx context.Context) (*ShipResult, error) {
        return shipOrder(ctx, charge.ID)
    })
    return err
})
```

### Sleep and resume

```go
w.Handle("reminder", func(ctx context.Context, tc *teguh.TaskContext) error {
    if tc.Attempt() == 1 {
        // Suspend for 24 hours. The worker re-queues the task after the delay.
        return tc.SleepFor(ctx, 24*time.Hour)
    }
    return sendReminder(ctx)
})
```

### Event coordination

```go
// Wait for an external event (e.g. payment confirmed).
w.Handle("wait-for-payment", func(ctx context.Context, tc *teguh.TaskContext) error {
    payload, err := tc.AwaitEvent(ctx, "payment-received", "payment:"+orderID,
        teguh.WithTimeout(10*time.Minute),
    )
    if errors.Is(err, teguh.ErrSuspended) {
        return teguh.ErrSuspended
    }
    if err != nil {
        return err
    }
    return processPayment(ctx, payload)
})

// Emit the event from another handler or service.
if err := client.EmitEvent(ctx, "jobs", "payment:"+orderID, paymentData); err != nil {
    return err
}
```

## Low-level API

The `Client` exposes the full low-level API for direct use without the Worker abstraction:

```go
// Spawn and claim manually.
res, err := client.SpawnTask(ctx, "jobs", "ping", map[string]any{"val": 42}, nil)
runs, err := client.ClaimTask(ctx, "jobs", "worker-1", 30, 1)

// Complete, fail, or sleep.
client.CompleteRun(ctx, "jobs", runs[0].RunID, result)
client.FailRun(ctx, "jobs", runs[0].RunID, reason, time.Time{})
client.ScheduleRun(ctx, "jobs", runs[0].RunID, time.Now().Add(1*time.Minute))

// Checkpoints.
client.SetCheckpoint(ctx, "jobs", taskID, "step-name", stateJSON, runID, 0)
cps, err := client.GetCheckpoints(ctx, "jobs", taskID, runID)

// Events.
client.AwaitEvent(ctx, "jobs", taskID, runID, "step", "event-name", nil)
client.EmitEvent(ctx, "jobs", "event-name", payload)

// Cancellation and retry.
client.CancelTask(ctx, "jobs", taskID)
client.RetryTask(ctx, "jobs", taskID, false)

// Ticker: re-queues sleeping tasks whose wake time has arrived.
// In production, call teguh.start() to schedule this via pg_cron.
n, err := client.Ticker(ctx)
```

## Worker options

| Option | Default | Description |
|---|---|---|
| `WithConcurrency(n)` | 10 | Maximum in-flight runs. |
| `WithClaimTimeout(secs)` | 30 | Lease duration in seconds. |
| `WithPollInterval(d)` | 30s | Fallback poll interval when no NOTIFY arrives. |
| `WithHeartbeatInterval(d)` | 10s | How often to extend the lease while a run is active. |
| `WithBatchSize(n)` | concurrency | Maximum tasks claimed per poll cycle. |
| `WithWorkerID(id)` | hostname:pid | Identifier stored in the run lease. |

## Per-queue tables

| Table | Purpose |
|---|---|
| `t_<queue>` | Canonical task record, durable across all attempts. |
| `p_<queue>` | Pending dispatch queue, insert on spawn and delete on claim, zero dead tuples. |
| `r_<queue>` | Active leases only, one row per in-flight run. |
| `c_<queue>` | Step checkpoints, keyed by task ID and step name. |
| `e_<queue>` | Events, first-write-wins. |
| `w_<queue>` | Wait registrations for tasks suspended on an event. |

## pg_cron integration (optional)

```sql
-- Schedule the ticker every minute and a daily cleanup job.
SELECT teguh.start();

-- Unschedule both jobs.
SELECT teguh.stop();
```

If pg_cron is not installed, `teguh.start()` emits a notice and returns without error. Call `teguh.ticker()` manually from your own scheduler or from tests.

## Development

```sh
# Copy the SQL schema into e2e/testdata.
make fetch-schema

# Run unit tests.
make test

# Run e2e tests (starts embedded PostgreSQL automatically).
make test.e2e

# Lint all modules.
make lint

# Format all modules.
make format
```

## License

Apache-2.0. Copyright 2026 dio@rockybars.com.

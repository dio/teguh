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
- Single-file SQL install, no external dependencies beyond PostgreSQL itself.

## Requirements

- PostgreSQL 14 or later.
- Go 1.21 or later (for generic `Step[T]`).

No PostgreSQL extensions are required. The UUIDv7 generator uses `gen_random_uuid()` and
`uuid_send()`, both core built-ins since PostgreSQL 13.

## Managed PostgreSQL compatibility

Teguh works out of the box on every major managed provider. No extension pre-configuration is needed.

| Provider | Works? | Notes |
|---|---|---|
| Google Cloud SQL | ✅ | No restrictions |
| Amazon RDS for PostgreSQL | ✅ | No restrictions |
| Amazon Aurora PostgreSQL | ✅ | No restrictions |
| Azure Database for PostgreSQL | ✅ | No restrictions |
| Supabase | ✅ | No restrictions |
| Neon | ✅ | No restrictions |
| Aiven | ✅ | No restrictions |
| Crunchy Bridge | ✅ | No restrictions |
| DigitalOcean Managed Databases | ✅ | No restrictions |
| Render | ✅ | No restrictions |
| Heroku Postgres / EDB | ✅ | No restrictions |
| Railway | ✅ | No official extension list, but no extension needed |

### pgcrypto auto-detection

`portable_uuidv7()` defaults to `uuid_send(gen_random_uuid())` — no extension needed.
If pgcrypto is already installed, `teguh.sql` detects it at install time and automatically
upgrades the function to use `gen_random_bytes(10)` instead. Re-running `teguh.sql` after
installing pgcrypto picks up the upgrade with zero per-call overhead.

**Implementation paths:**

| | `uuid_send(gen_random_uuid())` (default) | `gen_random_bytes(10)` (pgcrypto auto-upgrade) |
|---|---|---|
| Extension required | None | `pgcrypto` (auto-detected) |
| Azure extra step | None | Must allowlist via `azure.extensions` param |
| Random source | OS CSPRNG via `gen_random_uuid()` | OS CSPRNG directly |
| Bytes of entropy | 80 bits (first 10 of 16) | 80 bits |
| Performance | One UUID generation + substr | One syscall |
| Portability | All providers, all PG14+ | All providers except possibly Railway |

Both paths provide 80 bits of OS CSPRNG entropy. The difference is negligible in practice.

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
client.FailRun(ctx, "jobs", runs[0].RunID, reason, nil)
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

### Prerequisites

- Go 1.21 or later.
- No PostgreSQL installation needed — e2e tests start an embedded PostgreSQL instance automatically via [embedded-postgres](https://github.com/fergusstrange/embedded-postgres).

### Workflow

```sh
# First-time setup: copy sql/teguh.sql into e2e/testdata.
make fetch-schema

# Unit tests (root package).
make test

# E2e tests — starts embedded PostgreSQL, installs the schema, runs all tests.
make test.e2e

# Lint all modules (golangci-lint).
make lint

# Format all Go code in-place (run before committing).
make format
```

### SQL style

`sql/teguh.sql` is hand-formatted. Automated SQL formatters (pg_format, sql-formatter) do not preserve the style, so formatting is enforced by convention rather than tooling. Please follow these rules when editing:

- **Lowercase keywords** — `create or replace function`, `select`, `insert`, etc.
- **2-space indentation** inside function bodies and SQL blocks.
- **No space before `()`** — `current_time()`, not `current_time ()`.
- **Multi-line function signatures** with each clause on its own line:
  ```sql
  create or replace function teguh.my_func(p_arg text)
    returns void
    language plpgsql
  as $$
  ```
- **Column-aligned declarations** when a block has multiple variables:
  ```sql
  declare
    v_millis  bigint;
    v_hex     text;
    v_b       bytea;
  ```

### Go formatting

`make format` rewrites Go files in-place via `gofmt` and `goimports`. Run it before committing. `make lint` enforces linter rules but does not check formatting. `make check-format` (used in CI) fails if any file needs reformatting.

## License

Apache-2.0. Copyright 2026 dio@rockybars.com.

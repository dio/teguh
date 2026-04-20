# teguh e2e tests

End-to-end tests for the teguh Go client. They run against a real PostgreSQL instance managed by [embedded-postgres](https://github.com/fergusstrange/embedded-postgres), so no external database setup is required.

## Prerequisites

- Go 1.21 or later.
- The teguh SQL schema copied into `testdata/teguh.sql`. Run `make fetch-schema` from the repository root to do this.

## Running the tests

From the repository root:

```sh
make test.e2e
```

Or directly from this directory:

```sh
go test -race -count=1 -timeout 180s ./...
```

The test suite starts PostgreSQL 18 on port 5455, installs the teguh schema, runs all tests, and stops the database. If `testdata/teguh.sql` is missing, the suite exits with code 0 (skip, not fail) and prints a reminder to run `make fetch-schema`.

## Test structure

| File | Contents |
|---|---|
| `e2e_test.go` | `TestMain`: starts embedded PostgreSQL, installs the schema, and tears everything down after the suite. |
| `teguh_test.go` | All test cases, plus shared helpers (`newClient`, `setupQueue`, `ticker`). |

### Helpers

- `newClient(t)`: opens a `teguh.Client` against the embedded database and registers `t.Cleanup(client.Close)`.
- `setupQueue(t, client, queue)`: calls `CreateQueue` and registers `t.Cleanup` to drop the queue after the test.
- `ticker(t, client)`: calls `teguh.ticker()` manually, replacing the pg_cron scheduler in tests.

## Test coverage

| Area | Tests |
|---|---|
| Basic lifecycle | `TestSpawnTask`, `TestSpawnClaimComplete`, `TestClaimEmptyQueue` |
| Idempotency | `TestIdempotencyKey` |
| Retry | `TestRetryOnFail`, `TestExhaustedRetries` |
| Checkpoints | `TestStepCheckpointExactlyOnce` |
| Sleep and resume | `TestSleepAndResume`, `TestWorkerSleepResume` |
| Event coordination | `TestAwaitAndEmitEvent`, `TestEmitBeforeAwait` |
| Cancellation | `TestCancelPendingTask` |
| Manual retry | `TestManualRetry` |
| Worker dispatch | `TestWorkerBasicDispatch`, `TestWorkerConcurrency`, `TestWorkerCatchAll` |
| Durable multi-step workflow | `TestDurableMultiStep` |

## Notes

- Worker tests use `context.WithTimeout`. The worker goroutine may log a `complete_run: context canceled` error when the test context is cancelled before the terminal call completes. This is expected and does not cause test failures.
- The embedded PostgreSQL binary is cached in `~/.embedded-postgres-go` after the first download.
- Port 5455 is used to avoid conflicts with a local PostgreSQL instance on the default port 5432.

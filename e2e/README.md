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

The test suite starts PostgreSQL on port 5455, installs the teguh schema, runs all tests, and stops the database. If `testdata/teguh.sql` is missing, the suite exits with code 2 (setup error) and prints a reminder to run `make fetch-schema`.

## Test structure

| File | Contents |
|---|---|
| `e2e_test.go` | `TestMain`: starts embedded PostgreSQL, installs the schema, and tears everything down after the suite. |
| `teguh_test.go` | `TeguhSuite` + all test methods, plus shared helpers (`newClient`, `setupQueue`, `ticker`). |

### How TestMain and TeguhSuite relate

```
TestMain
  └─ m.Run()                         ← Go test runner, discovers all Test* functions
       └─ TestTeguhSuite(t)          ← single entry point in teguh_test.go
            └─ suite.Run(t, &TeguhSuite{})  ← testify runs each method as a subtest
                 ├─ TeguhSuite.TestSpawnTask
                 ├─ TeguhSuite.TestSpawnClaimComplete
                 └─ ...
```

`TestMain` is infrastructure only — it has no knowledge of the suite. It starts and stops the embedded database and calls `m.Run()`, which discovers `TestTeguhSuite` like any other test function. `TestTeguhSuite` then hands control to testify, which runs each `Test*` method as an isolated subtest with its own `*testing.T`.

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
| Event coordination | `TestAwaitAndEmitEvent`, `TestEmitBeforeAwait`, `TestAwaitEventTimeout`, `TestAwaitEventNoTimeoutNullAvailableAt` |
| Cancellation | `TestCancelPendingTask`, `TestConcurrentCancelVsComplete`, `TestCancelledTaskNotRequeueOnEmit` |
| Manual retry | `TestManualRetry`, `TestRetryTaskSpawnNew` |
| Worker dispatch | `TestWorkerBasicDispatch`, `TestWorkerConcurrency`, `TestWorkerCatchAll`, `TestWorkerAwaitEvent` |
| Durable multi-step workflow | `TestDurableMultiStep` |
| Inline recovery / delayed spawn | `TestClaimTaskInlineRecovery`, `TestAvailableAt` |
| Panic recovery | `TestWorkerHandlerPanic` |
| UUIDv7 / pgcrypto auto-detection | `TestPortableUuidv7Format` |

## Notes

- Worker tests use `context.WithTimeout`. The worker goroutine may log a `complete_run: context canceled` error when the test context is cancelled before the terminal call completes. This is expected and does not cause test failures.
- The embedded PostgreSQL binary is cached in `~/.embedded-postgres-go` after the first download.
- Port 5455 is used to avoid conflicts with a local PostgreSQL instance on the default port 5432.

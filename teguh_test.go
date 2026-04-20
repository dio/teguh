// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package teguh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SpawnOptions.toJSONB

func TestSpawnOptionsToJSONB_nil(t *testing.T) {
	var o *SpawnOptions
	b, err := o.toJSONB()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != "{}" {
		t.Fatalf("got %q, want {}", string(b))
	}
}

func TestSpawnOptionsToJSONB(t *testing.T) {
	maxAttempts := 3
	availAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	o := &SpawnOptions{
		Headers:        map[string]any{"x-tenant": "t1"},
		RetryStrategy:  map[string]any{"kind": "exponential", "base_seconds": 30},
		MaxAttempts:    &maxAttempts,
		Cancellation:   map[string]any{"max_delay": 60},
		IdempotencyKey: "idem-1",
		AvailableAt:    availAt,
	}
	b, err := o.toJSONB()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["idempotency_key"] != "idem-1" {
		t.Errorf("idempotency_key: got %v", m["idempotency_key"])
	}
	if m["max_attempts"].(float64) != 3 {
		t.Errorf("max_attempts: got %v", m["max_attempts"])
	}
	if m["available_at"] != availAt.Format(time.RFC3339Nano) {
		t.Errorf("available_at: got %v", m["available_at"])
	}
}

func TestSpawnOptionsToJSONB_zeroAvailableAt(t *testing.T) {
	o := &SpawnOptions{}
	b, err := o.toJSONB()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["available_at"]; ok {
		t.Error("available_at must be omitted when zero")
	}
}

// nullableJSON

func TestNullableJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantNil bool
	}{
		{"nil slice", nil, true},
		{"empty slice", []byte{}, true},
		{"non-empty", []byte(`{"a":1}`), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := nullableJSON(tc.input)
			if tc.wantNil && got != nil {
				t.Errorf("want nil, got %v", got)
			}
			if !tc.wantNil && got == nil {
				t.Error("want non-nil, got nil")
			}
		})
	}
}

// isCancelledError

func TestIsCancelledError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if isCancelledError(nil) {
			t.Error("nil should not be cancelled")
		}
	})
	t.Run("AB001", func(t *testing.T) {
		err := &pgconn.PgError{Code: "AB001"}
		if !isCancelledError(err) {
			t.Error("AB001 should be cancelled")
		}
	})
	t.Run("wrapped AB001", func(t *testing.T) {
		err := fmt.Errorf("wrapper: %w", &pgconn.PgError{Code: "AB001"})
		if !isCancelledError(err) {
			t.Error("wrapped AB001 should be cancelled")
		}
	})
	t.Run("other pgError", func(t *testing.T) {
		err := &pgconn.PgError{Code: "23505"}
		if isCancelledError(err) {
			t.Error("23505 should not be cancelled")
		}
	})
	t.Run("generic error", func(t *testing.T) {
		if isCancelledError(errors.New("oops")) {
			t.Error("generic error should not be cancelled")
		}
	})
}

// defaultWorkerID

func TestDefaultWorkerID(t *testing.T) {
	id := defaultWorkerID()
	if id == "" {
		t.Fatal("workerID must not be empty")
	}
	// Must be either "worker" (hostname unavailable) or "hostname:pid".
	if id != "worker" {
		// Should contain a colon separating hostname and pid.
		found := false
		for _, c := range id {
			if c == ':' {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("workerID %q missing colon separator", id)
		}
	}
}

// TaskContext accessors

func TestTaskContextAccessors(t *testing.T) {
	wakeEvent := "payment:123"
	payload := json.RawMessage(`{"amount":99}`)
	params := json.RawMessage(`{"order":"abc"}`)
	tc := &TaskContext{
		run: Run{
			RunID:        "run-1",
			TaskID:       "task-1",
			Attempt:      2,
			WakeEvent:    &wakeEvent,
			EventPayload: payload,
			Params:       params,
		},
	}
	if tc.RunID() != "run-1" {
		t.Errorf("RunID: %q", tc.RunID())
	}
	if tc.TaskID() != "task-1" {
		t.Errorf("TaskID: %q", tc.TaskID())
	}
	if tc.Attempt() != 2 {
		t.Errorf("Attempt: %d", tc.Attempt())
	}
	if tc.WakeEvent() == nil || *tc.WakeEvent() != wakeEvent {
		t.Errorf("WakeEvent: %v", tc.WakeEvent())
	}
	if string(tc.EventPayload()) != `{"amount":99}` {
		t.Errorf("EventPayload: %s", tc.EventPayload())
	}
	if string(tc.Params()) != `{"order":"abc"}` {
		t.Errorf("Params: %s", tc.Params())
	}
}

// Worker option functions

func TestWorkerOptions(t *testing.T) {
	c := &Client{}
	w := c.NewWorker("q",
		WithConcurrency(5),
		WithClaimTimeout(60),
		WithPollInterval(15*time.Second),
		WithHeartbeatInterval(5*time.Second),
		WithBatchSize(3),
		WithWorkerID("test-worker"),
	)
	if w.concurrency != 5 {
		t.Errorf("concurrency: got %d", w.concurrency)
	}
	if w.claimTimeout != 60 {
		t.Errorf("claimTimeout: got %d", w.claimTimeout)
	}
	if w.pollInterval != 15*time.Second {
		t.Errorf("pollInterval: got %v", w.pollInterval)
	}
	if w.heartbeatInterval != 5*time.Second {
		t.Errorf("heartbeatInterval: got %v", w.heartbeatInterval)
	}
	if w.batchSize != 3 {
		t.Errorf("batchSize: got %d", w.batchSize)
	}
	if w.workerID != "test-worker" {
		t.Errorf("workerID: got %q", w.workerID)
	}
}

func TestWorkerDefaultBatchSize(t *testing.T) {
	c := &Client{}
	w := c.NewWorker("q", WithConcurrency(7))
	if w.batchSize != 7 {
		t.Errorf("default batchSize should equal concurrency, got %d", w.batchSize)
	}
}

func TestWorkerHandle(t *testing.T) {
	c := &Client{}
	w := c.NewWorker("q")
	w.Handle("my-task", func(_ context.Context, _ *TaskContext) error { return nil })
	if _, ok := w.handlers["my-task"]; !ok {
		t.Error("handler not registered")
	}
}

func TestWorkerCatchAllHandle(t *testing.T) {
	c := &Client{}
	w := c.NewWorker("q")
	w.Handle("*", func(_ context.Context, _ *TaskContext) error { return nil })
	if w.catchAll == nil {
		t.Error("catch-all handler not set")
	}
}

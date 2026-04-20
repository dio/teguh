// Copyright 2026 dio@rockybars.com. Apache-2.0 license.

package teguh

import (
	"encoding/json"
	"time"
)

// SpawnResult is returned by Client.SpawnTask.
type SpawnResult struct {
	TaskID string `json:"task_id"`
	// RunID is a pre-generated UUID stored as a hint; it is NOT the run ID
	// that will be active once the task is claimed. Use the run_id returned
	// by ClaimTask for all run-scoped APIs (CompleteRun, FailRun, etc.).
	RunID   string `json:"run_id"`
	Attempt int    `json:"attempt"`
	Created bool   `json:"created"` // false on idempotency hit
}

// Run is a claimed task execution returned by Client.ClaimTask.
type Run struct {
	RunID         string          `json:"run_id"`
	TaskID        string          `json:"task_id"`
	Attempt       int             `json:"attempt"`
	TaskName      string          `json:"task_name"`
	Params        json.RawMessage `json:"params"`
	RetryStrategy json.RawMessage `json:"retry_strategy"`
	MaxAttempts   *int            `json:"max_attempts"`
	Headers       json.RawMessage `json:"headers"`
	WakeEvent     *string         `json:"wake_event"`
	EventPayload  json.RawMessage `json:"event_payload"`
}

// TaskResult is returned by Client.GetTaskResult.
type TaskResult struct {
	TaskID           string          `json:"task_id"`
	State            string          `json:"state"`
	Attempts         int             `json:"attempts"`
	CompletedPayload json.RawMessage `json:"completed_payload"`
	CancelledAt      *time.Time      `json:"cancelled_at"`
}

// Checkpoint is a single step result returned by Client.GetCheckpoints.
type Checkpoint struct {
	Name  string          `json:"name"`
	State json.RawMessage `json:"state"`
}

// SpawnOptions configures an optional spawn_task call.
type SpawnOptions struct {
	// Headers are arbitrary key/value pairs attached to the task (not params).
	Headers map[string]any `json:"headers,omitempty"`
	// RetryStrategy controls retry behaviour: {"kind":"exponential","base_seconds":30,"factor":2}
	RetryStrategy map[string]any `json:"retry_strategy,omitempty"`
	// MaxAttempts caps the number of execution attempts (nil = unlimited).
	MaxAttempts *int `json:"max_attempts,omitempty"`
	// Cancellation configures auto-cancel thresholds:
	// {"max_delay": <seconds>}: cancel if not started within N seconds.
	// {"max_duration": <seconds>}: cancel if running longer than N seconds total.
	Cancellation map[string]any `json:"cancellation,omitempty"`
	// IdempotencyKey makes the spawn idempotent; a second call returns the
	// existing task without creating a new one.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// AvailableAt schedules delayed start (zero = immediate).
	AvailableAt time.Time `json:"available_at,omitempty"`
}

func (o *SpawnOptions) toJSONB() ([]byte, error) {
	if o == nil {
		return []byte("{}"), nil
	}
	m := map[string]any{}
	if o.Headers != nil {
		m["headers"] = o.Headers
	}
	if o.RetryStrategy != nil {
		m["retry_strategy"] = o.RetryStrategy
	}
	if o.MaxAttempts != nil {
		m["max_attempts"] = *o.MaxAttempts
	}
	if o.Cancellation != nil {
		m["cancellation"] = o.Cancellation
	}
	if o.IdempotencyKey != "" {
		m["idempotency_key"] = o.IdempotencyKey
	}
	if !o.AvailableAt.IsZero() {
		m["available_at"] = o.AvailableAt.Format(time.RFC3339Nano)
	}
	return json.Marshal(m)
}

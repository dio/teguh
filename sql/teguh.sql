-- teguh.sql -- Durable execution with zero-bloat queue
-- Requires: pgcrypto (for gen_random_bytes used in portable_uuidv7)
create extension if not exists pgcrypto;

--
-- Teguh combines absurd's durable execution API with pgque's dispatch
-- efficiency.  Pull-based claim_task() is preserved for full API
-- compatibility; pg_notify is added on top so workers can LISTEN for
-- immediate wakeup instead of sleeping the full poll interval.
--
-- Key architectural changes vs absurd:
--
--   p_<queue>   Pending queue. INSERT on spawn/retry/wakeup, DELETE on
--               claim (SKIP LOCKED). Never updated, no dead tuples in
--               the hot dispatch path.
--
--   r_<queue>   Active leases only. INSERT on claim, heartbeat UPDATE,
--               DELETE on terminal state (complete/fail/cancel).  The
--               table stays small: one row per in-flight run.
--
--   t_<queue>   Tasks, the canonical durable record. Gains available_at
--               and wake_event columns to track sleeping state without a
--               run row.
--
--   LISTEN/NOTIFY  spawn_task() fires pg_notify('teguh_<queue>', task_id).
--                  emit_event() fires pg_notify('teguh_<queue>', event_name).
--                  Workers can LISTEN and skip the poll sleep.
--
-- Per-queue tables follow the same prefix convention as absurd:
--   t_  tasks, p_  pending queue, r_  active runs,
--   c_  checkpoints, e_  events, w_  wait registrations
--
-- Single-file install: \i teguh.sql
-- Start automated ticker: SELECT teguh.start();

create schema if not exists teguh;

-- ============================================================
-- Time helper (test-overridable, same as absurd)
-- ============================================================

create or replace function teguh.current_time()
  returns timestamptz
  language plpgsql volatile
as $$
declare
  v_fake text;
begin
  v_fake := current_setting('teguh.fake_now', true);
  if v_fake is not null and length(trim(v_fake)) > 0 then
    return v_fake::timestamptz;
  end if;
  return clock_timestamp();
end;
$$;

-- ============================================================
-- UUIDv7 (time-ordered, collision-resistant)
-- ============================================================

create or replace function teguh.portable_uuidv7()
  returns uuid
  language plpgsql volatile
as $$
declare
  v_millis  bigint;
  v_hex     text;
  v_b       bytea;
begin
  v_millis := (extract(epoch from clock_timestamp()) * 1000)::bigint;
  v_hex    := lpad(to_hex(v_millis), 12, '0');
  v_b      := gen_random_bytes(10);
  return (
    substring(v_hex, 1, 8) || '-' ||
    substring(v_hex, 9, 4) || '-' ||
    '7' || lpad(to_hex((get_byte(v_b, 0) & 15)), 3, '0') || '-' ||
    to_hex((get_byte(v_b, 1) & 63) | 128) ||
    lpad(to_hex(get_byte(v_b, 2)), 2, '0') || '-' ||
    encode(substring(v_b, 4, 6), 'hex')
  )::uuid;
end;
$$;

-- ============================================================
-- Queue name validation
-- ============================================================

create or replace function teguh.validate_queue_name(p_queue_name text)
  returns text
  language plpgsql immutable
as $$
begin
  if p_queue_name is null or p_queue_name = '' then
    raise exception 'queue name must be provided';
  end if;
  if octet_length(p_queue_name) > 55 then
    raise exception 'queue name "%" is too long (max 55 bytes)', p_queue_name;
  end if;
  return p_queue_name;
end;
$$;

-- ============================================================
-- Queue registry
-- ============================================================

create table if not exists teguh.queues (
  queue_name        text primary key,
  created_at        timestamptz not null default teguh.current_time(),
  cleanup_ttl       interval    not null default interval '30 days'
                      check (cleanup_ttl >= interval '0 seconds'),
  cleanup_limit     integer     not null default 1000
                      check (cleanup_limit >= 1)
);

-- ============================================================
-- Singleton config (pg_cron job IDs)
-- ============================================================

create table if not exists teguh.config (
  singleton         bool primary key default true check (singleton),
  ticker_job_id     bigint,
  maint_job_id      bigint,
  installed_at      timestamptz not null default clock_timestamp()
);

insert into teguh.config (singleton) values (true)
on conflict (singleton) do nothing;

-- ============================================================
-- Per-queue table provisioning
-- ============================================================

create or replace function teguh.ensure_queue_tables(p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  perform teguh.validate_queue_name(p_queue_name);

  -- t_<q>: tasks, canonical durable record.
  -- Gains available_at + wake_event vs absurd for sleeping-state tracking
  execute format(
    'create table if not exists teguh.%I (
        task_id           uuid          primary key,
        task_name         text          not null,
        params            jsonb         not null,
        headers           jsonb,
        retry_strategy    jsonb,
        max_attempts      integer,
        cancellation      jsonb,
        idempotency_key   text          unique,
        enqueue_at        timestamptz   not null default teguh.current_time(),
        first_started_at  timestamptz,
        state             text          not null
          check (state in (''pending'',''running'',''sleeping'',
                           ''completed'',''failed'',''cancelled'')),
        attempts          integer       not null default 0,
        last_attempt_run  uuid,
        available_at        timestamptz,
        wake_event          text,
        completed_payload   jsonb,
        last_failure_reason jsonb,
        cancelled_at        timestamptz
     ) with (fillfactor=80)',
    't_' || p_queue_name
  );

  -- p_<q>: pending queue, INSERT on spawn/retry/wakeup, DELETE on claim.
  -- SKIP LOCKED targets this table.  Never updated: no dead tuples in
  -- the dispatch hot path.
  execute format(
    'create table if not exists teguh.%I (
        p_id          bigserial     primary key,
        task_id       uuid          not null unique,
        attempt       integer       not null default 1,
        available_at  timestamptz   not null default clock_timestamp(),
        wake_event    text,
        event_payload jsonb
     ) with (fillfactor=70)',
    'p_' || p_queue_name
  );

  -- r_<q>: active leases only, INSERT on claim, heartbeat UPDATE, DELETE on terminal.
  execute format(
    'create table if not exists teguh.%I (
        run_id            uuid          primary key,
        task_id           uuid          not null unique,
        attempt           integer       not null,
        claimed_by        text          not null,
        claim_expires_at  timestamptz,
        claimed_at        timestamptz   not null default clock_timestamp(),
        last_heartbeat    timestamptz
     ) with (fillfactor=80)',
    'r_' || p_queue_name
  );

  -- c_<q>: step checkpoints
  execute format(
    'create table if not exists teguh.%I (
        task_id           uuid          not null,
        checkpoint_name   text          not null,
        state             jsonb,
        status            text          not null default ''committed'',
        owner_run_id      uuid,
        updated_at        timestamptz   not null default teguh.current_time(),
        primary key (task_id, checkpoint_name)
     ) with (fillfactor=70)',
    'c_' || p_queue_name
  );

  -- e_<q>: emitted events (first-write-wins)
  execute format(
    'create table if not exists teguh.%I (
        event_name  text          primary key,
        payload     jsonb,
        emitted_at  timestamptz   not null default teguh.current_time()
     )',
    'e_' || p_queue_name
  );

  -- w_<q>: event wait registrations
  execute format(
    'create table if not exists teguh.%I (
        task_id     uuid          not null,
        run_id      uuid          not null,
        step_name   text          not null,
        event_name  text          not null,
        timeout_at  timestamptz,
        created_at  timestamptz   not null default teguh.current_time(),
        primary key (run_id, step_name)
     )',
    'w_' || p_queue_name
  );

  -- Indexes

  execute format(
    'create index if not exists %I on teguh.%I (available_at)',
    'p_' || p_queue_name || '_aai',
    'p_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (claim_expires_at)
       where claim_expires_at is not null',
    'r_' || p_queue_name || '_cei',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (task_id)',
    'r_' || p_queue_name || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (available_at)
       where state = ''sleeping''',
    't_' || p_queue_name || '_sai',
    't_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (event_name)',
    'w_' || p_queue_name || '_eni',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (task_id)',
    'w_' || p_queue_name || '_ti',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on teguh.%I (emitted_at)',
    'e_' || p_queue_name || '_eai',
    'e_' || p_queue_name
  );

  -- Partial index for cancellation policy scan in claim_task.
  -- Only tasks with a cancellation policy need this scan.
  execute format(
    'create index if not exists %I on teguh.%I (enqueue_at, first_started_at)
       where cancellation is not null
         and state in (''pending'',''sleeping'',''running'')',
    't_' || p_queue_name || '_cani',
    't_' || p_queue_name
  );
end;
$$;

-- ============================================================
-- Queue management
-- ============================================================

create or replace function teguh.create_queue(p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  perform teguh.validate_queue_name(p_queue_name);

  insert into teguh.queues (queue_name)
  values (p_queue_name)
  on conflict (queue_name) do nothing;

  perform teguh.ensure_queue_tables(p_queue_name);
end;
$$;

create or replace function teguh.drop_queue(p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  perform teguh.validate_queue_name(p_queue_name);

  execute format('drop table if exists teguh.%I cascade', 't_' || p_queue_name);
  execute format('drop table if exists teguh.%I cascade', 'p_' || p_queue_name);
  execute format('drop table if exists teguh.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists teguh.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists teguh.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists teguh.%I cascade', 'w_' || p_queue_name);

  delete from teguh.queues where queue_name = p_queue_name;
end;
$$;

create or replace function teguh.list_queues()
  returns table (queue_name text, created_at timestamptz)
  language sql stable
as $$
  select queue_name, created_at
    from teguh.queues
   order by queue_name;
$$;

-- ============================================================
-- spawn_task
-- Same signature as absurd.spawn_task().
-- Adds: INSERT into p_<q> + pg_notify for immediate worker wakeup.
-- ============================================================

create or replace function teguh.spawn_task(
  p_queue_name  text,
  p_task_name   text,
  p_params      jsonb,
  p_options     jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id  uuid,      -- first pending run id (p_<q>.p_id is internal; we generate a stable uuid here)
    attempt integer,
    created boolean
  )
  language plpgsql
as $$
declare
  v_task_id         uuid    := teguh.portable_uuidv7();
  v_run_id          uuid    := teguh.portable_uuidv7();  -- logical run id for the first attempt
  v_attempt         integer := 1;
  v_headers         jsonb;
  v_retry_strategy  jsonb;
  v_max_attempts    integer;
  v_cancellation    jsonb;
  v_idempotency_key text;
  v_existing_task   uuid;
  v_existing_run    uuid;
  v_existing_attempt integer;
  v_row_count       integer;
  v_now             timestamptz := teguh.current_time();
  v_params          jsonb       := coalesce(p_params, 'null'::jsonb);
  v_available_at    timestamptz;
begin
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

  if p_options is not null then
    v_headers           := p_options->'headers';
    v_retry_strategy    := p_options->'retry_strategy';
    v_cancellation      := p_options->'cancellation';
    v_idempotency_key   := p_options->>'idempotency_key';
    if p_options ? 'max_attempts' then
      v_max_attempts := (p_options->>'max_attempts')::int;
      if v_max_attempts is not null and v_max_attempts < 1 then
        raise exception 'max_attempts must be >= 1';
      end if;
    end if;
    if p_options ? 'available_at' then
      v_available_at := (p_options->>'available_at')::timestamptz;
    end if;
  end if;

  v_available_at := coalesce(v_available_at, v_now);

  -- Idempotency key handling
  if v_idempotency_key is not null then
    execute format(
      'insert into teguh.%I
          (task_id, task_name, params, headers, retry_strategy, max_attempts,
           cancellation, idempotency_key, enqueue_at, state, attempts,
           last_attempt_run, available_at)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,''pending'',$10,$11,$12)
       on conflict (idempotency_key) do nothing',
      't_' || p_queue_name
    )
    using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy,
          v_max_attempts, v_cancellation, v_idempotency_key, v_now,
          v_attempt, v_run_id, v_available_at;

    get diagnostics v_row_count = row_count;

    if v_row_count = 0 then
      -- Idempotency hit: return existing task
      execute format(
        'select task_id, last_attempt_run, attempts
           from teguh.%I
          where idempotency_key = $1',
        't_' || p_queue_name
      )
      into v_existing_task, v_existing_run, v_existing_attempt
      using v_idempotency_key;

      return query select v_existing_task,
                          coalesce(v_existing_run, v_run_id),
                          coalesce(v_existing_attempt, 1),
                          false;
      return;
    end if;
  else
    execute format(
      'insert into teguh.%I
          (task_id, task_name, params, headers, retry_strategy, max_attempts,
           cancellation, enqueue_at, state, attempts, last_attempt_run, available_at)
       values ($1,$2,$3,$4,$5,$6,$7,$8,''pending'',$9,$10,$11)',
      't_' || p_queue_name
    )
    using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy,
          v_max_attempts, v_cancellation, v_now,
          v_attempt, v_run_id, v_available_at;
  end if;

  -- Enqueue into pending dispatch table
  if v_available_at <= v_now then
    execute format(
      'insert into teguh.%I (task_id, attempt, available_at)
       values ($1, $2, $3)
       on conflict (task_id) do nothing',
      'p_' || p_queue_name
    )
    using v_task_id, v_attempt, v_available_at;

    -- Notify waiting workers for immediate wakeup
    perform pg_notify('teguh_' || p_queue_name, v_task_id::text);
  end if;
  -- Future available_at: ticker will re-queue when the time arrives

  return query select v_task_id, v_run_id, v_attempt, true;
end;
$$;

-- ============================================================
-- claim_task
-- Same signature as absurd.claim_task().
-- Internally: sweeps expired leases from r_<q>, recovers sleeping
-- tasks whose wake time has arrived, then atomically DELETEs from
-- p_<q> (SKIP LOCKED) and INSERTs into r_<q>.
-- ============================================================

create or replace function teguh.claim_task(
  p_queue_name    text,
  p_worker_id     text,
  p_claim_timeout integer default 30,
  p_qty           integer default 1
)
  returns table (
    run_id        uuid,
    task_id       uuid,
    attempt       integer,
    task_name     text,
    params        jsonb,
    retry_strategy jsonb,
    max_attempts  integer,
    headers       jsonb,
    wake_event    text,
    event_payload jsonb
  )
  language plpgsql
as $$
declare
  v_now            timestamptz := teguh.current_time();
  v_claim_timeout  integer     := greatest(coalesce(p_claim_timeout, 30), 0);
  v_worker_id      text        := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty            integer     := greatest(coalesce(p_qty, 1), 1);
  v_claim_until    timestamptz := null;
  v_expired_run    record;
  v_cancel_cand    record;
  v_sweep_limit    integer;
begin
  if v_claim_timeout > 0 then
    v_claim_until := v_now + make_interval(secs => v_claim_timeout);
  end if;

  v_sweep_limit := greatest(v_qty, 1);

  -- Apply cancellation policies before claiming (same logic as absurd)
  for v_cancel_cand in
    execute format(
      'select task_id
         from teguh.%I
        where state in (''pending'',''sleeping'',''running'')
          and (
            (
              (cancellation->>''max_delay'')::bigint is not null
              and first_started_at is null
              and extract(epoch from ($1 - enqueue_at)) >= (cancellation->>''max_delay'')::bigint
            ) or (
              (cancellation->>''max_duration'')::bigint is not null
              and first_started_at is not null
              and extract(epoch from ($1 - first_started_at)) >= (cancellation->>''max_duration'')::bigint
            )
          )
        order by task_id',
      't_' || p_queue_name
    )
  using v_now
  loop
    perform teguh.cancel_task(p_queue_name, v_cancel_cand.task_id);
  end loop;

  -- Sweep expired leases: fail runs whose heartbeat deadline has passed
  for v_expired_run in
    execute format(
      'select run_id, claimed_by, claim_expires_at, attempt
         from teguh.%I
        where claim_expires_at is not null
          and claim_expires_at <= $1
        order by claim_expires_at, run_id
        limit $2
        for update skip locked',
      'r_' || p_queue_name
    )
  using v_now, v_sweep_limit
  loop
    perform teguh.fail_run(
      p_queue_name,
      v_expired_run.run_id,
      jsonb_strip_nulls(jsonb_build_object(
        'name',          '$ClaimTimeout',
        'message',       'worker did not finish task within claim interval',
        'workerId',      v_expired_run.claimed_by,
        'claimExpiredAt', v_expired_run.claim_expires_at,
        'attempt',       v_expired_run.attempt
      )),
      null
    );
  end loop;

  -- Recovery sweep: re-queue sleeping and delayed-pending tasks whose wake time
  -- has arrived (handles cases where ticker hasn't fired yet or pg_cron isn't installed).
  execute format(
    'insert into teguh.%I (task_id, attempt, available_at, wake_event)
     select t.task_id, t.attempts + 1, $1, t.wake_event
       from teguh.%I t
      where t.state in (''sleeping'', ''pending'')
        and t.available_at is not null
        and t.available_at <= $1
        and not exists (
          select 1 from teguh.%I p where p.task_id = t.task_id
        )
        and not exists (
          select 1 from teguh.%I r where r.task_id = t.task_id
        )
     on conflict (task_id) do nothing',
    'p_' || p_queue_name,
    't_' || p_queue_name,
    'p_' || p_queue_name,
    'r_' || p_queue_name
  )
  using v_now;

  -- Claim: atomically DELETE from p_<q> (SKIP LOCKED) + INSERT into r_<q>
  return query execute format(
    'with candidate as (
         delete from teguh.%1$I
          where p_id in (
            select p_id
              from teguh.%1$I
             where available_at <= $1
             order by available_at, p_id
             limit $2
             for update skip locked
          )
          returning task_id, attempt, available_at, wake_event, event_payload
     ),
     new_runs as (
         insert into teguh.%2$I
             (run_id, task_id, attempt, claimed_by, claim_expires_at, claimed_at)
         select teguh.portable_uuidv7(), c.task_id, c.attempt, $3, $4, $1
           from candidate c
         returning run_id, task_id, attempt, claimed_by, claimed_at
     ),
     task_upd as (
         update teguh.%3$I t
            set state            = ''running'',
                attempts         = greatest(t.attempts, nr.attempt),
                first_started_at = coalesce(t.first_started_at, $1),
                last_attempt_run = nr.run_id,
                available_at     = null,
                wake_event       = null
           from new_runs nr
          where t.task_id = nr.task_id
          returning t.task_id
     ),
     wait_cleanup as (
         delete from teguh.%4$I w
          using new_runs nr
          where w.task_id = nr.task_id
            and w.timeout_at is not null
            and w.timeout_at <= $1
          returning w.task_id, w.run_id, w.step_name
     ),
     cp_timeout as (
         insert into teguh.%5$I
             (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
         select wc.task_id, wc.step_name, ''null''::jsonb, ''committed'', wc.run_id, $1
           from wait_cleanup wc
          on conflict (task_id, checkpoint_name) do nothing
     )
     select
       nr.run_id,
       nr.task_id,
       nr.attempt,
       t.task_name,
       t.params,
       t.retry_strategy,
       t.max_attempts,
       t.headers,
       c.wake_event,
       c.event_payload
     from new_runs nr
     join teguh.%3$I t on t.task_id = nr.task_id
     join candidate c on c.task_id = nr.task_id
     order by c.available_at, nr.run_id',
    'p_' || p_queue_name,
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name,
    'c_' || p_queue_name
  )
  using v_now, v_qty, v_worker_id, v_claim_until;
end;
$$;

-- ============================================================
-- complete_run
-- Same signature as absurd.complete_run().
-- Deletes the run row (r_<q>) instead of updating state.
-- ============================================================

create or replace function teguh.complete_run(
  p_queue_name  text,
  p_run_id      uuid,
  p_state       jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id   uuid;
  v_task_state text;
  v_now       timestamptz := teguh.current_time();
begin
  -- Verify run exists and lock both r_<q> and t_<q> to prevent concurrent
  -- state mutations (e.g., cancel_task racing with complete_run).
  execute format(
    'select r.task_id, t.state
       from teguh.%I r
       join teguh.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_id, v_task_state
  using p_run_id;

  if v_task_id is null then
    raise exception 'run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'task has been cancelled';
  end if;

  -- Delete the active lease row (run is done)
  execute format(
    'delete from teguh.%I where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id;

  -- Mark task completed
  execute format(
    'update teguh.%I
        set state             = ''completed'',
            completed_payload = $2,
            last_attempt_run  = $3
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, p_state, p_run_id;

  -- Clean up any dangling waits
  execute format(
    'delete from teguh.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;
end;
$$;

-- ============================================================
-- schedule_run (sleep)
-- Same signature as absurd.schedule_run().
-- Deletes the run row and marks task sleeping; ticker (or inline
-- recovery sweep in claim_task) re-queues when wake time arrives.
-- ============================================================

create or replace function teguh.schedule_run(
  p_queue_name  text,
  p_run_id      uuid,
  p_wake_at     timestamptz
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
begin
  execute format(
    'select task_id
       from teguh.%I
      where run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_task_id
  using p_run_id;

  if v_task_id is null then
    raise exception 'run "%" is not currently active in queue "%"', p_run_id, p_queue_name;
  end if;

  -- Delete the active lease row
  execute format(
    'delete from teguh.%I where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id;

  -- Record sleep on the task row
  execute format(
    'update teguh.%I
        set state        = ''sleeping'',
            available_at = $2,
            wake_event   = null
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, p_wake_at;
end;
$$;

-- ============================================================
-- fail_run
-- Same signature as absurd.fail_run().
-- Deletes the run row; if retrying, inserts into p_<q> and notifies.
-- ============================================================

create or replace function teguh.fail_run(
  p_queue_name  text,
  p_run_id      uuid,
  p_reason      jsonb,
  p_retry_at    timestamptz default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id          uuid;
  v_attempt          integer;
  v_retry_strategy   jsonb;
  v_max_attempts     integer;
  v_now              timestamptz := teguh.current_time();
  v_next_attempt     integer;
  v_delay_seconds    double precision := 0;
  v_next_available   timestamptz;
  v_retry_kind       text;
  v_base             double precision;
  v_factor           double precision;
  v_max_seconds      double precision;
  v_first_started    timestamptz;
  v_cancellation     jsonb;
  v_max_duration     bigint;
  v_task_cancel      boolean := false;
  v_new_run_id       uuid;
  v_task_state_after text;
  v_recorded_attempt integer;
  v_last_attempt_run uuid := p_run_id;
  v_cancelled_at     timestamptz := null;
begin
  -- Lock both r_<q> and t_<q> in a single query to eliminate the TOCTOU window
  -- between the two separate lock acquisitions. Lock order matches cancel_task:
  -- r_ then t_ (via JOIN, Postgres locks in row-fetch order which is r_ first).
  execute format(
    'select r.task_id, r.attempt,
            t.retry_strategy, t.max_attempts, t.first_started_at, t.cancellation
       from teguh.%I r
       join teguh.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_id, v_attempt, v_retry_strategy, v_max_attempts, v_first_started, v_cancellation
  using p_run_id;

  if v_task_id is null then
    raise exception 'run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  -- Remove the active lease
  execute format(
    'delete from teguh.%I where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id;

  -- Clean up wait registrations for this run
  execute format(
    'delete from teguh.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;

  v_next_attempt     := v_attempt + 1;
  v_task_state_after := 'failed';
  v_recorded_attempt := v_attempt;

  -- Compute retry if within attempt budget
  if v_max_attempts is null or v_next_attempt <= v_max_attempts then
    if p_retry_at is not null then
      v_next_available := p_retry_at;
    else
      v_retry_kind := coalesce(v_retry_strategy->>'kind', 'none');
      if v_retry_kind = 'fixed' then
        v_base          := coalesce((v_retry_strategy->>'base_seconds')::double precision, 60);
        v_delay_seconds := v_base;
      elsif v_retry_kind = 'exponential' then
        v_base          := coalesce((v_retry_strategy->>'base_seconds')::double precision, 30);
        v_factor        := coalesce((v_retry_strategy->>'factor')::double precision, 2);
        v_delay_seconds := v_base * power(v_factor, greatest(v_attempt - 1, 0));
        v_max_seconds   := (v_retry_strategy->>'max_seconds')::double precision;
        if v_max_seconds is not null then
          v_delay_seconds := least(v_delay_seconds, v_max_seconds);
        end if;
      else
        v_delay_seconds := 0;
      end if;
      v_next_available := v_now + (v_delay_seconds * interval '1 second');
    end if;

    if v_next_available < v_now then
      v_next_available := v_now;
    end if;

    -- Check cancellation max_duration
    if v_cancellation is not null then
      v_max_duration := (v_cancellation->>'max_duration')::bigint;
      if v_max_duration is not null and v_first_started is not null then
        if extract(epoch from (v_next_available - v_first_started)) >= v_max_duration then
          v_task_cancel := true;
        end if;
      end if;
    end if;

    if not v_task_cancel then
      v_new_run_id       := teguh.portable_uuidv7();
      v_recorded_attempt := v_next_attempt;
      v_last_attempt_run := v_new_run_id;
      v_task_state_after := case
        when v_next_available > v_now then 'sleeping'
        else 'pending'
      end;

      if v_task_state_after = 'pending' then
        -- Enqueue immediately into pending dispatch table
        execute format(
          'insert into teguh.%I (task_id, attempt, available_at)
           values ($1, $2, $3)
           on conflict (task_id) do nothing',
          'p_' || p_queue_name
        )
        using v_task_id, v_next_attempt, v_next_available;

        -- Notify workers
        perform pg_notify('teguh_' || p_queue_name, v_task_id::text);
      end if;
      -- sleeping: ticker / inline recovery sweep will re-queue at v_next_available
    end if;
  end if;

  if v_task_cancel then
    v_task_state_after := 'cancelled';
    v_cancelled_at     := v_now;
    v_recorded_attempt := greatest(v_recorded_attempt, v_attempt);
    v_last_attempt_run := p_run_id;
  end if;

  execute format(
    'update teguh.%I
        set state            = $2,
            attempts         = greatest(attempts, $3),
            last_attempt_run = $4,
            available_at     = $5,
            cancelled_at     = coalesce(cancelled_at, $6),
            last_failure_reason = $7
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run,
          case when v_task_state_after = 'sleeping' then v_next_available else null end,
          v_cancelled_at,
          p_reason;
end;
$$;

-- Add last_failure_reason to task table if not present (forward-compat helper)
-- (included inline in ensure_queue_tables above; this is a no-op on fresh installs)

-- ============================================================
-- set_task_checkpoint_state
-- Same signature as absurd.set_task_checkpoint_state().
-- r_<q> is now keyed only on run_id of active runs; attempt ordering
-- is preserved via the attempt column.
-- ============================================================

create or replace function teguh.set_task_checkpoint_state(
  p_queue_name      text,
  p_task_id         uuid,
  p_step_name       text,
  p_state           jsonb,
  p_owner_run       uuid,
  p_extend_claim_by integer default null
)
  returns void
  language plpgsql
as $$
declare
  v_now              timestamptz := teguh.current_time();
  v_new_attempt      integer;
  v_task_state       text;
  v_existing_owner   uuid;
  v_existing_attempt integer;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  -- Verify run is still active and lock both rows to prevent concurrent cancellation.
  execute format(
    'select r.attempt, t.state
       from teguh.%I r
       join teguh.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_new_attempt, v_task_state
  using p_owner_run;

  if v_new_attempt is null then
    raise exception 'run "%" not found for checkpoint', p_owner_run;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'task has been cancelled';
  end if;

  -- Optionally extend the lease
  if p_extend_claim_by is not null and p_extend_claim_by > 0 then
    execute format(
      'update teguh.%I
          set claim_expires_at = $2 + make_interval(secs => $3),
              last_heartbeat   = $2
        where run_id = $1
          and claim_expires_at is not null',
      'r_' || p_queue_name
    )
    using p_owner_run, v_now, p_extend_claim_by;
  end if;

  -- Check if existing checkpoint belongs to an older or same attempt
  execute format(
    'select c.owner_run_id, r.attempt
       from teguh.%I c
       left join teguh.%I r on r.run_id = c.owner_run_id
      where c.task_id = $1
        and c.checkpoint_name = $2',
    'c_' || p_queue_name,
    'r_' || p_queue_name
  )
  into v_existing_owner, v_existing_attempt
  using p_task_id, p_step_name;

  if v_existing_owner is null or v_existing_attempt is null or v_new_attempt >= v_existing_attempt then
    execute format(
      'insert into teguh.%I
          (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state        = excluded.state,
                     status       = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at   = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, p_state, p_owner_run, v_now;
  end if;
end;
$$;

-- ============================================================
-- extend_claim (heartbeat)
-- Same signature as absurd.extend_claim().
-- ============================================================

create or replace function teguh.extend_claim(
  p_queue_name  text,
  p_run_id      uuid,
  p_extend_by   integer
)
  returns void
  language plpgsql
as $$
declare
  v_now             timestamptz := teguh.current_time();
  v_task_state      text;
  v_claim_expires_at timestamptz;
begin
  execute format(
    'select t.state, r.claim_expires_at
       from teguh.%I r
       join teguh.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_state, v_claim_expires_at
  using p_run_id;

  if v_task_state is null then
    raise exception 'run "%" not found for heartbeat', p_run_id;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'task has been cancelled';
  end if;

  execute format(
    'update teguh.%I
        set claim_expires_at = greatest(claim_expires_at, $2) + make_interval(secs => $3),
            last_heartbeat   = $2
      where run_id = $1',
    'r_' || p_queue_name
  )
  using p_run_id, v_now, p_extend_by;
end;
$$;

-- ============================================================
-- get_task_checkpoint_state / get_task_checkpoint_states
-- Same signatures as absurd.
-- ============================================================

create or replace function teguh.get_task_checkpoint_state(
  p_queue_name    text,
  p_task_id       uuid,
  p_checkpoint_name text
)
  returns jsonb
  language plpgsql stable
as $$
declare
  v_state jsonb;
begin
  execute format(
    'select state from teguh.%I
      where task_id = $1 and checkpoint_name = $2',
    'c_' || p_queue_name
  )
  into v_state
  using p_task_id, p_checkpoint_name;

  return v_state;
end;
$$;

create or replace function teguh.get_task_checkpoint_states(
  p_queue_name  text,
  p_task_id     uuid,
  p_run_id      uuid
)
  returns table (checkpoint_name text, state jsonb)
  language plpgsql stable
as $$
declare
  v_attempt integer;
begin
  -- Resolve the attempt number for this run
  execute format(
    'select attempt from teguh.%I where run_id = $1',
    'r_' || p_queue_name
  )
  into v_attempt
  using p_run_id;

  if v_attempt is null then
    raise exception 'run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  return query execute format(
    'select c.checkpoint_name, c.state
       from teguh.%I c
      where c.task_id = $1
        and c.status = ''committed''
        and (
          c.owner_run_id is null
          or not exists (
            select 1 from teguh.%I r2
             where r2.run_id = c.owner_run_id
               and r2.attempt > $2
          )
        )
      order by c.checkpoint_name',
    'c_' || p_queue_name,
    'r_' || p_queue_name
  )
  using p_task_id, v_attempt;
end;
$$;

-- ============================================================
-- await_event
-- Same signature as absurd.await_event().
-- On suspend: deletes the run row and records sleep on t_<q>.
-- Lock order preserved: e_<q> FOR SHARE first, then r_<q>+t_<q>.
-- ============================================================

create or replace function teguh.await_event(
  p_queue_name  text,
  p_task_id     uuid,
  p_run_id      uuid,
  p_step_name   text,
  p_event_name  text,
  p_timeout     integer default null
)
  returns table (
    should_suspend boolean,
    payload        jsonb
  )
  language plpgsql
as $$
declare
  v_now                timestamptz := teguh.current_time();
  v_timeout_at         timestamptz;
  v_available_at       timestamptz;
  v_checkpoint_payload jsonb;
  v_event_payload      jsonb;
  v_existing_payload   jsonb;
  v_resolved_payload   jsonb;
  v_wake_event         text;
  v_task_state         text;
  v_run_exists         boolean;
  v_check_id           uuid;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  if p_timeout is not null then
    if p_timeout < 0 then
      raise exception 'timeout must be non-negative';
    end if;
    v_timeout_at := v_now + (p_timeout::double precision * interval '1 second');
  end if;

  v_available_at := coalesce(v_timeout_at, 'infinity'::timestamptz);

  -- Fast path: checkpoint already committed (step was completed on a prior attempt)
  execute format(
    'select state from teguh.%I
      where task_id = $1 and checkpoint_name = $2',
    'c_' || p_queue_name
  )
  into v_checkpoint_payload
  using p_task_id, p_step_name;

  if v_checkpoint_payload is not null then
    return query select false, v_checkpoint_payload;
    return;
  end if;

  -- Ensure event row exists as sentinel (payload=NULL means not yet emitted)
  execute format(
    'insert into teguh.%I (event_name, payload, emitted_at)
     values ($1, null, ''epoch''::timestamptz)
     on conflict (event_name) do nothing',
    'e_' || p_queue_name
  ) using p_event_name;

  -- Lock order: event row FOR SHARE first to prevent races with emit_event
  execute format(
    'select 1 from teguh.%I where event_name = $1 for share',
    'e_' || p_queue_name
  ) using p_event_name;

  -- Lock run row FOR UPDATE first (consistent lock order with cancel_task)
  execute format(
    'select run_id from teguh.%I where run_id = $1 for update',
    'r_' || p_queue_name
  ) into v_check_id using p_run_id;
  v_run_exists := v_check_id is not null;

  -- Lock task row FOR UPDATE
  execute format(
    'select t.state, t.wake_event
       from teguh.%I t
      where t.task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_task_state, v_wake_event
  using p_task_id;

  if not v_run_exists then
    raise exception 'run "%" not found while awaiting event', p_run_id;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'task has been cancelled';
  end if;

  -- Read event payload (under the shared lock)
  execute format(
    'select payload from teguh.%I where event_name = $1',
    'e_' || p_queue_name
  )
  into v_event_payload
  using p_event_name;

  -- If event is already emitted, return immediately with payload
  if v_event_payload is not null then
    v_resolved_payload := v_event_payload;

    execute format(
      'insert into teguh.%I
          (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
       values ($1, $2, $3, ''committed'', $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state        = excluded.state,
                     status       = excluded.status,
                     owner_run_id = excluded.owner_run_id,
                     updated_at   = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, v_resolved_payload, p_run_id, v_now;

    return query select false, v_resolved_payload;
    return;
  end if;

  -- Detect timeout resume: wake_event matches this event but we have no payload
  if v_wake_event = p_event_name then
    -- Clear the wake_event since we are resuming from timeout
    execute format(
      'update teguh.%I set wake_event = null, available_at = null where task_id = $1',
      't_' || p_queue_name
    ) using p_task_id;
    return query select false, null::jsonb;
    return;
  end if;

  -- Suspend: register wait, delete run row, mark task sleeping

  execute format(
    'insert into teguh.%I (task_id, run_id, step_name, event_name, timeout_at, created_at)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (run_id, step_name)
     do update set event_name = excluded.event_name,
                   timeout_at = excluded.timeout_at,
                   created_at = excluded.created_at',
    'w_' || p_queue_name
  ) using p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now;

  execute format(
    'delete from teguh.%I where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id;

  execute format(
    'update teguh.%I
        set state        = ''sleeping'',
            available_at = $2,
            wake_event   = $3
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id, v_available_at, p_event_name;

  return query select true, null::jsonb;
end;
$$;

-- ============================================================
-- emit_event
-- Same signature as absurd.emit_event().
-- After waking sleepers: inserts into p_<q> and fires pg_notify.
-- ============================================================

create or replace function teguh.emit_event(
  p_queue_name  text,
  p_event_name  text,
  p_payload     jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_now          timestamptz := teguh.current_time();
  v_payload      jsonb       := coalesce(p_payload, 'null'::jsonb);
  v_emit_applied integer;
  v_woke_any     integer;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  -- First-write-wins: only update if payload is currently NULL (sentinel)
  execute format(
    'insert into teguh.%1$I as e (event_name, payload, emitted_at)
     values ($1, $2, $3)
     on conflict (event_name)
     do update set payload    = excluded.payload,
                   emitted_at = excluded.emitted_at
      where e.payload is null',
    'e_' || p_queue_name
  ) using p_event_name, v_payload, v_now;

  get diagnostics v_emit_applied = row_count;

  if v_emit_applied = 0 then
    return;  -- already emitted; nothing to do
  end if;

  -- Wake sleeping tasks waiting on this event.
  -- Also re-queue tasks whose event wait timed out before the event was emitted.
  execute format(
    'with expired_waits as (
         delete from teguh.%1$I w
          where w.event_name = $1
            and w.timeout_at is not null
            and w.timeout_at <= $2
          returning w.task_id, w.run_id, w.step_name
     ),
     expired_cp as (
         insert into teguh.%3$I
             (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
         select e.task_id, e.step_name, ''null''::jsonb, ''committed'', e.run_id, $2
           from expired_waits e
          on conflict (task_id, checkpoint_name) do nothing
     ),
     expired_task_upd as (
         update teguh.%4$I t
            set state        = ''pending'',
                available_at = null,
                wake_event   = null
          where t.task_id in (select task_id from expired_waits)
            and t.state = ''sleeping''
     ),
     expired_pending as (
         insert into teguh.%2$I (task_id, attempt, available_at)
         select t.task_id, t.attempts + 1, $2
           from expired_waits ew
           join teguh.%4$I t on t.task_id = ew.task_id
          where not exists (
            select 1 from teguh.%5$I r where r.task_id = t.task_id
          )
         on conflict (task_id) do nothing
     ),
     active_waiters as (
         select w.run_id, w.task_id, w.step_name
           from teguh.%1$I w
          where w.event_name = $1
            and (w.timeout_at is null or w.timeout_at > $2)
     ),
     ckpt_upd as (
         insert into teguh.%3$I
             (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
         select aw.task_id, aw.step_name, $3, ''committed'', aw.run_id, $2
           from active_waiters aw
         on conflict (task_id, checkpoint_name)
         do update set state        = excluded.state,
                       status       = excluded.status,
                       owner_run_id = excluded.owner_run_id,
                       updated_at   = excluded.updated_at
     ),
     task_upd as (
         update teguh.%4$I t
            set state        = ''pending'',
                available_at = null,
                wake_event   = null
          where t.task_id in (select task_id from active_waiters)
            and t.state = ''sleeping''
     ),
     new_pending as (
         insert into teguh.%2$I (task_id, attempt, available_at, wake_event, event_payload)
         select t.task_id, t.attempts + 1, $2, $1, $3
           from active_waiters aw
           join teguh.%4$I t on t.task_id = aw.task_id
         on conflict (task_id) do nothing
         returning task_id
     )
     delete from teguh.%1$I w
      where w.event_name = $1
        and w.run_id in (select run_id from active_waiters)',
    'w_' || p_queue_name,
    'p_' || p_queue_name,
    'c_' || p_queue_name,
    't_' || p_queue_name,
    'r_' || p_queue_name
  ) using p_event_name, v_now, v_payload;

  get diagnostics v_woke_any = row_count;

  -- Always notify when the event is newly emitted. Expired-wait re-queues
  -- (inserted by the expired_pending CTE above) do not appear in v_woke_any
  -- since that counts only the active-waiter DELETE. The notify is harmless
  -- if no tasks were waiting.
  perform pg_notify('teguh_' || p_queue_name, p_event_name);
end;
$$;

-- ============================================================
-- cancel_task
-- Same signature as absurd.cancel_task().
-- Deletes from r_<q> and p_<q> instead of updating state.
-- ============================================================

create or replace function teguh.cancel_task(
  p_queue_name  text,
  p_task_id     uuid
)
  returns void
  language plpgsql
as $$
declare
  v_now        timestamptz := teguh.current_time();
  v_task_state text;
begin
  -- Lock the task (consistent lock order: r_ then t_)
  execute format(
    'select run_id from teguh.%I
      where task_id = $1
      for update',
    'r_' || p_queue_name
  ) using p_task_id;

  execute format(
    'select state from teguh.%I where task_id = $1 for update',
    't_' || p_queue_name
  )
  into v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_task_state in ('completed', 'failed', 'cancelled') then
    return;
  end if;

  -- Mark task cancelled
  execute format(
    'update teguh.%I
        set state        = ''cancelled'',
            cancelled_at = coalesce(cancelled_at, $2)
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id, v_now;

  -- Remove active lease if any
  execute format(
    'delete from teguh.%I where task_id = $1',
    'r_' || p_queue_name
  ) using p_task_id;

  -- Remove from pending dispatch queue if any
  execute format(
    'delete from teguh.%I where task_id = $1',
    'p_' || p_queue_name
  ) using p_task_id;

  -- Remove wait registrations
  execute format(
    'delete from teguh.%I where task_id = $1',
    'w_' || p_queue_name
  ) using p_task_id;
end;
$$;

-- ============================================================
-- retry_task
-- Same signature as absurd.retry_task().
-- ============================================================

create or replace function teguh.retry_task(
  p_queue_name  text,
  p_task_id     uuid,
  p_options     jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id  uuid,
    attempt integer,
    created boolean
  )
  language plpgsql
as $$
declare
  v_task_state    text;
  v_attempts      integer;
  v_max_attempts  integer;
  v_spawn_new     boolean := coalesce((p_options->>'spawn_new')::boolean, false);
  v_new_max       integer := (p_options->>'max_attempts')::integer;
  v_now           timestamptz := teguh.current_time();
  v_new_task_id   uuid;
  v_run_id        uuid    := teguh.portable_uuidv7();
begin
  execute format(
    'select state, attempts, max_attempts
       from teguh.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_task_state, v_attempts, v_max_attempts
  using p_task_id;

  if v_task_state is null then
    raise exception 'task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_spawn_new then
    -- Spawn a new task with the same inputs
    return query execute format(
      'select (teguh.spawn_task($1, t.task_name, t.params,
         jsonb_strip_nulls(jsonb_build_object(
           ''headers'',         t.headers,
           ''retry_strategy'',  t.retry_strategy,
           ''max_attempts'',    coalesce($2, t.max_attempts),
           ''cancellation'',    t.cancellation
         ))
       )).* from teguh.%I t where task_id = $3',
      't_' || p_queue_name
    )
    using p_queue_name, v_new_max, p_task_id;
    return;
  end if;

  if v_task_state not in ('failed', 'cancelled') then
    raise exception 'task "%" cannot be retried in state "%"', p_task_id, v_task_state;
  end if;

  -- In-place retry: bump max_attempts if needed, enqueue into p_<q>
  declare
    v_target_max integer := coalesce(
      v_new_max,
      greatest(coalesce(v_max_attempts, v_attempts), v_attempts) + 1
    );
  begin
    if v_target_max <= v_attempts then
      raise exception 'max_attempts must be greater than current attempts (%)''', v_attempts;
    end if;

    execute format(
      'update teguh.%I
          set state            = ''pending'',
              max_attempts     = $2,
              cancelled_at     = null,
              available_at     = null,
              wake_event       = null,
              last_attempt_run = $3
        where task_id = $1',
      't_' || p_queue_name
    ) using p_task_id, v_target_max, v_run_id;

    execute format(
      'insert into teguh.%I (task_id, attempt, available_at)
       values ($1, $2, $3)
       on conflict (task_id) do nothing',
      'p_' || p_queue_name
    ) using p_task_id, v_attempts + 1, v_now;

    perform pg_notify('teguh_' || p_queue_name, p_task_id::text);

    return query select p_task_id, v_run_id, v_attempts + 1, true;
  end;
end;
$$;

-- ============================================================
-- get_task_result
-- Same signature as absurd.get_task_result().
-- ============================================================

create or replace function teguh.get_task_result(
  p_queue_name  text,
  p_task_id     uuid
)
  returns table (
    task_id           uuid,
    state             text,
    attempts          integer,
    completed_payload jsonb,
    cancelled_at      timestamptz
  )
  language plpgsql stable
as $$
begin
  return query execute format(
    'select task_id, state, attempts, completed_payload, cancelled_at
       from teguh.%I
      where task_id = $1',
    't_' || p_queue_name
  )
  using p_task_id;
end;
$$;

-- ============================================================
-- Ticker
-- Periodically re-queues sleeping tasks whose wake time has arrived
-- and fires pg_notify so LISTEN-based workers wake immediately.
-- Called by pg_cron every second; also safe to call manually.
-- ============================================================

create or replace function teguh.ticker(p_queue_name text default null)
  returns integer
  language plpgsql
as $$
declare
  v_queue     record;
  v_count     integer := 0;
  v_now       timestamptz := clock_timestamp();
  v_inserted  integer;
  v_rows      integer;
begin
  for v_queue in
    select queue_name from teguh.queues
     where p_queue_name is null or queue_name = p_queue_name
     order by queue_name
  loop
    -- Re-queue sleeping and delayed-pending tasks whose available_at has arrived.
    execute format(
      'insert into teguh.%I (task_id, attempt, available_at, wake_event)
       select t.task_id, t.attempts + 1, $1, t.wake_event
         from teguh.%I t
        where t.state in (''sleeping'', ''pending'')
          and t.available_at is not null
          and t.available_at <= $1
          and not exists (
            select 1 from teguh.%I p where p.task_id = t.task_id
          )
          and not exists (
            select 1 from teguh.%I r where r.task_id = t.task_id
          )
       on conflict (task_id) do nothing',
      'p_' || v_queue.queue_name,
      't_' || v_queue.queue_name,
      'p_' || v_queue.queue_name,
      'r_' || v_queue.queue_name
    )
    using v_now;

    get diagnostics v_inserted = row_count;

    -- Also expire timed-out event waits (move back to pending).
    -- Write a committed null checkpoint for each expired wait so that
    -- await_event's fast-path detects the timeout on re-entry and returns
    -- nil payload immediately, preserving the exactly-once guarantee.
    execute format(
      'with expired as (
           delete from teguh.%I w
            where w.timeout_at is not null
              and w.timeout_at <= $1
            returning w.task_id, w.run_id, w.step_name
       ),
       task_upd as (
           update teguh.%I t
              set state        = ''pending'',
                  available_at = null,
                  wake_event   = null
            where t.task_id in (select task_id from expired)
              and t.state = ''sleeping''
       ),
       cp_write as (
           insert into teguh.%I (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
           select e.task_id, e.step_name, ''null''::jsonb, ''committed'', e.run_id, $1
             from expired e
            on conflict (task_id, checkpoint_name) do nothing
       )
       insert into teguh.%I (task_id, attempt, available_at)
       select t.task_id, t.attempts + 1, $1
         from expired e
         join teguh.%I t on t.task_id = e.task_id
        where not exists (
          select 1 from teguh.%I r where r.task_id = t.task_id
        )
        on conflict (task_id) do nothing',
      'w_' || v_queue.queue_name,
      't_' || v_queue.queue_name,
      'c_' || v_queue.queue_name,
      'p_' || v_queue.queue_name,
      't_' || v_queue.queue_name,
      'r_' || v_queue.queue_name
    )
    using v_now;

    get diagnostics v_rows = row_count;
    v_inserted := v_inserted + v_rows;

    if v_inserted > 0 then
      perform pg_notify('teguh_' || v_queue.queue_name, 'tick');
      v_count := v_count + v_inserted;
    end if;
  end loop;

  return v_count;
end;
$$;

-- ============================================================
-- Cleanup
-- ============================================================

create or replace function teguh.cleanup_tasks(
  p_queue_name    text,
  p_ttl_seconds   integer default null,
  p_limit         integer default null
)
  returns integer
  language plpgsql
as $$
declare
  v_ttl_seconds integer;
  v_limit       integer;
  v_deleted     integer;
  v_cutoff      timestamptz;
  v_q           record;
begin
  select cleanup_ttl, cleanup_limit into v_q
    from teguh.queues
   where queue_name = p_queue_name;

  v_ttl_seconds := coalesce(p_ttl_seconds,
    extract(epoch from v_q.cleanup_ttl)::integer, 2592000);
  v_limit := coalesce(p_limit, v_q.cleanup_limit, 1000);
  v_cutoff := clock_timestamp() - (v_ttl_seconds * interval '1 second');

  execute format(
    'with to_delete as (
         select task_id from teguh.%1$I
          where state in (''completed'',''failed'',''cancelled'')
            and enqueue_at < $1
          limit $2
     ),
     del_ckpts as (
         delete from teguh.%2$I
          where task_id in (select task_id from to_delete)
     )
     delete from teguh.%1$I
      where task_id in (select task_id from to_delete)',
    't_' || p_queue_name,
    'c_' || p_queue_name
  )
  using v_cutoff, v_limit;

  get diagnostics v_deleted = row_count;

  -- Also clean up orphaned events
  execute format(
    'delete from teguh.%I
      where emitted_at < $1',
    'e_' || p_queue_name
  ) using v_cutoff;

  return v_deleted;
end;
$$;

create or replace function teguh.cleanup_all_queues(p_queue_name text default null)
  returns table (queue_name text, tasks_deleted integer)
  language plpgsql
as $$
declare
  v_queue record;
  v_deleted integer;
begin
  for v_queue in
    select q.queue_name from teguh.queues q
     where p_queue_name is null or q.queue_name = p_queue_name
     order by q.queue_name
  loop
    v_deleted := teguh.cleanup_tasks(v_queue.queue_name);
    return query select v_queue.queue_name, v_deleted;
  end loop;
end;
$$;

-- ============================================================
-- Lifecycle (pg_cron-based, same pattern as pgque.start/stop)
-- ============================================================

create or replace function teguh.start()
  returns void
  language plpgsql
as $$
declare
  v_ticker_id bigint;
  v_maint_id  bigint;
begin
  if not exists (
    select 1 from pg_extension where extname = 'pg_cron'
  ) then
    raise notice 'pg_cron is not installed; call teguh.ticker() manually or from your own scheduler';
    return;
  end if;

  -- Ticker: re-queue sleeping tasks every second
  select cron.schedule('teguh_ticker', '* * * * *',
    $cmd$select teguh.ticker()$cmd$)
  into v_ticker_id;

  -- pg_cron minimum granularity is 1 minute; for sub-minute ticking,
  -- schedule via pg_cron with a looping approach or call ticker() from
  -- your application. For now, 1-minute tick is the safe default.

  -- Cleanup: daily
  select cron.schedule('teguh_maint', '0 3 * * *',
    $cmd$select teguh.cleanup_all_queues()$cmd$)
  into v_maint_id;

  update teguh.config
     set ticker_job_id = v_ticker_id,
         maint_job_id  = v_maint_id
   where singleton;

  raise notice 'Teguh started. Ticker job: %, Maint job: %', v_ticker_id, v_maint_id;
end;
$$;

create or replace function teguh.stop()
  returns void
  language plpgsql
as $$
declare
  v_cfg record;
begin
  select ticker_job_id, maint_job_id into v_cfg from teguh.config where singleton;

  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    return;
  end if;

  if v_cfg.ticker_job_id is not null then
    perform cron.unschedule(v_cfg.ticker_job_id);
  end if;

  if v_cfg.maint_job_id is not null then
    perform cron.unschedule(v_cfg.maint_job_id);
  end if;

  update teguh.config
     set ticker_job_id = null,
         maint_job_id  = null
   where singleton;
end;
$$;

create or replace function teguh.version()
  returns text
  language sql immutable
as $$
  select 'teguh-0.1.0-dev'::text;
$$;

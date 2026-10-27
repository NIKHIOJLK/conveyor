-- Conveyor schema. PostgreSQL is both the job store and the coordination layer:
-- row locks (FOR UPDATE SKIP LOCKED) make sure two workers never claim the same job,
-- and LISTEN/NOTIFY wakes idle workers the moment a job arrives.

CREATE TYPE job_state AS ENUM ('queued', 'running', 'succeeded', 'dead', 'cancelled');

-- Per-queue settings. Rows are created automatically when a job is enqueued.
CREATE TABLE queues (
  name        text PRIMARY KEY,
  paused      boolean NOT NULL DEFAULT false,
  rate_limit  double precision CHECK (rate_limit > 0),   -- jobs/second across ALL workers; NULL = unlimited
  burst       integer NOT NULL DEFAULT 1 CHECK (burst >= 1),
  tokens      double precision NOT NULL DEFAULT 0,       -- token bucket state
  refilled_at timestamptz NOT NULL DEFAULT now(),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id              bigserial PRIMARY KEY,
  queue           text NOT NULL DEFAULT 'default',
  name            text NOT NULL,                       -- which handler runs it, e.g. 'send-email'
  payload         jsonb NOT NULL DEFAULT '{}',
  state           job_state NOT NULL DEFAULT 'queued',
  priority        integer NOT NULL DEFAULT 0,          -- higher runs first
  run_at          timestamptz NOT NULL DEFAULT now(),  -- not before this time (scheduling + retry backoff)
  attempts        integer NOT NULL DEFAULT 0,
  max_attempts    integer NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  backoff_base_ms integer NOT NULL DEFAULT 1000 CHECK (backoff_base_ms >= 0),
  timeout_ms      integer NOT NULL DEFAULT 30000 CHECK (timeout_ms > 0),
  idempotency_key text,
  locked_by       text,                                -- worker that holds the lease
  locked_until    timestamptz,                         -- lease expiry; renewed by heartbeats
  last_error      text,
  result          jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  finished_at     timestamptz,
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- The claim query walks this index in order and stops after LIMIT rows.
-- Partial index: only queued jobs, so it stays small however many finished jobs pile up.
CREATE INDEX jobs_ready_idx ON jobs (queue, priority DESC, run_at, id) WHERE state = 'queued';
-- The reaper looks for running jobs whose lease ran out.
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE state = 'running';
-- Retention cleanup and the dashboard's job list.
CREATE INDEX jobs_finished_idx ON jobs (finished_at) WHERE state IN ('succeeded', 'dead', 'cancelled');
CREATE INDEX jobs_state_idx ON jobs (state, id DESC);
-- Enqueueing the same idempotency key twice in a queue returns the existing job.
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Timeline of everything that happened to a job (attempts, errors, retries).
CREATE TABLE job_events (
  id          bigserial PRIMARY KEY,
  job_id      bigint NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  type        text NOT NULL,  -- enqueued | started | succeeded | failed | dead | lease_expired | released | retried | cancelled
  attempt     integer,
  worker      text,
  message     text,
  duration_ms integer,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX job_events_job_idx ON job_events (job_id, id);
CREATE INDEX job_events_time_idx ON job_events (created_at);

-- Live worker registry (for the dashboard). Heartbeats update last_seen.
CREATE TABLE workers (
  id          text PRIMARY KEY,
  queues      text[] NOT NULL,
  concurrency integer NOT NULL,
  hostname    text,
  pid         integer,
  started_at  timestamptz NOT NULL DEFAULT now(),
  last_seen   timestamptz NOT NULL DEFAULT now()
);

-- After any INSERT into jobs: log 'enqueued', make sure the queue row exists,
-- and NOTIFY workers. Because this is a trigger, any service in any language can
-- enqueue with a plain INSERT and workers still wake up instantly.
CREATE FUNCTION conveyor_after_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO job_events (job_id, type, message)
  SELECT id, 'enqueued',
         CASE WHEN run_at > now() THEN 'scheduled for ' || to_char(run_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || ' UTC' END
  FROM new_jobs;

  INSERT INTO queues (name) SELECT DISTINCT queue FROM new_jobs ON CONFLICT DO NOTHING;

  PERFORM pg_notify('conveyor_jobs', q.queue)
  FROM (SELECT DISTINCT queue FROM new_jobs WHERE run_at <= now()) q;
  RETURN NULL;
END $$;

CREATE TRIGGER jobs_after_insert
  AFTER INSERT ON jobs
  REFERENCING NEW TABLE AS new_jobs
  FOR EACH STATEMENT EXECUTE FUNCTION conveyor_after_insert();

-- Global rate limiting with a token bucket stored in the queue row.
-- Returns how many jobs the caller may claim right now (0..wanted).
-- The row lock (FOR UPDATE) serialises concurrent callers, so the limit holds
-- across any number of worker processes.
CREATE FUNCTION conveyor_reserve(p_queue text, p_wanted integer) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  q queues%ROWTYPE;
  available double precision;
  granted integer;
BEGIN
  IF p_wanted <= 0 THEN RETURN 0; END IF;

  -- Cheap unlocked check first: unlimited queues never take the lock.
  SELECT * INTO q FROM queues WHERE name = p_queue;
  IF NOT FOUND THEN RETURN p_wanted; END IF;
  IF q.paused THEN RETURN 0; END IF;
  IF q.rate_limit IS NULL THEN RETURN p_wanted; END IF;

  SELECT * INTO q FROM queues WHERE name = p_queue FOR UPDATE;
  available := LEAST(q.burst, q.tokens + q.rate_limit * EXTRACT(EPOCH FROM clock_timestamp() - q.refilled_at));
  granted := GREATEST(0, LEAST(p_wanted, FLOOR(available)::integer));
  UPDATE queues SET tokens = available - granted, refilled_at = clock_timestamp() WHERE name = p_queue;
  RETURN granted;
END $$;

-- Give back tokens that were reserved but not used (fewer jobs were ready than reserved).
CREATE FUNCTION conveyor_refund(p_queue text, p_n integer) RETURNS void LANGUAGE sql AS $$
  UPDATE queues SET tokens = LEAST(burst, tokens + p_n)
  WHERE name = p_queue AND rate_limit IS NOT NULL AND p_n > 0;
$$;

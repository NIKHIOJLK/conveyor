/**
 * Queue: every SQL statement Conveyor runs lives here.
 *
 * Producers use enqueue(); workers use claim/complete/fail/heartbeat; the API and
 * dashboard use the inspection and admin methods. Each state change also writes a
 * row to job_events in the same statement (a data-modifying CTE), so the job and
 * its history can never disagree.
 */
import type { Pool } from '../db/pool.js';
import type { EnqueueOptions, EnqueueResult, Job, JobEvent, JobState } from './types.js';

export const DEFAULTS = {
  queue: 'default',
  priority: 0,
  maxAttempts: 5,
  backoffBaseMs: 1000,
  timeoutMs: 30_000,
};

export const NOTIFY_CHANNEL = 'conveyor_jobs';

/** States the dashboard shows. "queued" is split by whether the job is due yet. */
export type DisplayState = 'waiting' | 'scheduled' | 'retrying' | Exclude<JobState, 'queued'>;
export const DISPLAY_STATES: DisplayState[] = [
  'waiting',
  'scheduled',
  'retrying',
  'running',
  'succeeded',
  'dead',
  'cancelled',
];

export interface ListFilter {
  state?: DisplayState | JobState;
  queue?: string;
  name?: string;
  /** Return jobs with id < before (pagination cursor). */
  before?: number;
  limit?: number;
}

export interface FailUpdate {
  /** 'queued' = retry later, 'dead' = give up. */
  state: 'queued' | 'dead';
  delayMs: number;
  error: string;
  eventType: 'failed' | 'dead';
  message: string;
  durationMs: number;
}

const STATE_SQL: Record<string, string> = {
  waiting: "state = 'queued' AND run_at <= now()",
  scheduled: "state = 'queued' AND run_at > now() AND attempts = 0",
  retrying: "state = 'queued' AND run_at > now() AND attempts > 0",
  queued: "state = 'queued'",
  running: "state = 'running'",
  succeeded: "state = 'succeeded'",
  dead: "state = 'dead'",
  cancelled: "state = 'cancelled'",
};

export class Queue {
  constructor(readonly pool: Pool) {}

  // ------------------------------------------------------------------ producing

  async enqueue<P>(opts: EnqueueOptions<P>): Promise<EnqueueResult<P>> {
    const queue = opts.queue ?? DEFAULTS.queue;
    const { rows } = await this.pool.query<Job<P>>(
      `INSERT INTO jobs (queue, name, payload, priority, run_at, max_attempts, backoff_base_ms, timeout_ms, idempotency_key)
       VALUES ($1, $2, $3, $4,
               COALESCE($5::timestamptz, now() + make_interval(secs => COALESCE($6::double precision, 0) / 1000)),
               $7, $8, $9, $10)
       ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
       RETURNING *`,
      [
        queue,
        opts.name,
        JSON.stringify(opts.payload ?? {}),
        opts.priority ?? DEFAULTS.priority,
        opts.runAt ?? null,
        opts.delayMs ?? null,
        opts.maxAttempts ?? DEFAULTS.maxAttempts,
        opts.backoffBaseMs ?? DEFAULTS.backoffBaseMs,
        opts.timeoutMs ?? DEFAULTS.timeoutMs,
        opts.idempotencyKey ?? null,
      ],
    );
    if (rows[0]) return { job: rows[0], deduplicated: false };

    // Conflict: a job with this idempotency key already exists. Return it.
    const existing = await this.pool.query<Job<P>>(
      'SELECT * FROM jobs WHERE queue = $1 AND idempotency_key = $2',
      [queue, opts.idempotencyKey],
    );
    return { job: existing.rows[0]!, deduplicated: true };
  }

  /** Insert many jobs in one statement (one round trip). Returns the jobs actually created. */
  async enqueueMany(list: EnqueueOptions[]): Promise<Job[]> {
    if (list.length === 0) return [];
    const records = list.map((o) => ({
      queue: o.queue ?? DEFAULTS.queue,
      name: o.name,
      payload: o.payload ?? {},
      priority: o.priority ?? DEFAULTS.priority,
      run_at: o.runAt?.toISOString() ?? null,
      delay_ms: o.delayMs ?? null,
      max_attempts: o.maxAttempts ?? DEFAULTS.maxAttempts,
      backoff_base_ms: o.backoffBaseMs ?? DEFAULTS.backoffBaseMs,
      timeout_ms: o.timeoutMs ?? DEFAULTS.timeoutMs,
      idempotency_key: o.idempotencyKey ?? null,
    }));
    const { rows } = await this.pool.query<Job>(
      `INSERT INTO jobs (queue, name, payload, priority, run_at, max_attempts, backoff_base_ms, timeout_ms, idempotency_key)
       SELECT queue, name, payload, priority,
              COALESCE(run_at, now() + make_interval(secs => COALESCE(delay_ms, 0)::double precision / 1000)),
              max_attempts, backoff_base_ms, timeout_ms, idempotency_key
       FROM jsonb_to_recordset($1::jsonb) AS x(
         queue text, name text, payload jsonb, priority int, run_at timestamptz, delay_ms int,
         max_attempts int, backoff_base_ms int, timeout_ms int, idempotency_key text)
       ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
       RETURNING *`,
      [JSON.stringify(records)],
    );
    return rows;
  }

  // ------------------------------------------------------------------ worker side

  /** How many jobs this worker may take from `queue` right now (pause + global rate limit). */
  async reserve(queue: string, wanted: number): Promise<number> {
    const { rows } = await this.pool.query<{ n: number }>('SELECT conveyor_reserve($1, $2) AS n', [
      queue,
      wanted,
    ]);
    return rows[0]?.n ?? 0;
  }

  async refund(queue: string, n: number): Promise<void> {
    if (n > 0) await this.pool.query('SELECT conveyor_refund($1, $2)', [queue, n]);
  }

  /**
   * Atomically take up to `limit` due jobs and lease them to `workerId`.
   *
   * FOR UPDATE SKIP LOCKED is the key: rows another worker is claiming at this
   * very moment are skipped instead of waited on, so workers never block each
   * other and never get the same job.
   */
  async claim(queue: string, limit: number, workerId: string, leaseMs: number): Promise<Job[]> {
    const { rows } = await this.pool.query<Job>(
      `WITH next AS (
         SELECT id FROM jobs
         WHERE state = 'queued' AND queue = $1 AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       ), claimed AS (
         UPDATE jobs j
         SET state = 'running', attempts = j.attempts + 1, locked_by = $3,
             locked_until = now() + make_interval(secs => $4::double precision / 1000),
             started_at = now(), updated_at = now()
         FROM next WHERE j.id = next.id
         RETURNING j.*
       ), ev AS (
         INSERT INTO job_events (job_id, type, attempt, worker)
         SELECT id, 'started', attempts, $3 FROM claimed
       )
       SELECT * FROM claimed ORDER BY priority DESC, run_at, id`,
      [queue, limit, workerId, leaseMs],
    );
    return rows;
  }

  /**
   * Mark a job done. The `locked_by = workerId` condition is a fence: if this
   * worker's lease expired and another worker took the job over, this update
   * matches nothing and returns false instead of overwriting the new owner's work.
   */
  async complete(id: number, workerId: string, result: unknown, durationMs: number): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `WITH done AS (
         UPDATE jobs SET state = 'succeeded', result = $3, last_error = NULL, finished_at = now(),
                         locked_by = NULL, locked_until = NULL, updated_at = now()
         WHERE id = $1 AND locked_by = $2 AND state = 'running'
         RETURNING id, attempts
       ), ev AS (
         INSERT INTO job_events (job_id, type, attempt, worker, duration_ms)
         SELECT id, 'succeeded', attempts, $2, $4 FROM done
       )
       SELECT id FROM done`,
      [id, workerId, JSON.stringify(result ?? null), Math.round(durationMs)],
    );
    return rowCount === 1;
  }

  /** Record a failed attempt: schedule a retry or move to dead. Fenced like complete(). */
  async fail(id: number, workerId: string, f: FailUpdate): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `WITH f AS (
         UPDATE jobs SET state = $3::job_state,
                         run_at = CASE WHEN $3 = 'queued' THEN now() + make_interval(secs => $4::double precision / 1000) ELSE run_at END,
                         finished_at = CASE WHEN $3 = 'dead' THEN now() END,
                         last_error = $5, locked_by = NULL, locked_until = NULL, updated_at = now()
         WHERE id = $1 AND locked_by = $2 AND state = 'running'
         RETURNING id, attempts
       ), ev AS (
         INSERT INTO job_events (job_id, type, attempt, worker, message, duration_ms)
         SELECT id, $6, attempts, $2, $7, $8 FROM f
       )
       SELECT id FROM f`,
      [id, workerId, f.state, f.delayMs, f.error, f.eventType, f.message, Math.round(f.durationMs)],
    );
    return rowCount === 1;
  }

  /** Extend the lease on jobs this worker still owns. Returns the ids it still owns. */
  async heartbeat(workerId: string, ids: number[], leaseMs: number): Promise<number[]> {
    const { rows } = await this.pool.query<{ id: number }>(
      `WITH w AS (
         UPDATE workers SET last_seen = now() WHERE id = $1
       )
       UPDATE jobs SET locked_until = now() + make_interval(secs => $3::double precision / 1000)
       WHERE id = ANY($2::bigint[]) AND locked_by = $1 AND state = 'running'
       RETURNING id`,
      [workerId, ids, leaseMs],
    );
    return rows.map((r) => r.id);
  }

  /**
   * Hand unfinished jobs back to the queue (graceful shutdown ran out of time).
   * The attempt is not counted against the job: it wasn't the job's fault.
   */
  async release(workerId: string, ids: number[]): Promise<number[]> {
    if (ids.length === 0) return [];
    const { rows } = await this.pool.query<{ id: number; queue: string }>(
      `WITH r AS (
         UPDATE jobs SET state = 'queued', attempts = GREATEST(attempts - 1, 0), run_at = now(),
                         locked_by = NULL, locked_until = NULL, updated_at = now()
         WHERE id = ANY($2::bigint[]) AND locked_by = $1 AND state = 'running'
         RETURNING id, queue, attempts
       ), ev AS (
         INSERT INTO job_events (job_id, type, attempt, worker, message)
         SELECT id, 'released', attempts, $1, 'Worker shut down before the job finished; returned to the queue' FROM r
       )
       SELECT id, queue FROM r`,
      [workerId, ids],
    );
    await this.notify(rows.map((r) => r.queue));
    return rows.map((r) => r.id);
  }

  async registerWorker(w: {
    id: string;
    queues: string[];
    concurrency: number;
    hostname: string;
    pid: number;
  }) {
    await this.pool.query(
      `INSERT INTO workers (id, queues, concurrency, hostname, pid) VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (id) DO UPDATE SET last_seen = now()`,
      [w.id, w.queues, w.concurrency, w.hostname, w.pid],
    );
  }

  async removeWorker(id: string): Promise<void> {
    await this.pool.query('DELETE FROM workers WHERE id = $1', [id]);
  }

  /** Wake workers listening on these queues. */
  async notify(queues: string[]): Promise<void> {
    const unique = [...new Set(queues)];
    if (unique.length) {
      await this.pool.query('SELECT pg_notify($1, q) FROM unnest($2::text[]) q', [NOTIFY_CHANNEL, unique]);
    }
  }

  // ------------------------------------------------------------------ maintenance

  /**
   * Crash recovery. A running job whose lease expired belongs to a worker that
   * crashed, froze or lost its network. Put it back in the queue (or dead if it
   * has used all its attempts, so one "poison" job can't crash workers forever).
   */
  async reapExpiredLeases(): Promise<Array<{ id: number; queue: string; state: JobState }>> {
    const { rows } = await this.pool.query<{ id: number; queue: string; state: JobState }>(
      `WITH expired AS (
         SELECT id FROM jobs WHERE state = 'running' AND locked_until < now()
         LIMIT 1000 FOR UPDATE SKIP LOCKED
       ), r AS (
         UPDATE jobs j
         SET state = CASE WHEN j.attempts >= j.max_attempts THEN 'dead'::job_state ELSE 'queued'::job_state END,
             run_at = now(),
             finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
             last_error = 'Lease expired: worker ' || COALESCE(j.locked_by, '?') || ' stopped responding',
             locked_by = NULL, locked_until = NULL, updated_at = now()
         FROM expired WHERE j.id = expired.id
         RETURNING j.id, j.queue, j.state, j.attempts, j.last_error
       ), ev AS (
         INSERT INTO job_events (job_id, type, attempt, message)
         SELECT id, CASE WHEN state = 'dead' THEN 'dead' ELSE 'lease_expired' END, attempts, last_error FROM r
       )
       SELECT id, queue, state FROM r`,
    );
    await this.notify(rows.filter((r) => r.state === 'queued').map((r) => r.queue));
    return rows;
  }

  /** Delete finished jobs older than the retention period (and their events), in batches. */
  async purgeFinished(olderThanMs: number, batch = 5000): Promise<number> {
    let total = 0;
    for (;;) {
      const { rowCount } = await this.pool.query(
        `DELETE FROM jobs WHERE id IN (
           SELECT id FROM jobs
           WHERE state IN ('succeeded', 'cancelled', 'dead')
             AND finished_at < now() - make_interval(secs => $1::double precision / 1000)
           LIMIT $2)`,
        [olderThanMs, batch],
      );
      total += rowCount ?? 0;
      if ((rowCount ?? 0) < batch) return total;
    }
  }

  /** Forget workers that haven't sent a heartbeat for a while. */
  async pruneWorkers(olderThanMs = 10 * 60_000): Promise<void> {
    await this.pool.query(
      'DELETE FROM workers WHERE last_seen < now() - make_interval(secs => $1::double precision / 1000)',
      [olderThanMs],
    );
  }

  // ------------------------------------------------------------------ inspection

  async get(id: number): Promise<Job | null> {
    const { rows } = await this.pool.query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
    return rows[0] ?? null;
  }

  async events(jobId: number): Promise<JobEvent[]> {
    const { rows } = await this.pool.query<JobEvent>(
      'SELECT * FROM job_events WHERE job_id = $1 ORDER BY id',
      [jobId],
    );
    return rows;
  }

  async list(filter: ListFilter = {}): Promise<Job[]> {
    const where: string[] = [];
    const params: unknown[] = [];
    if (filter.state) {
      const sql = STATE_SQL[filter.state];
      if (!sql) throw new Error(`Unknown state '${filter.state}'`);
      where.push(sql);
    }
    if (filter.queue) {
      params.push(filter.queue);
      where.push(`queue = $${params.length}`);
    }
    if (filter.name) {
      params.push(filter.name);
      where.push(`name = $${params.length}`);
    }
    if (filter.before) {
      params.push(filter.before);
      where.push(`id < $${params.length}`);
    }
    params.push(Math.min(Math.max(filter.limit ?? 50, 1), 200));
    const { rows } = await this.pool.query<Job>(
      `SELECT * FROM jobs ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY id DESC LIMIT $${params.length}`,
      params,
    );
    return rows;
  }

  /** Counts per queue and display state, queue settings, and lag (age of the oldest due job). */
  async queueStats() {
    const { rows } = await this.pool.query(
      `SELECT q.name AS queue, q.paused, q.rate_limit, q.burst,
              COALESCE(s.waiting, 0)::int AS waiting, COALESCE(s.scheduled, 0)::int AS scheduled,
              COALESCE(s.retrying, 0)::int AS retrying, COALESCE(s.running, 0)::int AS running,
              COALESCE(s.succeeded, 0)::int AS succeeded, COALESCE(s.dead, 0)::int AS dead,
              COALESCE(s.cancelled, 0)::int AS cancelled, COALESCE(s.lag_seconds, 0)::float AS lag_seconds
       FROM queues q LEFT JOIN (
         SELECT queue,
           count(*) FILTER (WHERE state = 'queued' AND run_at <= now()) AS waiting,
           count(*) FILTER (WHERE state = 'queued' AND run_at > now() AND attempts = 0) AS scheduled,
           count(*) FILTER (WHERE state = 'queued' AND run_at > now() AND attempts > 0) AS retrying,
           count(*) FILTER (WHERE state = 'running') AS running,
           count(*) FILTER (WHERE state = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE state = 'dead') AS dead,
           count(*) FILTER (WHERE state = 'cancelled') AS cancelled,
           EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE state = 'queued' AND run_at <= now())) AS lag_seconds
         FROM jobs GROUP BY queue
       ) s ON s.queue = q.name
       ORDER BY q.name`,
    );
    return rows as Array<{
      queue: string;
      paused: boolean;
      rate_limit: number | null;
      burst: number;
      waiting: number;
      scheduled: number;
      retrying: number;
      running: number;
      succeeded: number;
      dead: number;
      cancelled: number;
      lag_seconds: number;
    }>;
  }

  /** Completions and failures per second over the last `seconds` seconds. */
  async throughput(seconds = 60) {
    const { rows } = await this.pool.query<{ ts: number; succeeded: number; failed: number; dead: number }>(
      `SELECT floor(EXTRACT(EPOCH FROM created_at))::bigint AS ts,
              count(*) FILTER (WHERE type = 'succeeded')::int AS succeeded,
              count(*) FILTER (WHERE type IN ('failed', 'lease_expired'))::int AS failed,
              count(*) FILTER (WHERE type = 'dead')::int AS dead
       FROM job_events
       WHERE created_at > now() - make_interval(secs => $1) AND type IN ('succeeded', 'failed', 'lease_expired', 'dead')
       GROUP BY 1 ORDER BY 1`,
      [seconds],
    );
    const now = Math.floor(Date.now() / 1000);
    const byTs = new Map(rows.map((r) => [Number(r.ts), r]));
    return Array.from({ length: seconds }, (_, i) => {
      const ts = now - seconds + 1 + i;
      const r = byTs.get(ts);
      return { ts, succeeded: r?.succeeded ?? 0, failed: r?.failed ?? 0, dead: r?.dead ?? 0 };
    });
  }

  async workers() {
    const { rows } = await this.pool.query(
      `SELECT w.id, w.queues, w.concurrency, w.hostname, w.pid, w.started_at, w.last_seen,
              (w.last_seen > now() - interval '15 seconds') AS alive,
              (SELECT count(*) FROM jobs j WHERE j.state = 'running' AND j.locked_by = w.id)::int AS running
       FROM workers w ORDER BY w.started_at`,
    );
    return rows as Array<{
      id: string;
      queues: string[];
      concurrency: number;
      hostname: string;
      pid: number;
      started_at: Date;
      last_seen: Date;
      alive: boolean;
      running: number;
    }>;
  }

  // ------------------------------------------------------------------ admin

  /** Re-run a dead or cancelled job from scratch. */
  async retry(id: number): Promise<Job | null> {
    const { rows } = await this.pool.query<Job>(
      `WITH r AS (
         UPDATE jobs SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
         WHERE id = $1 AND state IN ('dead', 'cancelled')
         RETURNING *
       ), ev AS (
         INSERT INTO job_events (job_id, type, message) SELECT id, 'retried', 'Manually retried' FROM r
       )
       SELECT * FROM r`,
      [id],
    );
    if (rows[0]) await this.notify([rows[0].queue]);
    return rows[0] ?? null;
  }

  /** Retry every dead job in a queue. Returns how many were requeued. */
  async retryAllDead(queue: string): Promise<number> {
    const { rowCount } = await this.pool.query(
      `WITH r AS (
         UPDATE jobs SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
         WHERE queue = $1 AND state = 'dead'
         RETURNING id
       ), ev AS (
         INSERT INTO job_events (job_id, type, message) SELECT id, 'retried', 'Retried in bulk from the dead-letter queue' FROM r
       )
       SELECT id FROM r`,
      [queue],
    );
    await this.notify([queue]);
    return rowCount ?? 0;
  }

  /** Cancel a job that hasn't started. Running jobs can't be cancelled (returns null). */
  async cancel(id: number): Promise<Job | null> {
    const { rows } = await this.pool.query<Job>(
      `WITH c AS (
         UPDATE jobs SET state = 'cancelled', finished_at = now(), updated_at = now()
         WHERE id = $1 AND state = 'queued'
         RETURNING *
       ), ev AS (
         INSERT INTO job_events (job_id, type, message) SELECT id, 'cancelled', 'Cancelled before it ran' FROM c
       )
       SELECT * FROM c`,
      [id],
    );
    return rows[0] ?? null;
  }

  async setPaused(queue: string, paused: boolean): Promise<void> {
    await this.pool.query(
      `INSERT INTO queues (name, paused) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET paused = $2`,
      [queue, paused],
    );
    if (!paused) await this.notify([queue]);
  }

  /** Limit a queue to `rate` jobs/second across all workers (null = unlimited). */
  async setRateLimit(queue: string, rate: number | null, burst = 1): Promise<void> {
    await this.pool.query(
      `INSERT INTO queues (name, rate_limit, burst, tokens, refilled_at)
       VALUES ($1, $2::double precision, $3::int, $3::int, now())
       ON CONFLICT (name) DO UPDATE
       SET rate_limit = EXCLUDED.rate_limit, burst = EXCLUDED.burst, tokens = EXCLUDED.tokens, refilled_at = now()`,
      [queue, rate, Math.max(1, Math.floor(burst))],
    );
  }
}

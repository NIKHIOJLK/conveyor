import { afterAll, beforeAll, beforeEach } from 'vitest';
import { config } from '../src/config.js';
import { Queue } from '../src/core/queue.js';
import { Worker, type WorkerOptions } from '../src/core/worker.js';
import { migrate } from '../src/db/migrate.js';
import { createPool, type Pool } from '../src/db/pool.js';

/**
 * Each test file gets a pool on the TEST database (never the dev one), the
 * schema is migrated once, and every table is emptied before each test.
 */
export function useTestDb() {
  const ctx = {} as { pool: Pool; queue: Queue; workers: Worker[] };

  beforeAll(async () => {
    ctx.pool = createPool(config.testDatabaseUrl, 30);
    await migrate(ctx.pool);
    ctx.queue = new Queue(ctx.pool);
    ctx.workers = [];
  });

  beforeEach(async () => {
    await Promise.all(ctx.workers.map((w) => w.stop()));
    ctx.workers = [];
    await ctx.pool.query('TRUNCATE jobs, job_events, queues, workers RESTART IDENTITY CASCADE');
  });

  afterAll(async () => {
    await Promise.all(ctx.workers.map((w) => w.stop()));
    await ctx.pool.end();
  });

  /** A worker with fast timings so tests don't wait around. Stopped automatically. */
  function worker(opts: Partial<WorkerOptions> & Pick<WorkerOptions, 'handlers'>): Worker {
    const w = new Worker(ctx.pool, {
      pollIntervalMs: 50,
      leaseMs: 1000,
      reapIntervalMs: 200,
      shutdownTimeoutMs: 2000,
      retentionMs: 0,
      ...opts,
    });
    w.on('error', (err) => {
      if (!/terminat|Connection/i.test(err.message)) console.error('[worker error]', err.message);
    });
    ctx.workers.push(w);
    return w;
  }

  return Object.assign(ctx, { worker });
}

export async function waitFor<T>(
  fn: () => Promise<T | false | null | undefined>,
  timeoutMs = 10_000,
  everyMs = 25,
) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs} ms`);
    await sleep(everyMs);
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function countByState(pool: Pool): Promise<Record<string, number>> {
  const { rows } = await pool.query<{ state: string; n: number }>(
    'SELECT state, count(*)::int AS n FROM jobs GROUP BY state',
  );
  return Object.fromEntries(rows.map((r) => [r.state, r.n]));
}

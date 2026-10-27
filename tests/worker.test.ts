import { describe, expect, it } from 'vitest';
import { PermanentError } from '../src/core/errors.js';
import { countByState, sleep, useTestDb, waitFor } from './helpers.js';

const db = useTestDb();

describe('processing', () => {
  it('runs every job exactly once across several competing workers', async () => {
    const N = 1000;
    await db.queue.enqueueMany(Array.from({ length: N }, (_, i) => ({ name: 'count', payload: { i } })));
    const seen = new Map<number, number>();
    const handlers = {
      count: async (p: { i: number }) => {
        seen.set(p.i, (seen.get(p.i) ?? 0) + 1);
        await sleep(Math.random() * 3);
      },
    };
    const workers = Array.from({ length: 4 }, () => db.worker({ handlers, concurrency: 10 }));
    await Promise.all(workers.map((w) => w.start()));

    await waitFor(async () => (await countByState(db.pool)).succeeded === N, 20_000);
    expect(seen.size).toBe(N);
    expect([...seen.values()].every((n) => n === 1)).toBe(true);
  });

  it('stores the handler result', async () => {
    const { job } = await db.queue.enqueue({ name: 'add', payload: { a: 2, b: 3 } });
    await db
      .worker({ handlers: { add: async (p: { a: number; b: number }) => ({ sum: p.a + p.b }) } })
      .start();
    const done = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'succeeded' && j;
    });
    expect(done.result).toEqual({ sum: 5 });
  });

  it('picks up a new job within milliseconds thanks to LISTEN/NOTIFY', async () => {
    let startedAt = 0;
    const w = db.worker({
      handlers: {
        ping: async () => {
          startedAt = Date.now();
        },
      },
      pollIntervalMs: 60_000,
    });
    await w.start();
    await sleep(100);
    const enqueuedAt = Date.now();
    await db.queue.enqueue({ name: 'ping' });
    await waitFor(async () => startedAt > 0, 3000, 5);
    expect(startedAt - enqueuedAt).toBeLessThan(500); // polling alone would take up to 60 s here
  });
});

describe('failures', () => {
  it('retries with backoff until the job succeeds', async () => {
    let calls = 0;
    const { job } = await db.queue.enqueue({ name: 'flaky', backoffBaseMs: 20, maxAttempts: 5 });
    await db
      .worker({
        handlers: {
          flaky: async () => {
            calls++;
            if (calls < 3) throw new Error(`fail #${calls}`);
            return 'ok';
          },
        },
      })
      .start();

    const done = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'succeeded' && j;
    });
    expect(done.attempts).toBe(3);
    const types = (await db.queue.events(job.id)).map((e) => e.type);
    expect(types).toEqual(['enqueued', 'started', 'failed', 'started', 'failed', 'started', 'succeeded']);
  });

  it('moves a job to dead after max attempts', async () => {
    const { job } = await db.queue.enqueue({ name: 'broken', backoffBaseMs: 10, maxAttempts: 3 });
    await db
      .worker({
        handlers: {
          broken: async () => {
            throw new Error('always fails');
          },
        },
      })
      .start();
    const dead = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'dead' && j;
    });
    expect(dead.attempts).toBe(3);
    expect(dead.last_error).toBe('Error: always fails');
  });

  it('does not retry a PermanentError', async () => {
    const { job } = await db.queue.enqueue({ name: 'bad-input', maxAttempts: 5 });
    await db
      .worker({
        handlers: {
          'bad-input': async () => {
            throw new PermanentError('email address is invalid');
          },
        },
      })
      .start();
    const dead = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'dead' && j;
    });
    expect(dead.attempts).toBe(1);
  });

  it('kills an attempt that exceeds its timeout and retries it', async () => {
    const { job } = await db.queue.enqueue({
      name: 'slow',
      timeoutMs: 100,
      maxAttempts: 2,
      backoffBaseMs: 10,
    });
    await db
      .worker({
        handlers: {
          slow: (_p, { signal }) =>
            new Promise((_, reject) => {
              const t = setTimeout(() => reject(new Error('should have been aborted')), 5000);
              signal.addEventListener('abort', () => {
                clearTimeout(t);
                reject(signal.reason);
              });
            }),
        },
      })
      .start();
    const dead = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'dead' && j;
    });
    expect(dead.attempts).toBe(2);
    expect(dead.last_error).toMatch(/TimeoutError: Timed out after 100 ms/);
  });

  it('sends jobs with no registered handler straight to dead', async () => {
    const { job } = await db.queue.enqueue({ name: 'unknown-task' });
    await db.worker({ handlers: {} }).start();
    const dead = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'dead' && j;
    });
    expect(dead.last_error).toMatch(/No handler registered/);
  });
});

describe('crash recovery', () => {
  it('re-runs the jobs of a worker that died mid-job', async () => {
    await db.queue.enqueueMany(Array.from({ length: 5 }, () => ({ name: 'work' })));
    const crashing = db.worker({
      handlers: { work: () => new Promise(() => {}) }, // hangs forever
      concurrency: 5,
      leaseMs: 300,
      maintenance: false,
    });
    await crashing.start();
    await waitFor(async () => (await countByState(db.pool)).running === 5);
    crashing.crash(); // like `kill -9`: no cleanup, leases just stop being renewed

    const healthy = db.worker({ handlers: { work: async () => 'recovered' }, concurrency: 5 });
    await healthy.start();
    await waitFor(async () => (await countByState(db.pool)).succeeded === 5);

    const { rows } =
      await db.pool.query(`SELECT attempts, (SELECT array_agg(type ORDER BY id) FROM job_events e
      WHERE e.job_id = j.id) AS types FROM jobs j`);
    for (const r of rows) {
      expect(r.attempts).toBe(2);
      expect(r.types).toEqual(['enqueued', 'started', 'lease_expired', 'started', 'succeeded']);
    }
  });

  it('discards the result of a worker whose lease was taken over (fencing)', async () => {
    const { job } = await db.queue.enqueue({ name: 'x' });
    const [claimed] = await db.queue.claim('default', 1, 'slow-worker', 50);
    await sleep(80);
    await db.queue.reapExpiredLeases(); // slow-worker presumed dead
    await db.queue.claim('default', 1, 'new-worker', 5000);

    const late = await db.queue.complete(claimed!.id, 'slow-worker', 'stale result', 1);
    expect(late).toBe(false);
    const current = await db.queue.get(job.id);
    expect(current).toMatchObject({ state: 'running', locked_by: 'new-worker' });
  });

  it('heartbeats keep long jobs alive beyond the lease length', async () => {
    const { job } = await db.queue.enqueue({ name: 'long' });
    await db
      .worker({
        handlers: {
          long: async () => {
            await sleep(1200);
            return 'done';
          },
        },
        leaseMs: 300,
        heartbeatMs: 100,
        reapIntervalMs: 50,
      })
      .start();
    const done = await waitFor(async () => {
      const j = await db.queue.get(job.id);
      return j?.state === 'succeeded' && j;
    });
    expect(done.attempts).toBe(1); // never presumed dead
  });
});

describe('shutdown', () => {
  it('lets running jobs finish before stopping', async () => {
    const { job } = await db.queue.enqueue({ name: 'x' });
    const w = db.worker({
      handlers: {
        x: async () => {
          await sleep(300);
          return 'finished';
        },
      },
    });
    await w.start();
    await waitFor(async () => w.inFlight === 1);
    await w.stop();
    expect((await db.queue.get(job.id))?.state).toBe('succeeded');
  });

  it('hands unfinished jobs back to the queue when the grace period runs out', async () => {
    const { job } = await db.queue.enqueue({ name: 'x' });
    const w = db.worker({
      handlers: {
        x: (_p, { signal }) =>
          new Promise((_, rej) => signal.addEventListener('abort', () => rej(signal.reason))),
      },
      shutdownTimeoutMs: 200,
    });
    await w.start();
    await waitFor(async () => w.inFlight === 1);
    await w.stop();
    const j = await db.queue.get(job.id);
    expect(j).toMatchObject({ state: 'queued', attempts: 0, locked_by: null });
    expect((await db.queue.events(job.id)).at(-1)?.type).toBe('released');
  });
});

describe('flow control', () => {
  it('a paused queue is skipped until resumed', async () => {
    await db.queue.setPaused('email', true);
    await db.queue.enqueue({ name: 'x', queue: 'email' });
    await db.worker({ queues: ['email'], handlers: { x: async () => null } }).start();
    await sleep(300);
    expect((await countByState(db.pool)).queued).toBe(1);
    await db.queue.setPaused('email', false);
    await waitFor(async () => (await countByState(db.pool)).succeeded === 1);
  });

  it('enforces a global rate limit across workers', async () => {
    await db.queue.setRateLimit('api', 20, 5); // 20 jobs/s, burst 5
    await db.queue.enqueueMany(Array.from({ length: 45 }, () => ({ name: 'call', queue: 'api' })));
    const handlers = { call: async () => null };
    const started = Date.now();
    await Promise.all([1, 2, 3].map(() => db.worker({ queues: ['api'], handlers, concurrency: 10 }).start()));
    await waitFor(async () => (await countByState(db.pool)).succeeded === 45, 15_000);
    const seconds = (Date.now() - started) / 1000;
    // 5 immediately, the other 40 at 20/s => ~2 s, no matter how many workers.
    expect(seconds).toBeGreaterThan(1.6);
    expect(seconds).toBeLessThan(4);
  });

  it('per-queue caps stop slow jobs from hogging every slot', async () => {
    await db.queue.enqueueMany(Array.from({ length: 10 }, () => ({ name: 'slow', queue: 'reports' })));
    await db.queue.enqueueMany(Array.from({ length: 10 }, () => ({ name: 'fast', queue: 'email' })));
    let maxReports = 0;
    let reports = 0;
    const w = db.worker({
      queues: ['reports', 'email'],
      concurrency: 6,
      queueConcurrency: { reports: 2 },
      handlers: {
        slow: async () => {
          reports++;
          maxReports = Math.max(maxReports, reports);
          await sleep(150);
          reports--;
        },
        fast: async () => null,
      },
    });
    await w.start();
    await waitFor(async () => (await countByState(db.pool)).succeeded === 20);
    expect(maxReports).toBe(2);
  });
});

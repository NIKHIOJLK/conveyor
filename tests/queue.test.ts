import { describe, expect, it } from 'vitest';
import { useTestDb } from './helpers.js';

const db = useTestDb();

describe('enqueue', () => {
  it('stores the job with defaults and logs an "enqueued" event', async () => {
    const { job, deduplicated } = await db.queue.enqueue({ name: 'send-email', payload: { to: 'a@b.c' } });
    expect(deduplicated).toBe(false);
    expect(job).toMatchObject({
      queue: 'default',
      name: 'send-email',
      state: 'queued',
      attempts: 0,
      max_attempts: 5,
      priority: 0,
      payload: { to: 'a@b.c' },
    });
    const events = await db.queue.events(job.id);
    expect(events.map((e) => e.type)).toEqual(['enqueued']);
  });

  it('creates the queue row automatically', async () => {
    await db.queue.enqueue({ name: 'x', queue: 'invoices' });
    const stats = await db.queue.queueStats();
    expect(stats.map((s) => s.queue)).toEqual(['invoices']);
  });

  it('returns the existing job for a repeated idempotency key', async () => {
    const a = await db.queue.enqueue({ name: 'charge', idempotencyKey: 'order-42' });
    const b = await db.queue.enqueue({ name: 'charge', idempotencyKey: 'order-42' });
    expect(b.deduplicated).toBe(true);
    expect(b.job.id).toBe(a.job.id);
    const { rows } = await db.pool.query('SELECT count(*)::int AS n FROM jobs');
    expect(rows[0].n).toBe(1);
  });

  it('treats the same idempotency key in different queues as different jobs', async () => {
    const a = await db.queue.enqueue({ name: 'x', queue: 'q1', idempotencyKey: 'k' });
    const b = await db.queue.enqueue({ name: 'x', queue: 'q2', idempotencyKey: 'k' });
    expect(b.deduplicated).toBe(false);
    expect(b.job.id).not.toBe(a.job.id);
  });

  it('inserts many jobs in one statement, skipping duplicate keys', async () => {
    const jobs = await db.queue.enqueueMany([
      { name: 'a' },
      { name: 'b', idempotencyKey: 'dup' },
      { name: 'c', idempotencyKey: 'dup' },
    ]);
    expect(jobs).toHaveLength(2);
  });

  it('accepts plain SQL inserts from any language', async () => {
    await db.pool.query(`INSERT INTO jobs (name, queue) VALUES ('from-python', 'etl')`);
    const [job] = await db.queue.claim('etl', 1, 'w', 1000);
    expect(job?.name).toBe('from-python');
  });
});

describe('claim', () => {
  it('does not hand out scheduled jobs before their time', async () => {
    await db.queue.enqueue({ name: 'later', delayMs: 60_000 });
    await db.queue.enqueue({ name: 'now' });
    const jobs = await db.queue.claim('default', 10, 'w1', 1000);
    expect(jobs.map((j) => j.name)).toEqual(['now']);
  });

  it('returns higher priority first, then oldest first', async () => {
    await db.queue.enqueue({ name: 'low-1' });
    await db.queue.enqueue({ name: 'high', priority: 10 });
    await db.queue.enqueue({ name: 'low-2' });
    const jobs = await db.queue.claim('default', 3, 'w1', 1000);
    expect(jobs.map((j) => j.name)).toEqual(['high', 'low-1', 'low-2']);
  });

  it('never gives the same job to two concurrent claimers', async () => {
    await db.queue.enqueueMany(Array.from({ length: 200 }, (_, i) => ({ name: `j${i}` })));
    const batches = await Promise.all(
      Array.from({ length: 10 }, (_, i) => db.queue.claim('default', 30, `w${i}`, 1000)),
    );
    const ids = batches.flat().map((j) => j.id);
    expect(ids).toHaveLength(200);
    expect(new Set(ids).size).toBe(200);
  });

  it('leases the job: running, attempt counted, owner recorded', async () => {
    await db.queue.enqueue({ name: 'x' });
    const [job] = await db.queue.claim('default', 1, 'worker-a', 5000);
    expect(job).toMatchObject({ state: 'running', attempts: 1, locked_by: 'worker-a' });
    expect(job!.locked_until!.getTime()).toBeGreaterThan(Date.now() + 3000);
  });
});

describe('admin', () => {
  it('cancels only jobs that have not started', async () => {
    const { job: a } = await db.queue.enqueue({ name: 'a' });
    const { job: b } = await db.queue.enqueue({ name: 'b' });
    await db.queue.claim('default', 1, 'w', 1000); // claims a
    expect(await db.queue.cancel(a.id)).toBeNull();
    expect((await db.queue.cancel(b.id))?.state).toBe('cancelled');
  });

  it('retries dead jobs from scratch', async () => {
    const { job } = await db.queue.enqueue({ name: 'x', maxAttempts: 1 });
    await db.queue.claim('default', 1, 'w', 1000);
    await db.queue.fail(job.id, 'w', {
      state: 'dead',
      delayMs: 0,
      error: 'boom',
      eventType: 'dead',
      message: 'boom',
      durationMs: 1,
    });
    const retried = await db.queue.retry(job.id);
    expect(retried).toMatchObject({ state: 'queued', attempts: 0 });
    expect((await db.queue.events(job.id)).map((e) => e.type)).toEqual([
      'enqueued',
      'started',
      'dead',
      'retried',
    ]);
  });

  it('pausing a queue stops reservations', async () => {
    await db.queue.enqueue({ name: 'x', queue: 'email' });
    await db.queue.setPaused('email', true);
    expect(await db.queue.reserve('email', 10)).toBe(0);
    await db.queue.setPaused('email', false);
    expect(await db.queue.reserve('email', 10)).toBe(10);
  });

  it('token bucket hands out at most `burst` at once, then refills at `rate`', async () => {
    await db.queue.setRateLimit('api', 10, 3); // 10/s, burst 3
    expect(await db.queue.reserve('api', 10)).toBe(3);
    expect(await db.queue.reserve('api', 10)).toBe(0);
    await new Promise((r) => setTimeout(r, 250)); // ~2.5 tokens refill
    const n = await db.queue.reserve('api', 10);
    expect(n).toBeGreaterThanOrEqual(2);
    expect(n).toBeLessThanOrEqual(3);
  });
});

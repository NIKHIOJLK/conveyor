/**
 * Worker: claims jobs and runs their handlers with bounded concurrency.
 *
 * Life of a job inside a worker:
 *   claim (lease for leaseMs) -> run handler (with timeout) -> complete | fail
 *   While it runs, a heartbeat keeps extending the lease. If the process dies,
 *   heartbeats stop, the lease expires, and any worker's reaper puts the job back.
 *
 * Wake-ups come from three places: LISTEN/NOTIFY (a job was just enqueued),
 * a slow poll (scheduled jobs and retries becoming due), and a job finishing
 * (a slot just freed up).
 */

import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { hostname } from 'node:os';
import type pg from 'pg';
import type { Pool } from '../db/pool.js';
import { backoffDelay } from './backoff.js';
import { isPermanent, LeaseLostError, ShutdownError, TimeoutError } from './errors.js';
import { NOTIFY_CHANNEL, Queue } from './queue.js';
import type { Job, JobContext, JobHandler } from './types.js';

export interface WorkerOptions {
  /** Queues to take jobs from, served round-robin. Default ['default']. */
  queues?: string[];
  /** Handlers by job name. */
  handlers: Record<string, JobHandler>;
  /** Max jobs running at once in this worker. Default 5. */
  concurrency?: number;
  /**
   * Optional per-queue caps inside this worker, e.g. { reports: 2 }, so slow jobs
   * can't occupy every slot and starve the other queues (the "bulkhead" pattern).
   */
  queueConcurrency?: Record<string, number>;
  /** Fallback poll interval. Default 1000 ms. */
  pollIntervalMs?: number;
  /** How long a claim is valid without a heartbeat. Default 30 s. */
  leaseMs?: number;
  /** How often leases are renewed. Default leaseMs / 3. */
  heartbeatMs?: number;
  /** On stop(), how long to let running jobs finish before handing them back. Default 30 s. */
  shutdownTimeoutMs?: number;
  /** Use LISTEN/NOTIFY for instant pickup. Default true. */
  listen?: boolean;
  /** Run crash recovery and cleanup in this worker. Default true. */
  maintenance?: boolean;
  /** How often to look for expired leases. Default 5 s. */
  reapIntervalMs?: number;
  /** Delete finished jobs older than this. Default 7 days. 0 disables. */
  retentionMs?: number;
  id?: string;
  /** Injected for deterministic tests. */
  random?: () => number;
}

interface Running {
  job: Job;
  controller: AbortController;
  done: Promise<void>;
}

export interface WorkerEvents {
  started: [];
  stopped: [];
  'job:succeeded': [job: Job, durationMs: number];
  'job:retry': [job: Job, error: Error, delayMs: number];
  'job:dead': [job: Job, error: Error];
  'job:lost': [job: Job];
  'jobs:recovered': [count: number];
  'job:log': [job: Job, message: string];
  error: [error: Error];
}

export class Worker extends EventEmitter<WorkerEvents> {
  readonly id: string;
  readonly queues: string[];
  readonly concurrency: number;
  private readonly queue: Queue;
  private readonly opts: Required<
    Omit<WorkerOptions, 'id' | 'queues' | 'concurrency' | 'handlers' | 'queueConcurrency'>
  >;
  private readonly queueCaps: Record<string, number>;
  private readonly handlers: Record<string, JobHandler>;
  private readonly running = new Map<number, Running>();
  private timers: NodeJS.Timeout[] = [];
  private listener: pg.PoolClient | null = null;
  private filling = false;
  private refillRequested = false;
  private roundRobin = 0;
  private state: 'idle' | 'running' | 'stopping' | 'stopped' = 'idle';

  constructor(
    readonly pool: Pool,
    options: WorkerOptions,
  ) {
    super();
    this.queue = new Queue(pool);
    this.queues = options.queues?.length ? options.queues : ['default'];
    this.concurrency = Math.max(1, options.concurrency ?? 5);
    this.handlers = options.handlers;
    this.queueCaps = options.queueConcurrency ?? {};
    this.id = options.id ?? `${hostname()}-${process.pid}-${randomBytes(3).toString('hex')}`;
    const leaseMs = options.leaseMs ?? 30_000;
    this.opts = {
      pollIntervalMs: options.pollIntervalMs ?? 1000,
      leaseMs,
      heartbeatMs: options.heartbeatMs ?? Math.max(100, Math.floor(leaseMs / 3)),
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? 30_000,
      listen: options.listen ?? true,
      maintenance: options.maintenance ?? true,
      reapIntervalMs: options.reapIntervalMs ?? 5000,
      retentionMs: options.retentionMs ?? 7 * 24 * 3600_000,
      random: options.random ?? Math.random,
    };
  }

  get inFlight(): number {
    return this.running.size;
  }

  async start(): Promise<void> {
    if (this.state !== 'idle') throw new Error('Worker already started');
    this.state = 'running';
    await this.queue.registerWorker({
      id: this.id,
      queues: this.queues,
      concurrency: this.concurrency,
      hostname: hostname(),
      pid: process.pid,
    });
    if (this.opts.listen) await this.listen();

    this.every(this.opts.pollIntervalMs, () => this.fill());
    this.every(this.opts.heartbeatMs, () => this.heartbeat());
    if (this.opts.maintenance) {
      this.every(this.opts.reapIntervalMs, () => this.reap());
      if (this.opts.retentionMs > 0) {
        this.every(10 * 60_000, () => this.queue.purgeFinished(this.opts.retentionMs).then(() => {}));
      }
    }
    this.emit('started');
    void this.fill();
  }

  /**
   * Graceful shutdown: stop taking jobs, let running ones finish, and hand back
   * whatever is still running after shutdownTimeoutMs so another worker can pick it up.
   */
  async stop(): Promise<void> {
    if (this.state !== 'running') return;
    this.state = 'stopping';
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    await this.unlisten();

    const settled = Promise.all([...this.running.values()].map((r) => r.done));
    const finished = await within(settled, this.opts.shutdownTimeoutMs);
    if (!finished && this.running.size > 0) {
      const ids = [...this.running.keys()];
      await this.queue.release(this.id, ids).catch((err) => this.emit('error', err));
      for (const r of this.running.values()) r.controller.abort(new ShutdownError());
      await within(settled, 1000);
    }
    await this.queue.removeWorker(this.id).catch(() => {});
    this.state = 'stopped';
    this.emit('stopped');
  }

  /** Test helper: simulate `kill -9`. Timers stop and nothing is written back. */
  crash(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    this.state = 'stopped';
    this.listener?.release(true);
    this.listener = null;
    for (const r of this.running.values()) r.controller.abort(new LeaseLostError());
  }

  // ------------------------------------------------------------------ claiming

  /** Claim as many jobs as there are free slots. Safe to call any time; calls coalesce. */
  private async fill(): Promise<void> {
    if (this.state !== 'running') return;
    if (this.filling) {
      this.refillRequested = true;
      return;
    }
    this.filling = true;
    try {
      do {
        this.refillRequested = false;
        for (let i = 0; i < this.queues.length && this.state === 'running'; i++) {
          if (this.concurrency - this.running.size <= 0) break;
          const queue = this.queues[(this.roundRobin + i) % this.queues.length]!;
          const free = this.freeSlots(queue);
          if (free <= 0) continue;
          const granted = await this.queue.reserve(queue, free);
          if (granted === 0) continue;
          const jobs = await this.queue.claim(queue, granted, this.id, this.opts.leaseMs);
          if (jobs.length < granted) await this.queue.refund(queue, granted - jobs.length);
          for (const job of jobs) this.execute(job);
          if (jobs.length === granted) this.refillRequested = true; // there may be more waiting
        }
        this.roundRobin = (this.roundRobin + 1) % this.queues.length;
      } while (this.refillRequested && this.state === 'running' && this.running.size < this.concurrency);
    } catch (err) {
      this.emit('error', err as Error);
    } finally {
      this.filling = false;
    }
  }

  private freeSlots(queue: string): number {
    const total = this.concurrency - this.running.size;
    const cap = this.queueCaps[queue];
    if (cap === undefined) return total;
    let inQueue = 0;
    for (const r of this.running.values()) if (r.job.queue === queue) inQueue++;
    return Math.min(total, cap - inQueue);
  }

  // ------------------------------------------------------------------ running

  private execute(job: Job): void {
    const controller = new AbortController();
    const entry = { job, controller } as Running;
    entry.done = this.run(job, controller).finally(() => {
      this.running.delete(job.id);
      void this.fill(); // a slot just freed up
    });
    this.running.set(job.id, entry);
  }

  private async run(job: Job, controller: AbortController): Promise<void> {
    const started = performance.now();
    const timer = setTimeout(() => controller.abort(new TimeoutError(job.timeout_ms)), job.timeout_ms);
    const ctx: JobContext = {
      job: {
        id: job.id,
        name: job.name,
        queue: job.queue,
        attempts: job.attempts,
        max_attempts: job.max_attempts,
        created_at: job.created_at,
      },
      signal: controller.signal,
      log: (message) => this.emit('job:log', job, message),
    };
    try {
      const handler = this.handlers[job.name];
      if (!handler)
        throw Object.assign(new Error(`No handler registered for '${job.name}'`), { name: 'PermanentError' });
      const result = await abortable(handler(job.payload, ctx), controller.signal);
      const ok = await this.queue.complete(job.id, this.id, result, performance.now() - started);
      if (ok) this.emit('job:succeeded', job, performance.now() - started);
      else this.emit('job:lost', job);
    } catch (raw) {
      const err = raw instanceof Error ? raw : new Error(String(raw));
      // Lost lease or forced shutdown: someone else is responsible for the job now.
      if (err instanceof LeaseLostError || err instanceof ShutdownError) return;
      await this.recordFailure(job, err, performance.now() - started);
    } finally {
      clearTimeout(timer);
    }
  }

  private async recordFailure(job: Job, err: Error, durationMs: number): Promise<void> {
    const permanent = isPermanent(err);
    const exhausted = job.attempts >= job.max_attempts;
    const error = `${err.name}: ${err.message}`;
    try {
      if (permanent || exhausted) {
        const why = permanent ? 'permanent error, not retrying' : `gave up after ${job.attempts} attempts`;
        const ok = await this.queue.fail(job.id, this.id, {
          state: 'dead',
          delayMs: 0,
          error,
          eventType: 'dead',
          message: `${error} (${why})`,
          durationMs,
        });
        if (ok) this.emit('job:dead', job, err);
        else this.emit('job:lost', job);
      } else {
        const delayMs = backoffDelay(job.attempts, job.backoff_base_ms, this.opts.random);
        const ok = await this.queue.fail(job.id, this.id, {
          state: 'queued',
          delayMs,
          error,
          eventType: 'failed',
          message: `${error} (retry ${job.attempts + 1}/${job.max_attempts} in ${formatMs(delayMs)})`,
          durationMs,
        });
        if (ok) this.emit('job:retry', job, err, delayMs);
        else this.emit('job:lost', job);
      }
    } catch (dbErr) {
      // The database is unreachable: the lease will expire and the reaper will retry the job.
      this.emit('error', dbErr as Error);
    }
  }

  // ------------------------------------------------------------------ background loops

  /** Renew leases; abort any job whose lease another worker has taken over. */
  private async heartbeat(): Promise<void> {
    const ids = [...this.running.keys()];
    try {
      const owned = new Set(await this.queue.heartbeat(this.id, ids, this.opts.leaseMs));
      for (const id of ids) {
        if (!owned.has(id)) this.running.get(id)?.controller.abort(new LeaseLostError());
      }
    } catch (err) {
      this.emit('error', err as Error);
    }
  }

  private async reap(): Promise<void> {
    try {
      const recovered = await this.queue.reapExpiredLeases();
      if (recovered.length) this.emit('jobs:recovered', recovered.length);
      await this.queue.pruneWorkers();
    } catch (err) {
      this.emit('error', err as Error);
    }
  }

  private async listen(): Promise<void> {
    try {
      const client = await this.pool.connect();
      client.on('notification', (msg) => {
        if (msg.payload && this.queues.includes(msg.payload)) void this.fill();
      });
      client.on('error', (err) => {
        // Connection dropped: fall back to polling and try to re-subscribe.
        this.emit('error', err);
        this.listener?.release(true);
        this.listener = null;
        if (this.state === 'running') setTimeout(() => this.state === 'running' && this.listen(), 2000);
      });
      await client.query(`LISTEN ${NOTIFY_CHANNEL}`);
      this.listener = client;
    } catch (err) {
      // Database unreachable: keep polling meanwhile and try to subscribe again shortly.
      this.emit('error', err as Error);
      if (this.state === 'running') setTimeout(() => this.state === 'running' && this.listen(), 2000);
    }
  }

  private async unlisten(): Promise<void> {
    if (!this.listener) return;
    try {
      await this.listener.query(`UNLISTEN ${NOTIFY_CHANNEL}`);
      this.listener.release();
    } catch {
      this.listener.release(true);
    }
    this.listener = null;
  }

  private every(ms: number, fn: () => Promise<unknown>): void {
    this.timers.push(setInterval(() => void fn().catch((err) => this.emit('error', err)), ms));
  }
}

/** Reject as soon as the signal aborts. (JS can't kill the handler; it should honour ctx.signal.) */
function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (v) => {
        signal.removeEventListener('abort', onAbort);
        resolve(v);
      },
      (e) => {
        signal.removeEventListener('abort', onAbort);
        reject(e);
      },
    );
  });
}

/** Resolve true if `promise` settles within `ms`, false otherwise (and don't leave the timer running). */
async function within(promise: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<false>((r) => {
    timer = setTimeout(() => r(false), ms);
  });
  try {
    return await Promise.race([promise.then(() => true as const), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

export function formatMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600_000) return `${Math.round(ms / 60_000)}m`;
  return `${(ms / 3600_000).toFixed(1)}h`;
}

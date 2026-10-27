/**
 * REST API + live dashboard.
 *
 * Producers POST jobs here (or use the Queue class directly). The dashboard at /
 * gets a stats snapshot every second over Server-Sent Events.
 */
import { fileURLToPath } from 'node:url';
import express, { type NextFunction, type Request, type Response } from 'express';
import { z } from 'zod';
import { DISPLAY_STATES, Queue } from '../core/queue.js';
import type { Pool } from '../db/pool.js';
import { demoJobs } from '../demo/handlers.js';
import { renderMetrics } from './metrics.js';

const PUBLIC_DIR = fileURLToPath(new URL('../../public/', import.meta.url));

const name = z
  .string()
  .trim()
  .min(1)
  .max(100)
  .regex(/^[\w.:-]+$/, 'letters, digits, _ . : - only');

const enqueueSchema = z
  .object({
    name,
    queue: name.optional(),
    payload: z.record(z.string(), z.unknown()).optional(),
    priority: z.number().int().min(-1000).max(1000).optional(),
    runAt: z.coerce.date().optional(),
    delayMs: z
      .number()
      .int()
      .min(0)
      .max(365 * 24 * 3600_000)
      .optional(),
    maxAttempts: z.number().int().min(1).max(100).optional(),
    backoffBaseMs: z.number().int().min(0).max(3600_000).optional(),
    timeoutMs: z
      .number()
      .int()
      .min(1)
      .max(24 * 3600_000)
      .optional(),
    idempotencyKey: z.string().min(1).max(200).optional(),
  })
  .refine((v) => !(v.runAt && v.delayMs !== undefined), { message: 'Use runAt or delayMs, not both' });

const listSchema = z.object({
  state: z.enum(['queued', ...DISPLAY_STATES]).optional(),
  queue: name.optional(),
  name: name.optional(),
  before: z.coerce.number().int().positive().optional(),
  limit: z.coerce.number().int().min(1).max(200).optional(),
});

const rateLimitSchema = z.object({
  rate: z.number().positive().max(100_000).nullable(),
  burst: z.number().int().min(1).max(100_000).optional(),
});

const idParam = z.coerce.number().int().positive();

export async function snapshot(queue: Queue) {
  const [queues, throughput, workers] = await Promise.all([
    queue.queueStats(),
    queue.throughput(60),
    queue.workers(),
  ]);
  const totals = { waiting: 0, scheduled: 0, retrying: 0, running: 0, succeeded: 0, dead: 0, cancelled: 0 };
  for (const q of queues)
    for (const k of Object.keys(totals) as Array<keyof typeof totals>) totals[k] += q[k];
  const last10 = throughput.slice(-10);
  const perSecond = last10.reduce((s, t) => s + t.succeeded, 0) / last10.length;
  return { at: new Date().toISOString(), totals, perSecond, queues, throughput, workers };
}

export function createApp(pool: Pool, opts: { demoMode?: boolean } = {}) {
  const queue = new Queue(pool);
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '256kb' }));

  app.get('/api/health', async (_req, res) => {
    await pool.query('SELECT 1');
    res.json({ status: 'ok' });
  });

  app.get('/api/stats', async (_req, res) => {
    res.json(await snapshot(queue));
  });

  // ---- jobs
  app.post('/api/jobs', async (req, res) => {
    const body = enqueueSchema.parse(req.body);
    const { job, deduplicated } = await queue.enqueue(body);
    res.status(deduplicated ? 200 : 201).json({ job, deduplicated });
  });

  app.get('/api/jobs', async (req, res) => {
    const filter = listSchema.parse(req.query) as Parameters<Queue['list']>[0];
    const jobs = await queue.list(filter);
    res.json({ jobs, nextCursor: jobs.length ? jobs[jobs.length - 1]!.id : null });
  });

  app.get('/api/jobs/:id', async (req, res) => {
    const id = idParam.parse(req.params.id);
    const job = await queue.get(id);
    if (!job) return void res.status(404).json({ error: `Job ${id} not found` });
    res.json({ job, events: await queue.events(id) });
  });

  app.post('/api/jobs/:id/retry', async (req, res) => {
    const id = idParam.parse(req.params.id);
    const job = await queue.retry(id);
    if (!job) return void res.status(409).json({ error: 'Only dead or cancelled jobs can be retried' });
    res.json({ job });
  });

  app.post('/api/jobs/:id/cancel', async (req, res) => {
    const id = idParam.parse(req.params.id);
    const job = await queue.cancel(id);
    if (!job) return void res.status(409).json({ error: 'Only jobs that have not started can be cancelled' });
    res.json({ job });
  });

  // ---- queues
  app.post('/api/queues/:queue/pause', async (req, res) => {
    await queue.setPaused(name.parse(req.params.queue), true);
    res.json({ ok: true });
  });

  app.post('/api/queues/:queue/resume', async (req, res) => {
    await queue.setPaused(name.parse(req.params.queue), false);
    res.json({ ok: true });
  });

  app.put('/api/queues/:queue/rate-limit', async (req, res) => {
    const { rate, burst } = rateLimitSchema.parse(req.body);
    await queue.setRateLimit(name.parse(req.params.queue), rate, burst ?? Math.max(1, Math.ceil(rate ?? 1)));
    res.json({ ok: true });
  });

  app.post('/api/queues/:queue/retry-dead', async (req, res) => {
    res.json({ retried: await queue.retryAllDead(name.parse(req.params.queue)) });
  });

  // ---- demo helpers (dashboard buttons)
  app.post('/api/demo/load', async (req, res) => {
    if (!opts.demoMode) return void res.status(403).json({ error: 'Demo mode is off (DEMO_MODE=false)' });
    const { count } = z.object({ count: z.number().int().min(1).max(10_000) }).parse(req.body);
    const created = await queue.enqueueMany(demoJobs(count));
    res.status(201).json({ created: created.length });
  });

  // ---- live updates: one shared poller, fanned out to every connected dashboard
  const clients = new Set<Response>();
  let poller: NodeJS.Timeout | null = null;
  const broadcast = async () => {
    try {
      const data = `event: stats\ndata: ${JSON.stringify(await snapshot(queue))}\n\n`;
      for (const c of clients) c.write(data);
    } catch (err) {
      console.error('[api] stats poll failed:', (err as Error).message);
    }
  };
  app.get('/api/events', (req, res) => {
    res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.flushHeaders();
    res.write('retry: 2000\n\n');
    clients.add(res);
    if (!poller) poller = setInterval(broadcast, 1000);
    void broadcast();
    req.on('close', () => {
      clients.delete(res);
      if (clients.size === 0 && poller) {
        clearInterval(poller);
        poller = null;
      }
    });
  });

  app.get('/metrics', async (_req, res) => {
    res.type('text/plain; version=0.0.4').send(renderMetrics(await snapshot(queue)));
  });

  app.use(express.static(PUBLIC_DIR));

  app.use('/api', (_req, res) => {
    res.status(404).json({ error: 'Not found' });
  });

  // Validation errors -> 400; anything else -> 500 (and logged).
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof z.ZodError) {
      return void res.status(400).json({
        error: 'Invalid request',
        issues: err.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
      });
    }
    if (err instanceof SyntaxError && 'body' in err) {
      return void res.status(400).json({ error: 'Malformed JSON' });
    }
    console.error('[api]', err);
    res.status(500).json({ error: 'Internal server error' });
  });

  const close = () => {
    if (poller) clearInterval(poller);
    for (const c of clients) c.end();
    clients.clear();
  };
  return Object.assign(app, { closeLive: close });
}

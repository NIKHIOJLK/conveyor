import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createApp } from '../src/api/server.js';
import { useTestDb } from './helpers.js';

const db = useTestDb();
let server: Server;
let base = '';
let app: ReturnType<typeof createApp>;

beforeAll(async () => {
  app = createApp(db.pool, { demoMode: true });
  server = app.listen(0);
  await new Promise((r) => server.once('listening', r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(() => {
  app.closeLive();
  server.close();
});

const post = (path: string, body?: unknown) =>
  fetch(base + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  });

describe('REST API', () => {
  it('enqueues a job (201) and returns the same job for a repeated idempotency key (200)', async () => {
    const body = {
      name: 'send-email',
      queue: 'email',
      payload: { to: 'x@y.z' },
      idempotencyKey: 'welcome-7',
    };
    const first = await post('/api/jobs', body);
    expect(first.status).toBe(201);
    const a = await first.json();
    const second = await post('/api/jobs', body);
    expect(second.status).toBe(200);
    const b = await second.json();
    expect(b).toMatchObject({ deduplicated: true, job: { id: a.job.id } });
  });

  it('rejects invalid input with a helpful 400', async () => {
    const res = await post('/api/jobs', { name: '', maxAttempts: 0 });
    expect(res.status).toBe(400);
    const body = await res.json();
    const fields = new Set(body.issues.map((i: { path: string }) => i.path));
    expect([...fields].sort()).toEqual(['maxAttempts', 'name']);
  });

  it('rejects malformed JSON', async () => {
    const res = await fetch(`${base}/api/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{"name": ',
    });
    expect(res.status).toBe(400);
  });

  it('returns a job with its timeline, and 404 for unknown ids', async () => {
    const { job } = await (await post('/api/jobs', { name: 'x' })).json();
    const res = await fetch(`${base}/api/jobs/${job.id}`);
    const body = await res.json();
    expect(body.job.id).toBe(job.id);
    expect(body.events[0].type).toBe('enqueued');
    expect((await fetch(`${base}/api/jobs/999999`)).status).toBe(404);
  });

  it('lists jobs filtered by state and queue', async () => {
    await post('/api/jobs', { name: 'a', queue: 'q1' });
    await post('/api/jobs', { name: 'b', queue: 'q2' });
    const res = await fetch(`${base}/api/jobs?state=waiting&queue=q2`);
    const { jobs } = await res.json();
    expect(jobs.map((j: { name: string }) => j.name)).toEqual(['b']);
  });

  it('cancels a waiting job and refuses to retry a job that is not dead', async () => {
    const { job } = await (await post('/api/jobs', { name: 'x' })).json();
    expect((await post(`/api/jobs/${job.id}/retry`)).status).toBe(409);
    expect((await post(`/api/jobs/${job.id}/cancel`)).status).toBe(200);
    expect((await post(`/api/jobs/${job.id}/retry`)).status).toBe(200); // cancelled jobs can be retried
  });

  it('pauses, resumes and rate-limits queues', async () => {
    await post('/api/jobs', { name: 'x', queue: 'email' });
    expect((await post('/api/queues/email/pause')).status).toBe(200);
    let stats = await (await fetch(`${base}/api/stats`)).json();
    expect(stats.queues[0]).toMatchObject({ queue: 'email', paused: true });

    await fetch(`${base}/api/queues/email/rate-limit`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rate: 5 }),
    });
    await post('/api/queues/email/resume');
    stats = await (await fetch(`${base}/api/stats`)).json();
    expect(stats.queues[0]).toMatchObject({ paused: false, rate_limit: 5 });
  });

  it('serves Prometheus metrics', async () => {
    await post('/api/jobs', { name: 'x', queue: 'email' });
    const text = await (await fetch(`${base}/metrics`)).text();
    expect(text).toContain('# TYPE conveyor_jobs gauge');
    expect(text).toContain('conveyor_jobs{queue="email",state="waiting"} 1');
  });

  it('adds demo jobs from the dashboard button', async () => {
    const res = await post('/api/demo/load', { count: 25 });
    expect(await res.json()).toEqual({ created: 25 });
  });

  it('streams live stats over Server-Sent Events', async () => {
    const controller = new AbortController();
    const res = await fetch(`${base}/api/events`, { signal: controller.signal });
    expect(res.headers.get('content-type')).toContain('text/event-stream');
    const reader = res.body!.getReader();
    let text = '';
    while (!text.includes('event: stats')) text += new TextDecoder().decode((await reader.read()).value);
    controller.abort();
    expect(text).toContain('"totals"');
  });
});

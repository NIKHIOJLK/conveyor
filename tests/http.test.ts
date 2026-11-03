import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createHttpHandler } from '../src/http/handler.js';
import { useTestDb, waitFor } from './helpers.js';

const db = useTestDb();

// A fake service that records requests and answers based on the job name.
let server: Server;
let base = '';
const received: Array<{ path: string; headers: IncomingMessage['headers']; body: unknown }> = [];
let flakyCalls = 0;

beforeAll(async () => {
  server = createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => {
      raw += c;
    });
    req.on('end', () => {
      received.push({ path: req.url ?? '', headers: req.headers, body: JSON.parse(raw || '{}') });
      const reply = (status: number, body: unknown) => {
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(body));
      };
      if (req.headers['x-conveyor-secret'] !== 's3cret') return reply(401, { error: 'bad secret' });
      switch (req.url) {
        case '/tasks/ok':
          return reply(200, { sent: true });
        case '/tasks/flaky':
          flakyCalls++;
          return flakyCalls < 3 ? reply(503, { error: 'mail server busy' }) : reply(200, { sent: true });
        case '/tasks/bad-input':
          return reply(422, { error: 'booking 99 does not exist' });
        case '/tasks/slow':
          return; // never answers
        default:
          return reply(404, { error: 'no such task' });
      }
    });
  });
  server.listen(0);
  await new Promise((r) => server.once('listening', r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/tasks`;
});

afterAll(() => {
  server.closeAllConnections();
  server.close();
});

beforeEach(() => {
  received.length = 0;
  flakyCalls = 0;
});

function startHttpWorker(secret = 's3cret') {
  return db
    .worker({ queues: ['tasks'], handlers: {}, defaultHandler: createHttpHandler({ baseUrl: base, secret }) })
    .start();
}

async function finalState(id: number) {
  return waitFor(async () => {
    const j = await db.queue.get(id);
    return j && ['succeeded', 'dead'].includes(j.state) && j;
  });
}

describe('HTTP tasks', () => {
  it('POSTs the job to {baseUrl}/{job name} with the secret, id and payload', async () => {
    const { job } = await db.queue.enqueue({ name: 'ok', queue: 'tasks', payload: { bookingId: 7 } });
    await startHttpWorker();
    const done = await finalState(job.id);

    expect(done.state).toBe('succeeded');
    expect(done.result).toEqual({ sent: true });
    expect(received[0]).toMatchObject({
      path: '/tasks/ok',
      headers: {
        'x-conveyor-secret': 's3cret',
        'x-conveyor-job-id': String(job.id),
        'x-conveyor-attempt': '1',
      },
      body: { jobId: job.id, attempt: 1, payload: { bookingId: 7 } },
    });
  });

  it('retries 5xx responses with backoff until they succeed', async () => {
    const { job } = await db.queue.enqueue({ name: 'flaky', queue: 'tasks', backoffBaseMs: 20 });
    await startHttpWorker();
    const done = await finalState(job.id);
    expect(done).toMatchObject({ state: 'succeeded', attempts: 3 });
  });

  it('treats other 4xx responses as permanent: no retries', async () => {
    const { job } = await db.queue.enqueue({ name: 'bad-input', queue: 'tasks', maxAttempts: 5 });
    await startHttpWorker();
    const done = await finalState(job.id);
    expect(done).toMatchObject({ state: 'dead', attempts: 1 });
    expect(done.last_error).toContain('HTTP 422');
    expect(done.last_error).toContain('booking 99 does not exist');
  });

  it('a wrong secret is rejected by the service and not retried', async () => {
    const { job } = await db.queue.enqueue({ name: 'ok', queue: 'tasks' });
    await startHttpWorker('wrong');
    const done = await finalState(job.id);
    expect(done).toMatchObject({ state: 'dead', attempts: 1 });
    expect(done.last_error).toContain('HTTP 401');
  });

  it('times out a request that never answers, and retries it', async () => {
    const { job } = await db.queue.enqueue({
      name: 'slow',
      queue: 'tasks',
      timeoutMs: 150,
      maxAttempts: 2,
      backoffBaseMs: 10,
    });
    await startHttpWorker();
    const done = await finalState(job.id);
    expect(done).toMatchObject({ state: 'dead', attempts: 2 });
    expect(done.last_error).toContain('TimeoutError');
  });

  it('retries when the service is down', async () => {
    const handler = createHttpHandler({ baseUrl: 'http://127.0.0.1:1/tasks', secret: 's3cret' });
    const { job } = await db.queue.enqueue({ name: 'ok', queue: 'down', maxAttempts: 2, backoffBaseMs: 10 });
    await db.worker({ queues: ['down'], handlers: {}, defaultHandler: handler }).start();
    const done = await finalState(job.id);
    expect(done).toMatchObject({ state: 'dead', attempts: 2 });
    expect(done.last_error).toContain('Could not reach');
  });
});

/**
 * HTTP task worker: delivers every job in the given queues to your service over HTTP.
 *
 *   npm run worker:http -- --queues booking-tasks,booking-emails
 *
 * Settings (in .env):
 *   TASKS_URL     base URL; job "send-ticket-email" goes to {TASKS_URL}/send-ticket-email
 *   TASKS_SECRET  shared secret, sent as the X-Conveyor-Secret header
 */
import { parseArgs } from 'node:util';
import { formatMs, Worker } from './core/worker.js';
import { createPool } from './db/pool.js';
import { createHttpHandler } from './http/handler.js';

const { values } = parseArgs({
  options: {
    queues: { type: 'string' },
    concurrency: { type: 'string', default: '10' },
    quiet: { type: 'boolean', default: false },
  },
});

const tasksUrl = process.env.TASKS_URL;
if (!tasksUrl || !values.queues) {
  console.error('Usage: TASKS_URL=http://host/path npm run worker:http -- --queues q1,q2');
  console.error('Set TASKS_URL (and TASKS_SECRET) in .env.');
  process.exit(1);
}
if (!process.env.TASKS_SECRET) {
  console.warn('[http-worker] TASKS_SECRET is not set: requests will be sent without a secret.');
}

const concurrency = Number(values.concurrency);
const queues: string[] = [];
const queueConcurrency: Record<string, number> = {};
for (const part of values.queues
  .split(',')
  .map((q) => q.trim())
  .filter(Boolean)) {
  const [name, cap] = part.split(':');
  queues.push(name!);
  if (cap) queueConcurrency[name!] = Number(cap);
}

const pool = createPool(undefined, concurrency + 4);
const worker = new Worker(pool, {
  queues,
  queueConcurrency,
  concurrency,
  handlers: {},
  defaultHandler: createHttpHandler({ baseUrl: tasksUrl, secret: process.env.TASKS_SECRET }),
});

const tag = `[http-worker ${worker.id}]`;
const log = (msg: string) =>
  !values.quiet && console.log(`${new Date().toISOString().slice(11, 23)} ${tag} ${msg}`);

worker.on('started', () =>
  console.log(`${tag} started: queues=${queues.join(',')} -> ${tasksUrl}/<job name>`),
);
worker.on('job:succeeded', (job, ms) => log(`OK    #${job.id} ${job.name} (${formatMs(ms)})`));
worker.on('job:retry', (job, err, delay) =>
  log(
    `RETRY #${job.id} ${job.name} attempt ${job.attempts}/${job.max_attempts}: ${err.message} -> next in ${formatMs(delay)}`,
  ),
);
worker.on('job:dead', (job, err) => log(`DEAD  #${job.id} ${job.name}: ${err.message}`));
worker.on('job:lost', (job) => log(`LOST  #${job.id} ${job.name}: lease was taken over, result discarded`));
worker.on('jobs:recovered', (n) => log(`recovered ${n} job(s) from crashed workers`));
worker.on('error', (err) => console.error(`${tag} error: ${err.message}`));

let stopping = false;
async function shutdown(signal: string) {
  if (stopping) return;
  stopping = true;
  console.log(`${tag} ${signal} received: finishing ${worker.inFlight} running job(s)...`);
  await worker.stop();
  await pool.end();
  console.log(`${tag} stopped cleanly`);
  process.exit(0);
}
process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

await worker.start();

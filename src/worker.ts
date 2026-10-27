/**
 * Worker process.
 *
 *   npm run worker
 *   npm run worker -- --queues email,images --concurrency 10
 *   npm run worker -- --queues email,reports:2      (at most 2 reports at once)
 *
 * Start as many as you like (in separate terminals or containers); they share the
 * work through PostgreSQL. Ctrl+C shuts down gracefully.
 */
import { parseArgs } from 'node:util';
import { formatMs, Worker } from './core/worker.js';
import { createPool } from './db/pool.js';
import { demoHandlers } from './demo/handlers.js';

const { values } = parseArgs({
  options: {
    queues: { type: 'string', default: 'email,images,webhooks,reports:3' },
    concurrency: { type: 'string', default: '10' },
    quiet: { type: 'boolean', default: false },
  },
});

const concurrency = Number(values.concurrency);
// "reports:3" means: serve the reports queue, but run at most 3 of its jobs at once.
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
const worker = new Worker(pool, { queues, queueConcurrency, concurrency, handlers: demoHandlers });

const tag = `[worker ${worker.id}]`;
const log = (msg: string) =>
  !values.quiet && console.log(`${new Date().toISOString().slice(11, 23)} ${tag} ${msg}`);

worker.on('started', () =>
  console.log(`${tag} started: queues=${worker.queues.join(',')} concurrency=${concurrency}`),
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
worker.on('job:log', (job, msg) => log(`      #${job.id} ${msg}`));
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

/**
 * Benchmark: throughput and pickup latency.
 *
 *   npm run benchmark
 *
 * Uses TEST_DATABASE_URL (the test database, which is wiped anyway).
 * Workers run in this one process, so the numbers are a floor: separate worker
 * processes on separate machines would each bring their own CPU.
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { cpus } from 'node:os';
import { config } from '../src/config.js';
import { Queue } from '../src/core/queue.js';
import { Worker } from '../src/core/worker.js';
import { migrate } from '../src/db/migrate.js';
import { createPool, type Pool } from '../src/db/pool.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function reset(pool: Pool) {
  await pool.query('TRUNCATE jobs, job_events, queues, workers RESTART IDENTITY CASCADE');
}

async function throughput(pool: Pool, workers: number, jobs: number, concurrency: number) {
  await reset(pool);
  const queue = new Queue(pool);
  for (let i = 0; i < jobs; i += 5000) {
    await queue.enqueueMany(
      Array.from({ length: Math.min(5000, jobs - i) }, () => ({ name: 'noop', queue: 'bench' })),
    );
  }
  const pools = Array.from({ length: workers }, () => createPool(config.testDatabaseUrl, concurrency + 4));
  const ws = pools.map(
    (p) =>
      new Worker(p, {
        queues: ['bench'],
        concurrency,
        handlers: { noop: async () => null },
        maintenance: false,
      }),
  );
  const started = performance.now();
  await Promise.all(ws.map((w) => w.start()));
  for (;;) {
    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM jobs WHERE state = 'succeeded'`);
    if (rows[0].n >= jobs) break;
    await sleep(20);
  }
  const seconds = (performance.now() - started) / 1000;
  await Promise.all(ws.map((w) => w.stop()));
  await Promise.all(pools.map((p) => p.end()));
  return jobs / seconds;
}

async function pickupLatency(pool: Pool, listen: boolean, samples: number) {
  await reset(pool);
  const queue = new Queue(pool);
  const w = new Worker(pool, {
    queues: ['latency'],
    concurrency: 5,
    handlers: { noop: async () => null },
    listen,
    pollIntervalMs: 1000,
    maintenance: false,
  });
  await w.start();
  await sleep(200);
  for (let i = 0; i < samples; i++) {
    await queue.enqueue({ name: 'noop', queue: 'latency' });
    await sleep(25 + Math.random() * 25);
  }
  await sleep(1500);
  await w.stop();
  // Both timestamps come from the database clock, so app/DB clock skew doesn't matter.
  const { rows } = await pool.query<{ p50: number; p95: number }>(
    `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50, percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95
     FROM (SELECT EXTRACT(EPOCH FROM started_at - created_at) * 1000 AS ms FROM jobs WHERE started_at IS NOT NULL) t`,
  );
  return rows[0]!;
}

const pool = createPool(config.testDatabaseUrl, 10);
await migrate(pool);

const JOBS = 20_000;
const CONCURRENCY = 20;
const lines: string[] = [
  `# Benchmark results`,
  '',
  `Machine: ${cpus().length} CPU cores (${cpus()[0]?.model.trim()}), Node ${process.version}, PostgreSQL on the same machine.`,
  `All workers run inside one Node process, so these are conservative numbers.`,
  '',
  `## Throughput (${JOBS.toLocaleString()} no-op jobs, concurrency ${CONCURRENCY} per worker)`,
  '',
  '| Workers | Jobs / second |',
  '|---|---|',
];
for (const n of [1, 2, 4]) {
  const rate = await throughput(pool, n, JOBS, CONCURRENCY);
  console.log(`throughput: ${n} worker(s) -> ${Math.round(rate).toLocaleString()} jobs/s`);
  lines.push(`| ${n} | ${Math.round(rate).toLocaleString()} |`);
}

lines.push(
  '',
  '## Pickup latency (time from enqueue to a worker starting the job)',
  '',
  '| Wake-up method | p50 | p95 |',
  '|---|---|---|',
);
for (const listen of [true, false]) {
  const { p50, p95 } = await pickupLatency(pool, listen, 100);
  const label = listen ? 'LISTEN/NOTIFY' : 'Polling every 1 s';
  console.log(`latency: ${label} -> p50 ${p50.toFixed(1)} ms, p95 ${p95.toFixed(1)} ms`);
  lines.push(`| ${label} | ${p50.toFixed(1)} ms | ${p95.toFixed(1)} ms |`);
}

await reset(pool);
await pool.end();
await mkdir(new URL('../docs/', import.meta.url), { recursive: true });
await writeFile(new URL('../docs/benchmark.md', import.meta.url), `${lines.join('\n')}\n`);
console.log('Saved docs/benchmark.md');

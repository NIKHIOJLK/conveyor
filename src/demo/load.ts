/**
 * Enqueue a batch of realistic demo jobs.
 *
 *   npm run load                 # 500 jobs
 *   npm run load -- --count 5000
 */
import { parseArgs } from 'node:util';
import { Queue } from '../core/queue.js';
import { createPool } from '../db/pool.js';
import { demoJobs } from './handlers.js';

const { values } = parseArgs({ options: { count: { type: 'string', default: '500' } } });
const count = Number(values.count);

const pool = createPool();
const queue = new Queue(pool);
const started = performance.now();
let created = 0;
for (let i = 0; i < count; i += 1000) {
  created += (await queue.enqueueMany(demoJobs(Math.min(1000, count - i)))).length;
}
console.log(`Enqueued ${created} jobs in ${Math.round(performance.now() - started)} ms`);
await pool.end();

/**
 * API server entry point.
 *
 *   npm run api      -> http://localhost:3000 (dashboard) and /api/*
 */
import { config } from '../config.js';
import { createPool } from '../db/pool.js';
import { createApp } from './server.js';

const pool = createPool();
const app = createApp(pool, { demoMode: config.demoMode });
const server = app.listen(config.port, () => {
  console.log(`[api] dashboard: http://localhost:${config.port}`);
  console.log(`[api] REST API:  http://localhost:${config.port}/api   metrics: /metrics`);
});

async function shutdown() {
  app.closeLive();
  server.close();
  await pool.end();
  process.exit(0);
}
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());

import pg from 'pg';
import { config } from '../config.js';

// Return BIGINT (int8) columns such as job ids as JS numbers instead of strings.
// Safe here: ids stay far below Number.MAX_SAFE_INTEGER (9 quadrillion).
pg.types.setTypeParser(20, (value) => Number.parseInt(value, 10));

export type Pool = pg.Pool;

export function createPool(connectionString = config.databaseUrl, max = 10): pg.Pool {
  const pool = new pg.Pool({ connectionString, max });
  // An idle client erroring (e.g. the database restarted) must not crash the process.
  pool.on('error', (err) => console.error('[db] idle client error:', err.message));
  return pool;
}

/**
 * Tiny migration runner: applies migrations/*.sql in filename order, once each.
 * An advisory lock means several containers starting at once can't run the same
 * migration twice.
 *
 *   npm run migrate
 */
import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createPool, type Pool } from './pool.js';

const MIGRATIONS_DIR = fileURLToPath(new URL('../../migrations/', import.meta.url));
const LOCK_ID = 727_001; // any constant; identifies "conveyor migrations"

export async function migrate(pool: Pool, log: (msg: string) => void = () => {}): Promise<string[]> {
  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query('SELECT pg_advisory_lock($1)', [LOCK_ID]);
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
    const done = new Set(
      (await client.query<{ name: string }>('SELECT name FROM schema_migrations')).rows.map((r) => r.name),
    );
    const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();
    for (const file of files) {
      if (done.has(file)) continue;
      const sql = await readFile(MIGRATIONS_DIR + file, 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        throw new Error(`Migration ${file} failed: ${(err as Error).message}`);
      }
      applied.push(file);
      log(`applied ${file}`);
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [LOCK_ID]).catch(() => {});
    client.release();
  }
  return applied;
}

// Run directly: `tsx src/db/migrate.ts`
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const pool = createPool();
  migrate(pool, (m) => console.log(`[migrate] ${m}`))
    .then((applied) => console.log(`[migrate] ${applied.length ? 'done' : 'already up to date'}`))
    .catch((err) => {
      console.error(`[migrate] ${err.message}`);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

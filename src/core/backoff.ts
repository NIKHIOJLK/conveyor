export const MAX_BACKOFF_MS = 60 * 60 * 1000; // never wait more than an hour between attempts

/**
 * Exponential backoff with "equal jitter".
 *
 * attempt 1 failed -> ~base, attempt 2 -> ~2x base, attempt 3 -> ~4x base, ...
 * The delay is randomised between 50% and 100% of that value so that thousands
 * of jobs that failed together (e.g. an email provider outage) don't all retry at
 * the same instant and knock the provider over again ("thundering herd").
 */
export function backoffDelay(
  attempt: number,
  baseMs: number,
  random: () => number = Math.random,
  maxMs = MAX_BACKOFF_MS,
): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(exp / 2 + random() * (exp / 2));
}

/**
 * HTTP tasks: run any job by calling your own service over HTTP.
 *
 * A job named `send-ticket-email` in a queue served by the HTTP worker becomes
 *
 *   POST {baseUrl}/send-ticket-email
 *   X-Conveyor-Secret: <shared secret>
 *   X-Conveyor-Job-Id: 42
 *   X-Conveyor-Attempt: 1
 *   { "jobId": 42, "attempt": 1, "payload": { ...the job's payload... } }
 *
 * So a service written in any language (Java, Python, ...) can enqueue jobs with a
 * plain SQL INSERT and receive them as ordinary HTTP requests, with Conveyor doing
 * the scheduling, retries, backoff, dead-lettering and rate limiting. This is the
 * same model as Google Cloud Tasks.
 *
 * How the response decides what happens next:
 *   2xx                      -> succeeded (a JSON body is stored as the job's result)
 *   408, 429, 5xx, no answer -> failed, retried with backoff
 *   any other 4xx            -> permanent failure, straight to the dead-letter queue
 */
import { PermanentError } from '../core/errors.js';
import type { JobContext, JobHandler } from '../core/types.js';

export interface HttpHandlerOptions {
  /** Base URL; the job name is appended, e.g. http://localhost:8080/internal/tasks */
  baseUrl: string;
  /** Sent as X-Conveyor-Secret so the service can reject requests that aren't from Conveyor. */
  secret?: string;
  /** Injected for tests. */
  fetch?: typeof fetch;
}

const RETRYABLE_4XX = new Set([408, 425, 429]);
const MAX_ERROR_BODY = 300;

export function createHttpHandler(options: HttpHandlerOptions): JobHandler {
  const baseUrl = options.baseUrl.replace(/\/+$/, '');
  const doFetch = options.fetch ?? fetch;

  return async (payload: unknown, ctx: JobContext) => {
    const url = `${baseUrl}/${encodeURIComponent(ctx.job.name)}`;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      'X-Conveyor-Job-Id': String(ctx.job.id),
      'X-Conveyor-Attempt': String(ctx.job.attempts),
      'X-Conveyor-Queue': ctx.job.queue,
    };
    if (options.secret) headers['X-Conveyor-Secret'] = options.secret;

    let res: Response;
    try {
      res = await doFetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ jobId: ctx.job.id, attempt: ctx.job.attempts, payload }),
        signal: ctx.signal, // aborted on timeout or shutdown
      });
    } catch (err) {
      if (ctx.signal.aborted) throw ctx.signal.reason;
      // Connection refused, DNS failure, reset: the service may just be restarting.
      const cause = (err as { cause?: { code?: string } }).cause?.code;
      throw new Error(`Could not reach ${url}${cause ? ` (${cause})` : ''}`);
    }

    const text = await res.text();
    if (res.ok) {
      if (!text) return { status: res.status };
      try {
        return JSON.parse(text);
      } catch {
        return { status: res.status, body: text.slice(0, MAX_ERROR_BODY) };
      }
    }

    const message = `HTTP ${res.status} from ${url}${text ? `: ${summarize(text)}` : ''}`;
    if (res.status >= 500 || RETRYABLE_4XX.has(res.status)) throw new Error(message);
    throw new PermanentError(message);
  };
}

/** Pull the useful part out of an error body (JSON {"error": ...} or plain text). */
function summarize(text: string): string {
  try {
    const body = JSON.parse(text) as { error?: unknown; message?: unknown };
    const msg = body.error ?? body.message;
    if (typeof msg === 'string') return msg.slice(0, MAX_ERROR_BODY);
  } catch {
    // not JSON
  }
  return text.replace(/\s+/g, ' ').slice(0, MAX_ERROR_BODY);
}

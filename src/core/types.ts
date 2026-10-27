export type JobState = 'queued' | 'running' | 'succeeded' | 'dead' | 'cancelled';

/** A row of the jobs table. */
export interface Job<P = unknown> {
  id: number;
  queue: string;
  name: string;
  payload: P;
  state: JobState;
  priority: number;
  run_at: Date;
  attempts: number;
  max_attempts: number;
  backoff_base_ms: number;
  timeout_ms: number;
  idempotency_key: string | null;
  locked_by: string | null;
  locked_until: Date | null;
  last_error: string | null;
  result: unknown;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  updated_at: Date;
}

export interface JobEvent {
  id: number;
  job_id: number;
  type: string;
  attempt: number | null;
  worker: string | null;
  message: string | null;
  duration_ms: number | null;
  created_at: Date;
}

export interface EnqueueOptions<P = unknown> {
  /** Handler name, e.g. 'send-email'. */
  name: string;
  payload?: P;
  queue?: string;
  /** Higher runs first. Default 0. */
  priority?: number;
  /** Run no earlier than this time. */
  runAt?: Date;
  /** Or: run after this many milliseconds. */
  delayMs?: number;
  /** Total tries including the first. Default 5. */
  maxAttempts?: number;
  /** First retry waits ~this long, then it doubles. Default 1000. */
  backoffBaseMs?: number;
  /** Per-attempt time limit. Default 30000. */
  timeoutMs?: number;
  /** Enqueueing the same key twice (in the same queue) returns the original job. */
  idempotencyKey?: string;
}

export interface EnqueueResult<P = unknown> {
  job: Job<P>;
  /** True when an existing job was returned because of the idempotency key. */
  deduplicated: boolean;
}

/** What a handler receives besides the payload. */
export interface JobContext {
  job: Pick<Job, 'id' | 'name' | 'queue' | 'attempts' | 'max_attempts' | 'created_at'>;
  /** Aborted on timeout, lost lease or forced shutdown. Long-running handlers should listen to it. */
  signal: AbortSignal;
  log: (message: string) => void;
}

// biome-ignore lint/suspicious/noExplicitAny: lets a handler map hold handlers with different payload types
export type JobHandler<P = any> = (payload: P, ctx: JobContext) => Promise<unknown>;

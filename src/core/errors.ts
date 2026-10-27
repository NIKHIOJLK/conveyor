/** Throw from a handler when retrying cannot help (bad input, record deleted...). The job goes straight to dead. */
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentError';
  }
}

/** The attempt ran longer than the job's timeout_ms. Retried like any other failure. */
export class TimeoutError extends Error {
  constructor(ms: number) {
    super(`Timed out after ${ms} ms`);
    this.name = 'TimeoutError';
  }
}

/** This worker's lease was taken over (it was presumed dead). Its result must be discarded. */
export class LeaseLostError extends Error {
  constructor() {
    super('Lease lost: another worker now owns this job');
    this.name = 'LeaseLostError';
  }
}

/** The worker shut down before the job finished; the job was returned to the queue. */
export class ShutdownError extends Error {
  constructor() {
    super('Worker shutting down');
    this.name = 'ShutdownError';
  }
}

export function isPermanent(err: unknown): boolean {
  return err instanceof PermanentError || (err instanceof Error && err.name === 'PermanentError');
}

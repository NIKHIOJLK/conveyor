/**
 * Demo job handlers that behave like real background work: variable latency,
 * occasional transient failures (worth retrying), rare permanent failures (not
 * worth retrying) and the odd job that runs past its timeout.
 */
import { PermanentError } from '../core/errors.js';
import type { EnqueueOptions, JobContext, JobHandler } from '../core/types.js';

const rand = (min: number, max: number) => min + Math.random() * (max - min);

/** Sleep that stops early if the job is aborted (timeout or shutdown). */
function work(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(t);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}

export const demoHandlers: Record<string, JobHandler> = {
  async 'send-email'(payload: { to: string; template: string }, { signal }: JobContext) {
    await work(rand(80, 300), signal);
    const r = Math.random();
    if (r < 0.02) throw new PermanentError(`Mailbox ${payload.to} does not exist`);
    if (r < 0.1) throw new Error('SMTP server timed out');
    return { messageId: `msg_${Math.random().toString(36).slice(2, 10)}` };
  },

  async 'resize-image'(payload: { imageId: string; sizes: number[] }, { signal }: JobContext) {
    await work(rand(200, 700), signal);
    if (Math.random() < 0.04) throw new Error('Object storage temporarily unavailable');
    return { imageId: payload.imageId, variants: payload.sizes.length };
  },

  /** Slow and sometimes slower than its 3 s timeout. */
  async 'generate-report'(payload: { reportId: string }, { signal }: JobContext) {
    await work(rand(1000, 3600), signal);
    return { reportId: payload.reportId, url: `/reports/${payload.reportId}.pdf` };
  },

  /** A flaky partner API: about a third of webhooks fail their first two attempts. */
  async 'deliver-webhook'(payload: { url: string; flaky: boolean }, { job, signal }: JobContext) {
    await work(rand(50, 200), signal);
    if (payload.flaky && job.attempts < 3) throw new Error('Partner API responded 503 Service Unavailable');
    return { status: 200 };
  },

  /** Does nothing; used by the benchmark to measure pure queue overhead. */
  async noop() {
    return null;
  },
};

/** A realistic mix of demo jobs across four queues. */
export function demoJobs(count: number): EnqueueOptions[] {
  const jobs: EnqueueOptions[] = [];
  for (let i = 0; i < count; i++) {
    const r = Math.random();
    const n = Math.floor(Math.random() * 1e6);
    if (r < 0.45) {
      jobs.push({
        queue: 'email',
        name: 'send-email',
        backoffBaseMs: 1000,
        payload: { to: `user${n}@example.com`, template: 'welcome' },
        priority: Math.random() < 0.1 ? 10 : 0, // password resets jump the line
      });
    } else if (r < 0.75) {
      jobs.push({
        queue: 'images',
        name: 'resize-image',
        payload: { imageId: `img_${n}`, sizes: [64, 256, 1024] },
      });
    } else if (r < 0.9) {
      jobs.push({
        queue: 'webhooks',
        name: 'deliver-webhook',
        backoffBaseMs: 500,
        payload: { url: `https://partner.example.com/hooks/${n}`, flaky: Math.random() < 0.33 },
      });
    } else {
      jobs.push({
        queue: 'reports',
        name: 'generate-report',
        timeoutMs: 3000,
        maxAttempts: 3,
        payload: { reportId: `rep_${n}` },
      });
    }
  }
  return jobs;
}

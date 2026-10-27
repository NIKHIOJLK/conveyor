/** Prometheus text exposition format, so the queue can be graphed and alerted on (e.g. in Grafana). */
import type { snapshot } from './server.js';

type Snapshot = Awaited<ReturnType<typeof snapshot>>;

export function renderMetrics(s: Snapshot): string {
  const lines: string[] = [];
  const metric = (name: string, help: string, type: 'gauge' | 'counter') => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
  };
  const esc = (v: string) => v.replace(/\\/g, '\\\\').replace(/"/g, '\\"');

  metric('conveyor_jobs', 'Jobs by queue and state', 'gauge');
  for (const q of s.queues) {
    for (const state of [
      'waiting',
      'scheduled',
      'retrying',
      'running',
      'succeeded',
      'dead',
      'cancelled',
    ] as const) {
      lines.push(`conveyor_jobs{queue="${esc(q.queue)}",state="${state}"} ${q[state]}`);
    }
  }
  metric('conveyor_queue_lag_seconds', 'Age of the oldest job that is due but not started', 'gauge');
  for (const q of s.queues)
    lines.push(`conveyor_queue_lag_seconds{queue="${esc(q.queue)}"} ${q.lag_seconds.toFixed(3)}`);

  metric('conveyor_queue_paused', '1 if the queue is paused', 'gauge');
  for (const q of s.queues) lines.push(`conveyor_queue_paused{queue="${esc(q.queue)}"} ${q.paused ? 1 : 0}`);

  metric('conveyor_workers_alive', 'Workers that sent a heartbeat in the last 15 seconds', 'gauge');
  lines.push(`conveyor_workers_alive ${s.workers.filter((w) => w.alive).length}`);

  metric('conveyor_throughput_per_second', 'Jobs completed per second (10 s average)', 'gauge');
  lines.push(`conveyor_throughput_per_second ${s.perSecond.toFixed(2)}`);
  return `${lines.join('\n')}\n`;
}

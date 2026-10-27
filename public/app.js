// Conveyor dashboard: plain JavaScript, no build step.
// Live numbers arrive once a second over Server-Sent Events (/api/events).

const $ = (sel, el = document) => el.querySelector(sel);
const fmt = (n) => Number(n).toLocaleString();
const esc = (s) =>
  String(s ?? '').replace(
    /[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c],
  );

const ui = { state: '', queue: '', openJob: null, tick: 0, snapshot: null };
const STATE_LABEL = {
  waiting: 'Waiting',
  scheduled: 'Scheduled',
  retrying: 'Retrying',
  running: 'Running',
  succeeded: 'Succeeded',
  dead: 'Dead',
  cancelled: 'Cancelled',
};

// ---------------------------------------------------------------- helpers

function ago(iso) {
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return new Date(iso).toLocaleDateString();
}

function duration(ms) {
  if (ms == null) return '';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 60000)} min`;
}

function displayState(job) {
  if (job.state !== 'queued') return job.state;
  if (new Date(job.run_at) <= new Date()) return 'waiting';
  return job.attempts > 0 ? 'retrying' : 'scheduled';
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `Request failed (${res.status})`);
  return body;
}

let toastTimer;
function toast(message) {
  const el = $('#toast');
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.hidden = true;
  }, 3200);
}

// ---------------------------------------------------------------- the line

// Build the belt chevrons once.
for (const id of ['chev-a', 'chev-b']) {
  const g = document.getElementById(id);
  const x0 = id === 'chev-a' ? 232 : 572;
  let html = '';
  for (let x = x0 - 24; x < x0 + 190; x += 24) html += `<path d="M${x} 132 l7 6 -7 6"/>`;
  g.innerHTML = html;
}

function renderLine(s) {
  const t = s.totals;
  for (const el of document.querySelectorAll('[data-count]')) {
    el.textContent = fmt(t[el.dataset.count] ?? 0);
  }
  for (const st of document.querySelectorAll('.station')) {
    st.classList.toggle(
      'is-active',
      (t[st.dataset.state] ?? 0) > 0 && ['running', 'retrying', 'dead'].includes(st.dataset.state),
    );
  }
  // The belt only moves while jobs are actually flowing; faster flow, faster belt.
  const line = $('#line');
  if (s.perSecond > 0.05 || t.running > 0) {
    const secs = Math.max(0.25, Math.min(2.5, 3 / Math.sqrt(Math.max(s.perSecond, 1))));
    line.style.setProperty('--belt-duration', `${secs.toFixed(2)}s`);
    line.style.setProperty('--belt-play', 'running');
  } else {
    line.style.setProperty('--belt-play', 'paused');
  }
  $('#line-list').innerHTML = [
    'waiting',
    'scheduled',
    'running',
    'retrying',
    'succeeded',
    'dead',
    'cancelled',
  ]
    .map(
      (k) => `<li style="--c: var(--${k})"><span>${STATE_LABEL[k]}</span><strong>${fmt(t[k])}</strong></li>`,
    )
    .join('');
}

// ---------------------------------------------------------------- throughput chart

function renderChart(series) {
  const svg = $('#chart-svg');
  const W = 600,
    H = 180,
    gap = 2;
  const max = Math.max(5, ...series.map((p) => p.succeeded + p.failed + p.dead));
  const bw = W / series.length;
  let html = '';
  for (const f of [0.25, 0.5, 0.75])
    html += `<line class="grid" x1="0" x2="${W}" y1="${H * f}" y2="${H * f}"/>`;
  series.forEach((p, i) => {
    const x = i * bw + gap / 2;
    const okH = (p.succeeded / max) * H;
    const failH = ((p.failed + p.dead) / max) * H;
    if (okH > 0)
      html += `<rect class="bar-ok" x="${x}" y="${H - okH}" width="${bw - gap}" height="${okH}" rx="1.5"/>`;
    if (failH > 0)
      html += `<rect class="bar-fail" x="${x}" y="${H - okH - failH - (okH > 0 ? 1.5 : 0)}" width="${bw - gap}" height="${failH}" rx="1.5"/>`;
    html += `<rect class="hit" data-i="${i}" x="${i * bw}" y="0" width="${bw}" height="${H}"/>`;
  });
  svg.innerHTML = html;
  svg.dataset.max = max;
  renderChart.series = series;
}

$('#chart-svg').addEventListener('mousemove', (e) => {
  const i = e.target.dataset?.i;
  const tip = $('#tooltip');
  if (i == null) {
    tip.hidden = true;
    return;
  }
  const p = renderChart.series[i];
  const rect = $('#chart').getBoundingClientRect();
  tip.innerHTML = `${new Date(p.ts * 1000).toLocaleTimeString()}<br>${p.succeeded} completed, ${p.failed + p.dead} failed`;
  tip.style.left = `${e.clientX - rect.left}px`;
  tip.style.top = `${e.clientY - rect.top}px`;
  tip.hidden = false;
});
$('#chart-svg').addEventListener('mouseleave', () => {
  $('#tooltip').hidden = true;
});

// ---------------------------------------------------------------- workers

function renderWorkers(workers) {
  const el = $('#workers');
  if (!workers.length) {
    el.innerHTML =
      '<li class="workers-empty">No workers running. Start one with <code>npm run worker</code>.</li>';
    return;
  }
  el.innerHTML = workers
    .map(
      (w) => `
    <li class="worker" data-alive="${w.alive}">
      <span class="worker-name" title="${esc(w.id)}">${esc(w.id)}</span>
      <span class="worker-status">${w.alive ? `${w.running}/${w.concurrency} busy` : `silent for ${ago(w.last_seen).replace(' ago', '')}`}</span>
      <div class="load" aria-hidden="true"><span style="width:${w.alive ? (100 * w.running) / w.concurrency : 0}%"></span></div>
      <span class="worker-meta">Queues: ${esc(w.queues.join(', '))}. Up ${ago(w.started_at).replace(' ago', '')}.</span>
    </li>`,
    )
    .join('');
}

// ---------------------------------------------------------------- queues

function cell(n, extra = '') {
  return `<td class="num ${n === 0 ? 'zero' : ''} ${extra}">${fmt(n)}</td>`;
}

function renderQueues(queues) {
  const tbody = $('#queues tbody');
  if (tbody.contains(document.activeElement) && document.activeElement.tagName === 'INPUT') return; // don't wipe an edit in progress
  tbody.innerHTML = queues
    .map(
      (q) => `
    <tr>
      <td><span class="queue-name">${esc(q.queue)}</span>${q.paused ? '<span class="paused-tag">paused</span>' : ''}</td>
      ${cell(q.waiting)}${cell(q.running)}${cell(q.retrying)}${cell(q.dead)}${cell(q.succeeded)}
      <td class="num">${q.lag_seconds < 1 ? '<span class="zero">&lt;1s</span>' : duration(q.lag_seconds * 1000)}</td>
      <td>
        <form class="rate" data-queue="${esc(q.queue)}">
          <label class="visually-hidden" for="rate-${esc(q.queue)}">Jobs per second for ${esc(q.queue)}</label>
          <input id="rate-${esc(q.queue)}" type="number" min="0.1" step="any" placeholder="none" value="${q.rate_limit ?? ''}">
          <span class="rate-value">/s</span>
          <button class="btn btn-ghost btn-small" type="submit">Set</button>
        </form>
      </td>
      <td>
        <div class="row-actions">
          ${q.dead > 0 ? `<button class="btn btn-ghost btn-small" data-action="retry-dead" data-queue="${esc(q.queue)}">Retry ${fmt(q.dead)} dead</button>` : ''}
          <button class="btn btn-ghost btn-small" data-action="${q.paused ? 'resume' : 'pause'}" data-queue="${esc(q.queue)}">${q.paused ? 'Resume' : 'Pause'}</button>
        </div>
      </td>
    </tr>`,
    )
    .join('');

  const select = $('#queue-filter');
  const names = queues.map((q) => q.queue);
  if (select.options.length - 1 !== names.length) {
    select.innerHTML =
      '<option value="">All queues</option>' +
      names
        .map((n) => `<option value="${esc(n)}" ${n === ui.queue ? 'selected' : ''}>${esc(n)}</option>`)
        .join('');
  }
}

$('#queues').addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const { action, queue } = btn.dataset;
  btn.disabled = true;
  try {
    if (action === 'retry-dead') {
      const { retried } = await api(`/api/queues/${encodeURIComponent(queue)}/retry-dead`, {
        method: 'POST',
      });
      toast(`Retrying ${retried} dead job${retried === 1 ? '' : 's'} in ${queue}`);
    } else {
      await api(`/api/queues/${encodeURIComponent(queue)}/${action}`, { method: 'POST' });
      toast(action === 'pause' ? `Paused ${queue}: workers stop taking its jobs` : `Resumed ${queue}`);
    }
  } catch (err) {
    toast(err.message);
  } finally {
    btn.disabled = false;
  }
});

$('#queues').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const queue = form.dataset.queue;
  const raw = form.querySelector('input').value.trim();
  const rate = raw === '' ? null : Number(raw);
  try {
    await api(`/api/queues/${encodeURIComponent(queue)}/rate-limit`, {
      method: 'PUT',
      body: JSON.stringify({ rate }),
    });
    form.querySelector('input').blur();
    toast(
      rate ? `${queue} limited to ${rate} jobs/s across all workers` : `Removed the rate limit on ${queue}`,
    );
  } catch (err) {
    toast(err.message);
  }
});

// ---------------------------------------------------------------- jobs table

async function loadJobs() {
  const params = new URLSearchParams({ limit: '15' });
  if (ui.state) params.set('state', ui.state);
  if (ui.queue) params.set('queue', ui.queue);
  const { jobs } = await api(`/api/jobs?${params}`);
  $('#jobs-empty').hidden = jobs.length > 0;
  $('#jobs tbody').innerHTML = jobs
    .map((j) => {
      const st = displayState(j);
      return `
      <tr>
        <td><button class="job-link" data-id="${j.id}"><span class="job-id">#${j.id}</span><strong>${esc(j.name)}</strong></button></td>
        <td>${esc(j.queue)}</td>
        <td><span class="pill" data-state="${st}">${STATE_LABEL[st]}</span></td>
        <td class="num">${j.attempts}/${j.max_attempts}</td>
        <td>${ago(j.created_at)}</td>
        <td class="error-cell" title="${esc(j.last_error)}">${esc(j.last_error ?? '')}</td>
      </tr>`;
    })
    .join('');
}

$('#state-tabs').addEventListener('click', (e) => {
  const btn = e.target.closest('button');
  if (!btn) return;
  for (const b of $('#state-tabs').children) b.setAttribute('aria-selected', String(b === btn));
  ui.state = btn.dataset.state;
  loadJobs().catch((err) => toast(err.message));
});

$('#queue-filter').addEventListener('change', (e) => {
  ui.queue = e.target.value;
  loadJobs().catch((err) => toast(err.message));
});

$('#jobs').addEventListener('click', (e) => {
  const btn = e.target.closest('.job-link');
  if (btn) openJob(Number(btn.dataset.id));
});

// ---------------------------------------------------------------- job drawer

const EVENT_LABEL = {
  enqueued: 'Enqueued',
  started: 'Started',
  succeeded: 'Succeeded',
  failed: 'Failed',
  dead: 'Moved to dead letter',
  lease_expired: 'Worker stopped responding',
  released: 'Returned to queue',
  retried: 'Retried',
  cancelled: 'Cancelled',
};

async function openJob(id) {
  ui.openJob = id;
  const drawer = $('#drawer');
  drawer.hidden = false;
  await renderJob(id);
  $('#drawer-close').focus();
}

async function renderJob(id) {
  let data;
  try {
    data = await api(`/api/jobs/${id}`);
  } catch (err) {
    $('#drawer-body').innerHTML = `<p class="error-box">${esc(err.message)}</p>`;
    return;
  }
  if (ui.openJob !== id) return;
  const { job, events } = data;
  const st = displayState(job);
  const t0 = new Date(events[0]?.created_at ?? job.created_at).getTime();
  $('#drawer-title').textContent = `#${job.id} ${job.name}`;
  $('#drawer-body').innerHTML = `
    <dl class="facts">
      <dt>State</dt><dd><span class="pill" data-state="${st}">${STATE_LABEL[st]}</span></dd>
      <dt>Queue</dt><dd>${esc(job.queue)}</dd>
      <dt>Attempts</dt><dd>${job.attempts} of ${job.max_attempts}</dd>
      <dt>Priority</dt><dd>${job.priority}</dd>
      <dt>Timeout</dt><dd>${duration(job.timeout_ms)}</dd>
      ${job.idempotency_key ? `<dt>Idempotency key</dt><dd>${esc(job.idempotency_key)}</dd>` : ''}
      ${st === 'retrying' || st === 'scheduled' ? `<dt>Next run</dt><dd>${new Date(job.run_at).toLocaleTimeString()}</dd>` : ''}
      ${job.locked_by ? `<dt>Worker</dt><dd>${esc(job.locked_by)}</dd>` : ''}
    </dl>
    <div class="actions">
      ${['dead', 'cancelled'].includes(job.state) ? '<button class="btn btn-brand btn-small" data-job-action="retry">Retry this job</button>' : ''}
      ${job.state === 'queued' ? '<button class="btn btn-danger btn-small" data-job-action="cancel">Cancel job</button>' : ''}
    </div>
    ${job.last_error ? `<h3>Last error</h3><p class="error-box">${esc(job.last_error)}</p>` : ''}
    <h3>Timeline</h3>
    <ol class="timeline">
      ${events
        .map(
          (ev) => `
        <li data-type="${esc(ev.type)}">
          <div class="ev-head">
            <span class="ev-type">${esc(EVENT_LABEL[ev.type] ?? ev.type)}${ev.attempt ? ` (attempt ${ev.attempt})` : ''}</span>
            <span class="ev-time">+${duration(new Date(ev.created_at).getTime() - t0) || '0 ms'}</span>
          </div>
          ${ev.message ? `<div class="ev-msg">${esc(ev.message)}</div>` : ''}
          ${ev.duration_ms != null ? `<div class="ev-msg">Took ${duration(ev.duration_ms)}${ev.worker ? ` on ${esc(ev.worker)}` : ''}</div>` : ''}
        </li>`,
        )
        .join('')}
    </ol>
    <h3>Payload</h3>
    <pre>${esc(JSON.stringify(job.payload, null, 2))}</pre>
    ${job.result != null ? `<h3>Result</h3><pre>${esc(JSON.stringify(job.result, null, 2))}</pre>` : ''}
  `;
}

$('#drawer-body').addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-job-action]');
  if (!btn || !ui.openJob) return;
  const action = btn.dataset.jobAction;
  try {
    await api(`/api/jobs/${ui.openJob}/${action}`, { method: 'POST' });
    toast(action === 'retry' ? `Job #${ui.openJob} is back in the queue` : `Cancelled job #${ui.openJob}`);
    await renderJob(ui.openJob);
    loadJobs();
  } catch (err) {
    toast(err.message);
  }
});

function closeDrawer() {
  $('#drawer').hidden = true;
  ui.openJob = null;
}
$('#drawer-close').addEventListener('click', closeDrawer);
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeDrawer();
});

// ---------------------------------------------------------------- demo load buttons

document.querySelectorAll('[data-load]').forEach((btn) => {
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    try {
      const { created } = await api('/api/demo/load', {
        method: 'POST',
        body: JSON.stringify({ count: Number(btn.dataset.load) }),
      });
      toast(`Added ${fmt(created)} jobs across email, images, webhooks and reports`);
      loadJobs();
    } catch (err) {
      toast(err.message);
    } finally {
      btn.disabled = false;
    }
  });
});

// ---------------------------------------------------------------- live connection

function render(s) {
  ui.snapshot = s;
  $('#stat-rate').textContent = s.perSecond >= 10 ? Math.round(s.perSecond) : s.perSecond.toFixed(1);
  $('#stat-workers').textContent = s.workers.filter((w) => w.alive).length;
  renderLine(s);
  renderChart(s.throughput);
  renderWorkers(s.workers);
  renderQueues(s.queues);
  ui.tick++;
  if (ui.tick % 2 === 0) loadJobs().catch(() => {});
  if (ui.openJob && ui.tick % 2 === 0) renderJob(ui.openJob);
}

function connect() {
  const conn = $('#connection');
  const es = new EventSource('/api/events');
  es.addEventListener('stats', (e) => {
    conn.dataset.state = 'live';
    $('#connection-text').textContent = 'Live';
    render(JSON.parse(e.data));
  });
  es.onerror = () => {
    conn.dataset.state = 'lost';
    $('#connection-text').textContent = 'Reconnecting';
  };
}

connect();
loadJobs().catch((err) => toast(err.message));

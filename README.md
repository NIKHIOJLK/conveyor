# Conveyor

A reliable background job queue built on PostgreSQL, with a live dashboard.

Web apps hand slow work (sending emails, resizing images, calling partner APIs, generating reports)
to Conveyor instead of making the user wait. Workers pick the jobs up, retry the ones that fail,
recover the ones a crashed worker left behind, and set aside the ones that can never succeed.

**TypeScript · Node.js · PostgreSQL · Express · Server-Sent Events · Vitest · Docker · GitHub Actions**

![Conveyor dashboard: live job lifecycle, throughput and workers](docs/dashboard.png)

> 📄 How it works, design decisions and interview prep: [WALKTHROUGH.md](WALKTHROUGH.md)

## Features

- **No job is ever run by two workers at once.** Workers claim jobs with `SELECT … FOR UPDATE SKIP LOCKED`,
  so they never block each other and never get the same job.
- **Retries with exponential backoff and jitter**: 1 s, 2 s, 4 s, 8 s… randomised so a burst of failures
  doesn't retry in lockstep.
- **Dead-letter queue**: jobs that keep failing (or throw a `PermanentError`) are set aside with their full
  error history, and can be retried from the dashboard.
- **Crash recovery**: a worker holds a *lease* on each job and renews it with heartbeats. Kill a worker
  with `kill -9` and its jobs are automatically re-run by another worker.
- **Fencing**: if a slow worker comes back after its lease was taken over, its late result is discarded
  instead of overwriting the new owner's.
- **Instant pickup** with PostgreSQL `LISTEN/NOTIFY`: about 2 ms from enqueue to start, versus ~470 ms with
  one-second polling.
- **Scheduling and priorities**: run a job later, or let urgent jobs jump the line.
- **Idempotency keys**: enqueue the same job twice (e.g. a retried HTTP request) and get the original back.
- **Global rate limits**: "at most 20 emails per second" holds across any number of workers (a token bucket
  stored in PostgreSQL).
- **Per-queue concurrency caps**, so slow jobs can't occupy every slot and starve other queues.
- **Timeouts**, **graceful shutdown** (running jobs finish; stragglers are handed back), **pause/resume**.
- **Live dashboard** over Server-Sent Events, a full timeline for every job, and **Prometheus metrics**.
- **43 tests against a real PostgreSQL**, including concurrency, crash recovery and rate limiting.

<p align="center"><img src="docs/job-timeline.png" width="380" alt="Timeline of a job that failed twice with backoff, then succeeded"></p>
<p align="center"><em>Every job keeps its history: a webhook that failed twice, backed off, and succeeded on attempt 3.</em></p>

## How it fits together

```mermaid
flowchart LR
    App["Your app<br/>POST /api/jobs<br/>or queue.enqueue()"] --> PG[("PostgreSQL<br/>jobs · job_events · queues")]
    PG -- "NOTIFY: new job" --> W1["Worker 1"]
    PG -- "NOTIFY" --> W2["Worker 2"]
    W1 -- "claim (SKIP LOCKED)<br/>heartbeat · complete · fail" --> PG
    W2 -- "claim · heartbeat<br/>complete · fail" --> PG
    PG --> API["API + dashboard<br/>(Server-Sent Events)"]
```

PostgreSQL is the only infrastructure. It stores the jobs *and* coordinates the workers, so there is no
Redis or RabbitMQ to run, and a job can be enqueued in the same database transaction as the data it
belongs to.

## Quick start (Windows, macOS or Linux)

You need **Node.js 20+** and **PostgreSQL 14+** (your local install is fine).

### 1. Install

```powershell
git clone https://github.com/NIKHIOJLK/conveyor.git
cd conveyor
npm install
```

### 2. Create the databases and configure

In pgAdmin or `psql`, create two empty databases: `conveyor` and `conveyor_test`.

```sql
CREATE DATABASE conveyor;
CREATE DATABASE conveyor_test;
```

Copy `.env.example` to `.env` and put your PostgreSQL password in both URLs:

```powershell
copy .env.example .env      # macOS/Linux: cp .env.example .env
```

```
DATABASE_URL=postgres://postgres:YOUR_PASSWORD@localhost:5432/conveyor
TEST_DATABASE_URL=postgres://postgres:YOUR_PASSWORD@localhost:5432/conveyor_test
```

Then create the tables:

```powershell
npm run migrate
```

### 3. Run it

Open three terminals in the project folder:

```powershell
npm run api        # terminal 1: API + dashboard on http://localhost:3000
npm run worker     # terminal 2: a worker
npm run worker     # terminal 3: another worker (run as many as you like)
```

Open **http://localhost:3000** and click **Add 1,000** to watch jobs flow through the line.

### 4. Try the interesting parts

| Try this | What you'll see |
|---|---|
| Close a worker's terminal window (or `kill -9` it) while jobs run | After the lease runs out (30 s), the other worker logs `recovered N job(s)` and re-runs them |
| Press **Ctrl+C** in a worker | It stops taking jobs, finishes the running ones, then exits cleanly |
| Set a rate limit of `5` on the email queue | Emails slow to 5/s no matter how many workers are running |
| Click **Pause** on a queue | Its jobs wait; other queues keep flowing |
| Open a job in the Jobs table | Its full timeline: each attempt, error, backoff delay and which worker ran it |
| Click **Retry N dead** | Failed jobs go back into the line |

## Using the API

Examples in PowerShell (on macOS/Linux, `curl` works the same way):

```powershell
# Enqueue a job
Invoke-RestMethod -Method Post -Uri http://localhost:3000/api/jobs -ContentType 'application/json' -Body '{
  "name": "send-email",
  "queue": "email",
  "payload": { "to": "nikhil@example.com", "template": "welcome" },
  "idempotencyKey": "welcome-user-42",
  "maxAttempts": 5
}'

# Look at it, with its timeline
Invoke-RestMethod http://localhost:3000/api/jobs/1
```

| Method & path | Purpose |
|---|---|
| `POST /api/jobs` | Enqueue (`name`, `queue`, `payload`, `priority`, `delayMs` or `runAt`, `maxAttempts`, `backoffBaseMs`, `timeoutMs`, `idempotencyKey`) |
| `GET /api/jobs?state=dead&queue=email` | List jobs (states: waiting, scheduled, retrying, running, succeeded, dead, cancelled) |
| `GET /api/jobs/:id` | One job plus its event timeline |
| `POST /api/jobs/:id/retry` · `/cancel` | Retry a dead/cancelled job · cancel a job that hasn't started |
| `POST /api/queues/:queue/pause` · `/resume` | Stop / restart a queue |
| `PUT /api/queues/:queue/rate-limit` | `{ "rate": 20, "burst": 20 }` jobs/second, or `{ "rate": null }` |
| `POST /api/queues/:queue/retry-dead` | Retry every dead job in a queue |
| `GET /api/stats` · `GET /api/events` | Snapshot · live stream (Server-Sent Events) |
| `GET /metrics` | Prometheus metrics |

## Using it as a library

```ts
import { createPool } from './src/db/pool.js';
import { PermanentError, Queue, Worker } from './src/core/index.js';

const pool = createPool();

// Producer
await new Queue(pool).enqueue({ name: 'send-email', queue: 'email', payload: { to: 'a@b.com' } });

// Worker
const worker = new Worker(pool, {
  queues: ['email'],
  concurrency: 10,
  handlers: {
    'send-email': async ({ to }, { signal }) => {
      if (!to.includes('@')) throw new PermanentError('invalid address'); // don't retry
      await sendEmail(to, { signal });                                   // throw = retry with backoff
    },
  },
});
await worker.start();
process.on('SIGTERM', () => worker.stop()); // graceful shutdown
```

Any service in any language can also enqueue with a plain SQL `INSERT INTO jobs (...)`. A database
trigger wakes the workers.

## Performance

Measured with `npm run benchmark` on a 2-core cloud VM, with PostgreSQL on the same machine and all
workers inside one Node process (so these are conservative):

| | Result |
|---|---|
| Throughput (no-op jobs) | **~2,200–2,500 jobs/second** |
| Pickup latency with LISTEN/NOTIFY | **p50 1.7 ms**, p95 3.5 ms |
| Pickup latency with 1 s polling only | p50 471 ms, p95 906 ms |

Full results: [docs/benchmark.md](docs/benchmark.md). On this machine the CPU is the limit (PostgreSQL and
Node share two cores), so adding workers in the same process doesn't add throughput; on separate machines
each worker brings its own CPU.

## Tests

```powershell
npm test          # 43 tests against the conveyor_test database
npm run lint      # Biome
npm run typecheck
```

Highlights: 1,000 jobs across 4 competing workers each run exactly once; a worker "killed" mid-job has its
jobs recovered; a stale worker's late result is rejected; a 20 jobs/s limit holds across 3 workers;
graceful shutdown finishes or hands back running jobs. The suite was also checked by deliberately removing
`SKIP LOCKED` and the fencing condition: the relevant tests fail.

## Docker

```powershell
docker compose up --build --scale worker=3
```

Starts PostgreSQL, runs migrations, then the API (http://localhost:3000) and three workers.

## Project structure

```
src/
├── core/
│   ├── queue.ts     # every SQL statement: enqueue, claim, complete, fail, heartbeat, reaper, stats
│   ├── worker.ts    # claim loop, concurrency, leases + heartbeats, timeouts, retries, shutdown
│   ├── backoff.ts   # exponential backoff with jitter
│   ├── errors.ts    # PermanentError, TimeoutError, ...
│   └── types.ts
├── api/             # Express API, Server-Sent Events, Prometheus metrics
├── db/              # connection pool + migration runner
├── demo/            # realistic demo handlers and load generator
└── worker.ts        # worker process (CLI)
migrations/001_init.sql   # schema, indexes, trigger, rate-limit functions
public/                   # dashboard (plain HTML/CSS/JS, no build step)
tests/                    # Vitest, real PostgreSQL
scripts/benchmark.ts
```

## Limitations

- **At-least-once delivery.** If a worker finishes the work but crashes before recording it, the job runs
  again. Handlers should be idempotent (safe to run twice), the same rule as with SQS, Sidekiq or Celery.
- **Running jobs can't be cancelled** from the dashboard yet, only jobs that haven't started.
- PostgreSQL-based queues comfortably handle thousands of jobs per second; beyond tens of thousands per
  second, a dedicated broker (Kafka, RabbitMQ) becomes the better tool.

## License

MIT

# Conveyor — How It Works, and How to Talk About It

This document explains **why** Conveyor is built the way it is. Read it next to the code, run every
demo in the README yourself, and break things on purpose (kill a worker, pause a queue, remove
`SKIP LOCKED` and watch a test fail). That is what makes a project yours in an interview.

Suggested reading order: `migrations/001_init.sql` → `src/core/queue.ts` → `src/core/worker.ts` →
`src/core/backoff.ts` → `tests/worker.test.ts` → `src/api/server.ts` → `public/app.js`.

---

## 1. The 30-second pitch

> "Conveyor is a background job queue built on PostgreSQL. Web apps push slow work like emails or
> webhooks onto it, and workers process it reliably. The interesting part is the failure handling:
> workers claim jobs with `FOR UPDATE SKIP LOCKED` so a job never runs on two workers at once; each claim
> is a lease renewed by heartbeats, so if a worker crashes its jobs are recovered automatically; failed
> jobs retry with exponential backoff and jitter, and hopeless ones go to a dead-letter queue. There's a
> global rate limiter implemented as a token bucket inside Postgres, LISTEN/NOTIFY for ~2 ms pickup, and
> a live dashboard. It's tested against a real database: 1,000 jobs across 4 workers run exactly once,
> and crash recovery is tested by killing a worker mid-job."

## 2. The problem

Some work shouldn't happen while a user waits for a web page: sending an email takes 300 ms to 5 s,
generating a report takes 30 s, and a partner's API might be down. So the web server writes a *job*
("send welcome email to user 42") and returns immediately; background *workers* do the work.

The hard part isn't running the job, it's everything that can go wrong:

| What goes wrong | What Conveyor does |
|---|---|
| Two workers grab the same job, so the email is sent twice | Atomic claim with row locks (`SKIP LOCKED`) |
| A worker crashes halfway through | Lease expires, reaper puts the job back |
| A worker freezes, then wakes up and writes a stale result | Fencing: the write is rejected |
| The email provider is down for 2 minutes | Retries with growing delays (backoff) |
| 10,000 jobs fail at once and retry at the same instant | Jitter spreads the retries out |
| A job can never succeed (invalid address) | `PermanentError` → dead-letter queue, no retries |
| A bug makes a job crash its worker every time | Lease expiry still counts the attempt, so it ends up dead instead of looping forever |
| The partner API allows only 20 calls/s | Global token-bucket rate limit across all workers |
| Slow report jobs occupy every worker slot | Per-queue concurrency caps (bulkhead) |
| Deploying new code kills running jobs | Graceful shutdown: finish, or hand back without counting an attempt |
| The client retries an HTTP request that enqueues a job | Idempotency key returns the original job |

## 3. The job lifecycle

```
             enqueue (delayMs / runAt)
                 │
          ┌──────▼──────┐  run_at in the future = "scheduled"
          │   queued    │◄──────────────────────────────┐
          └──────┬──────┘                               │ retry after backoff
                 │ claim (SKIP LOCKED) + lease          │ (failed, attempts left)
          ┌──────▼──────┐  lease expired (crash) ───────┤
          │   running   │──────────────────────────────►┘
          └──┬───────┬──┘
     success │       │ PermanentError / out of attempts
     ┌───────▼─┐  ┌──▼─────┐
     │succeeded│  │  dead  │ ── manual retry ──► queued
     └─────────┘  └────────┘
   (queued jobs can also be cancelled)
```

The database only stores five states (`queued, running, succeeded, dead, cancelled`). The dashboard
splits `queued` into *waiting* (due now), *scheduled* (not yet due) and *retrying* (failed, waiting for
its backoff) using `run_at` and `attempts`, so there's no extra state to keep in sync.

## 4. Design decisions (and the reasoning to give in an interview)

### 4.1 PostgreSQL as the queue
**Choice:** no Redis, RabbitMQ or Kafka; jobs live in a table.
**Why:** most apps already run PostgreSQL. One less system to deploy, monitor and back up. Jobs are as
durable as the rest of your data, and you can enqueue a job *in the same transaction* as the row it
belongs to: the order and its confirmation email commit together, or not at all (this is the
"transactional outbox" idea, for free).
**Trade-off:** a database queue tops out around thousands to low tens of thousands of jobs per second and
adds write load to your main database. Beyond that, a dedicated broker wins. Mention this before the
interviewer does.

### 4.2 Claiming with `FOR UPDATE SKIP LOCKED` (`queue.ts: claim`)
```sql
SELECT id FROM jobs WHERE state='queued' AND queue=$1 AND run_at <= now()
ORDER BY priority DESC, run_at, id LIMIT $2
FOR UPDATE SKIP LOCKED
```
`FOR UPDATE` locks the selected rows so nobody else can take them. `SKIP LOCKED` means "if another
worker has a row locked, skip it instead of waiting." So 10 workers claiming at the same moment each get
*different* jobs without blocking each other. The select, the update to `running`, and the "started"
event happen in **one statement** (data-modifying CTEs), so it's atomic.
A partial index `(queue, priority DESC, run_at, id) WHERE state = 'queued'` means the query reads just the
first few rows of a small index, however many finished jobs the table holds.

### 4.3 Leases + heartbeats + reaper (crash recovery)
A claim isn't "mine forever", it's "mine until `locked_until`" (30 s by default). While the job runs,
the worker's heartbeat extends the lease every 10 s. If the worker dies (crash, `kill -9`, lost network,
machine gone), heartbeats stop, the lease expires, and the **reaper** (which runs in every worker)
moves the job back to `queued`.
- **Why not hold a database lock while the job runs?** It would hold a connection for minutes per job,
  and a dead worker's lock is only released when the database notices the dead connection.
- **Why heartbeats instead of one long lease?** Then a 2-hour job would mean waiting 2 hours to detect a
  crash. With heartbeats, crash detection takes ~30 s regardless of job length.
- **Why does an expired lease count as an attempt?** If a job crashes its worker every time (a "poison
  message"), it must eventually stop being retried. It goes dead after `max_attempts`.

### 4.4 Fencing (`complete`/`fail` include `AND locked_by = $workerId`)
The classic failure: worker A freezes (a long garbage-collection pause), its lease expires, worker B
takes over and finishes the job, then A wakes up and writes its stale result over B's. Conveyor's
updates only apply if the job is *still leased to that worker*, so A's write matches zero rows and is
discarded. The worker also notices during heartbeats that it lost a job and aborts it.
`tests/worker.test.ts` covers this, and the test fails if you delete the condition.

### 4.5 At-least-once delivery and idempotency
Conveyor guarantees each job runs **at least once**, not exactly once. If a worker sends the email and
then crashes before recording success, the job runs again. No queue can fix this alone (it's the same
with SQS, Sidekiq or Celery). The fix is in the handler: make it idempotent, e.g. pass the job id to
the payment provider as an idempotency key. On the producer side, Conveyor supports `idempotencyKey`, so
a retried HTTP request doesn't enqueue the same job twice (unique partial index + `ON CONFLICT DO NOTHING`).

### 4.6 Retries: exponential backoff with jitter (`backoff.ts`)
Delay = base × 2^(attempt−1), capped at 1 hour, then randomised between 50% and 100%.
- **Exponential:** a provider that's down for minutes isn't hammered every second.
- **Jitter:** if 5,000 emails fail during an outage, without jitter they all retry at exactly the same
  moment and can knock the recovering provider over again (the "thundering herd").
- **`PermanentError`:** some failures will never succeed (invalid address, deleted record). Retrying wastes
  work and delays the alert, so they go straight to dead.

### 4.7 Dead-letter queue
Jobs that exhaust their attempts aren't deleted: they're kept with their error and full timeline, so a
person can fix the cause and press **Retry**. Every attempt, error and backoff is in `job_events`,
written in the same statement as the state change, so history can never disagree with the job.

### 4.8 LISTEN/NOTIFY plus polling
Polling every second means jobs wait ~500 ms on average and idle workers query constantly. A trigger
runs `pg_notify` on every insert, and workers that `LISTEN` wake instantly: **p50 1.7 ms vs 471 ms**
(measured). Polling stays as a slow fallback because notifications aren't stored (a worker that was
reconnecting misses them) and scheduled jobs/retries become due without any insert. Because the notify
lives in a trigger, a service in another language can enqueue with a plain `INSERT` and workers still wake.

### 4.9 Global rate limit: a token bucket in PostgreSQL (`conveyor_reserve`)
"At most 20 emails/s" must hold across *all* workers, so the limiter can't live in one worker's memory.
Each queue row stores `tokens` and `refilled_at`. Before claiming, a worker calls `conveyor_reserve(queue, n)`,
which refills tokens by elapsed time × rate (capped at `burst`), grants up to `n`, and subtracts them,
all under the queue row's lock, so concurrent workers can't over-spend. Unused tokens are refunded.
Unlimited queues skip the lock entirely. Tested: 45 jobs at 20/s with burst 5 across 3 workers take
~2 s.

### 4.10 Bulkheads: per-queue concurrency caps
With one shared pool of 10 slots, slow 3-second reports can occupy all 10 and starve fast emails.
`--queues email,reports:3` caps reports at 3 slots per worker. (You can see the problem with the demo
jobs: start a worker with `--queues email,images,webhooks,reports` and no cap, add 1,000 jobs, and watch
the email queue's lag grow while reports hold most of the slots.)

### 4.11 Timeouts with `AbortSignal`
JavaScript can't forcibly stop a running promise. So the worker stops *waiting* at the timeout, records
a `TimeoutError` (retryable), and aborts `ctx.signal`, which well-behaved handlers pass to `fetch`, database
calls, etc. so the actual work stops too.

### 4.12 Graceful shutdown
On `SIGTERM` (a deploy) or Ctrl+C: stop claiming, wait for running jobs (up to 30 s), then **release**
anything still running back to the queue *without* counting the attempt (it wasn't the job's fault).
Kubernetes and Docker send SIGTERM and then SIGKILL after a grace period, which is why
`docker-compose.yml` sets `stop_grace_period: 35s`.

### 4.13 Smaller decisions worth mentioning
- **Database clock for time:** `run_at = now() + delay` uses PostgreSQL's clock, not the app server's, so
  servers with drifting clocks can't schedule jobs early or late.
- **Retention:** finished jobs older than 7 days are deleted in batches of 5,000 (one huge `DELETE` would
  lock a lot of rows and bloat the table).
- **Dashboard updates:** one shared stats query per second, fanned out to every open dashboard over
  Server-Sent Events (one-way server→browser, simpler than WebSockets, reconnects automatically).
- **Validation:** every API input goes through a zod schema; bad input gets a 400 listing each field.
- **Migrations** run under a PostgreSQL advisory lock, so containers starting together can't apply the
  same migration twice.

### 4.14 HTTP tasks (`src/http/handler.ts`)
A worker with a `defaultHandler` that POSTs each job to `{TASKS_URL}/{job name}` with a shared secret.
The response code decides the outcome: 2xx succeeded, 5xx/408/429/timeouts retried, other 4xx are
permanent. This makes Conveyor usable from any language: the ticket-booking project (Spring Boot)
inserts jobs with SQL inside its own transactions and receives them back as HTTP calls. It is the
same design as Google Cloud Tasks, and a good answer to "how would another team use your queue?"

## 5. Testing strategy
Tests run against **real PostgreSQL**, because the guarantees live in SQL (`SKIP LOCKED`, row locks,
triggers) and an in-memory fake would test nothing. Highlights:
- 1,000 jobs, 4 workers × 10 concurrency: every job handled exactly once.
- Crash recovery: a worker is "killed" mid-job (timers stop, nothing written back); another worker
  recovers all 5 jobs and the timeline shows `started → lease_expired → started → succeeded`.
- Fencing, heartbeats keeping a 1.2 s job alive under a 0.3 s lease, graceful shutdown both ways, rate
  limiting timing, bulkhead caps, LISTEN/NOTIFY latency, API validation and SSE.
- **The tests were checked to catch real bugs:** removing `FOR UPDATE SKIP LOCKED` fails the two
  concurrency tests, removing the fencing condition fails the fencing test.

A real `kill -9` was also run by hand: 5 jobs were mid-flight, a new worker logged
`recovered 5 job(s) from crashed workers` once the leases expired, and all 20 jobs succeeded. A database outage was
simulated too: PostgreSQL was stopped for ~25 s while a worker had 10 jobs running. The worker logged
connection errors but stayed up, and once the database returned it recovered those 10 jobs and drained
all 400.

## 6. Numbers you can quote
From `npm run benchmark` (2-core VM, PostgreSQL on the same machine, workers in one process):
- ~2,200–2,500 no-op jobs/second.
- Pickup latency p50 1.7 ms / p95 3.5 ms with LISTEN/NOTIFY, vs 471 ms / 906 ms polling.
Be honest that more workers didn't add throughput on that machine because the CPU was saturated, and
that real jobs (emails, API calls) are I/O-bound, so the queue overhead is rarely the bottleneck.

## 7. Likely interview questions

**Why not just use Redis or RabbitMQ?**
For many apps, an extra system costs more than it gives. Postgres gives durability, transactions with
the app's own data, and `SKIP LOCKED` makes it a solid queue for thousands of jobs per second. At much
higher volume, or for fan-out/streaming, I'd pick RabbitMQ or Kafka.

**How do you guarantee a job isn't processed twice?**
Two workers can't *claim* the same job (row locks + `SKIP LOCKED`). But delivery is at-least-once: a
crash after the work but before the "done" write means a re-run. So handlers must be idempotent, and the
fencing check stops a stale worker from overwriting results.

**What happens if the database goes down?**
Workers can't claim or report; their errors are logged and they keep retrying on the next poll. Jobs that
were running finish, but their results can't be recorded, so after the database returns, the leases expire
and those jobs run again (at-least-once). No job is lost, because the database is the source of truth.

**How does a worker know another worker died?**
It doesn't need to. Every running job has a lease; the reaper periodically requeues jobs whose lease
expired. It's a timeout-based failure detector: simple, with no worker-to-worker communication.

**Why add jitter to backoff?**
To avoid synchronized retries (thundering herd) after a mass failure.

**How would you scale this further?**
More worker processes/machines first (they coordinate through the database). Then batch completions,
use separate tables/partitions per queue, move finished jobs to an archive table, and tune
`autovacuum` for the high-churn jobs table. Past that, a dedicated broker.

**Why does the jobs table need vacuuming attention?**
Every state change in PostgreSQL writes a new row version (MVCC); old versions are dead tuples until
vacuum removes them. A busy queue table churns fast, so autovacuum must keep up or the table and indexes bloat.
The partial index on queued jobs and retention purges help.

**How do you test concurrency bugs?**
Run many real workers against a real database with randomized delays and assert invariants (each job
exactly once). Then mutate the code (remove the lock) to prove the test would catch the bug.

**What's the difference between a timeout and a lease?**
A timeout limits one attempt's run time (the job is too slow). A lease detects a worker that stopped
responding (the worker is gone). A healthy worker running a long job keeps renewing its lease but still
respects the job's timeout.

**Walk me through what happens when I POST a job.**
zod validates the body → `INSERT … ON CONFLICT DO NOTHING` (idempotency) → the trigger writes an
"enqueued" event, creates the queue row if needed and `NOTIFY`s → a listening worker wakes → reserves
rate-limit tokens → claims with `SKIP LOCKED` and gets a lease → runs the handler with a timeout,
heartbeating meanwhile → `complete` (fenced) writes the result and a "succeeded" event → the dashboard
shows it within a second over SSE.

## 8. Limitations and next steps
- Cancelling *running* jobs (a cancel flag checked on heartbeat, then abort the signal).
- Recurring cron jobs (e.g. "every day at 9:00").
- Job dependencies/workflows (run B after A succeeds).
- Batch completions to cut round trips; partition the jobs table by state or time.
- Authentication on the API and dashboard (currently open, fine for local/demo use).
- `worker_threads` for CPU-heavy handlers (Node runs JavaScript on one thread).

## 9. Resume bullets

**Conveyor — Distributed Background Job Queue** | TypeScript, Node.js, PostgreSQL, Express, Docker
- Built a PostgreSQL-backed job queue where competing workers claim jobs with `FOR UPDATE SKIP LOCKED`;
  1,000 jobs across 4 concurrent workers each ran exactly once in tests against a real database.
- Implemented fault tolerance: lease-based crash recovery with heartbeats, fencing against stale workers,
  exponential backoff with jitter, a dead-letter queue, timeouts and graceful shutdown; verified by
  killing workers mid-job.
- Added a global token-bucket rate limiter in PL/pgSQL, per-queue concurrency caps and idempotency keys;
  cut job pickup latency from 471 ms to 1.7 ms (p50) with LISTEN/NOTIFY; ~2,300 jobs/s on a 2-core VM.
- Shipped a real-time dashboard (Server-Sent Events) with per-job timelines, Prometheus metrics, 49
  integration tests and GitHub Actions CI; added HTTP tasks so services in any language (e.g. a Spring
  Boot app) can enqueue jobs with SQL and receive them as retried HTTP callbacks.

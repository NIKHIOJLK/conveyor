# Benchmark results

Machine: 2 CPU cores (Intel(R) Xeon(R) Processor @ 2.10GHz), Node v22.23.4, PostgreSQL on the same machine.
All workers run inside one Node process, so these are conservative numbers.

## Throughput (20,000 no-op jobs, concurrency 20 per worker)

| Workers | Jobs / second |
|---|---|
| 1 | 2,158 |
| 2 | 2,560 |
| 4 | 2,259 |

## Pickup latency (time from enqueue to a worker starting the job)

| Wake-up method | p50 | p95 |
|---|---|---|
| LISTEN/NOTIFY | 1.7 ms | 3.5 ms |
| Polling every 1 s | 471.0 ms | 905.9 ms |

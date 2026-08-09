# Load testing

`scripts/load_test.py` is a bounded, reproducible smoke/load client for the
TaskMesh submission endpoint. It rotates HIGH, MEDIUM, and LOW priorities and
reports requests, successes, failures, elapsed time, throughput, and p50/p95/p99
latency.

## Usage

Start the full stack, then run:

```powershell
.\.venv\Scripts\python.exe -m scripts.load_test --tasks 100 --concurrency 10
```

Defaults are intentionally small: 30 requests, concurrency 5, and
`http://127.0.0.1:8000`. The client only accepts loopback hosts unless
`--allow-remote` is supplied deliberately. Use `--help` for bounds and options.

## Reading results

- Throughput is successful submissions divided by elapsed wall-clock time.
- p50 is representative latency; p95/p99 expose tail behavior.
- A successful POST proves acceptance and broker publication, not job completion.
- Use the dashboard or API afterward to inspect queue drain, terminal status,
  retries, and attempt history.

Results vary with CPU, worker concurrency, database I/O, Redis, warm-up, and host
virtualization. Record the command, environment, sample size, and observed output
before making performance claims. This tool is not a substitute for sustained,
distributed benchmarking or production capacity planning.

## Running in CI against the real multi-worker stack

`python -m scripts.load_test --tasks 200 --concurrency 20` runs automatically as
a step in the `integration` GitHub Actions job, immediately after the
correctness-focused integration tests, against the same live stack: real
PostgreSQL, real Redis, and the four real Celery worker processes
(`worker-high`/`-medium`/`-low`/`-control`). The step fails the job if any
submission fails. This is a genuine, reproducible, timestamped run against
production-shaped infrastructure (GitHub Actions' `ubuntu-latest` runner) — not
a one-off number pasted into documentation. See the `integration` job's "Load
test against the real 4-worker stack" step in the
[most recent workflow run](https://github.com/yashaswini1444-code/TaskMesh/actions/workflows/tests.yml)
for the actual current result; a specific throughput figure is deliberately not
hardcoded here, since it would go stale and vary run to run — read it from the
live job output instead of trusting a number in a document.

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any

import httpx2 as httpx


@dataclass
class SubmissionResult:
    success: bool
    latency_seconds: float
    task_id: str | None
    error: str | None


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * percent
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize(results: list[SubmissionResult], duration: float) -> dict[str, Any]:
    latencies = [result.latency_seconds for result in results]
    successes = sum(result.success for result in results)
    return {
        "requests_attempted": len(results),
        "successful_submissions": successes,
        "failed_submissions": len(results) - successes,
        "submission_duration_seconds": round(duration, 4),
        "requests_per_second": round(len(results) / duration, 2) if duration else 0.0,
        "latency_seconds": {
            "min": round(min(latencies), 4) if latencies else 0.0,
            "mean": round(statistics.fmean(latencies), 4) if latencies else 0.0,
            "p50": round(percentile(latencies, 0.50), 4),
            "p95": round(percentile(latencies, 0.95), 4),
            "p99": round(percentile(latencies, 0.99), 4),
            "max": round(max(latencies), 4) if latencies else 0.0,
        },
        "errors": [result.error for result in results if result.error][:10],
    }


async def submit_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    number: int,
    priority: str,
) -> SubmissionResult:
    async with semaphore:
        started = time.perf_counter()
        try:
            response = await client.post(
                "/tasks",
                json={
                    "task_type": "echo",
                    "payload": {"message": f"load-test-{number}"},
                    "priority": priority,
                },
            )
            latency = time.perf_counter() - started
            if response.status_code == 201:
                return SubmissionResult(True, latency, response.json()["id"], None)
            return SubmissionResult(
                False,
                latency,
                None,
                f"HTTP {response.status_code}",
            )
        except httpx.HTTPError as exc:
            return SubmissionResult(
                False,
                time.perf_counter() - started,
                None,
                type(exc).__name__,
            )


async def run(args: argparse.Namespace) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(args.concurrency)
    timeout = httpx.Timeout(args.timeout)
    priorities = ("HIGH", "MEDIUM", "LOW")
    started = time.perf_counter()
    async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout) as client:
        results = await asyncio.gather(
            *[
                submit_one(client, semaphore, number, priorities[number % 3])
                for number in range(args.tasks)
            ]
        )
    duration = time.perf_counter() - started
    report = summarize(results, duration)
    report["configuration"] = {
        "base_url": args.base_url,
        "tasks": args.tasks,
        "concurrency": args.concurrency,
        "timeout_seconds": args.timeout,
    }
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit a bounded TaskMesh task burst")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--tasks", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if not 1 <= args.tasks <= 10_000:
        parser.error("--tasks must be between 1 and 10000")
    if not 1 <= args.concurrency <= 500:
        parser.error("--concurrency must be between 1 and 500")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run(parse_args())), indent=2))

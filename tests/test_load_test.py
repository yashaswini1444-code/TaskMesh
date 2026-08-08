import pytest

from scripts.load_test import (
    SubmissionResult,
    percentile,
    summarize,
    validate_base_url,
)


def test_percentile_interpolates_and_handles_empty_values() -> None:
    assert percentile([], 0.95) == 0.0
    assert percentile([1.0, 2.0, 3.0], 0.5) == 2.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.95) == 3.8499999999999996


def test_load_report_counts_successes_failures_and_latency() -> None:
    report = summarize(
        [
            SubmissionResult(True, 0.1, "one", None),
            SubmissionResult(True, 0.2, "two", None),
            SubmissionResult(False, 0.3, None, "HTTP 503"),
        ],
        duration=1.5,
    )
    assert report["requests_attempted"] == 3
    assert report["successful_submissions"] == 2
    assert report["failed_submissions"] == 1
    assert report["requests_per_second"] == 2.0
    assert report["latency_seconds"]["p50"] == 0.2
    assert report["errors"] == ["HTTP 503"]


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1:8000", "http://localhost:8000/", "http://[::1]:8000"],
)
def test_load_target_accepts_loopback_hosts(url: str) -> None:
    assert validate_base_url(url).startswith("http://")


def test_load_target_rejects_remote_host_without_explicit_override() -> None:
    with pytest.raises(ValueError, match="remote targets are disabled"):
        validate_base_url("https://example.com")

    assert validate_base_url(
        "https://example.com/",
        allow_remote=True,
    ) == "https://example.com"

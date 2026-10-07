"""How a suite names its evaluation runs and how long, and how patiently, it polls each one."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EvalRunOptions:
    """How a suite names and polls its runs.

    Attributes:
        git_sha: The commit the run names carry, as its first seven characters; empty or
            `UNKNOWN_GIT_SHA` when the project is not in a git work tree.
        timestamp: The run names' `ts` token; None uses the current UTC time.
        poll_interval_ms: The wait between two status reads of a run, in milliseconds.
        deadline_ms: How long a run may take to reach a terminal status, in milliseconds of
            the clock's monotonic time from just before its start; slow status reads count
            against it. Once it passes, one last read decides, given at most
            `read_timeout_ms`; 20 minutes by default, 240 polls at the default interval.
        max_read_failures: The status reads in a row that may fail in transit before the
            attempt fails, after one last read; any read that succeeds resets the count.
        read_timeout_ms: The longest one status read may take, network retries included,
            before it fails as transient; never longer than what is left of the deadline.
        heartbeat_ms: How often, at most, the suite reports the runs it is still waiting on.
    """

    git_sha: str
    timestamp: str | None = None
    poll_interval_ms: int = 5_000
    deadline_ms: int = 1_200_000
    max_read_failures: int = 5
    read_timeout_ms: int = 30_000
    heartbeat_ms: int = 60_000

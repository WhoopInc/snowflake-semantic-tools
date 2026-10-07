"""What a suite tells its caller while its evaluation runs go on, which can take many minutes.

`RunWatch` follows every run in flight and reports an event when an attempt starts, when a
run's status changes, when a status read fails and is retried, and when an attempt ends;
while runs are in flight, it reports at most one `StillWaiting` per heartbeat interval. It
reports nothing for a poll that saw no change, so the events stay few however often runs are
polled. Each event renders as one line of text; where the lines go, and whether they go
anywhere, is the caller's choice.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock

from snowflake_semantic_tools.domain.ports.clock import ClockPort

# The longest failure reason a line carries; a driver message can run to many lines.
_REASON_LIMIT = 120


@dataclass(frozen=True, slots=True)
class AttemptStarted:
    """An attempt is about to start its run: attempt `attempt` of the eval's `attempt_limit`."""

    run_name: str
    attempt: int
    attempt_limit: int

    @property
    def line(self) -> str:
        """Render the event as one progress line."""
        return f"eval run {self.run_name}: starting (attempt {self.attempt} of {self.attempt_limit})"


@dataclass(frozen=True, slots=True)
class StatusChanged:
    """A status read found the run in another status than the read before it, or for the first time."""

    run_name: str
    status: str
    elapsed_ms: int

    @property
    def line(self) -> str:
        """Render the event as one progress line."""
        return f"eval run {self.run_name}: {self.status} after {duration_text(self.elapsed_ms)}"


@dataclass(frozen=True, slots=True)
class StatusReadFailed:
    """A status read failed in transit; it is retried until `limit` reads in a row have failed.

    Attributes:
        failures: The reads in a row that have failed, this one included.
    """

    run_name: str
    failures: int
    limit: int
    reason: str

    @property
    def line(self) -> str:
        """Render the event as one progress line."""
        return f"eval run {self.run_name}: status read failed ({self.failures}/{self.limit}), retrying: {self.reason}"


@dataclass(frozen=True, slots=True)
class AttemptEnded:
    """An attempt's run ended in `status`: a terminal one, or why it ended without one."""

    run_name: str
    status: str
    elapsed_ms: int

    @property
    def line(self) -> str:
        """Render the event as one progress line."""
        return f"eval run {self.run_name}: ended {self.status} after {duration_text(self.elapsed_ms)}"


@dataclass(frozen=True, slots=True)
class StillWaiting:
    """The runs still in flight, in name order, each with its last status and time since it started.

    A run no status read has described yet shows as `STARTING`.
    """

    runs: tuple[tuple[str, str, int], ...]

    @property
    def line(self) -> str:
        """Render the event as one progress line."""
        shown = ", ".join(f"{name} {status} {duration_text(elapsed)}" for name, status, elapsed in self.runs)
        noun = "run" if len(self.runs) == 1 else "runs"
        return f"waiting on {len(self.runs)} {noun}: {shown}"


EvalProgressEvent = AttemptStarted | StatusChanged | StatusReadFailed | AttemptEnded | StillWaiting
EvalProgress = Callable[[EvalProgressEvent], None]


def no_progress(event: EvalProgressEvent) -> None:
    """Report nothing: the sink of a suite whose caller does not follow its runs."""
    del event


class RunWatch:
    """The runs one suite has in flight, and the events their attempts report, shared by every worker.

    Each event goes to `progress` under the watch's lock, so two workers never interleave
    their lines. Times come from `clock`'s monotonic reading.
    """

    def __init__(self, progress: EvalProgress, clock: ClockPort, heartbeat_ms: int) -> None:
        self._progress = progress
        self._clock = clock
        self._heartbeat_ms = heartbeat_ms
        self._lock = Lock()
        # Each run in flight: when its attempt started, and its last status; None before one.
        self._runs: dict[str, tuple[int, str | None]] = {}
        self._last_beat: int | None = None

    def started(self, run_name: str, attempt: int, attempt_limit: int) -> None:
        """Follow a run from now on, reporting that its attempt starts."""
        with self._lock:
            now = self._clock.monotonic_ms()
            self._runs[run_name] = (now, None)
            if self._last_beat is None:
                self._last_beat = now
            self._progress(AttemptStarted(run_name, attempt, attempt_limit))

    def status(self, run_name: str, status: str) -> None:
        """Record a status a read found, reporting it only when it differs from the run's last one."""
        with self._lock:
            began, last = self._runs.get(run_name, (self._clock.monotonic_ms(), None))
            if status == last:
                return
            self._runs[run_name] = (began, status)
            self._progress(StatusChanged(run_name, status, self._clock.monotonic_ms() - began))

    def read_failed(self, run_name: str, failures: int, limit: int, reason: str) -> None:
        """Report a status read that failed in transit and will be retried."""
        with self._lock:
            self._progress(StatusReadFailed(run_name, failures, limit, short_reason(reason)))

    def ended(self, run_name: str, status: str) -> None:
        """Stop following a run, reporting how its attempt ended; a run never started is ignored."""
        with self._lock:
            found = self._runs.pop(run_name, None)
            if found is not None:
                self._progress(AttemptEnded(run_name, status, self._clock.monotonic_ms() - found[0]))

    def heartbeat(self) -> None:
        """Report the runs still in flight, once a heartbeat interval has passed since the last report.

        The first interval counts from the first run's start, so a suite whose runs end within
        it never reports one.
        """
        with self._lock:
            now = self._clock.monotonic_ms()
            if not self._runs or self._last_beat is None or now - self._last_beat < self._heartbeat_ms:
                return
            self._last_beat = now
            runs = tuple(
                (name, status or "STARTING", now - began) for name, (began, status) in sorted(self._runs.items())
            )
            self._progress(StillWaiting(runs))


def duration_text(milliseconds: int) -> str:
    """Render a duration as whole seconds, minutes and seconds, or hours and minutes.

    Example:
        `45_000` renders `45s`, `250_000` renders `4m10s`, and `3_720_000` renders `1h02m`.
    """
    seconds = max(0, milliseconds) // 1000
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def short_reason(reason: str) -> str:
    """Return a failure's first line, cut to fit one progress line."""
    first = reason.strip().splitlines()[0] if reason.strip() else "no reason given"
    return first if len(first) <= _REASON_LIMIT else f"{first[: _REASON_LIMIT - 3]}..."

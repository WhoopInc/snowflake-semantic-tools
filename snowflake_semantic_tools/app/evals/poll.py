"""Poll one evaluation run's status until it is terminal, through reads that fail in transit.

An eval run takes minutes in Snowflake, and the network between two of its status reads may
drop. A read that fails in transit (`SnowflakeTransientError`) is retried at the next poll;
only `max_read_failures` such failures in a row fail the attempt, and a read that succeeds
resets the count. A read Snowflake answered with something SST cannot use -- a row describing
another run, a malformed row -- fails the attempt at once, as no retry would change it.

The deadline is the clock's, not a count of reads: a slow read uses up the time it took, a
read is never given longer than what is left of the deadline, and the wait before the next
read never runs past it. Before an attempt is declared failed or out of time, one last read is
made, so a run that completed while its status could not be read is still seen to complete;
made once the deadline has passed, it is given the whole read timeout, so an attempt ends at
most one read timeout after its deadline.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.progress import RunWatch, duration_text, short_reason
from snowflake_semantic_tools.app.evals.retrieve import _read_status
from snowflake_semantic_tools.domain.model.eval import eval_status_is_terminal
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort

_Status = tuple[str, tuple[str, ...]]


class StatusPoll:
    """One attempt's polling: its deadline, its failed reads in a row, and the last status it read."""

    def __init__(
        self,
        port: ExecutionPort,
        clock: ClockPort,
        options: EvalRunOptions,
        watch: RunWatch,
    ) -> None:
        self._port = port
        self._clock = clock
        self._options = options
        self._watch = watch
        self._deadline = clock.monotonic_ms() + options.deadline_ms
        self._failures = 0
        self._last_failure = ""
        self._last_status: str | None = None

    def until_terminal(self, compiled: CompiledEval, run_name: str, config_path: str) -> _Status:
        """Read the run's status until it is terminal, waiting the poll interval between two reads.

        Only the in-progress statuses keep a run polling, so a status Snowflake does not
        document ends it rather than polling it to the deadline. Every status change, failed
        read and heartbeat is reported to the watch.

        Raises:
            SnowflakePortError: `max_read_failures` reads in a row failed in transit and so did
                the last read; or the run was not terminal by the deadline; or a read failed
                for a reason a retry cannot cure, such as a row describing another run.
            ValueError: a status row was malformed.
        """
        while True:
            # The last read: the deadline passed, or the failures in a row ran out. It still
            # decides, so a run that ended while its status could not be read is seen to end.
            final = self._failures >= self._options.max_read_failures or self._remaining() <= 0
            found = self._read(compiled, run_name, config_path, final=final)
            if found is not None and eval_status_is_terminal(found[0]):
                return found
            self._watch.heartbeat()
            # A last read that succeeded with time left resets the failures: polling goes on.
            if final and (self._failures or self._remaining() <= 0):
                raise SnowflakePortError(self._why(run_name))
            self._pause()

    def _read(self, compiled: CompiledEval, run_name: str, config_path: str, *, final: bool) -> _Status | None:
        """Read the status once; None when the read failed in transit, which counts the failure.

        A read is given at most what is left of the deadline; the one made once it has passed
        is given the whole read timeout. A failed read is reported unless it is the last one.
        """
        remaining = self._remaining()
        timeout_ms = min(self._options.read_timeout_ms, remaining) if remaining > 0 else self._options.read_timeout_ms
        try:
            found = _read_status(
                self._port, compiled, run_name, config_path, timeout_seconds=max(1, -(-timeout_ms // 1000))
            )
        except SnowflakeTransientError as exc:
            self._failures += 1
            self._last_failure = str(exc)
            if not final:
                self._watch.read_failed(run_name, self._failures, self._options.max_read_failures, str(exc))
            return None
        self._failures = 0
        self._last_status = found[0]
        self._watch.status(run_name, found[0])
        return found

    def _pause(self) -> None:
        """Wait the poll interval, or what is left of the deadline when that is shorter."""
        remaining = self._remaining()
        if remaining > 0:
            self._clock.sleep(min(self._options.poll_interval_ms, remaining))

    def _remaining(self) -> int:
        return self._deadline - self._clock.monotonic_ms()

    def _why(self, run_name: str) -> str:
        """Say why the attempt gave up: its status reads kept failing, or its deadline passed."""
        if self._failures:
            return (
                f"evaluation run {run_name!r}: its status could not be read in {self._failures} reads in a row "
                f"({short_reason(self._last_failure)})"
            )
        last = self._last_status or "unread"
        within = duration_text(self._options.deadline_ms)
        return f"evaluation run {run_name!r} did not reach a terminal status within {within}; it was {last}"

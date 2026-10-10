"""Where one run's time goes: its Snowflake round trips, its dbt processes, and its phases.

`RUN_TIMINGS` is the one recorder of the process. The connector records every statement it
hands the driver and every login, the dbt runner every process it starts, and the use cases
each phase they name through the `PhaseTimer` port; `--output json` (and `yaml`) reports
`snapshot()` as the envelope's `invocation.timings`. `sst` resets the recorder, switched off, as
each run starts, and switches it on only for a run that prints an envelope, so a run that
prints none records nothing: each hook is then a `nullcontext`. Times are as the client observes
them, from the process's monotonic clock. Workers on plan's and apply's thread pools record at
once, so every update takes the recorder's lock; a call still in flight when the recorder is
reset is left out of the new run's totals.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager

from snowflake_semantic_tools.domain.ports.timing import PhaseTimer


class RunTimings(PhaseTimer):
    """Totals of one run's timed work, safe to record into from several threads at once.

    Snowflake time is kept two ways: `client_s` sums each statement's own time, so statements on
    parallel sessions add up past the wall clock, while `busy_s` is the wall time during which
    at least one statement or login was in flight. A phase entered again adds to its total.

    Args:
        clock: Returns monotonic seconds; tests pass a fake.
        enabled: Whether the hooks record from the start; `sst` switches them on with `enable`.
    """

    def __init__(self, clock: Callable[[], float] = time.perf_counter, *, enabled: bool = False) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._enabled = enabled
        self._generation = 0
        self._phases: dict[str, float] = {}
        self._queries = 0
        self._query_s = 0.0
        self._max_query_s = 0.0
        self._connects = 0
        self._connect_s = 0.0
        self._busy_s = 0.0
        self._in_flight = 0
        self._busy_since = 0.0
        self._dbt_runs = 0
        self._dbt_s = 0.0

    @property
    def enabled(self) -> bool:
        """Whether the hooks record; when not, each is a `nullcontext`."""
        return self._enabled

    def enable(self) -> None:
        """Start recording, for a run whose envelope reports the totals."""
        self._enabled = True

    def reset(self) -> None:
        """Forget everything recorded and stop recording, as a new run starts.

        A call entered before the reset and finished after it counts in nothing.
        """
        with self._lock:
            self._enabled = False
            self._generation += 1
            self._phases = {}
            self._queries = self._connects = self._dbt_runs = self._in_flight = 0
            self._query_s = self._max_query_s = self._connect_s = self._busy_s = self._dbt_s = 0.0

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Time the block as phase `name`, however it ends; a phase entered again adds to its total."""
        return self._timed_phase(name) if self._enabled else contextlib.nullcontext()

    def round_trip(self) -> AbstractContextManager[None]:
        """Time the block as one statement Snowflake ran, however it ends."""
        return self._snowflake_call(login=False) if self._enabled else contextlib.nullcontext()

    def login(self) -> AbstractContextManager[None]:
        """Time the block as one connection opened to Snowflake, however it ends."""
        return self._snowflake_call(login=True) if self._enabled else contextlib.nullcontext()

    def dbt_process(self) -> AbstractContextManager[None]:
        """Time the block as one dbt process, however it ends."""
        return self._timed_dbt() if self._enabled else contextlib.nullcontext()

    @contextlib.contextmanager
    def _timed_phase(self, name: str) -> Iterator[None]:
        generation, started = self._generation, self._clock()
        try:
            yield
        finally:
            elapsed = self._clock() - started
            with self._lock:
                if generation == self._generation:
                    self._phases[name] = self._phases.get(name, 0.0) + elapsed

    @contextlib.contextmanager
    def _timed_dbt(self) -> Iterator[None]:
        generation, started = self._generation, self._clock()
        try:
            yield
        finally:
            elapsed = self._clock() - started
            with self._lock:
                if generation == self._generation:
                    self._dbt_runs += 1
                    self._dbt_s += elapsed

    @contextlib.contextmanager
    def _snowflake_call(self, *, login: bool) -> Iterator[None]:
        """Time the block as one login or statement, and as Snowflake busy while it runs."""
        with self._lock:
            generation, started = self._generation, self._clock()
            if self._in_flight == 0:
                self._busy_since = started
            self._in_flight += 1
        try:
            yield
        finally:
            with self._lock:
                ended = self._clock()
                if generation == self._generation:
                    self._finish(ended, ended - started, login=login)

    def _finish(self, ended: float, elapsed: float, *, login: bool) -> None:
        """Count one finished Snowflake call; the caller holds the lock."""
        self._in_flight -= 1
        if self._in_flight == 0:
            self._busy_s += ended - self._busy_since
        if login:
            self._connects += 1
            self._connect_s += elapsed
        else:
            self._queries += 1
            self._query_s += elapsed
            self._max_query_s = max(self._max_query_s, elapsed)

    def snapshot(self) -> dict[str, object]:
        """Return the totals as the envelope reports them, in seconds rounded to microseconds.

        Returns:
            `phases` by name; `snowflake` with `queries`, `client_s` (their summed time),
            `max_s` (the slowest), `busy_s`, `connects` and `connect_s`; `dbt_runs` and `dbt_s`.
            A Snowflake call still in flight counts in none of them.
        """
        with self._lock:
            return {
                "phases": {name: round(seconds, 6) for name, seconds in sorted(self._phases.items())},
                "snowflake": {
                    "queries": self._queries,
                    "client_s": round(self._query_s, 6),
                    "max_s": round(self._max_query_s, 6),
                    "busy_s": round(self._busy_s, 6),
                    "connects": self._connects,
                    "connect_s": round(self._connect_s, 6),
                },
                "dbt_runs": self._dbt_runs,
                "dbt_s": round(self._dbt_s, 6),
            }


RUN_TIMINGS = RunTimings()

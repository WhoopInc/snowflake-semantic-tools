"""Clocks and tickers a test controls: no test waits on, or reads, the wall clock.

`FixedClock` is the use cases' `Clock`: each read advances it by one, and `sleep` records the
wait instead of waiting; `PollClock` is one whose sleeps pass, for deadlines.
`SteppedTicker` is a heartbeat `Ticker` that never waits on the wall clock: each `step` lets
exactly one beat run and returns once the heartbeat waits again or has stopped.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta


class FixedClock:
    def __init__(self) -> None:
        self.current = 0
        self.sleeps: list[int] = []

    def now_iso(self) -> str:
        self.current += 1
        return f"2026-01-01T00:00:0{self.current}Z"

    def monotonic_ms(self) -> int:
        self.current += 1
        return self.current

    def sleep(self, milliseconds: int) -> None:
        self.sleeps.append(milliseconds)

    def new_run_id(self) -> str:
        return "run-1"


class PollClock(FixedClock):
    """A `FixedClock` whose sleeps pass: each `sleep` moves monotonic time on by its wait.

    `advance` moves it on too, as a slow status read would; a read still moves it by one. Its
    wall time is as many milliseconds past the same midnight, so it stays a valid instant
    however long the waits run.
    """

    def now_iso(self) -> str:
        self.current += 1
        instant = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(milliseconds=self.current)
        return instant.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def sleep(self, milliseconds: int) -> None:
        super().sleep(milliseconds)
        self.current += milliseconds

    def advance(self, milliseconds: int) -> None:
        self.current += milliseconds


class SteppedTicker:
    """A `Ticker` whose waits end only when the test steps it, or it is stopped.

    `now` advances by each granted wait's seconds, so the heartbeat's clock moves one interval
    per beat. Every hand-off has a 5-second safety bound that a passing test never reaches.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.beats = 0
        self._condition = threading.Condition()
        self._granted = 0
        self._waiting = False
        self._stopped = False

    def wait(self, seconds: float) -> bool:
        with self._condition:
            self._waiting = True
            self._condition.notify_all()
            while not self._granted and not self._stopped:
                self._condition.wait()
            self._waiting = False
            if self._stopped:
                return True
            self._granted -= 1
            self.now += seconds
            self.beats += 1
            return False

    def stop(self) -> None:
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    def monotonic(self) -> float:
        return self.now

    def step(self, beats: int = 1) -> None:
        """Let `beats` beats run, one at a time, returning once the last has been handled."""
        for _ in range(beats):
            with self._condition:
                assert self._condition.wait_for(lambda: self._waiting or self._stopped, timeout=5)
                if self._stopped:
                    return
                self._granted += 1
                self._condition.notify_all()
                # Handled once the heartbeat waits again, having taken this grant, or has stopped.
                assert self._condition.wait_for(
                    lambda: (self._waiting and not self._granted) or self._stopped, timeout=5
                )

    @property
    def stopped(self) -> bool:
        return self._stopped

"""The clock port: wall time, monotonic time, sleeping, and run ids, injected so a test can fix them."""

from __future__ import annotations

from typing import Protocol


class ClockPort(Protocol):
    """The time and run ids a use case reads, injected so that a test can fix them.

    Wall time stamps what state and eval records store; monotonic time measures durations.
    """

    def now_iso(self) -> str:
        """Return the current time in UTC as ISO 8601 text ending in `Z`.

        State and eval records store their timestamps in this form, and the state table reads
        its timestamps back in it, so an entry compares equal to the one SST wrote.
        """
        ...

    def monotonic_ms(self) -> int:
        """Return a monotonic clock reading in whole milliseconds, for measuring a duration.

        Only the difference between two readings means anything, and a later reading is
        never smaller.
        """
        ...

    def sleep(self, milliseconds: int) -> None:
        """Block the caller for `milliseconds`: a retry's backoff, or the wait between two polls."""
        ...

    def new_run_id(self) -> str:
        """Return a new identifier, unique to one run, which names it in state and as the lock holder."""
        ...

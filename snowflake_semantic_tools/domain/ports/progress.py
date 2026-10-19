"""The progress port: how far through its work a use case is, so a long run can show it.

The domain only counts; the adapter behind the port decides whether and how anyone sees it. A
use case given no port reports to `NO_PROGRESS`, which shows nothing.
"""

from __future__ import annotations

from typing import Protocol


class Progress(Protocol):
    """Count the items of a step, such as the changes `apply` publishes."""

    def expect(self, step: str, total: int) -> None:
        """Say that `step` has `total` items to do, before the first is done.

        Never raises; safe to call from several threads at once. Calling it again restarts the count.
        """
        ...

    def advance(self, step: str, count: int = 1) -> None:
        """Count `count` more items of `step` done, however each ended.

        Never raises; safe to call from several threads at once.
        """
        ...

    def unit_started(self, step: str, key: str, status: str = "") -> None:
        """Say one unit of `step`'s work, named `key`, is now in flight, as `status` describes it.

        A unit is one thing a run does among others at once, such as one eval run or one change
        apply publishes. Never raises; safe to call from several threads at once.
        """
        ...

    def unit_status(self, step: str, key: str, status: str) -> None:
        """Say the unit `key` of `step`, in flight, is now as `status` describes it; never raises."""
        ...

    def unit_finished(self, step: str, key: str, *, ok: bool, status: str) -> None:
        """Say the unit `key` of `step` ended, well or not, as `status` says; never raises."""
        ...


class NoProgress:
    """A progress port that shows nothing, for a run nobody watches."""

    def expect(self, step: str, total: int) -> None:
        """Ignore the total."""

    def advance(self, step: str, count: int = 1) -> None:
        """Ignore the count."""

    def unit_started(self, step: str, key: str, status: str = "") -> None:
        """Ignore the unit."""

    def unit_status(self, step: str, key: str, status: str) -> None:
        """Ignore the status."""

    def unit_finished(self, step: str, key: str, *, ok: bool, status: str) -> None:
        """Ignore the end."""


NO_PROGRESS: Progress = NoProgress()

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


class NoProgress:
    """A progress port that shows nothing, for a run nobody watches."""

    def expect(self, step: str, total: int) -> None:
        """Ignore the total."""

    def advance(self, step: str, count: int = 1) -> None:
        """Ignore the count."""


NO_PROGRESS: Progress = NoProgress()

"""A progress port that records what a use case reports, for tests of its units and counts."""

from __future__ import annotations


class RecordedProgress:
    """Record every count and unit event, in order, as `(method, args...)` tuples."""

    def __init__(self) -> None:
        self.events: list[tuple[object, ...]] = []

    def expect(self, step: str, total: int) -> None:
        self.events.append(("expect", step, total))

    def advance(self, step: str, count: int = 1) -> None:
        self.events.append(("advance", step, count))

    def unit_started(self, step: str, key: str, status: str = "") -> None:
        self.events.append(("started", step, key, status))

    def unit_status(self, step: str, key: str, status: str) -> None:
        self.events.append(("status", step, key, status))

    def unit_finished(self, step: str, key: str, *, ok: bool, status: str) -> None:
        self.events.append(("finished", step, key, ok, status))

    def units(self) -> list[tuple[object, ...]]:
        """The unit events alone, without the counts."""
        return [event for event in self.events if event[0] in ("started", "status", "finished")]

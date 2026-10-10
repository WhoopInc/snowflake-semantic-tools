"""The phase timer port: the steps a use case names, so a run can report where its time went.

The domain only names the phases; the adapter behind the port reads the clock. A use case
given no timer reports to `NO_PHASES`, which records nothing.
"""

from __future__ import annotations

import contextlib
from contextlib import AbstractContextManager
from typing import Protocol


class PhaseTimer(Protocol):
    """Time the named steps of a run, such as `observe` or `publish`."""

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Time the block as phase `name`; a phase entered again adds to its total.

        Never raises, and times the block however it ends; an exception in the block
        propagates unchanged. Safe to call from several threads at once.
        """
        ...


class NoPhases:
    """A timer that records nothing, for a use case whose run nobody times."""

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Run the block untimed."""
        return contextlib.nullcontext()


NO_PHASES: PhaseTimer = NoPhases()

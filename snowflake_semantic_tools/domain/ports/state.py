"""The port to the local state cache of one target, and the lock that lets one run at a time use it."""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.domain.state import State


class StateStore(Protocol):
    """The local cache of one target's state, and the lock that lets one run at a time use it.

    The state table is authoritative; the cache only mirrors it. The lock is held by run id,
    and only its holder releases it.
    """

    @property
    def config_path(self) -> str:
        """Return the configuration path recorded in each `State` a use case builds for this store."""
        ...

    @property
    def location(self) -> str:
        """Return where the cache lives, as a diagnostic names it, such as its file path."""
        ...

    def read_local(self) -> State | None:
        """Return the cached state; None when there is no cache.

        Never writes. A cache that exists and cannot be used raises rather than reading as absent.

        Raises:
            ProjectError: the cache is unreadable (SST-MAN022) or declares a schema SST does not
                support (SST-MAN023).
            OSError: the cache exists and cannot be opened.
        """
        ...

    def write_local(self, value: State) -> None:
        """Replace the cache with `value` atomically, so a reader sees the old state or the new one.

        Creates the cache's directory when it is missing.

        Raises:
            OSError: the cache or its directory cannot be written.
        """
        ...

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        """Take the lock for `run_id` unless another run holds it, without waiting.

        A lock older than the store's time limit is stale, and `break_stale` takes it over; a
        lock whose holder or age cannot be read is never stale.

        Returns:
            `(acquired, holder, broke_stale)`: whether `run_id` now holds the lock; the run
            that held it, None when the lock was free or its holder is unreadable; and whether
            a stale lock was taken over.
        """
        ...

    def release_lock(self, run_id: str) -> None:
        """Release the lock when `run_id` holds it, and otherwise leave it alone.

        Idempotent: releasing a lock that is absent or held by another run does nothing.
        """
        ...

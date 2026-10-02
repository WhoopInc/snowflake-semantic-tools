"""A pool of sibling connectors, so parallel apply workers never share a connection.

The pool starts from the connector the command opened and lends it first; each further
concurrent lease opens a sibling through `open_sibling`, which connects with the same
settings, and later leases reuse it. Leaving the pool's block closes every sibling it opened;
the first connector stays its owner's to close.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from threading import Lock
from types import TracebackType
from typing import Generic, Protocol, TypeVar

from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool


class _Closable(Protocol):
    """A connection a pool can close."""

    def close(self) -> None:
        """Close the connection."""
        ...


SessionT = TypeVar("SessionT", bound=_Closable)


class ConnectorPool(SessionPool[SessionT], Generic[SessionT]):
    """Lend up to `size` connectors at once: `first`, then siblings opened as leases overlap.

    Use it as a context manager; leaving the block closes every sibling, even when closing one
    of them fails, and re-raises the first such failure.

    Raises:
        ValueError: `size` is below 1.
    """

    def __init__(self, first: SessionT, size: int, open_sibling: Callable[[], SessionT]) -> None:
        if size < 1:
            raise ValueError("a connector pool holds at least one connector")
        self._first = first
        self._open_sibling = open_sibling
        self._size = size
        self._idle: list[SessionT] = [first]
        self._opened: list[SessionT] = []
        self._opening = 0
        self._lock = Lock()

    def __enter__(self) -> ConnectorPool[SessionT]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @contextlib.contextmanager
    def lease(self) -> Iterator[SessionT]:
        connector = self._take()
        try:
            yield connector
        finally:
            with self._lock:
                self._idle.append(connector)

    def close(self) -> None:
        """Close every sibling the pool opened; the first connector is left open for its owner."""
        with self._lock:
            opened, self._opened = self._opened, []
            self._idle = [self._first]
        failure: BaseException | None = None
        for connector in opened:
            if connector is self._first:
                continue
            try:
                connector.close()
            except Exception as exc:
                failure = failure or exc
        if failure is not None:
            raise failure

    @property
    def opened(self) -> int:
        """How many siblings the pool has opened and not yet closed."""
        return len(self._opened)

    def _take(self) -> SessionT:
        """Take an idle connector, opening a sibling when none is idle.

        Raises:
            RuntimeError: every connector is leased and the pool is full; the caller leased more
                at once than the pool's size.
            SnowflakePortError: opening a sibling failed.
        """
        with self._lock:
            if self._idle:
                return self._idle.pop()
            if 1 + len(self._opened) + self._opening >= self._size:
                raise RuntimeError(f"connector pool of {self._size} is exhausted")
            # Counted before connecting, outside the lock: a login can take seconds.
            self._opening += 1
        try:
            sibling = self._open_sibling()
        finally:
            with self._lock:
                self._opening -= 1
        with self._lock:
            self._opened.append(sibling)
        return sibling

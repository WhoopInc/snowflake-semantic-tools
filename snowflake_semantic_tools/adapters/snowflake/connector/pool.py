"""A pool of sibling connectors, so no worker shares a connection with another or the command.

The pool opens each connector it lends through `open_sibling`, which connects with the
command's settings, and only as leases overlap; later leases reuse an idle one. It never lends
the connector the command opened, which stays the command's: its plan reads, its run lock,
and its state write never queue behind a worker's statements. Leaving the pool's block closes
every connector it opened. `halt` stops them all before their next statement.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from threading import Lock
from types import TracebackType
from typing import Generic, Protocol, TypeVar

from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool


class _Pooled(Protocol):
    """A connection a pool can halt and close."""

    def halt(self, reason: str) -> None:
        """Refuse every statement from now on with `reason`."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


SessionT = TypeVar("SessionT", bound=_Pooled)


class ConnectorPool(SessionPool[SessionT], Generic[SessionT]):
    """Lend up to `size` connectors at once, each a sibling opened as leases overlap.

    Use it as a context manager; leaving the block closes every connector it opened, even
    when closing one of them fails, and re-raises the first such failure.

    Raises:
        ValueError: `size` is below 1.
    """

    def __init__(self, size: int, open_sibling: Callable[[], SessionT]) -> None:
        if size < 1:
            raise ValueError("a connector pool holds at least one connector")
        self._open_sibling = open_sibling
        self._size = size
        self._idle: list[SessionT] = []
        self._opened: list[SessionT] = []
        self._opening = 0
        self._halted: str | None = None
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

    def halt(self, reason: str) -> None:
        with self._lock:
            if self._halted is None:
                self._halted = reason
            halted, opened = self._halted, tuple(self._opened)
        for connector in opened:
            connector.halt(halted)

    def close(self) -> None:
        """Close every connector the pool opened."""
        with self._lock:
            opened, self._opened = self._opened, []
            self._idle = []
        failure: BaseException | None = None
        for connector in opened:
            try:
                connector.close()
            except Exception as exc:
                failure = failure or exc
        if failure is not None:
            raise failure

    @property
    def opened(self) -> int:
        """How many connectors the pool has opened and not yet closed."""
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
            if len(self._opened) + self._opening >= self._size:
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
            halted = self._halted
        # Registered before the check: a halt either reaches this sibling or was recorded first.
        if halted is not None:
            sibling.halt(halted)
        return sibling

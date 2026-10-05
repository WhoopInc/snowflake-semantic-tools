"""Run independent reads concurrently, each on a session of its own, and keep their order.

`Fanout` is how `--threads` reaches a use case. With one thread, or no pool, every item
runs in turn on the port the use case was given, which is how SST read before it had
threads. With more, items run on up to that many workers, each leasing a session from the
pool for the item alone, and the results come back in item order, whatever order the
workers finish in. A use case that merges what the items report in that order therefore
reports the same thing for any thread count.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Generic, TypeVar

from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool

PortT = TypeVar("PortT", covariant=True)
ItemT = TypeVar("ItemT")
ResultT = TypeVar("ResultT")


class Fanout(Generic[PortT]):
    """Map work over items on `port`, or concurrently on sessions leased from `sessions`.

    Every worker thread is joined before `map` returns or raises, and every item runs, whether
    or not another raises. An exception an item raises, opening its session included,
    propagates once every item has run; with several, the first item's in order.

    Args:
        port: Where items run one at a time; the session `sessions` was opened from.
        sessions: Where concurrent items lease their sessions; None runs every item on
            `port`.
        workers: The most items running at once; below 2, items run one at a time.
    """

    def __init__(self, port: PortT, sessions: SessionPool[PortT] | None = None, workers: int = 1) -> None:
        self._port = port
        self._sessions = sessions
        self._workers = workers

    @property
    def port(self) -> PortT:
        """Return the port items run on one at a time."""
        return self._port

    @property
    def workers(self) -> int:
        """Return the most items that run at once: 1 without a pool."""
        return self._workers if self._sessions is not None and self._workers > 1 else 1

    def map(self, work: Callable[[PortT, ItemT], ResultT], items: Sequence[ItemT]) -> tuple[ResultT, ...]:
        """Return `work(port, item)` for each item, in item order.

        Raises:
            SnowflakePortError: a session could not be opened for an item.
        """
        sessions = self._sessions
        if sessions is None or self.workers < 2 or len(items) < 2:
            return tuple(work(self._port, item) for item in items)

        def leased(item: ItemT) -> ResultT:
            with sessions.lease() as session:
                return work(session, item)

        with ThreadPoolExecutor(max_workers=min(self.workers, len(items)), thread_name_prefix="sst-worker") as pool:
            futures = tuple(pool.submit(leased, item) for item in items)
        # Leaving the block waits for every item. Results are read only then: `Executor.map`'s
        # results cancel the items not yet started once one raises, which ones depending on
        # how the workers were scheduled.
        return tuple(future.result() for future in futures)

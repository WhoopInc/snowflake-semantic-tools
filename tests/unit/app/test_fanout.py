"""`Fanout`: items run on the port one at a time, or on leased sessions at once, in item order."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


class Session:
    def __init__(self, name: str) -> None:
        self.name = name


class Pool:
    """Lends a fresh session per lease, and records how many were out at once."""

    def __init__(self) -> None:
        self.leased: list[str] = []
        self.out = 0
        self.most_out = 0
        self.overlapped = threading.Event()
        self._guard = threading.Lock()

    @contextmanager
    def lease(self) -> Iterator[Session]:
        with self._guard:
            self.out += 1
            self.most_out = max(self.most_out, self.out)
            if self.out > 1:
                self.overlapped.set()
            session = Session(f"session-{len(self.leased)}")
            self.leased.append(session.name)
        try:
            yield session
        finally:
            with self._guard:
                self.out -= 1

    def halt(self, reason: str) -> None:
        del reason


def _slower_first(session: Session, item: int) -> tuple[int, str]:
    # Earlier items take longer, so concurrent workers finish them last.
    time.sleep(0.002 * (8 - item))
    return item, session.name


def test_items_run_on_the_port_in_order_without_a_pool_or_with_one_worker() -> None:
    port = Session("port")
    pool = Pool()
    for fanout in (Fanout(port), Fanout(port, pool, 1), Fanout(port, None, 4)):
        assert fanout.workers == 1
        assert fanout.map(_slower_first, range(8)) == tuple((item, "port") for item in range(8))
    assert pool.leased == []


def test_concurrent_items_lease_a_session_each_and_keep_item_order() -> None:
    pool = Pool()
    fanout = Fanout(Session("port"), pool, 3)

    def work(session: Session, item: int) -> tuple[int, str]:
        if item == 0:
            # Hold the first item until a second is leased, so the items overlap however slowly
            # the workers are scheduled.
            assert pool.overlapped.wait(timeout=5)
        return _slower_first(session, item)

    results = fanout.map(work, tuple(range(8)))

    assert [item for item, _ in results] == list(range(8))
    assert all(name.startswith("session-") for _, name in results)
    assert len(pool.leased) == 8 and pool.out == 0
    assert 1 < pool.most_out <= 3


def test_a_single_item_runs_on_the_port() -> None:
    pool = Pool()
    assert Fanout(Session("port"), pool, 4).map(_slower_first, (3,)) == ((3, "port"),)
    assert pool.leased == []


def test_an_item_error_propagates_once_every_item_has_run() -> None:
    pool = Pool()
    finished: list[int] = []
    guard = threading.Lock()

    def work(session: Session, item: int) -> int:
        del session
        if item in (1, 4):
            raise SnowflakePortError(f"item {item} refused")
        with guard:
            finished.append(item)
        return item

    # Two workers and six items: most items are still queued when item 1 raises, and each
    # runs all the same, however the workers are scheduled. The first error in item order
    # is the one that propagates.
    with pytest.raises(SnowflakePortError, match="item 1 refused"):
        Fanout(Session("port"), pool, 2).map(work, tuple(range(6)))

    assert sorted(finished) == [0, 2, 3, 5]
    assert len(pool.leased) == 6 and pool.out == 0

"""Pool sessions that log in at once leave stdout where it was, so `--output json` stays on stdout.

A login sends the driver's stdout prints (browser SSO prompts) to stderr. Pool sessions log in on
worker threads together; a redirect each would interleave, and the last to finish could leave the
whole process writing stdout to stderr, envelope included.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Iterator

import click
import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool, SnowflakeConnector
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.driver import FakeDriverSession

LOGINS = 8


class PromptingDriver:
    """A driver whose logins print a prompt and stay in flight until every login has begun.

    The first login is let in alone, then the rest; once all are in flight the first finishes
    first and the others one by one after it, the order that leaves stdout on stderr when each
    login redirects stdout on its own.
    """

    def __init__(self, logins: int) -> None:
        self.all_in = threading.Barrier(logins, timeout=30)
        self.first_in = threading.Event()
        self.order = 0
        self._guard = threading.Lock()

    def connect(self, **params: object) -> FakeDriverSession:
        del params
        with self._guard:
            index, self.order = self.order, self.order + 1
        print(f"Going to open a browser window for login {index}")
        if index == 0:
            self.first_in.set()
        self.all_in.wait()
        time.sleep(0.01 * index)
        return FakeDriverSession()


@pytest.fixture
def driver(monkeypatch: pytest.MonkeyPatch) -> Iterator[PromptingDriver]:
    prompting = PromptingDriver(LOGINS)
    monkeypatch.setattr("snowflake.connector.connect", prompting.connect)
    yield prompting


def test_sessions_logging_in_at_once_restore_stdout_and_the_envelope_reaches_stdout(
    driver: PromptingDriver, capsys: pytest.CaptureFixture[str]
) -> None:
    stdout = sys.stdout
    failures: list[BaseException] = []
    with ConnectorPool(LOGINS, lambda: SnowflakeConnector({"account": "acme"})) as pool:

        def lease() -> None:
            try:
                with pool.lease():
                    pass
            except BaseException as exc:
                failures.append(exc)

        threads = [threading.Thread(target=lease) for _ in range(LOGINS)]
        threads[0].start()
        assert driver.first_in.wait(timeout=30)
        for thread in threads[1:]:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert pool.opened == LOGINS
    assert failures == []
    assert sys.stdout is stdout
    click.echo('{"tool": "sst", "status": "ok"}')
    printed = capsys.readouterr()
    assert printed.out == '{"tool": "sst", "status": "ok"}\n'
    assert sorted(printed.err.splitlines()) == sorted(
        f"Going to open a browser window for login {index}" for index in range(LOGINS)
    )


def test_a_login_that_fails_restores_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def refuse(**params: object) -> FakeDriverSession:
        print("Going to open a browser window")
        raise OSError("no route to host")

    monkeypatch.setattr("snowflake.connector.connect", refuse)
    stdout = sys.stdout
    with pytest.raises(SnowflakePortError):
        SnowflakeConnector({"account": "acme"})
    assert sys.stdout is stdout
    assert capsys.readouterr().out == ""

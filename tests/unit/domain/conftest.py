"""Shared pure-domain test fixtures."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def domain_tests_do_not_open_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refused(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("domain tests must not access the network")

    monkeypatch.setattr("socket.socket.connect", refused)
    monkeypatch.setattr("socket.create_connection", refused)

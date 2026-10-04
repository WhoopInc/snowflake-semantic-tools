"""The fast gate's network guard refuses an outbound connection or lookup and lets loopback through."""

from __future__ import annotations

import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest

from tests.helpers import network_guard
from tests.helpers.network_guard import NetworkAccessRefused

# TEST-NET-1 and a reserved top-level domain: neither reaches anything should the guard fail.
_REMOTE = ("192.0.2.1", 443)
_UNRESOLVABLE = "sst-network-guard.invalid"


@contextmanager
def _expect_refusal(count: int = 1) -> Iterator[None]:
    before = network_guard.refusal_count()
    with pytest.raises(NetworkAccessRefused, match="outside the `live` marker"):
        yield
    assert len(network_guard.refusals_since(before)) == count


def _ipv4() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    return sock


def test_an_outbound_connect_is_refused() -> None:
    with _ipv4() as sock, _expect_refusal():
        sock.connect(_REMOTE)


def _connect_ex() -> int:
    with _ipv4() as sock:
        return sock.connect_ex(_REMOTE)


@pytest.mark.parametrize(
    "attempt",
    [
        pytest.param(_connect_ex, id="connect_ex"),
        pytest.param(lambda: socket.create_connection(_REMOTE, timeout=1), id="create_connection"),
        pytest.param(lambda: socket.getaddrinfo(_UNRESOLVABLE, 443), id="getaddrinfo"),
    ],
)
def test_every_other_way_out_is_refused(attempt: Callable[[], object]) -> None:
    with _expect_refusal():
        attempt()


def test_a_refusal_names_the_destination() -> None:
    before = network_guard.refusal_count()
    with pytest.raises(NetworkAccessRefused):
        socket.getaddrinfo(_UNRESOLVABLE, 443)
    assert network_guard.refusals_since(before) == [f"socket.getaddrinfo({_UNRESOLVABLE!r})"]


def test_loopback_stays_open() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(2)
        port = server.getsockname()[1]
        with _ipv4() as client:
            client.connect(("127.0.0.1", port))
        socket.create_connection(("localhost", port), timeout=1).close()
    assert socket.getaddrinfo("localhost", port)


@pytest.mark.parametrize(
    ("host", "local"),
    [
        (None, True),
        ("localhost", True),
        (b"127.0.0.1", True),
        ("127.8.9.10", True),
        ("::1", True),
        ("[::1]", True),
        ("example.com", False),
        ("10.0.0.1", False),
        ("", False),
        (8080, False),
    ],
)
def test_only_loopback_counts_as_local(host: object, local: bool) -> None:
    assert network_guard.is_local(host) is local


def test_a_unix_socket_and_a_live_test_pass() -> None:
    network_guard.check("unix", "/tmp/sst.sock", socket.AF_UNIX)
    with network_guard.allowed():
        network_guard.check("live", _REMOTE[0])
    with _expect_refusal():
        network_guard.check("after", _REMOTE[0])

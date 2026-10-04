"""The fast gate's network guard: an outbound connection or a name lookup fails the test that made it.

`installed` patches `socket.socket.connect`, `socket.socket.connect_ex`, `socket.create_connection`
and `socket.getaddrinfo` for as long as it is entered. A Unix-domain socket and the loopback
addresses (`127.0.0.0/8`, `::1`, `localhost`) stay open, since pytest-xdist and a test's own local
server may use them; anything else raises `NetworkAccessRefused` and is recorded, so a refusal the
code under test catches and swallows still fails the test (`refusals_since`). A test marked `live`
runs inside `allowed`, which lifts the guard for its duration.

The guard holds in the pytest process and its threads only. A subprocess a test starts (the
end-to-end layer's `run_sst`) is a fresh interpreter the patches do not reach.
"""

from __future__ import annotations

import ipaddress
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest

_LOCAL_NAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"})
_UNIX = getattr(socket, "AF_UNIX", None)


class NetworkAccessRefused(RuntimeError):
    """A test outside the `live` marker tried to reach the network.

    Not an `OSError`, so a driver's retry-on-transport-error loop does not catch it and spin.
    """


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.allowances = 0
        self.refused: list[str] = []


_STATE = _State()


def is_local(host: object) -> bool:
    """True for a host the guard lets through: no host at all, a loopback name, or a loopback address."""
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode("ascii", "replace")
    if not isinstance(host, str):
        return False
    name = host.strip("[]").lower()
    if name in _LOCAL_NAMES:
        return True
    try:
        return ipaddress.ip_address(name.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def check(what: str, host: object, family: object = None) -> None:
    """Raise `NetworkAccessRefused` unless `host` is local, `family` is AF_UNIX, or a live test runs."""
    if (_UNIX is not None and family == _UNIX) or is_local(host):
        return
    with _STATE.lock:
        if _STATE.allowances:
            return
        _STATE.refused.append(what)
    raise NetworkAccessRefused(
        f"a test outside the `live` marker tried to reach the network ({what}); fake the call with "
        "tests/helpers, or mark the test `live` if it must talk to Snowflake"
    )


def _host(address: object) -> object:
    return address[0] if isinstance(address, tuple) and address else address


def _guarded_method(original: Callable[..., Any], name: str) -> Callable[..., Any]:
    def guarded(self: socket.socket, address: Any) -> Any:
        check(f"socket.{name}({address!r})", _host(address), self.family)
        return original(self, address)

    return guarded


def _guarded_function(original: Callable[..., Any], name: str) -> Callable[..., Any]:
    def guarded(address: Any, *args: Any, **kwargs: Any) -> Any:
        host = address if name == "getaddrinfo" else _host(address)
        check(f"socket.{name}({address!r})", host)
        return original(address, *args, **kwargs)

    return guarded


@contextmanager
def installed() -> Iterator[None]:
    """Refuse every non-local connection and name lookup until the block exits."""
    with pytest.MonkeyPatch.context() as patch:
        for method in ("connect", "connect_ex"):
            patch.setattr(socket.socket, method, _guarded_method(getattr(socket.socket, method), method))
        for function in ("create_connection", "getaddrinfo"):
            patch.setattr(socket, function, _guarded_function(getattr(socket, function), function))
        yield


@contextmanager
def allowed() -> Iterator[None]:
    """Lift the guard, for a live test and the live fixtures that open and close its connection."""
    with _STATE.lock:
        _STATE.allowances += 1
    try:
        yield
    finally:
        with _STATE.lock:
            _STATE.allowances -= 1


def refusal_count() -> int:
    """How many attempts the guard has refused so far in this process."""
    with _STATE.lock:
        return len(_STATE.refused)


def refusals_since(count: int) -> list[str]:
    """The attempts refused after the first `count`, which the caller then owns: they are dropped."""
    with _STATE.lock:
        found = _STATE.refused[count:]
        del _STATE.refused[count:]
    return found

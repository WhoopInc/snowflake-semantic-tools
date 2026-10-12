"""The fast gate's network guard: an outbound connection or a name lookup fails the test that made it.

`installed` patches `socket.socket.connect`, `socket.socket.connect_ex`, `socket.create_connection`
and `socket.getaddrinfo` for as long as it is entered. A Unix-domain socket and the loopback
addresses (`127.0.0.0/8`, `::1`, `localhost`) stay open, since pytest-xdist and a test's own local
server may use them; anything else raises `NetworkAccessRefused` and is recorded, so a refusal the
code under test catches and swallows still fails the test (`refusals_since`). A test marked `live`
runs inside `allowed`, which lifts the guard for its duration.

A subprocess is a fresh interpreter the patches do not reach, so a test that starts one passes it
`guarded_environment`: the end-to-end layer's `run_sst` does, as do the tests that run `sst` in a
child of their own. That environment puts `subprocess_guard/` at the front of the child's
`PYTHONPATH`, whose `sitecustomize` installs the guard at startup (`install_in_subprocess`), or
stops the child with `UNGUARDED_EXIT` when it cannot, and then runs any `sitecustomize` it shadows.
In the child a refusal is also written to stderr, and a child that was refused anything exits
`REFUSED_EXIT` whatever it would have exited with, so a refusal it swallowed still fails the test.
While the guard is lifted or not installed -- a live test, or a helper run as a script -- the
environment is left unguarded.
"""

from __future__ import annotations

import atexit
import ipaddress
import os
import socket
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

# Set in a subprocess's environment to install the guard there; `subprocess_guard/` reads it.
GUARD_VARIABLE = "SST_TEST_NETWORK_GUARD"
# What a guarded subprocess exits with once it has been refused anything.
REFUSED_EXIT = 86
# What a subprocess asked for the guard exits with at startup when it cannot install it.
UNGUARDED_EXIT = 87
_SUBPROCESS_HOOK = Path(__file__).resolve().parent / "subprocess_guard"
_REPO_ROOT = Path(__file__).resolve().parents[2]
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
        self.installs = 0
        self.refused: list[str] = []
        self.to_stderr = False


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
        to_stderr = _STATE.to_stderr
    refusal = NetworkAccessRefused(
        f"a test outside the `live` marker tried to reach the network ({what}); fake the call with "
        "tests/helpers, or mark the test `live` if it must talk to Snowflake"
    )
    if to_stderr:
        sys.stderr.write(f"{type(refusal).__name__}: {refusal}\n")
    raise refusal


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
    """Refuse every non-local connection and name lookup until the block exits.

    It patches `socket` by hand, not with pytest's `MonkeyPatch`, so a subprocess that has no
    pytest to import can still hold it.
    """
    originals = [(socket.socket, name, getattr(socket.socket, name)) for name in ("connect", "connect_ex")]
    originals += [(socket, name, getattr(socket, name)) for name in ("create_connection", "getaddrinfo")]
    for owner, name, original in originals:
        wrap = _guarded_method if owner is socket.socket else _guarded_function
        setattr(owner, name, wrap(original, name))
    with _STATE.lock:
        _STATE.installs += 1
    try:
        yield
    finally:
        with _STATE.lock:
            _STATE.installs -= 1
        for owner, name, original in reversed(originals):
            setattr(owner, name, original)


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


def guarded_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """Return `environ` for a subprocess, which then holds the guard while it holds here.

    The repository root goes on `PYTHONPATH` too, so the child can import the guard. Otherwise
    -- the guard lifted for a live test, or not installed -- the guard variable is removed.
    """
    environment = dict(environ)
    with _STATE.lock:
        holds = _STATE.installs > 0 and not _STATE.allowances
    if not holds:
        environment.pop(GUARD_VARIABLE, None)
        return environment
    environment[GUARD_VARIABLE] = "1"
    paths = (str(_SUBPROCESS_HOOK), str(_REPO_ROOT), environment.get("PYTHONPATH"))
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, paths))
    return environment


_HELD = ExitStack()


def install_in_subprocess() -> None:
    """Hold the guard for the rest of this interpreter, reporting each refusal on stderr.

    An interpreter refused anything exits `REFUSED_EXIT` at exit, after flushing its output.
    """
    _HELD.enter_context(installed())
    with _STATE.lock:
        _STATE.to_stderr = True
    atexit.register(_exit_if_refused)


def _exit_if_refused() -> None:
    if refusal_count():
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(REFUSED_EXIT)

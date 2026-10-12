"""The fast gate's network guard refuses an outbound connection or lookup and lets loopback through."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

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


def _child(code: str, environ: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        env=environ,
        capture_output=True,
        text=True,
        timeout=60,
        stdin=subprocess.DEVNULL,
        check=False,
    )


_CONNECT = f"import socket; socket.create_connection({_REMOTE!r}, timeout=1)"


def test_a_guarded_subprocess_is_refused_and_fails_even_when_it_swallows_the_refusal() -> None:
    environment = network_guard.guarded_environment(os.environ)
    refused = _child(_CONNECT, environment)
    assert refused.returncode != 0 and "NetworkAccessRefused" in refused.stderr, refused.stderr
    swallowed = _child(f"try:\n    {_CONNECT}\nexcept Exception:\n    pass", environment)
    assert swallowed.returncode == network_guard.REFUSED_EXIT, swallowed.stderr
    assert "NetworkAccessRefused" in swallowed.stderr
    assert _child("import socket; socket.getaddrinfo('localhost', 80)", environment).returncode == 0


def test_a_subprocess_runs_unguarded_while_the_guard_is_lifted() -> None:
    guarded = network_guard.guarded_environment(os.environ)
    with network_guard.allowed():
        lifted = network_guard.guarded_environment(guarded)
    assert network_guard.GUARD_VARIABLE not in lifted
    checks = "import socket, sys; sys.exit(int(socket.create_connection.__module__ != 'socket'))"
    assert _child(checks, lifted).returncode == 0
    assert _child(checks, guarded).returncode == 1


_GUARDED = "import socket, sys; sys.exit(int(socket.create_connection.__module__ == 'socket'))"


def _with_path_after_guard(environ: dict[str, str], extra: Path) -> dict[str, str]:
    """`environ` with `extra` on `PYTHONPATH` right after the guard's own directory."""
    first, _, rest = environ["PYTHONPATH"].partition(os.pathsep)
    return {**environ, "PYTHONPATH": os.pathsep.join(filter(None, (first, str(extra), rest)))}


def test_a_guarded_subprocess_still_runs_the_sitecustomize_it_shadows(tmp_path: Path) -> None:
    (tmp_path / "sitecustomize.py").write_text("SHADOWED = 'ran'\n", encoding="utf-8")
    environment = _with_path_after_guard(network_guard.guarded_environment(os.environ), tmp_path)
    shadowed = "import sys; sys.exit(0 if getattr(sys.modules['sitecustomize'], 'SHADOWED', '') == 'ran' else 3)"
    assert _child(shadowed, environment).returncode == 0
    assert _child(_GUARDED, environment).returncode == 0


def test_a_guarded_subprocess_holds_the_guard_without_pytest(tmp_path: Path) -> None:
    (tmp_path / "pytest.py").write_text("raise ImportError('no pytest in this interpreter')\n", encoding="utf-8")
    environment = _with_path_after_guard(network_guard.guarded_environment(os.environ), tmp_path)
    done = _child(_GUARDED, environment)
    assert done.returncode == 0, done.stderr


def test_a_subprocess_that_cannot_install_the_guard_fails_loudly(tmp_path: Path) -> None:
    hook = network_guard.guarded_environment(os.environ)["PYTHONPATH"].split(os.pathsep)[0]
    # A stub `tests.helpers.network_guard` that cannot be imported, ahead of site-packages, where
    # an editable install would otherwise make the real one importable.
    stub = tmp_path / "stub" / "tests" / "helpers"
    stub.mkdir(parents=True)
    (stub.parent / "__init__.py").write_text("", encoding="utf-8")
    (stub / "__init__.py").write_text("", encoding="utf-8")
    (stub / "network_guard.py").write_text("raise ImportError('no guard in this interpreter')\n", encoding="utf-8")
    environment = {
        **os.environ,
        network_guard.GUARD_VARIABLE: "1",
        "PYTHONPATH": os.pathsep.join((hook, str(tmp_path / "stub"))),
    }
    done = subprocess.run(
        [sys.executable, "-c", "print('ran unguarded')"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        stdin=subprocess.DEVNULL,
        check=False,
    )
    assert done.returncode == network_guard.UNGUARDED_EXIT, done.stderr
    assert "ran unguarded" not in done.stdout
    assert "the test network guard could not be installed" in done.stderr

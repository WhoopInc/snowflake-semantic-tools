"""SIGTERM and SIGHUP end every command as Ctrl-C does: exit 130 with SST-PRT107, every lock released.

The unit tests drive `cli.signals` in this process; the end-to-end ones run `sst plan` in a child
process, held where it would connect, and signal it there. Every child has its stdin closed and
a bounded wait, so a signal that is not handled fails the test instead of hanging it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from snowflake_semantic_tools.cli import signals
from snowflake_semantic_tools.cli.output import aborted_line
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.network_guard import guarded_environment
from tests.helpers.reference_project import REPO_ROOT, project_copy

posix_only = pytest.mark.skipif(not hasattr(signal, "SIGHUP"), reason="needs POSIX signals")

# The child: `sst plan`, whose connection never opens; it writes `ready` where it would connect.
_HELD_AT_CONNECT = """
import sys, time
from pathlib import Path
import snowflake_semantic_tools.cli.main as main

def held(params):
    Path(sys.argv[1]).write_text("ready")
    time.sleep(60)
    raise SystemExit("not interrupted")

main.SnowflakeConnector = held
main.cli(sys.argv[2:], prog_name="sst")
"""


@posix_only
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_each_stop_signal_raises_an_interrupt_and_is_ignored_after_the_first(signum: int) -> None:
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        with signals.terminated_as_interrupt():
            with pytest.raises(KeyboardInterrupt):
                signal.raise_signal(signum)
            assert signals.received_signal() == signal.Signals(signum).name
            assert aborted_line() == f"Aborted: received {signal.Signals(signum).name}."
            # A CI runner's second SIGTERM cannot cut the cleanup short; Ctrl-C still can.
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGHUP)
            with pytest.raises(KeyboardInterrupt):
                signal.raise_signal(signal.SIGINT)
            assert signals.received_signal() == signal.Signals(signum).name
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    finally:
        signal.signal(signal.SIGINT, previous)


@posix_only
def test_an_ignored_sigint_stays_ignored_and_each_previous_handler_comes_back() -> None:
    def mine(signum: int, frame: object) -> None:
        del signum, frame

    before = signal.signal(signal.SIGINT, signal.SIG_IGN), signal.signal(signal.SIGHUP, mine)
    try:
        with signals.terminated_as_interrupt():
            assert signal.getsignal(signal.SIGINT) is signal.SIG_IGN
            assert signal.getsignal(signal.SIGHUP) is not mine
        assert signal.getsignal(signal.SIGHUP) is mine
        assert signals.received_signal() == "" and aborted_line() == "Aborted."
    finally:
        signal.signal(signal.SIGINT, before[0])
        signal.signal(signal.SIGHUP, before[1])


def test_a_handler_the_c_runtime_set_is_restored_as_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # `signal.signal` returns None for a handler not set from Python; SIG_DFL stands in for it.
    restored: list[object] = []

    def install(number: int, handler: object) -> object:
        restored.append(handler)
        return None

    monkeypatch.setattr(signal, "signal", install)
    monkeypatch.setattr(signal, "getsignal", lambda number: None)
    with signals.terminated_as_interrupt():
        pass
    assert restored[-len(signals._STOP_SIGNALS) :] == [signal.SIG_DFL] * len(signals._STOP_SIGNALS)


def test_a_worker_thread_runs_the_block_unchanged() -> None:
    seen: list[object] = []

    def in_worker() -> None:
        with signals.terminated_as_interrupt():
            seen.append(signal.getsignal(signal.SIGTERM))

    worker = threading.Thread(target=in_worker)
    worker.start()
    worker.join(timeout=10)
    assert seen == [signal.getsignal(signal.SIGTERM)]


@posix_only
@pytest.mark.parametrize("signum", [signal.SIGHUP, signal.SIGTERM])
def test_a_stop_signal_inherited_as_ignored_stays_ignored(signum: int) -> None:
    before = signal.signal(signum, signal.SIG_IGN)
    try:
        with signals.terminated_as_interrupt():
            assert signal.getsignal(signum) is signal.SIG_IGN
            signal.raise_signal(signum)
            assert signals.received_signal() == ""
        assert signal.getsignal(signum) is signal.SIG_IGN
    finally:
        signal.signal(signum, before)


def _held_plan(tmp_path: Path, sighup: signal.Handlers) -> subprocess.Popen[str]:
    """Start `sst plan` in a child with SIGHUP set to ``sighup``, returning once it is held at connect.

    The disposition is set explicitly, so the suite behaves the same when it runs under `nohup`.
    """
    project = project_copy(tmp_path)
    compile_project(project)
    ready = tmp_path / "ready"
    environment = guarded_environment(
        {**os.environ, "PYTHONPATH": os.pathsep.join([str(REPO_ROOT), os.environ.get("PYTHONPATH", "")])}
    )
    child = subprocess.Popen(
        [sys.executable, "-c", _HELD_AT_CONNECT, str(ready), "plan", *common(project), "--target", "dev"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=REPO_ROOT,
        env=environment,
        preexec_fn=lambda: signal.signal(signal.SIGHUP, sighup),
    )
    deadline = time.monotonic() + 60
    while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    if not ready.exists():
        child.kill()
        pytest.fail(str(child.communicate(timeout=30)))
    return child


def _stop(child: subprocess.Popen[str], *signums: int) -> str:
    """Send each signal in turn, pausing between them, and return the child's stderr once it exits."""
    try:
        for signum in signums[:-1]:
            child.send_signal(signum)
            time.sleep(1)
            assert child.poll() is None, child.communicate(timeout=30)
        child.send_signal(signums[-1])
        _, stderr = child.communicate(timeout=30)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=30)
    return stderr


@posix_only
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP])
def test_a_signalled_sst_plan_exits_130_with_prt107_naming_the_signal(tmp_path: Path, signum: int) -> None:
    child = _held_plan(tmp_path, signal.SIG_DFL)
    stderr = _stop(child, signum)

    name = signal.Signals(signum).name
    assert child.returncode == 130, stderr
    assert f"Aborted: received {name}." in stderr and "SST-PRT107" in stderr
    assert "not interrupted" not in stderr


@posix_only
def test_sst_plan_started_with_sighup_ignored_survives_it_and_still_stops_on_sigterm(tmp_path: Path) -> None:
    # As under `nohup`: a closed terminal must not interrupt the run.
    child = _held_plan(tmp_path, signal.SIG_IGN)
    stderr = _stop(child, signal.SIGHUP, signal.SIGTERM)

    assert child.returncode == 130, stderr
    assert "Aborted: received SIGTERM." in stderr and "SST-PRT107" in stderr
    assert "SIGHUP" not in stderr

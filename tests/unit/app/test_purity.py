"""The pure-phase guard: what it refuses, what it lets the import system do, and what it never runs."""

from __future__ import annotations

import importlib
import sys
from functools import partial
from pathlib import Path
from types import FrameType

import pytest

from snowflake_semantic_tools.app.purity import _HOOK, ImpureCall, pure_phase


def test_the_hook_is_getattr_in_c_so_an_unwatched_event_runs_no_python() -> None:
    called: list[str] = []

    def profile(frame: FrameType, event: str, arg: object) -> None:
        if event == "call":
            called.append(frame.f_code.co_name)

    assert isinstance(_HOOK, partial) and _HOOK.func is getattr
    with pure_phase("rendering view"):
        sys.setprofile(profile)
        try:
            _HOOK("sys.settrace", ())
            _HOOK("sys.setprofile", ())
        finally:
            sys.setprofile(None)
    assert called == []


def test_a_watched_event_is_refused_only_inside_a_pure_phase() -> None:
    assert _HOOK("socket.connect", ()) is None
    with pytest.raises(ImpureCall) as raised, pure_phase("rendering view"):
        _HOOK("socket.connect", ())
    assert (raised.value.phase, raised.value.event) == ("rendering view", "socket.connect")


def test_the_import_system_may_open_files_in_a_pure_phase_and_nothing_else_may(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "sst_purity_probe.py").write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys, "dont_write_bytecode", False)
    try:
        with pure_phase("rendering view"):
            probe = importlib.import_module("sst_purity_probe")
    finally:
        sys.modules.pop("sst_purity_probe", None)
    assert probe.VALUE == 1
    assert list((tmp_path / "__pycache__").glob("sst_purity_probe.*.pyc"))
    with pytest.raises(ImpureCall), pure_phase("rendering view"):
        (tmp_path / "sst_purity_probe.py").read_text(encoding="utf-8")

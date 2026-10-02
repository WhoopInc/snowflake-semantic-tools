"""SST-PRT012: output would carry a credential the run resolved, so it is withheld and the run exits 1."""

from __future__ import annotations

import json

import pytest

from snowflake_semantic_tools.cli import output
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag


def test_sst_prt012_fires(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(output, "_SECRETS", set())
    output.register_secrets(("hunter2", ""))
    leaked = output.json_envelope("debug", DiagnosticBag(), data={"error": "login hunter2 refused"})
    assert output.print_envelope(leaked) is True
    envelope = json.loads(capsys.readouterr().out)
    [diagnostic] = envelope["diagnostics"]
    assert (diagnostic["code"], diagnostic["severity"]) == ("SST-PRT012", "error")
    assert diagnostic["message"] == "a credential in the JSON envelope would be rendered verbatim"
    assert "hunter2" not in json.dumps(envelope) and envelope["exit_code"] == 1
    output.render_diagnostics(DiagnosticBag((D("SST-INT001", detail="hunter2"),)))
    assert "hunter2" not in capsys.readouterr().err


def test_sst_prt012_silent(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(output, "_SECRETS", set())
    output.register_secrets(("hunter2",))
    assert output.print_envelope(output.json_envelope("debug", DiagnosticBag(), data={"ok": True})) is False
    assert "SST-PRT012" not in capsys.readouterr().out

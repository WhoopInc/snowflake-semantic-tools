"""SST-VAL871: a CoWork profile (a Snowflake private preview) names no subagents."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.cowork_profiles import load_cowork_profiles
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import codes, only

FILE = "cowork_profiles/everyone/cowork_profile.yml"


def _load(tmp_path: Path, text: str, config: dict[str, object] | None = None) -> list[str]:
    path = tmp_path / FILE
    path.parent.mkdir(parents=True)
    path.write_text(text, encoding="utf-8")
    _, diagnostics = load_cowork_profiles(tmp_path, config=config)
    return codes(diagnostics)


@pytest.mark.parametrize("spec", ["", "spec: {}\n", "spec:\n  subagents: []\n"])
def test_sst_val871_fires(tmp_path: Path, spec: str) -> None:
    path = tmp_path / FILE
    path.parent.mkdir(parents=True)
    path.write_text(f"name: everyone\n{spec}", encoding="utf-8")
    _, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-VAL871")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "cowork profile 'everyone': spec.subagents names no agent"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", Origin(FILE, 1, 1))


def test_sst_val871_silent(tmp_path: Path) -> None:
    assert _load(tmp_path, "name: everyone\nspec:\n  subagents:\n    - agent: \"{{ agent('sales') }}\"\n") == []


def test_sst_val871_silent_for_a_disabled_profile(tmp_path: Path) -> None:
    assert "SST-VAL871" not in _load(tmp_path, "name: everyone\nconfig:\n  enabled: false\n")

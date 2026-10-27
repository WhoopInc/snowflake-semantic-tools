"""SST-VAL873: a CoWork profile (a Snowflake private preview) names one subagent twice."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.cowork_profiles import load_cowork_profiles
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import codes, only

FILE = "cowork_profiles/everyone/cowork_profile.yml"


def _write(tmp_path: Path, *agents: str) -> None:
    path = tmp_path / FILE
    path.parent.mkdir(parents=True)
    entries = "".join(f'    - agent: "{{{{ {call} }}}}"\n' for call in agents)
    path.write_text(f"name: everyone\nspec:\n  subagents:\n{entries}", encoding="utf-8")


def test_sst_val873_fires(tmp_path: Path) -> None:
    _write(tmp_path, "agent('sales')", "agent('Sales')")
    _, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-VAL873")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "cowork profile 'everyone': spec.subagents[1] names agent('Sales') again"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", Origin(FILE, 5, 7))


def test_sst_val873_silent(tmp_path: Path) -> None:
    # The same name through another function is another subagent: an agent tool member.
    _write(tmp_path, "agent('sales')", "tool('sales')")
    profiles, diagnostics = load_cowork_profiles(tmp_path)
    assert codes(diagnostics) == []
    assert [(item.reference, item.name) for item in profiles[0].subagents] == [("agent", "sales"), ("tool", "sales")]

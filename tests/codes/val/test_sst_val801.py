"""SST-VAL801: skill folder layout is wrong.

Fires when a skill folder is not kebab-case; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import (
    skill,
    skill_catalog,
)

REF = "---\nname: month-close\ndescription: Does things.\n---\n# Close\nRead {ref}.\n"
SKILLS = {"month-close": skill()}
TARGET = QualifiedName.parse("DB.S.KIT")


def test_sst_val801_fires() -> None:
    diagnostic = only(validate_skill_catalog(skill_catalog(skill("Month_Close", declared_name=None))), "SST-VAL801")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "'Month_Close': folder name 'Month_Close' is not lowercase kebab-case of at most 64 characters"
    )
    assert diagnostic.subject == "skill:Month_Close"


def test_sst_val801_silent() -> None:
    assert "SST-VAL801" not in codes(validate_skill_catalog(skill_catalog()))


AGENT = "name: {name}\n"


def test_sst_val801_fires_for_an_agent_folder_with_both_spellings(tmp_path: Path) -> None:
    folder = tmp_path / "agents" / "sales" / "zz_two"
    folder.mkdir(parents=True)
    (folder / "agent.yml").write_text(AGENT.format(name="zz_two"), encoding="utf-8")
    (folder / "agent.yaml").write_text(AGENT.format(name="zz_two_b"), encoding="utf-8")
    agents, diagnostics = load_agents(tmp_path)
    diagnostic = only(diagnostics, "SST-VAL801")
    assert diagnostic.message == "'agents/sales/zz_two': agent folder has both agent.yml and agent.yaml"
    assert diagnostic.origin == Origin("agents/sales/zz_two")
    assert agents == ()


@pytest.mark.parametrize("spelling", ["agent.yml", "agent.yaml"])
def test_sst_val801_silent_for_an_agent_folder_with_one_spelling(tmp_path: Path, spelling: str) -> None:
    folder = tmp_path / "agents" / "sales" / "zz_two"
    folder.mkdir(parents=True)
    (folder / spelling).write_text(AGENT.format(name="zz_two"), encoding="utf-8")
    (folder / "agent.yxml").write_text(AGENT.format(name="zz_two_b"), encoding="utf-8")
    agents, diagnostics = load_agents(tmp_path)
    assert "SST-VAL801" not in codes(diagnostics)
    assert [agent.name for agent in agents] == ["zz_two"]

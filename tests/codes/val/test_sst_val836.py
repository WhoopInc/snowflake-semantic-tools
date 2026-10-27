"""SST-VAL836: plugin member cannot be bundled.

A plugin whose member skill has an error (a file a stage rejects) is blocked with it; a plugin
of healthy members compiles.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.skills import CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import CATALOG_CHANNEL, plugin, skill, skill_catalog


def test_sst_val836_fires() -> None:
    broken = skill(files={"reference/q1+q2.md": "q"})
    result = CompileSkills(skill_catalog(broken, plugins=(plugin(),)), CATALOG_CHANNEL).run_result()
    diagnostic = only(result.diagnostics, "SST-VAL836")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message == "plugin 'finance-kit': member 'month-close' has errors, so the plugin cannot be bundled"
    )
    assert diagnostic.subject == "plugin:finance-kit"
    assert result.compiled == ()


def test_sst_val836_silent() -> None:
    result = CompileSkills(skill_catalog(skill(), plugins=(plugin(),)), CATALOG_CHANNEL).run_result()
    assert "SST-VAL836" not in codes(result.diagnostics)
    assert [item.artifact_key for item in result.compiled] == ["plugin:finance-kit", "skill:month-close"]

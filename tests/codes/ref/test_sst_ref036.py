"""SST-REF036: `plugin()` names a plugin this project does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.ref_codes import skill_entry_findings


def test_sst_ref036_fires() -> None:
    [diagnostic] = skill_entry_findings(AgentSkill("", "CORTEX_EXTENSION", "ghost-kit", "", ref="plugin"), "SST-REF036")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ plugin('ghost-kit') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref036_silent() -> None:
    assert skill_entry_findings(AgentSkill("", "CORTEX_EXTENSION", "toolkit", "", ref="plugin"), "SST-REF036") == []

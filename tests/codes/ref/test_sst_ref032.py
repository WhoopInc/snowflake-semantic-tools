"""SST-REF032: `skill()` names a skill this project does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.ref_codes import skill_entry_findings


def test_sst_ref032_fires() -> None:
    [diagnostic] = skill_entry_findings(AgentSkill("ghost", "CORTEX_EXTENSION", "ghost", "", ref="skill"), "SST-REF032")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ skill('ghost') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref032_silent() -> None:
    assert (
        skill_entry_findings(AgentSkill("semantics", "CORTEX_EXTENSION", "semantics", "", ref="skill"), "SST-REF032")
        == []
    )

"""SST-REF037: `extension()` names a skill this project publishes, which `skill()` must pin."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.ref_codes import skill_entry_findings


def test_sst_ref037_fires() -> None:
    [diagnostic] = skill_entry_findings(
        AgentSkill("semantics", "CORTEX_EXTENSION", "semantics", "V1", ref="extension"), "SST-REF037"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'router': extension('semantics') names a skill this project publishes"
    assert diagnostic.subject == "agent:router"


def test_sst_ref037_silent() -> None:
    assert (
        skill_entry_findings(
            AgentSkill("vendor", "CORTEX_EXTENSION", "vendor-pack", "V1", ref="extension"), "SST-REF037"
        )
        == []
    )

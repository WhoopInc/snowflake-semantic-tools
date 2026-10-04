"""SST-REF013: `extension()` names an extension `skills.extensions:` does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.ref_codes import skill_entry_findings


def test_sst_ref013_fires() -> None:
    [diagnostic] = skill_entry_findings(
        AgentSkill("unknown", "CORTEX_EXTENSION", "nowhere", "V1", ref="extension"), "SST-REF013"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ extension('nowhere') } does not resolve"
    assert diagnostic.subject == "agent:router"


def test_sst_ref013_silent() -> None:
    assert (
        skill_entry_findings(
            AgentSkill("vendor", "CORTEX_EXTENSION", "vendor-pack", "V1", ref="extension"), "SST-REF013"
        )
        == []
    )

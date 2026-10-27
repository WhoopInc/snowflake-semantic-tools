"""SST-VAL874: a CoWork profile's subagent has no display name or description after its defaults."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import (
    agent_member,
    compile_profiles,
    cowork_profile,
    described_agent,
    subagent,
)
from tests.helpers.diagnostic_filters import coded


def test_sst_val874_fires() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(comment=None),))
    [diagnostic] = coded(result.diagnostics, "SST-VAL874")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "cowork profile 'everyone': spec.subagents[0] agent('sales') has no description"
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", subagent().origin)


def test_sst_val874_fires_for_each_field_an_external_subagent_leaves_unset() -> None:
    result = compile_profiles(cowork_profile("everyone", subagent("partner", "tool")), members=(agent_member(),))
    assert [str(item.context["field"]) for item in coded(result.diagnostics, "SST-VAL874")] == [
        "display_name",
        "description",
    ]


def test_sst_val874_silent() -> None:
    # The entry's own description stands in for the comment the agent lacks.
    entry = subagent(description="Pipeline and quota.")
    result = compile_profiles(cowork_profile("everyone", entry), agents=(described_agent(comment=None),))
    assert coded(result.diagnostics, "SST-VAL874") == []

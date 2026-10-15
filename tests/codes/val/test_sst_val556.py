"""SST-VAL556: an allowed orchestration model could not be fully checked."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import AgentProbe, ProbeOutcome
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cortex_models import FakeCortexModels, checked, ga

LISTED = (ga("claude-sonnet-4-6"),)


def test_sst_val556_fires() -> None:
    refused = AgentProbe(ProbeOutcome.UNPROBED, "Insufficient privileges to operate on schema 'SALES_S'")
    [diagnostic] = checked(["claude-sonnet-4-6"], FakeCortexModels(LISTED, {"claude-sonnet-4-6": refused}))
    assert diagnostic.code == "SST-VAL556"
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "orchestration model 'claude-sonnet-4-6' was not fully checked: the probe agent could not run: "
        "Insufficient privileges to operate on schema 'SALES_S'"
    )


def test_sst_val556_fires_when_the_project_publishes_no_agent() -> None:
    port = FakeCortexModels(LISTED)
    [diagnostic] = checked(["claude-sonnet-4-6"], port, CompileResult(()))
    assert diagnostic.message == (
        "orchestration model 'claude-sonnet-4-6' was not fully checked: "
        "the project publishes no agent, so there is no schema to run one in"
    )
    assert port.probed == []


def test_sst_val556_fires_when_show_fails_or_the_name_is_refused() -> None:
    port = FakeCortexModels(
        (),
        {"bad name": SnowflakePortError("'bad name' is not a Cortex model name")},
        show_error=SnowflakePortError("denied"),
    )
    found = checked(["bad name"], port)
    assert [item.message for item in found] == [
        "orchestration model 'bad name' was not fully checked: the probe agent could not run: "
        "'bad name' is not a Cortex model name",
        "orchestration model 'bad name' was not fully checked: "
        "the list of Cortex base models could not be read: denied",
    ]


def test_sst_val556_silent() -> None:
    assert checked(["claude-sonnet-4-6"], FakeCortexModels(LISTED)) == []

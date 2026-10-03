"""SST-APL008: an object's explicit grants could not be read back after its replace."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyResult, GrantRow, OutcomeStatus
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, marker, observed, rendered
from tests.helpers.snowflake_fake import FakeSnowflake


class SecondReadFails(FakeSnowflake):
    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self.reads = 0
        self.fails = fail

    def show_grants(
        self, object_type: str, qualified_name: QualifiedName, routine_signature: tuple[str, ...] = ()
    ) -> tuple[GrantRow, ...]:
        self.reads += 1
        if self.fails and self.reads == 2:
            raise SnowflakePortError("SHOW GRANTS timed out")
        return (GrantRow("SELECT", "ROLE", "ANALYST"),)


def run(*, fail: bool) -> ApplyResult:
    artifact = rendered()
    port = SecondReadFails(fail=fail)
    port.markers[artifact.target.sql] = marker(artifact)
    plan = changeset(change(artifact, Action.UPDATE, live=observed(artifact, ownership=marker(artifact))))
    return apply_plan(plan, port)


def test_sst_apl008_fires() -> None:
    result = run(fail=True)
    diagnostic = only(result, "SST-APL008")
    assert (diagnostic.severity, diagnostic.message) == (
        Severity.WARNING,
        "semantic_view:v: grants could not be verified after replace",
    )
    assert result.outcomes[0].status is OutcomeStatus.APPLIED


def test_sst_apl008_silent() -> None:
    assert "SST-APL008" not in codes(run(fail=False))

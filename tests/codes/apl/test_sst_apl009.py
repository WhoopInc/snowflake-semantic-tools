"""SST-APL009: an explicit grant present before a replace is absent after it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyResult, GrantRow
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, marker, observed, rendered
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake

GRANT = GrantRow("SELECT", "ROLE", "ANALYST")


class GrantsAfter(FakeSnowflake):
    def __init__(self, after: tuple[GrantRow, ...]) -> None:
        super().__init__()
        self.after = after
        self.reads = 0

    def show_grants(
        self, object_type: str, qualified_name: QualifiedName, routine_signature: tuple[str, ...] = ()
    ) -> tuple[GrantRow, ...]:
        self.reads += 1
        return (GRANT,) if self.reads == 1 else self.after


def run(after: tuple[GrantRow, ...]) -> ApplyResult:
    artifact = rendered()
    port = GrantsAfter(after)
    port.markers[artifact.target.sql] = marker(artifact)
    return apply_plan(
        changeset(change(artifact, Action.UPDATE, live=observed(artifact, ownership=marker(artifact)))), port
    )


def test_sst_apl009_fires() -> None:
    diagnostic = only(run(()).diagnostics, "SST-APL009")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"semantic_view:v: grant {(GRANT,)!r} was present before replace and is absent after"


def test_sst_apl009_silent() -> None:
    assert "SST-APL009" not in codes(run((GRANT,)).diagnostics)

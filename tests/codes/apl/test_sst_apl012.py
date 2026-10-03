"""SST-APL012: the object changed between plan and apply."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl012_fires() -> None:
    artifact = rendered()
    port = FakeSnowflake()
    port.existing = {artifact.target.sql, *(relation.sql for relation in artifact.required_relations)}
    diagnostic = only(apply_plan(changeset(change(artifact)), port), "SST-APL012")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v: semantic_view:v changed since the plan"
    assert port.scripts == []


def test_sst_apl012_silent() -> None:
    assert "SST-APL012" not in codes(apply_plan(changeset(change(rendered()))))

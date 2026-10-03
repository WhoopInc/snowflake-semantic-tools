"""SST-MAN021: state was recorded against another manifest than the one being planned."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import manifest, rendered, state, target
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_man021_fires() -> None:
    current = manifest({"semantic_view:v": rendered()})
    plan = PlanArtifacts(FakeSnowflake()).run(
        {}, current, replace(state(), manifest_id="old"), target(), fetched_at="now"
    )
    [diagnostic] = [item for item in plan.diagnostics if item.code == "SST-MAN021"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == f"state was recorded against manifest old; current is {current.manifest_id}"


def test_sst_man021_silent() -> None:
    current = manifest({"semantic_view:v": rendered()})
    recorded = replace(state(), manifest_id=current.manifest_id)
    plan = PlanArtifacts(FakeSnowflake()).run({}, current, recorded, target(), fetched_at="now")
    assert "SST-MAN021" not in [item.code for item in plan.diagnostics]

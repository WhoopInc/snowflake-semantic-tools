"""SST-APL004: a replace that preserves grants by clause would run without COPY GRANTS."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet, RenderedArtifact
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, marker, observed, rendered
from tests.helpers.sql_values import statement


def update(artifact: RenderedArtifact) -> tuple[ChangeSet, InMemorySnowflake]:
    port = InMemorySnowflake()
    port.markers[artifact.target.sql] = marker(artifact)
    plan = changeset(change(artifact, Action.UPDATE, live=observed(artifact, ownership=marker(artifact))))
    return plan, port


def test_sst_apl004_fires() -> None:
    bare = statement("CREATE OR REPLACE SEMANTIC VIEW DB.SCHEMA.V TABLES (T AS DB.SCHEMA.T)")
    plan, port = update(replace(rendered(), statements=(bare,)))
    diagnostic = only(apply_plan(plan, port), "SST-APL004")
    assert (diagnostic.severity, diagnostic.message) == (
        Severity.ERROR,
        "semantic_view:v: replace statement omits COPY GRANTS",
    )
    assert port.scripts == []


def test_sst_apl004_silent() -> None:
    plan, port = update(rendered())
    assert "SST-APL004" not in codes(apply_plan(plan, port))

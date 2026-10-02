"""SST-PLN030: a write pins the published version of an artifact the plan leaves out."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    CompositeFacts,
    CompositeObservation,
    CompositePlan,
    PublishShape,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset
from tests.helpers.plan_codes import manifest_of, state_of, target
from tests.helpers.sql_values import statement


def _artifact(key: str, depends_on: tuple[str, ...] = ()) -> RenderedArtifact:
    kind = key.split(":", 1)[0]
    return RenderedArtifact.create(
        key=key,
        artifact_type=kind,
        target=QualifiedName.from_parts("db", "sch", key.split(":", 1)[1]),
        ddl=statement(f"payload {key}"),
        shape=PublishShape("AGENT" if kind == "agent" else ""),
        composite=None if kind == "agent" else CompositeFacts(),
        depends_on=depends_on,
    )


def _plan(*extra: RenderedArtifact) -> list[Diagnostic]:
    agent = _artifact("agent:analyst", ("skill:guide",))
    rendered = {item.key: item for item in (agent, *extra)}
    composite = {
        item.key: CompositePlan(Action.NOOP, ChangeReason.UNCHANGED, CompositeObservation(item.key)) for item in extra
    }
    planned_manifest = manifest_of(agent)
    planned = build_changeset(
        rendered,
        SnowflakeObservation(fetched_at="now"),
        planned_manifest,
        state_of(planned_manifest),
        SEMANTIC_REGISTRY,
        target(),
        composite_plans=composite,
    )
    return [item for item in planned.diagnostics if item.code == "SST-PLN030"]


def test_sst_pln030_fires() -> None:
    [diagnostic] = _plan()
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent:analyst: pins the published version of skill:guide; select skill:guide as well"
    )


def test_sst_pln030_silent() -> None:
    assert _plan(_artifact("skill:guide")) == []

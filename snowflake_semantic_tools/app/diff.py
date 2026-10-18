"""Read the states `sst diff` compares: the compiled manifest, a saved plan, or a live target.

Each reader returns a state by artifact key, as `domain.plan.diff` compares them. The live state
is what a target holds of SST's own: every artifact its state table records, observed by the
ownership marker on its object, so an object dropped or replaced by hand drops out or differs.
A composite artifact has no single object to observe, so the state table's entry stands for it.

`compiled_definitions` and `live_definitions` are the same states member by member, as plan
compares an update: what a compile renders, and what a target holds of each artifact its state
records. `name_map` labels a target's physical names, so two targets compare by what they publish.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.lifecycle.evals import config_folder
from snowflake_semantic_tools.app.plan_diff import COMPARISONS, LiveObject
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, Registry
from snowflake_semantic_tools.domain.plan.diff import ArtifactState
from snowflake_semantic_tools.domain.plan.member_diff import Definition, NameMap, normalize
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.state import DEACTIVATED, Manifest, SavedPlan

ArtifactStates = Mapping[str, ArtifactState]


class LiveStatePort(CatalogPort, StatePort, Protocol):
    """What reading a live target needs: its state table, and the markers on its objects."""


class LiveMembersPort(LiveStatePort, SnowflakePort, Protocol):
    """What reading a target's artifacts member by member needs: every read plan's diffs make."""


def manifest_states(manifest: Manifest) -> dict[str, ArtifactState]:
    """Return what the compiled manifest renders, by artifact key."""
    return {
        key: ArtifactState(entry.fingerprint, entry.publish_target, manifest.manifest_id)
        for key, entry in manifest.artifacts.items()
    }


def plan_states(plan: SavedPlan) -> dict[str, ArtifactState]:
    """Return what a saved plan would leave published: everything it renders, less what it prunes."""
    return {
        change.key: ArtifactState(change.fingerprint, change.target or "", plan.manifest_id)
        for change in plan.changes
        if change.action != Action.PRUNE.value and change.fingerprint is not None
    }


def live_states(
    port: LiveStatePort,
    state_table: QualifiedName,
    target_name: str,
    registry: Registry = SEMANTIC_REGISTRY,
) -> tuple[dict[str, ArtifactState] | None, tuple[Diagnostic, ...]]:
    """Return what a target holds of SST's artifacts, by key; None when its state cannot be read.

    Never writes. An artifact whose object is gone, or carries no SST marker, is not held.

    Raises:
        SnowflakePortError: a read Snowflake refused.

    Diagnostics:
        SST-MAN022: the state table exists and cannot be read.
    """
    entries = port.read_state(state_table, target_name)
    if entries is None:
        return None, (D("SST-MAN022", path=state_table.sql, detail="the state table cannot be read"),)
    held: dict[str, ArtifactState] = {}
    for key, entry in sorted(entries.items()):
        if entry.outcome == DEACTIVATED:
            continue
        artifact_type = registry.artifacts.get(split_artifact_key(key)[0])
        object_type = artifact_type.object_type if artifact_type is not None else ""
        if not object_type:
            held[key] = ArtifactState(entry.fingerprint, entry.qualified_name, entry.manifest_id)
            continue
        marker = port.describe_marker(QualifiedName.parse(entry.qualified_name), object_type)
        if marker is not None:
            held[key] = ArtifactState(marker.fingerprint, entry.qualified_name, marker.manifest_id)
    return held, ()


def compiled_definitions(result: CompileResult, names: NameMap) -> dict[str, Definition]:
    """Return what each compiled artifact renders, member by member, its names labelled by `names`."""
    definitions: dict[str, Definition] = {}
    for item in result.compiled:
        comparison = COMPARISONS.get(item.artifact_type)
        desired = comparison.desired(item) if comparison is not None else None
        if desired is not None:
            definitions[item.artifact_key] = normalize(desired, names)
    return definitions


def live_definitions(
    readers: Fanout[LiveMembersPort],
    state_table: QualifiedName,
    target_name: str,
    result: CompileResult,
    *,
    names: NameMap,
    eval_stage: str,
) -> tuple[dict[str, Definition], dict[str, str]]:
    """Return what the target holds of each artifact it compiles, by member, and why any was unread.

    An artifact is read when the target's state records it, it compiles for the target, and its
    type is compared; the reads run on `readers`. A read Snowflake refuses leaves the artifact
    out, with the reason; `sst diff` then compares it by fingerprint.

    Raises:
        SnowflakePortError: the state table could not be read.
    """
    entries = readers.port.read_state(state_table, target_name) or {}
    compiled = {item.artifact_key: item for item in result.compiled}
    keys = tuple(
        key
        for key, entry in sorted(entries.items())
        if entry.outcome != DEACTIVATED and key in compiled and split_artifact_key(key)[0] in COMPARISONS
    )

    def read(port: LiveMembersPort, key: str) -> Definition | str | None:
        item, entry = compiled[key], entries[key]
        comparison = COMPARISONS[item.artifact_type]
        if comparison.desired(item) is None:
            return None
        rendered = item.rendered_artifact
        name = QualifiedName.parse(entry.qualified_name)
        live = LiveObject(
            name,
            alias=rendered.desired_alias,
            comment=_shown_comment(port, rendered.object_type, name) if rendered.routine_signature else None,
            routine_signature=rendered.routine_signature,
            recorded=entry,
            config_folder=config_folder(rendered, eval_stage) if item.artifact_type == "eval" else None,
        )
        try:
            return normalize(comparison.live(port, item, live), names)
        except SnowflakePortError as exc:
            return str(exc)

    definitions: dict[str, Definition] = {}
    unread: dict[str, str] = {}
    for key, outcome in zip(keys, readers.map(read, keys), strict=True):
        if isinstance(outcome, Definition):
            definitions[key] = outcome
        elif isinstance(outcome, str):
            unread[key] = outcome
    return definitions, unread


def name_map(result: CompileResult) -> NameMap:
    """Return each physical name a target's compile names, mapped to the label it has on every target.

    An artifact's object is labelled by its key, `semantic_view:sales`, and each other object it
    spans by its key and type, `eval:a:table`; a stage by `stage:<name>`; a dbt model's relation by
    `ref('<model>')`. An object outside the project an agent's tool or skill names, which each
    target may name differently, is labelled by the tool or skill, `tool:lookup`, unless one of
    those names it already. A name two labels claim is left out, so it stays physical.
    """
    claims: dict[tuple[str, str, str], set[str]] = {}

    def claim(name: QualifiedName, label: str) -> None:
        claims.setdefault(name.folded, set()).add(label)

    for item in result.compiled:
        rendered = item.rendered_artifact
        claim(rendered.target, item.artifact_key)
        for object_type, resource in rendered.physical_resources:
            if object_type.upper() == "STAGE":
                claim(resource, f"stage:{resource.artifact_name}")
            elif resource.folded != rendered.target.folded:
                claim(resource, f"{item.artifact_key}:{object_type.lower()}")
        for model, relation in item.dbt_relations:
            if relation:
                claim(QualifiedName.parse(relation), f"ref('{model}')")
        if isinstance(item, CompiledProfile):
            claim(item.channel.stage, f"stage:{item.channel.stage.artifact_name}")
        if isinstance(item, CompiledExtension):
            claim(item.release.stage, f"stage:{item.release.stage.artifact_name}")
    referenced: dict[tuple[str, str, str], set[str]] = {}
    for item in result.compiled:
        if isinstance(item, CompiledAgent):
            for name, label in _agent_references(item):
                if name.folded not in claims:
                    referenced.setdefault(name.folded, set()).add(label)
    claims |= referenced
    return MappingProxyType({name: next(iter(labels)) for name, labels in claims.items() if len(labels) == 1})


def _agent_references(agent: CompiledAgent) -> Iterator[tuple[QualifiedName, str]]:
    """Each three-part name an agent's tools' resources and its skills' paths hold, with what holds it."""
    for tool in agent.resolved.tools:
        for value in tool.resources.values():
            name = _qualified(value)
            if name is not None:
                yield name, f"tool:{tool.member or tool.name}"
    for skill in agent.resolved.model.skills:
        name = _qualified(skill.path)
        if name is not None:
            yield name, f"extension:{skill.name or name.artifact_name}"


def _qualified(value: object) -> QualifiedName | None:
    if not isinstance(value, str):
        return None
    try:
        return QualifiedName.parse(value)
    except ValueError:
        return None


def _shown_comment(port: CatalogPort, object_type: str, name: QualifiedName) -> str | None:
    """A routine's comment as SHOW lists it, which its DESCRIBE does not report."""
    row = port.show_row(object_type, name)
    if row is None:
        return None
    return row.get("comment", row.get("description"))

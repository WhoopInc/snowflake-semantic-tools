"""Read what plan's preflight checks need from the target, for `domain.plan.preflight` to decide.

`read_preflight` makes the reads once per database, schema, relation, and object, and only
for what the plan may write or prune. A read Snowflake refuses is reported and treated as
"not known to be missing", so one refused read never blocks a write on its own.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from functools import partial
from types import MappingProxyType
from typing import TypeVar

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact, SnowflakeObservation
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.state import State

ValueT = TypeVar("ValueT")
_Folded = tuple[str, ...]

# The verb of the schema privilege a create needs, such as CREATE SEMANTIC VIEW.
_CREATE = "CREATE"


class _Reader:
    """Run preflight reads, recording each refused read as SST-PLN001."""

    def __init__(self, port: PreflightPort) -> None:
        self.port = port
        self.failures: list[Diagnostic] = []

    def read(self, what: str, call: Callable[[], ValueT]) -> ValueT | None:
        """Return what `call` reads; None when Snowflake refuses it.

        Diagnostics:
            SST-PLN001: the read of `what` failed.
        """
        try:
            return call()
        except SnowflakePortError as exc:
            self.failures.append(D("SST-PLN001", value=what, detail=str(exc)))
            return None

    def holds(self, what: str, call: Callable[[], bool], *, refused: bool) -> bool:
        """Return what a yes-or-no read answers; `refused` when Snowflake refuses it."""
        answer = self.read(what, call)
        return refused if answer is None else answer

    def names(self, what: str, call: Callable[[], tuple[QualifiedName, ...]]) -> tuple[QualifiedName, ...]:
        """Return the names a read lists; none when Snowflake refuses it."""
        return self.read(what, call) or ()


def read_preflight(
    port: PreflightPort,
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    state: State,
    target: TargetIdentity,
    *,
    include_prune: bool,
) -> tuple[Preflight, tuple[Diagnostic, ...]]:
    """Read the target's containers, relations, privileges, locks, references, and warehouse.

    Args:
        rendered: What the plan may publish, by key.
        observation: What plan observed, which says what each artifact would create.
        state: The authoritative state, which names the objects SST manages.
        include_prune: Whether the plan prunes, so the objects a prune may remove are read.

    Returns:
        What the reads found, and the failed reads.

    Diagnostics:
        SST-PLN001: a preflight read failed.
    """
    reader = _Reader(port)
    created = {key: artifact for key, artifact in rendered.items() if key not in observation.artifacts}
    missing_databases, missing_schemas, present = _containers(reader, rendered)
    managed = {artifact.target.folded for artifact in rendered.values()} | {
        QualifiedName.parse(entry.qualified_name).folded for entry in state.applied.values() if entry.qualified_name
    }
    preflight = Preflight(
        target_name=target.name,
        role=target.role or reader.read("the current role", port.current_role) or "the deploying role",
        missing_databases=missing_databases,
        missing_schemas=missing_schemas,
        missing_relations=MappingProxyType(_missing_relations(reader, rendered)),
        missing_privileges=MappingProxyType(_missing_privileges(reader, created, present)),
        occupied=_occupied(reader, created, present),
        locked=_locked(reader, present),
        referenced=MappingProxyType(_referenced(reader, rendered, observation, managed) if include_prune else {}),
        warehouse=target.warehouse,
        warehouse_usable=_warehouse_usable(reader, target.warehouse),
    )
    return preflight, tuple(reader.failures)


def _containers(
    reader: _Reader, rendered: Mapping[str, RenderedArtifact]
) -> tuple[frozenset[str], frozenset[_Folded], tuple[SchemaScope, ...]]:
    """Return the missing databases, the missing schemas, and the scopes that exist, in target order."""
    scopes = tuple(dict.fromkeys(SchemaScope.from_qualified_name(item.target) for item in rendered.values()))
    databases = tuple(dict.fromkeys(scope.database for scope in scopes))
    missing_databases = frozenset(
        database.folded
        for database in databases
        if not reader.holds(f"database {database.sql}", partial(reader.port.database_exists, database), refused=True)
    )
    missing_schemas: set[_Folded] = set()
    present: list[SchemaScope] = []
    for scope in scopes:
        if scope.database.folded in missing_databases:
            continue
        if reader.holds(f"schema {scope.sql}", partial(reader.port.schema_exists, scope), refused=True):
            present.append(scope)
        else:
            missing_schemas.add((scope.database.folded, scope.schema.folded))
    return missing_databases, frozenset(missing_schemas), tuple(present)


def _missing_relations(
    reader: _Reader, rendered: Mapping[str, RenderedArtifact]
) -> dict[str, tuple[QualifiedName, ...]]:
    """Return, by key, the required relations that do not exist; each relation is read once."""
    exists: dict[_Folded, bool] = {}
    for artifact in rendered.values():
        for relation in artifact.required_relations:
            if relation.folded not in exists:
                exists[relation.folded] = reader.holds(
                    f"relation {relation.sql}", partial(reader.port.relation_exists, relation), refused=True
                )
    found = {
        key: tuple(relation for relation in artifact.required_relations if not exists[relation.folded])
        for key, artifact in rendered.items()
    }
    return {key: relations for key, relations in found.items() if relations}


def _missing_privileges(
    reader: _Reader, created: Mapping[str, RenderedArtifact], present: tuple[SchemaScope, ...]
) -> dict[_Folded, tuple[str, ...]]:
    """Return, by existing scope, the create privileges the role lacks for what it would create there."""
    lacking: dict[_Folded, tuple[str, ...]] = {}
    for scope in present:
        needed = tuple(
            sorted(
                {
                    f"{_CREATE} {artifact.object_type.upper()}"
                    for artifact in created.values()
                    if artifact.object_type and SchemaScope.from_qualified_name(artifact.target) == scope
                }
            )
        )
        missing = (
            reader.read(f"grants on schema {scope.sql}", partial(reader.port.missing_privileges, scope, needed))
            if needed
            else None
        )
        if missing:
            lacking[(scope.database.folded, scope.schema.folded)] = missing
    return lacking


def _occupied(
    reader: _Reader, created: Mapping[str, RenderedArtifact], present: tuple[SchemaScope, ...]
) -> frozenset[str]:
    """Return the keys of what plan would create whose name an object of its type already holds."""
    return frozenset(
        key
        for key, artifact in created.items()
        if artifact.object_type
        and not artifact.temporary
        and SchemaScope.from_qualified_name(artifact.target) in present
        and reader.holds(
            f"{artifact.object_type} {artifact.target.sql}",
            partial(reader.port.object_exists, artifact.object_type, artifact.target),
            refused=False,
        )
    )


def _locked(reader: _Reader, present: tuple[SchemaScope, ...]) -> frozenset[_Folded]:
    """Return the folded names, in the existing scopes, another session holds a lock on."""
    return frozenset(
        name.folded
        for scope in present
        for name in reader.names(f"locks in {scope.sql}", partial(reader.port.locked_objects, scope))
    )


def _referenced(
    reader: _Reader,
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    managed: set[tuple[str, str, str]],
) -> dict[str, tuple[QualifiedName, ...]]:
    """Return, by prune candidate's key, the objects outside SST's management that name it."""
    found: dict[str, tuple[QualifiedName, ...]] = {}
    for key, observed in sorted(observation.artifacts.items()):
        if key in rendered or observed.marker is None:
            continue
        name = observed.qualified_name
        referrers = reader.names(f"references to {name.sql}", partial(reader.port.external_references, name))
        outside = tuple(referrer for referrer in referrers if referrer.folded not in managed)
        if outside:
            found[key] = outside
    return found


def _warehouse_usable(reader: _Reader, warehouse: str | None) -> bool:
    """Report whether the profile's warehouse exists for the role; True for none, or a failed read."""
    if not warehouse:
        return True
    try:
        name = Identifier.parse(warehouse)
    except ValueError:
        return False
    return reader.holds(f"warehouse {name.sql}", partial(reader.port.warehouse_exists, name), refused=True)

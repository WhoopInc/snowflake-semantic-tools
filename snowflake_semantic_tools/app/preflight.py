"""Read what plan's preflight checks need from the target, for `domain.plan.preflight` to decide.

`read_preflight` makes the reads once per database, schema, relation, and object, and only
for what the plan may write or prune. A read Snowflake refuses is reported and treated as
"not known to be missing", so one refused read never blocks a write on its own.

The lock read is advisory: it only warns of a competing writer (SST-PLN019), and reading
another user's locks needs MONITOR on the account, which a role that owns its schema need
not hold. A role refused it skips that warning (SST-VAL020) rather than failing the plan;
any other failure of the read is SST-PLN001, as for every other read.

The reference read is advisory the same way: it only warns that a prune removes an object
something outside the project names (SST-PLN017), and it reads `SNOWFLAKE.ACCOUNT_USAGE`,
which a schema owner need not be granted. A role refused it skips that warning for each
prune candidate (SST-VAL020) and the prune goes ahead; it was never what stopped a prune.

The reads run in phases -- databases, schemas, the role, relations, upload stages, privileges,
privileges on upload stages, occupied names, locks, references, the warehouse -- and the reads
of one phase are independent, so a `Fanout` may run them on sessions of their own. Their
answers and refusals are taken in the order the phase lists them, which is the order they
would run one at a time.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import TypeVar

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, detail_of, session_failure
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import SSE_STAGE_TYPE, RenderedArtifact, SnowflakeObservation
from snowflake_semantic_tools.domain.plan.preflight import CREATE_STAGE, STAGE_PRIVILEGES, Preflight
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort
from snowflake_semantic_tools.domain.state import State

ItemT = TypeVar("ItemT")
ValueT = TypeVar("ValueT")
_Scope = tuple[str, str]
_Name = tuple[str, str, str]
# What one read answered, or why Snowflake refused it.
_Answer = tuple[ValueT | None, SnowflakePortError | None]

# The verb of the schema privilege a create needs, such as CREATE SEMANTIC VIEW.
_CREATE = "CREATE"
# The checks the advisory reads serve, which a role refused the read skips.
_LOCK_CHECK = "SST-PLN019"
_REFERENCE_CHECK = "SST-PLN017"


class _Reader:
    """Run each phase's preflight reads, recording each refused read as SST-PLN001.

    Attributes:
        failures: The refused reads, as SST-PLN001, and the advisory reads the role may not
            make, as SST-VAL020, in the order they were read.
    """

    def __init__(self, readers: Fanout[PreflightPort]) -> None:
        self._readers = readers
        self.failures: list[Diagnostic] = []

    def read_each(
        self,
        items: Sequence[ItemT],
        read: Callable[[PreflightPort, ItemT], ValueT],
        what: Callable[[ItemT], str],
    ) -> tuple[ValueT | None, ...]:
        """Return what `read` reads of each item, in item order; None where Snowflake refuses it.

        Diagnostics:
            SST-PLN001: the read of an item failed; named by `what`, in item order.
        """
        answers = self._answers(items, read)
        self.failures.extend(
            D("SST-PLN001", value=what(item), detail=str(error))
            for item, (_, error) in zip(items, answers, strict=True)
            if error is not None
        )
        return tuple(value for value, _ in answers)

    def advise_each(
        self,
        items: Sequence[ItemT],
        read: Callable[[PreflightPort, ItemT], tuple[QualifiedName, ...]],
        what: Callable[[ItemT], str],
        *,
        check: str,
    ) -> tuple[tuple[QualifiedName, ...], ...]:
        """Return the names an advisory read lists of each item; none where Snowflake refuses it.

        Diagnostics:
            SST-VAL020: the role lacks a privilege the read of an item needs, so `check` is
                skipped for it.
            SST-PLN001: the read of an item failed for any other reason.
        """
        answers = self._answers(items, read)
        for item, (_, error) in zip(items, answers, strict=True):
            if error is None:
                continue
            if _not_permitted(error):
                detail = f"the role may not read {what(item)}: {detail_of(str(error))}"
                self.failures.append(D("SST-VAL020", rule_id=check, detail=detail))
            else:
                self.failures.append(D("SST-PLN001", value=what(item), detail=str(error)))
        return tuple(value or () for value, _ in answers)

    def _answers(
        self, items: Sequence[ItemT], read: Callable[[PreflightPort, ItemT], ValueT]
    ) -> tuple[_Answer[ValueT], ...]:
        """Read each item, in item order, keeping the error Snowflake refused each with."""

        def attempt(port: PreflightPort, item: ItemT) -> _Answer[ValueT]:
            try:
                return read(port, item), None
            except SnowflakePortError as exc:
                return None, exc

        return self._readers.map(attempt, items)

    def holds_each(
        self,
        items: Sequence[ItemT],
        read: Callable[[PreflightPort, ItemT], bool],
        what: Callable[[ItemT], str],
        *,
        refused: bool,
    ) -> tuple[bool, ...]:
        """Return what a yes-or-no read answers of each item; `refused` where Snowflake refuses it."""
        return tuple(refused if answer is None else answer for answer in self.read_each(items, read, what))


def read_preflight(
    port: PreflightPort,
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    state: State,
    target: TargetIdentity,
    *,
    include_prune: bool,
    readers: Fanout[PreflightPort] | None = None,
) -> tuple[Preflight, tuple[Diagnostic, ...]]:
    """Read the target's containers, relations, stages, privileges, locks, references, and warehouse.

    Args:
        rendered: What the plan may publish, by key.
        observation: What plan observed, which says what each artifact would create.
        state: The authoritative state, which names the objects SST manages.
        include_prune: Whether the plan prunes, so the objects a prune may remove are read.
        readers: Sessions to run each phase's reads on concurrently; None reads on `port`,
            one at a time. Either way the preflight and its failures are the same.

    Returns:
        What the reads found, and the failed reads.

    Diagnostics:
        SST-PLN001: a preflight read failed.
        SST-VAL020: the role may not read the target's locks, so SST-PLN019 is skipped; or
            what references a prune candidate, so SST-PLN017 is skipped for it.
    """
    reader = _Reader(readers or Fanout(port))
    created = {key: artifact for key, artifact in rendered.items() if key not in observation.artifacts}
    stages = _upload_stages(rendered)
    targets = tuple(dict.fromkeys(SchemaScope.from_qualified_name(item.target) for item in rendered.values()))
    stage_scopes = (SchemaScope.from_qualified_name(stage) for stage in stages)
    missing_databases, missing_schemas, present = _containers(reader, tuple(dict.fromkeys((*targets, *stage_scopes))))
    role = target.role or _current_role(reader) or "the deploying role"
    missing_relations = _missing_relations(reader, rendered)
    stage_types, absent_stages = _stage_types(reader, stages, present)
    managed = {artifact.target.folded for artifact in rendered.values()} | {
        QualifiedName.parse(entry.qualified_name).folded for entry in state.applied.values() if entry.qualified_name
    }
    preflight = Preflight(
        target_name=target.name,
        role=role,
        missing_databases=missing_databases,
        missing_schemas=missing_schemas,
        missing_relations=MappingProxyType(missing_relations),
        missing_privileges=MappingProxyType(_missing_privileges(reader, created, present, stages, absent_stages)),
        missing_stage_privileges=MappingProxyType(_missing_stage_privileges(reader, stages, stage_types)),
        occupied=_occupied(reader, created, present),
        locked=_locked(reader, tuple(scope for scope in present if scope in targets)),
        referenced=MappingProxyType(_referenced(reader, rendered, observation, managed) if include_prune else {}),
        warehouse=target.warehouse,
        warehouse_usable=_warehouse_usable(reader, target.warehouse),
        stage_types=MappingProxyType(stage_types),
        absent_stages=absent_stages,
    )
    return preflight, tuple(reader.failures)


def _current_role(reader: _Reader) -> str | None:
    """Return the session's current role; None when Snowflake refuses the read."""
    [role] = reader.read_each(("the current role",), lambda port, _: port.current_role(), lambda what: what)
    return role


def _containers(
    reader: _Reader, scopes: tuple[SchemaScope, ...]
) -> tuple[frozenset[str], frozenset[_Scope], tuple[SchemaScope, ...]]:
    """Return the missing databases, the missing schemas, and the scopes that exist, in scope order."""
    databases = tuple(dict.fromkeys(scope.database for scope in scopes))
    exists = reader.holds_each(
        databases,
        lambda port, database: port.database_exists(database),
        lambda item: f"database {item.sql}",
        refused=True,
    )
    missing_databases = frozenset(
        database.folded for database, found in zip(databases, exists, strict=True) if not found
    )
    candidates = tuple(scope for scope in scopes if scope.database.folded not in missing_databases)
    schema_exists = reader.holds_each(
        candidates, lambda port, scope: port.schema_exists(scope), lambda item: f"schema {item.sql}", refused=True
    )
    present = tuple(scope for scope, found in zip(candidates, schema_exists, strict=True) if found)
    missing_schemas = frozenset(
        (scope.database.folded, scope.schema.folded)
        for scope, found in zip(candidates, schema_exists, strict=True)
        if not found
    )
    return missing_databases, missing_schemas, present


def _missing_relations(
    reader: _Reader, rendered: Mapping[str, RenderedArtifact]
) -> dict[str, tuple[QualifiedName, ...]]:
    """Return, by key, the required relations that do not exist; each relation is read once."""
    first: dict[_Name, QualifiedName] = {}
    for artifact in rendered.values():
        for relation in artifact.required_relations:
            first.setdefault(relation.folded, relation)
    answers = reader.holds_each(
        tuple(first.values()),
        lambda port, relation: port.relation_exists(relation),
        lambda item: f"relation {item.sql}",
        refused=True,
    )
    exists = dict(zip(first, answers, strict=True))
    found = {
        key: tuple(relation for relation in artifact.required_relations if not exists[relation.folded])
        for key, artifact in rendered.items()
    }
    return {key: relations for key, relations in found.items() if relations}


def _upload_stages(rendered: Mapping[str, RenderedArtifact]) -> tuple[QualifiedName, ...]:
    """Return each stage an artifact stages a file on before it publishes, once, in rendered order."""
    first: dict[_Name, QualifiedName] = {}
    for artifact in rendered.values():
        if artifact.upload_stage is not None:
            first.setdefault(artifact.upload_stage.folded, artifact.upload_stage)
    return tuple(first.values())


def _stage_types(
    reader: _Reader, stages: tuple[QualifiedName, ...], present: tuple[SchemaScope, ...]
) -> tuple[dict[_Name, str], frozenset[_Name]]:
    """Return each upload stage's TYPE by folded name, and the folded names of those absent.

    A stage in a schema that does not exist is not read, and one whose read Snowflake refuses
    is in neither.
    """
    readable = tuple(stage for stage in stages if SchemaScope.from_qualified_name(stage) in present)
    # Wrapped, so an absent stage's None is not taken for a refused read's.
    answers = reader.read_each(
        readable, lambda port, stage: (port.stage_type(stage),), lambda item: f"stage {item.sql}"
    )
    types: dict[_Name, str] = {}
    absent: set[_Name] = set()
    for stage, answer in zip(readable, answers, strict=True):
        if answer is None:
            continue
        [found] = answer
        if found is None:
            absent.add(stage.folded)
        else:
            types[stage.folded] = found
    return types, frozenset(absent)


def _missing_privileges(
    reader: _Reader,
    created: Mapping[str, RenderedArtifact],
    present: tuple[SchemaScope, ...],
    stages: tuple[QualifiedName, ...],
    absent_stages: frozenset[_Name],
) -> dict[_Scope, tuple[str, ...]]:
    """Return, by existing scope, the create privileges the role lacks for what it would create there.

    What it would create is each created artifact's object, and each absent upload stage.
    """
    creating_stage = {SchemaScope.from_qualified_name(stage) for stage in stages if stage.folded in absent_stages}
    needs = tuple(
        (scope, needed)
        for scope in present
        if (
            needed := tuple(
                sorted(
                    {
                        f"{_CREATE} {artifact.object_type.upper()}"
                        for artifact in created.values()
                        if artifact.object_type and SchemaScope.from_qualified_name(artifact.target) == scope
                    }
                    | ({CREATE_STAGE} if scope in creating_stage else set())
                )
            )
        )
    )
    missing = reader.read_each(
        needs,
        lambda port, need: port.missing_privileges(need[0], need[1]),
        lambda need: f"grants on schema {need[0].sql}",
    )
    return {
        (scope.database.folded, scope.schema.folded): lacking
        for (scope, _), lacking in zip(needs, missing, strict=True)
        if lacking
    }


def _missing_stage_privileges(
    reader: _Reader, stages: tuple[QualifiedName, ...], stage_types: Mapping[_Name, str]
) -> dict[_Name, tuple[str, ...]]:
    """Return, by folded name, what the role lacks of READ and WRITE on each usable upload stage.

    Only an existing internal SSE stage is read: an absent one is created, and SST-PLN026
    blocks any other.
    """
    usable = tuple(stage for stage in stages if stage_types.get(stage.folded, "").upper() == SSE_STAGE_TYPE)
    lacking = reader.read_each(
        usable,
        lambda port, stage: port.missing_stage_privileges(stage, STAGE_PRIVILEGES),
        lambda item: f"grants on stage {item.sql}",
    )
    return {stage.folded: missing for stage, missing in zip(usable, lacking, strict=True) if missing}


def _occupied(
    reader: _Reader, created: Mapping[str, RenderedArtifact], present: tuple[SchemaScope, ...]
) -> frozenset[str]:
    """Return the keys of what plan would create whose name an object of its type already holds."""
    candidates = tuple(
        (key, artifact)
        for key, artifact in created.items()
        if artifact.object_type
        and not artifact.temporary
        and SchemaScope.from_qualified_name(artifact.target) in present
    )
    held = reader.holds_each(
        candidates,
        lambda port, item: port.object_exists(item[1].object_type, item[1].target),
        lambda item: f"{item[1].object_type} {item[1].target.sql}",
        refused=False,
    )
    return frozenset(key for (key, _), occupied in zip(candidates, held, strict=True) if occupied)


def _locked(reader: _Reader, present: tuple[SchemaScope, ...]) -> frozenset[_Name]:
    """Return the folded names, in the existing scopes, another session holds a lock on."""
    listed = reader.advise_each(
        present,
        lambda port, scope: port.locked_objects(scope),
        lambda item: f"locks in {item.sql}",
        check=_LOCK_CHECK,
    )
    return frozenset(name.folded for names in listed for name in names)


def _referenced(
    reader: _Reader,
    rendered: Mapping[str, RenderedArtifact],
    observation: SnowflakeObservation,
    managed: set[_Name],
) -> dict[str, tuple[QualifiedName, ...]]:
    """Return, by prune candidate's key, the objects outside SST's management that name it."""
    candidates = tuple(
        (key, observed.qualified_name)
        for key, observed in sorted(observation.artifacts.items())
        if key not in rendered and observed.marker is not None
    )
    listed = reader.advise_each(
        candidates,
        lambda port, item: port.external_references(item[1]),
        lambda item: f"references to {item[1].sql}",
        check=_REFERENCE_CHECK,
    )
    found: dict[str, tuple[QualifiedName, ...]] = {}
    for (key, _), referrers in zip(candidates, listed, strict=True):
        outside = tuple(referrer for referrer in referrers if referrer.folded not in managed)
        if outside:
            found[key] = outside
    return found


def _not_permitted(error: SnowflakePortError) -> bool:
    """Report whether Snowflake refused a read because the role lacks a privilege it needs."""
    return session_failure(str(error), errno=error.errno, sqlstate=error.sqlstate) is SessionFailure.PRIVILEGE


def _warehouse_usable(reader: _Reader, warehouse: str | None) -> bool:
    """Report whether the profile's warehouse exists for the role; True for none, or a failed read."""
    if not warehouse:
        return True
    try:
        name = Identifier.parse(warehouse)
    except ValueError:
        return False
    [usable] = reader.holds_each(
        (name,), lambda port, item: port.warehouse_exists(item), lambda item: f"warehouse {item.sql}", refused=True
    )
    return usable

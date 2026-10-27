"""Decide what a plan's preflight read means: the changes it blocks, and what it warns of.

Plan reads the target before it decides, so a write Snowflake would refuse is reported, and
blocked, before apply runs it. `Preflight` is what that read found; `check_preflight` turns
it into diagnostics on the decided changes. Each code is the precheck for the SNO code apply
would report instead: a missing database (SST-PLN011, for SST-SNO006) or schema
(SST-PLN010, for SST-SNO005; with `apply.create_schemas`, apply creates the schema instead,
and `schemas_to_create` lists it), a missing referenced relation (SST-PLN007, for SST-SNO003),
and a privilege the role lacks (SST-PLN008, for SST-SNO004) each block the write; a create
whose name is taken (SST-PLN009, for SST-SNO002) and an object another session holds a lock
on (SST-PLN019, for SST-SNO022) are warnings. A prune of an object something outside the
project names (SST-PLN017) is a warning, and an unusable warehouse (SST-PLN012, for
SST-SNO007) is reported once for the plan.

A write that stages a file first, an agent's specification, is checked against its stage
too: apply creates an absent stage (SST-PLN035 says so, once per stage), so the role needs
CREATE STAGE on its schema; an existing stage must be an internal server-side encrypted one
(SST-PLN026), on which the role holds READ and WRITE. Each failure blocks the write (SST-PLN008
for a privilege), so apply never fails part-way on a stage it cannot create or use.

A write that attaches a CoWork profile to the account's CoWork object (a Snowflake private
preview) needs MODIFY or OWNERSHIP on that object, held by any role in the session: without it,
the write is blocked (SST-PLN106), so apply never publishes a profile it cannot attach.

The state and lock tables are checked once for the plan: when it writes, CREATE TABLE on the
schema of an absent one (SST-PLN008); and always, the kind of one that exists, which must be
the kind `state.+table_kind` names (SST-APL031, which apply would report on taking the lock,
as it does even for a plan with nothing to change).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import SSE_STAGE_TYPE, Action, Change, ChangeReason
from snowflake_semantic_tools.domain.ports.snowflake.preflight import ROLE_HIERARCHY_DEPTH
from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES, STANDARD_TABLES

_Scope = tuple[str, str]
_Name = tuple[str, str, str]

# The verb of the schema privilege a create needs, such as CREATE SEMANTIC VIEW.
_CREATE = "CREATE"
# The schema privilege apply needs to create an absent upload stage.
CREATE_STAGE = "CREATE STAGE"
# The schema privilege apply needs to create an absent standard state or lock table.
CREATE_TABLE = "CREATE TABLE"
# The schema privilege apply needs to create an absent hybrid state or lock table; under
# behavior change bundle 2026_02 CREATE TABLE no longer covers a hybrid table.
CREATE_HYBRID_TABLE = "CREATE HYBRID TABLE"


def state_table_privilege(kind: str) -> str:
    """Return the schema privilege creating a state or lock table of `kind` needs."""
    return CREATE_HYBRID_TABLE if kind == HYBRID_TABLES else CREATE_TABLE


# The privileges on an existing upload stage that staging a file and reading it back need.
STAGE_PRIVILEGES = ("READ", "WRITE")
# Snowflake private preview (PrPr): CoWork profiles. The privilege on the CoWork object that
# attaching a profile needs, which OWNERSHIP of the object also gives.
COWORK_PRIVILEGES = ("MODIFY",)


def _scope_key(scope: SchemaScope) -> _Scope:
    return scope.database.folded, scope.schema.folded


@dataclass(frozen=True, slots=True)
class Preflight:
    """What plan read about the target before deciding, each name folded as `QualifiedName.folded`.

    Attributes:
        target_name: The profile target, as SST-PLN007 names it.
        role: The deploying role, as SST-PLN008 names it.
        missing_databases: The folded databases of the planned targets that do not exist.
        missing_schemas: The folded `(database, schema)` scopes of the planned targets that
            do not exist, in databases that do.
        missing_relations: By artifact key, the relations it references that do not exist.
        missing_privileges: By folded scope, the privileges the role lacks there, such as
            `CREATE SEMANTIC VIEW`. A `CREATE` privilege counts only when the session's primary
            role holds it, directly or through the roles it inherits.
        secondary_privileges: By folded scope, those of `missing_privileges` a secondary role in
            the session holds, which Snowflake does not let authorize a `CREATE`.
        unreached_privileges: By folded scope, those of `missing_privileges` the session holds
            but the walk up from the primary role stopped short of, at `ROLE_HIERARCHY_DEPTH`
            roles: a secondary role may hold them, or a more distant inheritance.
        occupied: The keys of artifacts nothing observed, whose target name is taken anyway.
        locked: The folded names another session holds a lock on.
        referenced: By observed key, the objects outside the project that name it.
        warehouse: The warehouse the profile names; None when it names none.
        warehouse_usable: Whether that warehouse exists and the role can see it.
        stage_types: By folded name, the TYPE of each upload stage that exists.
        absent_stages: The folded names of the upload stages that do not exist. A stage in
            neither, because its schema is missing or its read was refused, is not checked.
        missing_stage_privileges: By folded name, what the role lacks of `STAGE_PRIVILEGES` on
            an upload stage that exists. An absent stage's CREATE STAGE is in
            `missing_privileges`, under its schema.
        absent_state_tables: The state and lock tables that do not exist, which apply creates;
            their CREATE TABLE is in `missing_privileges`, under their schema. A state table
            that exists needs no CREATE privilege at all.
        state_table_kind: The kind `state.+table_kind` names, `standard` or `hybrid`.
        state_table_kinds: Each state and lock table that exists, with the kind it was read to
            be, in table order. A table whose kind could not be read is not listed.
        create_schemas: `apply.create_schemas`: apply creates each absent schema in a database
            that exists, so a missing one is no SST-PLN010.
        missing_state_schemas: The folded scopes of the state and lock tables' schemas that do
            not exist, read only with `create_schemas` on.
        missing_cowork_privileges: By the CoWork object's name, as SHOW SNOWFLAKE INTELLIGENCES
            lists it, what no role in the session holds of `COWORK_PRIVILEGES` on it; read only
            when a CoWork profile may be attached (Snowflake private preview).
    """

    target_name: str
    role: str
    missing_databases: frozenset[str] = frozenset()
    missing_schemas: frozenset[_Scope] = frozenset()
    missing_relations: Mapping[str, tuple[QualifiedName, ...]] = field(default_factory=lambda: MappingProxyType({}))
    missing_privileges: Mapping[_Scope, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    secondary_privileges: Mapping[_Scope, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    unreached_privileges: Mapping[_Scope, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    occupied: frozenset[str] = frozenset()
    locked: frozenset[_Name] = frozenset()
    referenced: Mapping[str, tuple[QualifiedName, ...]] = field(default_factory=lambda: MappingProxyType({}))
    warehouse: str | None = None
    warehouse_usable: bool = True
    stage_types: Mapping[_Name, str] = field(default_factory=lambda: MappingProxyType({}))
    absent_stages: frozenset[_Name] = frozenset()
    missing_stage_privileges: Mapping[_Name, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    absent_state_tables: tuple[QualifiedName, ...] = ()
    state_table_kind: str = STANDARD_TABLES
    state_table_kinds: tuple[tuple[QualifiedName, str], ...] = ()
    create_schemas: bool = False
    missing_state_schemas: frozenset[_Scope] = frozenset()
    missing_cowork_privileges: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))


def required_privilege(change: Change) -> str | None:
    """Return the schema privilege a change's write needs; None for one that needs none it can check.

    A create needs `CREATE <object type>` on its schema. An update replaces an object the
    role already owns, and a composite artifact's handler checks its own objects.
    """
    if change.action is not Action.CREATE or change.rendered is None or not change.rendered.object_type:
        return None
    return f"{_CREATE} {change.rendered.object_type.upper()}"


def check_preflight(
    changes: tuple[Change, ...], preflight: Preflight
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Block the writes the preflight shows Snowflake would refuse, and warn of the risky ones.

    Returns:
        The changes in their given order, each blocking diagnostic on its change, and every
        diagnostic for the plan's list: one per missing database or schema, then each
        change's own, in change order, then the state tables' SST-PLN008 and SST-APL031, then
        SST-PLN012.

    Diagnostics:
        SST-APL031: a state or lock table exists as the other kind than `state.+table_kind`
            names; reported whether or not the plan writes, since apply takes the lock either way.
        SST-PLN007: a write references a relation that does not exist in the target.
        SST-PLN008: the role lacks the privilege a create needs on its schema, CREATE STAGE
            on the schema of an absent upload stage, or READ or WRITE on an existing one; or,
            when the plan writes, CREATE TABLE on the schema of an absent state table, once.
        SST-PLN009: a create's target name is taken by an object observation did not list.
        SST-PLN010: a write's schema, or its upload stage's, does not exist.
        SST-PLN011: a write's database, or its upload stage's, does not exist.
        SST-PLN012: the profile's warehouse is not usable.
        SST-PLN017: a prune would remove an object something outside the project names.
        SST-PLN019: another session holds a lock on a change's object.
        SST-PLN026: a write's upload stage exists but is not an internal SSE stage.
        SST-PLN035: apply creates a write's absent upload stage; once per stage.
        SST-PLN106: no role in the session holds MODIFY or OWNERSHIP on the CoWork object a
            write attaches a CoWork profile to.
    """
    checked: list[Change] = []
    scoped: dict[_Scope, Diagnostic] = {}
    announced: set[_Name] = set()
    reported: list[Diagnostic] = []
    for change in changes:
        blocking, warnings = _change_findings(change, preflight, scoped, announced)
        reported.extend(warnings)
        reported.extend(item for item in blocking if item.code not in ("SST-PLN010", "SST-PLN011"))
        checked.append(_blocked(change, blocking) if blocking else change)
    if any(_writes(change) for change in changes):
        reported.extend(_state_tables_lacked(preflight))
    reported.extend(_state_tables_of_other_kind(preflight))
    if preflight.warehouse and not preflight.warehouse_usable:
        reported.append(D("SST-PLN012", value=preflight.warehouse))
    return tuple(checked), (*scoped.values(), *reported)


def _change_findings(
    change: Change, preflight: Preflight, scoped: dict[_Scope, Diagnostic], announced: set[_Name]
) -> tuple[tuple[Diagnostic, ...], tuple[Diagnostic, ...]]:
    """Return a change's blocking diagnostics and its warnings and notes.

    Each missing scope is recorded once in `scoped`, and each absent upload stage apply
    creates is announced once, by the first change that stages there, in `announced`.
    """
    if change.action is Action.PRUNE:
        return (), _prune_warnings(change, preflight)
    if change.action not in (Action.CREATE, Action.UPDATE) or change.rendered is None:
        return (), ()
    target = change.rendered.target
    blocking = list(_missing_scope(target, preflight, scoped))
    blocking.extend(
        D("SST-PLN007", subject=change.key, artifact=change.key, value=relation.sql, target=preflight.target_name)
        for relation in preflight.missing_relations.get(change.key, ())
    )
    privilege = required_privilege(change)
    scope = SchemaScope.from_qualified_name(target)
    if not blocking and privilege in preflight.missing_privileges.get(_scope_key(scope), ()):
        blocking.append(_lacks(change.key, privilege, scope, preflight))
    stage = change.rendered.upload_stage
    if not blocking and stage is not None:
        blocking.extend(_stage_blocking(change.key, stage, preflight, scoped))
    cowork_object = change.rendered.attaches_to
    if not blocking and cowork_object is not None and preflight.missing_cowork_privileges.get(cowork_object):
        blocking.append(D("SST-PLN106", subject=change.key, artifact=change.key, value=cowork_object))
    warnings: list[Diagnostic] = []
    if not blocking and stage is not None and stage.folded in preflight.absent_stages - announced:
        announced.add(stage.folded)
        warnings.append(D("SST-PLN035", subject=change.key, value=stage.sql))
    if change.action is Action.CREATE and change.key in preflight.occupied:
        warnings.append(D("SST-PLN009", subject=change.key, artifact=change.key, value=target.sql))
    if target.folded in preflight.locked:
        warnings.append(D("SST-PLN019", subject=change.key, value=target.sql))
    return tuple(blocking), tuple(warnings)


def _writes(change: Change) -> bool:
    """Report whether apply would execute the change: a create, an update, or an executable prune."""
    return change.action in (Action.CREATE, Action.UPDATE) or (
        change.action is Action.PRUNE and change.prune_executable
    )


def _state_tables_lacked(preflight: Preflight) -> tuple[Diagnostic, ...]:
    """SST-PLN008 for each schema of an absent state table where the role cannot create one.

    The privilege follows `state.+table_kind`: CREATE TABLE, or CREATE HYBRID TABLE.
    """
    privilege = state_table_privilege(preflight.state_table_kind)
    scopes = dict.fromkeys(SchemaScope.from_qualified_name(table) for table in preflight.absent_state_tables)
    return tuple(
        _lacks(None, privilege, scope, preflight)
        for scope in scopes
        if privilege in preflight.missing_privileges.get(_scope_key(scope), ())
    )


def _state_tables_of_other_kind(preflight: Preflight) -> tuple[Diagnostic, ...]:
    """SST-APL031 for each state or lock table that exists as the other kind than configured."""
    expected = preflight.state_table_kind
    return tuple(
        D("SST-APL031", value=table.sql, found=found, expected=expected)
        for table, found in preflight.state_table_kinds
        if found != expected
    )


def _missing_scope(
    target: QualifiedName, preflight: Preflight, scoped: dict[_Scope, Diagnostic]
) -> tuple[Diagnostic, ...]:
    """Return the diagnostic for a target whose database or schema does not exist, once per scope."""
    scope = SchemaScope.from_qualified_name(target)
    key = _scope_key(scope)
    if scope.database.folded in preflight.missing_databases:
        diagnostic = scoped.setdefault(key, D("SST-PLN011", value=scope.database.sql))
    elif key in preflight.missing_schemas and not preflight.create_schemas:
        diagnostic = scoped.setdefault(key, D("SST-PLN010", value=scope.sql))
    else:
        return ()
    return (diagnostic,)


def schemas_to_create(changes: tuple[Change, ...], preflight: Preflight) -> tuple[SchemaScope, ...]:
    """Return the absent schemas apply creates, with `create_schemas` on: none with it off.

    The state and lock tables' schemas come first, when the plan writes, as apply takes the lock
    before anything else; then each schema a write that is not blocked, or its upload stage,
    goes in, in change order. A schema in a missing database is SST-PLN011's, never created.
    """
    if not preflight.create_schemas:
        return ()
    writes = [change for change in changes if change.action in (Action.CREATE, Action.UPDATE) and change.rendered]
    found: dict[_Scope, SchemaScope] = {}
    if writes:
        for table in preflight.absent_state_tables:
            scope = SchemaScope.from_qualified_name(table)
            if _scope_key(scope) in preflight.missing_state_schemas:
                found.setdefault(_scope_key(scope), scope)
    for change in writes:
        assert change.rendered is not None
        names = (change.rendered.target, *((change.rendered.upload_stage,) if change.rendered.upload_stage else ()))
        for name in names:
            scope = SchemaScope.from_qualified_name(name)
            if (
                _scope_key(scope) in preflight.missing_schemas
                and scope.database.folded not in preflight.missing_databases
            ):
                found.setdefault(_scope_key(scope), scope)
    return tuple(found.values())


def _stage_blocking(
    key: str, stage: QualifiedName, preflight: Preflight, scoped: dict[_Scope, Diagnostic]
) -> tuple[Diagnostic, ...]:
    """Return what blocks a write from staging on `stage`: a missing container, the stage, or a privilege.

    An existing stage must be an internal SSE stage the role can READ and WRITE; an absent
    one, which apply creates, needs CREATE STAGE on its schema. A stage the preflight read
    nothing of blocks nothing.
    """
    missing = _missing_scope(stage, preflight, scoped)
    if missing:
        return missing
    found = preflight.stage_types.get(stage.folded)
    if found is not None and found.upper() != SSE_STAGE_TYPE:
        return (D("SST-PLN026", subject=key, artifact=key, value=stage.sql, found=found or "untyped"),)
    if found is not None:
        lacking = preflight.missing_stage_privileges.get(stage.folded, ())
        if not lacking:
            return ()
        return (D("SST-PLN008", subject=key, value=preflight.role, detail=", ".join(lacking), target=stage.sql),)
    scope = SchemaScope.from_qualified_name(stage)
    if stage.folded in preflight.absent_stages and CREATE_STAGE in preflight.missing_privileges.get(
        _scope_key(scope), ()
    ):
        return (_lacks(key, CREATE_STAGE, scope, preflight),)
    return ()


def _lacks(key: str | None, privilege: str, scope: SchemaScope, preflight: Preflight) -> Diagnostic:
    """SST-PLN008 for a CREATE privilege the primary role lacks, saying when the session holds it."""
    detail = privilege
    if privilege in preflight.secondary_privileges.get(_scope_key(scope), ()):
        detail = f"{privilege} (only a secondary role in the session holds it, and a CREATE uses the primary role)"
    elif privilege in preflight.unreached_privileges.get(_scope_key(scope), ()):
        detail = (
            f"{privilege} (the session holds it, but no inheritance path from the primary role was found "
            f"within {ROLE_HIERARCHY_DEPTH} levels, and a CREATE uses the primary role)"
        )
    return D("SST-PLN008", subject=key, value=preflight.role, detail=detail, target=scope.sql)


def _prune_warnings(change: Change, preflight: Preflight) -> tuple[Diagnostic, ...]:
    """Warn of an executable prune whose object something outside the project names."""
    if not change.prune_executable or change.observed is None or not preflight.referenced.get(change.key):
        return ()
    return (D("SST-PLN017", subject=change.key, value=change.observed.qualified_name.sql),)


def _blocked(change: Change, blocking: tuple[Diagnostic, ...]) -> Change:
    return replace(
        change,
        action=Action.BLOCKED,
        reason=ChangeReason.VALIDATION_ERRORS,
        diagnostics=DiagnosticBag((*change.diagnostics, *blocking)),
    )

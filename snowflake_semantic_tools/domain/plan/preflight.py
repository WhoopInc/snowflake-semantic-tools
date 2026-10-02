"""Decide what a plan's preflight read means: the changes it blocks, and what it warns of.

Plan reads the target before it decides, so a write Snowflake would refuse is reported, and
blocked, before apply runs it. `Preflight` is what that read found; `check_preflight` turns
it into diagnostics on the decided changes. Each code is the precheck for the SNO code apply
would report instead: a missing database (SST-PLN011, for SST-SNO006) or schema
(SST-PLN010, for SST-SNO005), a missing referenced relation (SST-PLN007, for SST-SNO003),
and a privilege the role lacks (SST-PLN008, for SST-SNO004) each block the write; a create
whose name is taken (SST-PLN009, for SST-SNO002) and an object another session holds a lock
on (SST-PLN019, for SST-SNO022) are warnings. A prune of an object something outside the
project names (SST-PLN017) is a warning, and an unusable warehouse (SST-PLN012, for
SST-SNO007) is reported once for the plan.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason

_Scope = tuple[str, str]
_Name = tuple[str, str, str]

# The verb of the schema privilege a create needs, such as CREATE SEMANTIC VIEW.
_CREATE = "CREATE"


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
            `CREATE SEMANTIC VIEW`.
        occupied: The keys of artifacts nothing observed, whose target name is taken anyway.
        locked: The folded names another session holds a lock on.
        referenced: By observed key, the objects outside the project that name it.
        warehouse: The warehouse the profile names; None when it names none.
        warehouse_usable: Whether that warehouse exists and the role can see it.
    """

    target_name: str
    role: str
    missing_databases: frozenset[str] = frozenset()
    missing_schemas: frozenset[_Scope] = frozenset()
    missing_relations: Mapping[str, tuple[QualifiedName, ...]] = field(default_factory=lambda: MappingProxyType({}))
    missing_privileges: Mapping[_Scope, tuple[str, ...]] = field(default_factory=lambda: MappingProxyType({}))
    occupied: frozenset[str] = frozenset()
    locked: frozenset[_Name] = frozenset()
    referenced: Mapping[str, tuple[QualifiedName, ...]] = field(default_factory=lambda: MappingProxyType({}))
    warehouse: str | None = None
    warehouse_usable: bool = True


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
        change's own, in change order, then SST-PLN012.

    Diagnostics:
        SST-PLN007: a write references a relation that does not exist in the target.
        SST-PLN008: the role lacks the privilege a create needs on its schema.
        SST-PLN009: a create's target name is taken by an object observation did not list.
        SST-PLN010: a write's schema does not exist.
        SST-PLN011: a write's database does not exist.
        SST-PLN012: the profile's warehouse is not usable.
        SST-PLN017: a prune would remove an object something outside the project names.
        SST-PLN019: another session holds a lock on a change's object.
    """
    checked: list[Change] = []
    scoped: dict[_Scope, Diagnostic] = {}
    reported: list[Diagnostic] = []
    for change in changes:
        blocking, warnings = _change_findings(change, preflight, scoped)
        reported.extend(warnings)
        reported.extend(item for item in blocking if item.code not in ("SST-PLN010", "SST-PLN011"))
        checked.append(_blocked(change, blocking) if blocking else change)
    if preflight.warehouse and not preflight.warehouse_usable:
        reported.append(D("SST-PLN012", value=preflight.warehouse))
    return tuple(checked), (*scoped.values(), *reported)


def _change_findings(
    change: Change, preflight: Preflight, scoped: dict[_Scope, Diagnostic]
) -> tuple[tuple[Diagnostic, ...], tuple[Diagnostic, ...]]:
    """Return a change's blocking diagnostics and its warnings; record each missing scope once in `scoped`."""
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
        blocking.append(D("SST-PLN008", subject=change.key, value=preflight.role, detail=privilege, target=scope.sql))
    warnings: list[Diagnostic] = []
    if change.action is Action.CREATE and change.key in preflight.occupied:
        warnings.append(D("SST-PLN009", subject=change.key, artifact=change.key, value=target.sql))
    if target.folded in preflight.locked:
        warnings.append(D("SST-PLN019", subject=change.key, value=target.sql))
    return tuple(blocking), tuple(warnings)


def _missing_scope(
    target: QualifiedName, preflight: Preflight, scoped: dict[_Scope, Diagnostic]
) -> tuple[Diagnostic, ...]:
    """Return the diagnostic for a target whose database or schema does not exist, once per scope."""
    scope = SchemaScope.from_qualified_name(target)
    key = _scope_key(scope)
    if scope.database.folded in preflight.missing_databases:
        diagnostic = scoped.setdefault(key, D("SST-PLN011", value=scope.database.sql))
    elif key in preflight.missing_schemas:
        diagnostic = scoped.setdefault(key, D("SST-PLN010", value=scope.sql))
    else:
        return ()
    return (diagnostic,)


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

"""Whether one role holds schema privileges, walked up from the schema's grants to the role.

The walk starts at the roles and database roles SHOW GRANTS ON SCHEMA names as holding a
privilege or owning the schema, and climbs SHOW GRANTS OF ROLE / OF DATABASE ROLE until it
reaches the role asked about. It reads only the roles a holder is granted to, never the
role's own grants: the role's hierarchy may be far larger than the schema's holders.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.sql import Sql, ident, join, sql

# The privilege that grants every other on a schema.
OWNERSHIP = "OWNERSHIP"
_ROLE = "ROLE"
_DATABASE_ROLE = "DATABASE_ROLE"


@dataclass(frozen=True, slots=True)
class Grantee:
    """An account role, or a database role with its database, each name in its shown form."""

    kind: str
    names: tuple[Identifier, ...]

    @classmethod
    def role(cls, name: Identifier) -> Grantee:
        """An account role, spelled as SHOW prints it, so two spellings of one role are equal."""
        return cls(_ROLE, (Identifier.shown(name.folded),))

    @classmethod
    def database_role(cls, database: Identifier, name: Identifier) -> Grantee:
        """A database role in `database`, each part spelled as SHOW prints it."""
        return cls(_DATABASE_ROLE, (Identifier.shown(database.folded), Identifier.shown(name.folded)))

    @property
    def grants_of(self) -> Sql:
        """The statement listing the roles this grantee is granted to."""
        if self.kind == _DATABASE_ROLE:
            return sql("SHOW GRANTS OF DATABASE ROLE {name}", name=join(".", (ident(part) for part in self.names)))
        return sql("SHOW GRANTS OF ROLE {name}", name=ident(self.names[0]))


def schema_holders(
    rows: Iterable[Mapping[str, object]], scope: SchemaScope, privileges: tuple[str, ...]
) -> dict[Grantee, frozenset[str]]:
    """Map each role or database role SHOW GRANTS ON SCHEMA names to the requested privileges it holds.

    Ownership of the schema holds every requested privilege. A database role's name is
    printed unqualified and belongs to the schema's database; a grant to anything else (a
    share, an application) confers nothing on a role.
    """
    wanted = {privilege.upper() for privilege in privileges}
    holders: dict[Grantee, set[str]] = {}
    for row in rows:
        privilege = str(row.get("privilege") or "").upper()
        held = wanted if privilege == OWNERSHIP else wanted & {privilege}
        name = str(row.get("grantee_name") or "")
        kind = str(row.get("granted_to") or "").upper()
        if not held or not name or kind not in (_ROLE, _DATABASE_ROLE):
            continue
        shown = Identifier.shown(name)
        grantee = Grantee.role(shown) if kind == _ROLE else Grantee.database_role(scope.database, shown)
        holders.setdefault(grantee, set()).update(held)
    return {grantee: frozenset(held) for grantee, held in holders.items()}


def grantees_of(grantee: Grantee, rows: Iterable[Mapping[str, object]]) -> tuple[Grantee, ...]:
    """The roles SHOW GRANTS OF lists `grantee` as granted to, users left out.

    A database role may also be granted to another database role, which SHOW prints as
    `DB.NAME` or, unqualified, in the granted role's own database.
    """
    found: list[Grantee] = []
    for row in rows:
        kind = str(row.get("granted_to") or "").upper()
        name = str(row.get("grantee_name") or "")
        if not name:
            continue
        if kind == _ROLE:
            found.append(Grantee.role(Identifier.shown(name)))
        elif kind == _DATABASE_ROLE and grantee.kind == _DATABASE_ROLE:
            found.append(_database_role(grantee.names[0], name))
    return tuple(found)


def held_by(
    role: Grantee,
    holders: Mapping[Grantee, frozenset[str]],
    read: Callable[[Grantee], tuple[Grantee, ...]],
    depth: int,
) -> frozenset[str]:
    """Return the privileges `role` holds: a holder reaches it in fewer than `depth` grants.

    One breadth-first walk over every holder at once, each frontier node carrying the
    privileges it passes on. Each grantee is read at most once, so a cycle ends; a pair of
    grantee and privilege travels on only the first, shortest time it is reached, and
    privileges already held travel no further.
    """
    held = set(holders.get(role, frozenset()))
    reached: dict[Grantee, set[str]] = {grantee: set(found) for grantee, found in holders.items()}
    edges: dict[Grantee, tuple[Grantee, ...]] = {}
    frontier = {grantee: set(found) for grantee, found in holders.items() if grantee != role}
    for _ in range(depth - 1):
        frontier = {grantee: carried - held for grantee, carried in frontier.items() if carried - held}
        if not frontier:
            break
        following: dict[Grantee, set[str]] = {}
        for grantee, carried in frontier.items():
            if grantee not in edges:
                edges[grantee] = read(grantee)
            for parent in edges[grantee]:
                arriving = carried - reached.setdefault(parent, set())
                reached[parent] |= arriving
                if parent == role:
                    held |= arriving
                elif arriving:
                    following.setdefault(parent, set()).update(arriving)
        frontier = following
    return frozenset(held)


def _database_role(database: Identifier, name: str) -> Grantee:
    """A database role SHOW printed as `DB.NAME`, or as `NAME` in `database`."""
    qualifier, dot, rest = name.partition(".")
    if dot and qualifier and rest:
        return Grantee.database_role(Identifier.shown(qualifier), Identifier.shown(rest))
    return Grantee.database_role(database, Identifier.shown(name))

"""An account's grants, answered below the real connector as SHOW GRANTS prints them.

`GrantGraph` holds the grants on one schema and the grants of roles and database roles to
roles, database roles and users, and answers, through `FakeDriverSession(respond=...)`:

- `SHOW GRANTS ON SCHEMA <db>.<schema>`: created_on, privilege, granted_on, name, granted_to
  (ROLE | DATABASE_ROLE), grantee_name -- a database role's unqualified, in the schema's
  database -- grant_option, granted_by, granted_by_role_type.
- `SHOW GRANTS OF ROLE <r>` and `SHOW GRANTS OF DATABASE ROLE <db>.<r>`: created_on, role
  (`<DB>.<NAME>` for a database role), granted_to (ROLE | USER | DATABASE_ROLE),
  grantee_name, granted_by.

A statement naming a role the graph does not know fails as Snowflake fails it; any other
statement -- SHOW GRANTS TO ROLE included -- fails as unsupported. Names are given as SHOW
prints them and matched as the connector must spell them, so a mis-quoted name fails.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from tests.helpers.snowflake_fake.driver import FakeDriverSession, Rows

_CREATED = "2026-01-01 00:00:00.000 -0800"


@dataclass
class GrantGraph:
    """Schema grants and role grants, answered as SHOW GRANTS ON SCHEMA / OF ROLE print them."""

    scope: SchemaScope
    _on_schema: list[dict[str, object]] = field(default_factory=list)
    _of: dict[str, list[dict[str, object]]] = field(default_factory=dict)

    def on_schema(self, privilege: str, *, role: str = "", database_role: str = "") -> GrantGraph:
        """Grant a privilege on the schema (OWNERSHIP included) to a role or a database role."""
        kind, name = ("DATABASE_ROLE", database_role) if database_role else ("ROLE", role)
        self._known(kind, name)
        self._on_schema.append(
            {
                "created_on": _CREATED,
                "privilege": privilege,
                "granted_on": "SCHEMA",
                "name": self.scope.sql,
                "granted_to": kind,
                "grantee_name": name,
                "grant_option": "false",
                "granted_by": "SYSADMIN",
                "granted_by_role_type": "ROLE",
            }
        )
        return self

    def role_to(self, role: str, *, role_to: str = "", user: str = "") -> GrantGraph:
        """Grant an account role to a role or a user."""
        return self._grant("ROLE", role, role_to, user)

    def database_role_to(
        self, name: str, *, role_to: str = "", database_role_to: str = "", user: str = ""
    ) -> GrantGraph:
        """Grant a database role of the schema's database to a role, a database role, or a user."""
        if database_role_to:
            self._known("DATABASE_ROLE", database_role_to)
            return self._row("DATABASE_ROLE", name, "DATABASE_ROLE", database_role_to)
        return self._grant("DATABASE_ROLE", name, role_to, user)

    def forget(self, role: str) -> GrantGraph:
        """Drop an account role, so SHOW GRANTS OF it fails as for a role that no longer exists."""
        self._of.pop(f"SHOW GRANTS OF ROLE {Identifier.shown(role).sql}", None)
        return self

    def session(self) -> FakeDriverSession:
        """A driver session answering from this graph, recording each statement it is sent."""
        return FakeDriverSession(respond=self.respond)

    def respond(self, statement: str, binds: tuple[object, ...]) -> tuple[Rows | None, int]:
        """Answer one statement as Snowflake would from these grants."""
        del binds
        if statement == f"SHOW GRANTS ON SCHEMA {self.scope.sql}":
            return list(self._on_schema), len(self._on_schema)
        for prefix in ("SHOW GRANTS OF DATABASE ROLE ", "SHOW GRANTS OF ROLE "):
            if statement.startswith(prefix):
                rows = self._of.get(statement)
                if rows is None:
                    raise ProgrammingError(
                        msg=f"Role '{statement[len(prefix) :]}' does not exist or not authorized.",
                        errno=2003,
                        sqlstate="02000",
                    )
                return list(rows), len(rows)
        raise ProgrammingError(msg=f"the grant graph does not answer {statement!r}", errno=1003, sqlstate="42000")

    def _grant(self, kind: str, name: str, role_to: str, user: str) -> GrantGraph:
        if role_to:
            self._known("ROLE", role_to)
            return self._row(kind, name, "ROLE", role_to)
        return self._row(kind, name, "USER", user)

    def _row(self, kind: str, name: str, granted_to: str, grantee: str) -> GrantGraph:
        printed = f"{self.scope.database.folded}.{name}" if kind == "DATABASE_ROLE" else name
        self._known(kind, name).append(
            {
                "created_on": _CREATED,
                "role": printed,
                "granted_to": granted_to,
                "grantee_name": grantee,
                "granted_by": "SYSADMIN",
            }
        )
        return self

    def _known(self, kind: str, name: str) -> list[dict[str, object]]:
        """The rows SHOW GRANTS OF lists for a role, keyed by the statement that must name it."""
        shown = Identifier.shown(name).sql
        if kind == "DATABASE_ROLE":
            statement = f"SHOW GRANTS OF DATABASE ROLE {Identifier.shown(self.scope.database.folded).sql}.{shown}"
        else:
            statement = f"SHOW GRANTS OF ROLE {shown}"
        return self._of.setdefault(statement, [])

"""Snowflake identifiers as values, with one comparison and quoting rule."""

from __future__ import annotations

import re
from dataclasses import dataclass

_UNQUOTED = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _split_qualified(value: str) -> tuple[str, ...]:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(value):
        character = value[index]
        if character == '"':
            current.append(character)
            if quoted and index + 1 < len(value) and value[index + 1] == '"':
                current.append('"')
                index += 1
            else:
                quoted = not quoted
        elif character == "." and not quoted:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(character)
        index += 1
    if quoted:
        raise ValueError(f"unterminated quoted identifier in {value!r}")
    parts.append("".join(current).strip())
    if any(not part for part in parts):
        raise ValueError(f"empty identifier component in {value!r}")
    return tuple(parts)


@dataclass(frozen=True, slots=True, order=True)
class Identifier:
    """One Snowflake identifier, retaining whether it was authored quoted."""

    value: str
    quoted: bool = False

    @classmethod
    def parse(cls, raw: str) -> Identifier:
        text = raw.strip()
        if text.startswith('"') or text.endswith('"'):
            if len(text) < 2 or not (text.startswith('"') and text.endswith('"')):
                raise ValueError(f"malformed quoted identifier {raw!r}")
            return cls(text[1:-1].replace('""', '"'), quoted=True)
        if not _UNQUOTED.fullmatch(text):
            raise ValueError(f"invalid unquoted identifier {raw!r}")
        return cls(text.upper(), quoted=False)

    @classmethod
    def shown(cls, raw: str) -> Identifier:
        """A name as SHOW prints it: unquoted when it can be, else the exact name it was created with."""
        if not raw:
            raise ValueError("empty identifier")
        return cls.parse(raw) if _UNQUOTED.fullmatch(raw.strip()) else cls(raw, quoted=True)

    @property
    def folded(self) -> str:
        return self.value if self.quoted else self.value.upper()

    @property
    def sql(self) -> str:
        if self.quoted:
            return '"' + self.value.replace('"', '""') + '"'
        return self.value.upper()


@dataclass(frozen=True, slots=True, order=True)
class QualifiedName:
    database: Identifier
    schema: Identifier
    name: Identifier

    @classmethod
    def parse(cls, raw: str) -> QualifiedName:
        parts = _split_qualified(raw)
        if len(parts) != 3:
            raise ValueError(f"expected a three-part Snowflake name, found {raw!r}")
        return cls(*(Identifier.parse(part) for part in parts))

    @classmethod
    def from_parts(cls, database: str, schema: str, name: str) -> QualifiedName:
        return cls(Identifier.parse(database), Identifier.parse(schema), Identifier.parse(name))

    @property
    def sql(self) -> str:
        return ".".join((self.database.sql, self.schema.sql, self.name.sql))

    @property
    def folded(self) -> tuple[str, str, str]:
        return self.database.folded, self.schema.folded, self.name.folded

    @property
    def artifact_name(self) -> str:
        return self.name.folded.casefold()

    @property
    def artifact_component(self) -> str:
        return f'"{self.name.value.replace(chr(34), chr(34) * 2)}"' if self.name.quoted else self.name.folded.casefold()


@dataclass(frozen=True, slots=True, order=True)
class SchemaScope:
    database: Identifier
    schema: Identifier

    @classmethod
    def from_qualified_name(cls, name: QualifiedName) -> SchemaScope:
        return cls(name.database, name.schema)

    @property
    def sql(self) -> str:
        return f"{self.database.sql}.{self.schema.sql}"


@dataclass(frozen=True, slots=True)
class TargetIdentity:
    name: str
    account_locator: str
    database: Identifier
    schema: Identifier
    role: str | None = None
    warehouse: str | None = None

    @property
    def scope(self) -> SchemaScope:
        return SchemaScope(self.database, self.schema)

    @property
    def key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.name,
            self.account_locator.upper(),
            self.database.folded,
            self.schema.folded,
            (self.role or "").upper(),
            (self.warehouse or "").upper(),
        )

    def as_dict(self) -> dict[str, str | None]:
        return {
            "name": self.name,
            "account_locator": self.account_locator,
            "database": self.database.sql,
            "schema": self.schema.sql,
            "role": self.role,
            "warehouse": self.warehouse,
        }

    @classmethod
    def from_dict(cls, value: object) -> TargetIdentity:
        if not isinstance(value, dict):
            raise ValueError("target identity must be an object")
        name = value.get("name")
        database = value.get("database")
        schema = value.get("schema")
        if not all(isinstance(item, str) and item for item in (name, database, schema)):
            raise ValueError("target identity requires name, database, and schema")
        assert isinstance(name, str)
        assert isinstance(database, str)
        assert isinstance(schema, str)
        account = value.get("account_locator")
        role = value.get("role")
        warehouse = value.get("warehouse")
        return cls(
            name=name,
            account_locator=account if isinstance(account, str) else "",
            database=Identifier.parse(database),
            schema=Identifier.parse(schema),
            role=role if isinstance(role, str) else None,
            warehouse=warehouse if isinstance(warehouse, str) else None,
        )

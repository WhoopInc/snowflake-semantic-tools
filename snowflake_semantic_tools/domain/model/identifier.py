"""Snowflake identifiers as values, with one comparison and quoting rule."""

from __future__ import annotations

import re
from dataclasses import dataclass

_UNQUOTED = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _split_qualified(value: str) -> tuple[str, ...]:
    """Split a dotted name at each dot outside double quotes, keeping each part's quoting as written.

    A doubled quote inside quotes stays doubled for `Identifier.parse` to unescape, and the
    whitespace around each part is dropped.

    Raises:
        ValueError: a quote is never closed, or a part is empty.
    """
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
    """One Snowflake identifier, retaining whether it was authored quoted.

    `value` is the name without its quotes: exactly as written when quoted, upper-cased by
    `parse` when not. Equality and ordering compare `value` and `quoted` as stored, so `"ABC"`
    and `ABC` differ although both name the object `ABC`; compare `folded` to match objects.
    """

    value: str
    quoted: bool = False

    @classmethod
    def parse(cls, raw: str) -> Identifier:
        """Parse one identifier: a double-quoted name keeps its exact text, an unquoted one is upper-cased.

        Surrounding whitespace is ignored, and a doubled quote inside quotes is one quote.

        Raises:
            ValueError: a quote is unbalanced, or an unquoted name is not a letter or underscore
                followed by letters, digits, `_`, or `$`.
        """
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
        """A name as SHOW prints it: unquoted when it can be, else the exact name it was created with.

        Snowflake stores an unquoted name upper-cased, so a shown name with a lowercase letter,
        like one with a character an unquoted name cannot hold, was created quoted.

        Raises:
            ValueError: `raw` is empty.
        """
        if not raw:
            raise ValueError("empty identifier")
        unquoted = _UNQUOTED.fullmatch(raw) is not None and raw == raw.upper()
        return cls(raw, quoted=not unquoted)

    @property
    def folded(self) -> str:
        """The name as Snowflake resolves it: a quoted value exactly, an unquoted one upper-cased."""
        return self.value if self.quoted else self.value.upper()

    @property
    def sql(self) -> str:
        """The identifier as SQL: double-quoted with inner quotes doubled when quoted, else upper-cased."""
        if self.quoted:
            return '"' + self.value.replace('"', '""') + '"'
        return self.value.upper()


@dataclass(frozen=True, slots=True, order=True)
class QualifiedName:
    """A three-part Snowflake object name: database, schema, and object.

    Equality and ordering compare the parts as stored, quoting included, so two spellings of
    one object can differ; compare `folded` to ask whether two names resolve to one object.
    """

    database: Identifier
    schema: Identifier
    name: Identifier

    @classmethod
    def parse(cls, raw: str) -> QualifiedName:
        """Parse a three-part dotted name, splitting only at dots outside double quotes.

        Each part is parsed as `Identifier.parse` parses it, so a quoted part may hold a dot.

        Raises:
            ValueError: a quote is unbalanced, the name does not have exactly three parts, or a
                part is empty or not a valid identifier.
        """
        parts = _split_qualified(raw)
        if len(parts) != 3:
            raise ValueError(f"expected a three-part Snowflake name, found {raw!r}")
        return cls(*(Identifier.parse(part) for part in parts))

    @classmethod
    def from_parts(cls, database: str, schema: str, name: str) -> QualifiedName:
        """Build a name from three separately written parts, parsing each as one identifier.

        A part is never split, so a dot is valid only inside a quoted part.

        Raises:
            ValueError: a part is not a valid identifier.
        """
        return cls(Identifier.parse(database), Identifier.parse(schema), Identifier.parse(name))

    @property
    def sql(self) -> str:
        """The name as SQL: each part's `Identifier.sql`, joined by dots."""
        return ".".join((self.database.sql, self.schema.sql, self.name.sql))

    @property
    def folded(self) -> tuple[str, str, str]:
        """The three parts as Snowflake resolves them; equal for any two spellings of one object."""
        return self.database.folded, self.schema.folded, self.name.folded

    @property
    def artifact_name(self) -> str:
        """The object name alone, resolved then casefolded, to compare with a name regardless of case."""
        return self.name.folded.casefold()

    @property
    def artifact_component(self) -> str:
        """The object name as the name part of an observed object's artifact key.

        An unquoted name is casefolded, as compile casefolds the names in the keys it builds; a
        quoted name keeps its quotes and case, so it cannot collide with an unquoted one.
        """
        return f'"{self.name.value.replace(chr(34), chr(34) * 2)}"' if self.name.quoted else self.name.folded.casefold()


@dataclass(frozen=True, slots=True, order=True)
class SchemaScope:
    """A database and schema: the scope objects are listed in and statements run in."""

    database: Identifier
    schema: Identifier

    @classmethod
    def from_qualified_name(cls, name: QualifiedName) -> SchemaScope:
        """Return the database and schema that hold `name`."""
        return cls(name.database, name.schema)

    @property
    def sql(self) -> str:
        """The scope as SQL, `DATABASE.SCHEMA`, each part spelled as its `Identifier.sql`."""
        return f"{self.database.sql}.{self.schema.sql}"


@dataclass(frozen=True, slots=True)
class TargetIdentity:
    """The dbt profile target SST plans and applies against, as saved plans and state record it.

    `key` decides whether two identities are one target, so a saved plan made for one target
    is refused on another.

    Attributes:
        name: The `profiles.yml` target name, such as `dev`.
        account_locator: The profile's `account` as written; "" when it sets none.
        role: The profile's role as written; None when it sets none.
        warehouse: The profile's warehouse as written; None when it sets none.
    """

    name: str
    account_locator: str
    database: Identifier
    schema: Identifier
    role: str | None = None
    warehouse: str | None = None

    @property
    def scope(self) -> SchemaScope:
        """The target's database and schema."""
        return SchemaScope(self.database, self.schema)

    @property
    def key(self) -> tuple[str, str, str, str, str, str]:
        """The identity as compared: `(name, account, database, schema, role, warehouse)`.

        The name is kept exactly; the account, role, and warehouse are upper-cased, with "" for
        an unset role or warehouse; the database and schema are folded.
        """
        return (
            self.name,
            self.account_locator.upper(),
            self.database.folded,
            self.schema.folded,
            (self.role or "").upper(),
            (self.warehouse or "").upper(),
        )

    def as_dict(self) -> dict[str, str | None]:
        """Return the identity as a JSON object, with the database and schema spelled as SQL.

        `from_dict` reads it back.
        """
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
        """Read an identity that `as_dict` wrote.

        An `account_locator` that is not a string reads as "", and a `role` or `warehouse` that
        is not a string reads as None.

        Raises:
            ValueError: the value is not an object; its `name`, `database`, or `schema` is
                missing, empty, or not a string; or the database or schema is not a valid
                identifier.
        """
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


# The target-name words that mark a production-like environment, matched as whole parts of
# the name split on `_`, `-` and `.`: `prod`, `prod_us` and `eu-production` match, `product` does not.
PRODUCTION_WORDS = frozenset({"prod", "production", "prd"})


def production_like(target_name: str) -> bool:
    """Report whether a target's name marks it as a production-like environment."""
    return any(part in PRODUCTION_WORDS for part in re.split(r"[_\-.]", target_name.casefold()))

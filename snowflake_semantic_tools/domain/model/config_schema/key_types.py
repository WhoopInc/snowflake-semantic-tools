"""The types of the `sst_config.yml` key table: what a key is, its kind, status and child policy.

`keys` builds the table itself out of these; they are split out so the table, which is data,
can grow without the types' module growing with it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

CONFIG_FILE = "sst_config.yml"
# How many Snowflake sessions `plan`, `validate` and `apply` work on at once when nothing says.
DEFAULT_THREADS = 4
# The two spellings discovery accepts in the project root; both present is an error, not a precedence.
CONFIG_NAMES: tuple[str, ...] = (CONFIG_FILE, "sst_config.yaml")


class KeyKind(Enum):
    """The type of value a key takes.

    `MAP` is a block of free-form names, `ENUM` a string from the key's `choices`, and
    `ANY` accepts every value unchecked. An `INTEGER` key never accepts a boolean. An
    `IDENTIFIER` is a string naming one Snowflake identifier, an `OBJECT_NAME` one
    identifier or a three-part name, and a `SCHEMA_NAME` a two-part `DATABASE.SCHEMA`; each may
    be empty, which reads as unset.
    """

    BLOCK = "block"
    MAP = "map"
    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    LIST = "list"
    ENUM = "enum"
    IDENTIFIER = "identifier"
    OBJECT_NAME = "object name"
    SCHEMA_NAME = "schema name"
    ANY = "any"


class KeyStatus(Enum):
    """Whether SST reads a key, reads it under its new name, or no longer reads it."""

    CURRENT = "current"
    # An error naming its replacement; the rest of the block is checked as that key.
    DEPRECATED = "deprecated"
    REMOVED = "removed"


class ChildPolicy(Enum):
    """Which undeclared children a block accepts.

    `DECLARED` accepts none, `NAMES` matches any other child to the block's `<name>`
    slot, and `ROUTES` matches any other child not starting with `+` to its `<route>`
    slot.
    """

    DECLARED = "declared"
    NAMES = "names"
    ROUTES = "routes"


@dataclass(frozen=True, slots=True)
class ConfigKey:
    """One key, block, or wildcard slot of the configuration file.

    `<name>` stands for any free-form entry of a name map and `<route>` for any
    unprefixed folder route or group key inside a routed block.

    Attributes:
        summary: The reference-page description; for a removed key, why it was removed.
        default: The default as the reference prints it, or None when the key has none.
        minimum, maximum: Inclusive lower and upper bounds of an integer value; None for no bound on that side.
        replacement: For a removed key, the reason its diagnostic gives; for a deprecated key, the key it is read as.
        code: The code reported instead of the generic one: a removed key's dedicated
            code, or the code for a value outside `choices` or bounds.
        folded: An `ENUM` value is matched against `choices` by `vocabulary_key`, ignoring case
            and runs of spaces, as the member key it is the default for is matched.
        root_only: A `+` key that only the block's root may set: a folder route or an artifact's
            `config:` that sets it is an error (SST-CFG055).
        unset_code: The code an unset key reports; `default` applies once that code is demoted.
    """

    path: str
    kind: KeyKind
    summary: str
    status: KeyStatus = KeyStatus.CURRENT
    default: str | None = None
    choices: tuple[str, ...] = ()
    minimum: int | None = None
    maximum: int | None = None
    children: ChildPolicy = ChildPolicy.DECLARED
    replacement: str | None = None
    code: str | None = None
    folded: bool = False
    root_only: bool = False
    unset_code: str | None = None

    @property
    def segments(self) -> tuple[str, ...]:
        """Return the dotted path split into its segments."""
        return tuple(self.path.split("."))

    @property
    def parent(self) -> str:
        """Return the path of the enclosing block; `""` for a top-level key."""
        return ".".join(self.segments[:-1])

    @property
    def name(self) -> str:
        """Return the last path segment: the key as it is written in its block."""
        return self.segments[-1]

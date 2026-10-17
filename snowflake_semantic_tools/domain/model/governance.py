"""Which Snowflake tags make a column sensitive, and what a column's YAML records about it.

`governance.sensitive_tags` names tags, or whole tag schemas. A column Snowflake lists with a
matching tag is treated as one whose dbt YAML carries `pii_tags`: its values are never sampled
and never reach a prompt. Matching compares names as Snowflake resolves them, so an unquoted part
matches without case and a quoted one exactly.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName


@dataclass(frozen=True, slots=True, order=True)
class TagPattern:
    """One `governance.sensitive_tags` entry: a tag by its three-part name, or every tag of a schema.

    Attributes:
        name: The tag; None for `<database>.<schema>.*`, which matches every tag in the schema.
    """

    database: Identifier
    schema: Identifier
    name: Identifier | None = None

    @classmethod
    def parse(cls, raw: str) -> TagPattern:
        """Parse `DB.SCHEMA.TAG`, or `DB.SCHEMA.*` for a whole schema, each part as a Snowflake name.

        Raises:
            ValueError: the entry is neither a three-part name nor two parts followed by `.*`.
        """
        text = raw.strip()
        if text.endswith(".*"):
            # `*` is no identifier: any valid name stands in for it while the schema is parsed.
            parsed = QualifiedName.parse(text[:-1] + "_")
            return cls(parsed.database, parsed.schema)
        parsed = QualifiedName.parse(text)
        return cls(parsed.database, parsed.schema, parsed.name)

    @property
    def text(self) -> str:
        """The entry as SQL names it: each part's `Identifier.sql`, the last `*` for a whole schema."""
        last = self.name.sql if self.name is not None else "*"
        return f"{self.database.sql}.{self.schema.sql}.{last}"

    def matches(self, tag: QualifiedName) -> bool:
        """Report whether the entry names `tag`, comparing each part as Snowflake resolves it."""
        if (tag.database.folded, tag.schema.folded) != (self.database.folded, self.schema.folded):
            return False
        return self.name is None or tag.name.folded == self.name.folded


def matching_tags(tags: Iterable[QualifiedName], patterns: Sequence[TagPattern]) -> tuple[str, ...]:
    """Return the tags any pattern matches, each as its SQL name, sorted and without repeats."""
    return tuple(sorted({tag.sql for tag in tags if any(pattern.matches(tag) for pattern in patterns)}))


def _tag_name(text: str) -> str:
    """Return a recorded tag as `matching_tags` names it; text that is no three-part name, upper-cased."""
    try:
        return QualifiedName.parse(text).sql
    except ValueError:
        return text.strip().upper()


def records_tag(column: DbtColumn | None, tag: str) -> bool:
    """Report whether a column's YAML records `tag`: it carries `pii_tags`, or lists the tag.

    `tag` is named as `matching_tags` names it; each `meta.sst.governance_tags` entry is compared
    the same way, so a lowercase entry records an unquoted tag.
    """
    if column is None:
        return False
    return column.pii_tagged or tag in {_tag_name(recorded) for recorded in column.governance_tags}


def sensitivity_reason(column: DbtColumn, found: Sequence[str] = ()) -> str:
    """Name why a column is sensitive: `pii_tags`, else the first tag recorded or found.

    A column with neither `pii_tags` nor any tag is named by `pii_tags` too.

    Args:
        found: The sensitive tags Snowflake lists on the column, read in this run; consulted
            after the tags its YAML records.
    """
    tags = (*column.governance_tags, *found)
    return "pii_tags" if column.pii_tagged or not tags else tags[0]

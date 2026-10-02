"""The prompts enrich sends Cortex for synonyms, the response shapes it requests, and their parsing.

Cortex answers through structured output, so a response follows the schema given with the
prompt; parsing still checks the shape, and every synonym is cleaned before it is written.
Values of a column that carries `pii_tags` never reach a prompt.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

# Columns described in one prompt; a model with more is asked about in batches.
COLUMNS_PER_PROMPT = 50

# Example values shown per column, each cut to `EXAMPLE_LENGTH` characters.
EXAMPLES_PER_COLUMN = 5
EXAMPLE_LENGTH = 40

# Other tables' names a table-synonym prompt lists, so the model can avoid them.
AVOIDED_PER_PROMPT = 50


def _strict(properties: Mapping[str, object]) -> dict[str, object]:
    """An object schema requiring every property and no other, as every Cortex model accepts it."""
    return {
        "type": "object",
        "properties": dict(properties),
        "required": list(properties),
        "additionalProperties": False,
    }


_STRINGS = {"type": "array", "items": {"type": "string"}}

COLUMN_SYNONYMS_SCHEMA: Mapping[str, object] = MappingProxyType(
    _strict({"columns": {"type": "array", "items": _strict({"name": {"type": "string"}, "synonyms": _STRINGS})}})
)
TABLE_SYNONYMS_SCHEMA: Mapping[str, object] = MappingProxyType(_strict({"synonyms": _STRINGS}))


@dataclass(frozen=True, slots=True)
class PromptColumn:
    """One column as a synonym prompt describes it.

    Attributes:
        name: The column's name as the model YAML writes it.
        data_type: Its data type, or None when neither the warehouse nor the YAML says.
        description: Its description, or None.
        examples: A few of its values; empty for a column that carries `pii_tags`.
    """

    name: str
    data_type: str | None
    description: str | None
    examples: tuple[str, ...] = ()


def _one_line(text: str | None) -> str:
    return " ".join((text or "").split()) or "none"


def _column_line(column: PromptColumn) -> str:
    line = f"- {column.name} ({column.data_type or 'unknown type'}): {_one_line(column.description)}"
    examples = [" ".join(value.split())[:EXAMPLE_LENGTH] for value in column.examples[:EXAMPLES_PER_COLUMN]]
    return f"{line} Examples: {', '.join(examples)}" if examples else line


def column_synonyms_prompt(
    table: str, description: str | None, columns: Sequence[PromptColumn], *, max_count: int
) -> str:
    """Return the prompt asking for synonyms of up to `COLUMNS_PER_PROMPT` columns of one table."""
    lines = [
        "You write synonyms for the columns of one table in a Snowflake semantic view. A synonym is",
        "another name a business user might use for the column in a question.",
        f"Give at most {max_count} synonyms per column. Each is a short phrase of plain lowercase words,",
        "without quotes, different from the column's own name and from every other column's name.",
        "",
        f"Table: {table}",
        f"Description: {_one_line(description)}",
        "",
        "Columns:",
        *(_column_line(column) for column in columns[:COLUMNS_PER_PROMPT]),
        "",
        "Answer with an entry for every column listed, using the column name exactly as written.",
    ]
    return "\n".join(lines)


def table_synonyms_prompt(
    table: str,
    description: str | None,
    columns: Sequence[str],
    avoided: Sequence[str],
    *,
    max_count: int,
) -> str:
    """Return the prompt asking for synonyms of one table, avoiding the other tables' names."""
    lines = [
        "You write synonyms for one table in a Snowflake semantic view. A synonym is another name a",
        "business user might use for the table in a question.",
        f"Give at most {max_count} synonyms. Each is a short phrase of plain lowercase words, without",
        "quotes, different from the table's own name and from the names listed to avoid.",
        "",
        f"Table: {table}",
        f"Description: {_one_line(description)}",
        f"Columns: {', '.join(columns[:COLUMNS_PER_PROMPT]) or 'none'}",
        f"Avoid: {', '.join(avoided[:AVOIDED_PER_PROMPT]) or 'nothing'}",
    ]
    return "\n".join(lines)


def parse_column_synonyms(response: object) -> dict[str, tuple[object, ...]] | None:
    """Read a column-synonym response into each column's proposals, by casefolded column name.

    An entry without a text name or a list of synonyms is skipped, and a later entry for a column
    replaces an earlier one.

    Returns:
        The proposals; None when the response is not an object with a `columns` list.
    """
    entries = response.get("columns") if isinstance(response, dict) else None
    if not isinstance(entries, list):
        return None
    proposals: dict[str, tuple[object, ...]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name, synonyms = entry.get("name"), entry.get("synonyms")
        if isinstance(name, str) and isinstance(synonyms, list):
            proposals[name.strip().casefold()] = tuple(synonyms)
    return proposals


def parse_table_synonyms(response: object) -> tuple[object, ...] | None:
    """Read a table-synonym response into its proposals; None when it has no `synonyms` list."""
    synonyms = response.get("synonyms") if isinstance(response, dict) else None
    return tuple(synonyms) if isinstance(synonyms, list) else None

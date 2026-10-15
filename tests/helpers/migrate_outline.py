"""Outlines for the migration tests: a file read by the YAML adapter, as `sst migrate` reads it."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.outline import outline as adapter_outline
from snowflake_semantic_tools.domain.migrate.changes import Changes, merged
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.refs import MigrationResult


def outline(text: str) -> Node:
    """Return the outline of a YAML text that parses."""
    root = adapter_outline(text)
    assert root is not None, text
    return root


def lines(text: str) -> list[str]:
    """Return the text's lines, endings kept, as the migrations take them."""
    return text.splitlines(keepends=True)


def applied(text: str, changes: Changes) -> str:
    """Return the text with a pass's edits applied."""
    return merged(MigrationResult(text), changes).text

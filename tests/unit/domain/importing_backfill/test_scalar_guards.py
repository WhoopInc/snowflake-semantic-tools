"""The guards that keep `sst migrate refs` from editing a scalar it cannot map to the file."""

from __future__ import annotations

from snowflake_semantic_tools.domain.importing.agent import _article
from snowflake_semantic_tools.domain.migrate.inferred_tables import _block, _scalar
from snowflake_semantic_tools.domain.migrate.outline import Node


def test_a_null_scalar_is_not_mapped() -> None:
    assert _scalar(["expr:"], Node("SCALAR", 0, 6, 0, 6, value=None)) is None


def test_a_block_with_no_content_line_is_not_mapped() -> None:
    lines = ["expr: |", "", "next: 1"]
    assert _block(lines, Node("SCALAR", 0, 6, 1, 0, value="", style="|"), "") is None


def test_a_block_whose_lines_are_not_in_its_value_is_not_mapped() -> None:
    lines = ["expr: |", "  SELECT a", "  FROM t"]
    # The value the parser reports does not hold the written lines: nothing may be edited.
    assert _block(lines, Node("SCALAR", 0, 6, 2, 8, value="SELECT b", style="|"), "SELECT b") is None


def test_the_article_follows_the_artifact_types_first_letter() -> None:
    assert (_article("agent"), _article("cowork_profile")) == ("an", "a")

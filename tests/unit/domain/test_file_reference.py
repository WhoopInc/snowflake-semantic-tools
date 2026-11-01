"""The authored-file reference page lists every key the tables declare, each as the table describes it."""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS, Field, FieldStatus, Node
from snowflake_semantic_tools.domain.render.file_reference import render_files

PAGE = render_files(generated="<!-- generated -->")


def _keys(node: Node, prefix: str = "") -> list[tuple[str, Field]]:
    found: list[tuple[str, Field]] = []
    for item in node.all_fields():
        key = f"{prefix}{item.name}"
        found.append((key, item))
        if item.nested is not None and item.config_block is None:
            found += _keys(item.nested, f"{key}[]." if isinstance(item.items, Node) else f"{key}.")
    return found


def _section(kind: str) -> str:
    return PAGE.split(f"\n## {kind}\n", 1)[1].split("\n## ", 1)[0]


def test_every_kind_has_a_section_listing_each_of_its_keys() -> None:
    for name, kind in FILE_KINDS.items():
        rows = re.findall(r"^\| `([^`]+)` \|", _section(name), re.MULTILINE)
        # A key two variants declare apart has a row for each, with the variant it applies to.
        assert set(rows) == {key for key, _ in _keys(kind.node)}, name


def test_a_row_says_how_sst_reads_its_key() -> None:
    for name, kind in FILE_KINDS.items():
        section = _section(name)
        for key, item in _keys(kind.node):
            rows = [line for line in section.splitlines() if line.startswith(f"| `{key}` |")]
            row = next((line for line in rows if item.summary.replace("|", "\\|") in line), "")
            assert row, (name, key)
            assert ("*Not kind-checked.*" in row) == item.loose, (name, key)
            if item.status is not FieldStatus.CURRENT:
                assert f"*{item.status.value}*" in row or "*ignored*" in row, (name, key, row)
            if item.required and item.read:
                assert row.split(" | ")[2] == "yes", (name, key)


def test_every_contents_link_names_a_section() -> None:
    for anchor in re.findall(r"\]\(#([a-z_]+)\)", PAGE):
        assert f"\n## {anchor}\n" in PAGE

"""The migrate adapter locates filter entries and reads only semantic YAML."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.migrate import filter_sites, semantic_files, write_file


def test_filter_sites_locate_entries_expressions_and_labels() -> None:
    text = (
        "snowflake_filters:\n"
        "  - name: completed\n"
        "    tables:\n"
        "      - {{ table('orders') }}\n"
        "    expr: |-\n"
        "      {{ column('orders', 'state') }} = 'completed'\n"
        "  - name: labelled\n"
        '    expr: "TRUE"\n'
        "    labels: [filter]\n"
        "  - justastring\n"
        "  - name: noexpr\n"
        "other: 1\n"
    )
    sites = filter_sites(text, "filters.yml")
    assert [(site.name, site.has_labels, site.last_line, site.indent) for site in sites] == [
        ("completed", False, 6, 4),
        ("labelled", True, 9, 4),
    ]
    assert sites[0].expr == "{{ column('orders', 'state') }} = 'completed'"
    assert filter_sites("other: 1\n", "x.yml") == ()
    assert filter_sites("snowflake_filters: [unclosed\n", "x.yml") == ()
    assert filter_sites("- snowflake_filters\n", "x.yml") == ()
    assert filter_sites("snowflake_filters: {}\n", "x.yml") == ()


def test_semantic_files_and_write(tmp_path: Path) -> None:
    root = tmp_path / "semantic_models"
    (root / "views").mkdir(parents=True)
    (root / ".hidden").mkdir()
    (root / "views" / "a.yml").write_text("a: 1\n", encoding="utf-8")
    (root / "views" / "b.yaml").write_text("b: 1\n", encoding="utf-8")
    (root / "views" / "notes.md").write_text("x", encoding="utf-8")
    (root / ".hidden" / "c.yml").write_text("c: 1\n", encoding="utf-8")
    files = semantic_files(tmp_path, "semantic_models")
    assert sorted(files) == ["semantic_models/views/a.yml", "semantic_models/views/b.yaml"]
    write_file(tmp_path, "semantic_models/views/a.yml", "a: 2\n")
    assert (root / "views" / "a.yml").read_text(encoding="utf-8") == "a: 2\n"
    # A CRLF file is read as LF and written back CRLF, so the diff is only the rewrite.
    (root / "views" / "b.yaml").write_bytes(b"b: 1\r\nc: 2\r\n")
    assert semantic_files(tmp_path, "semantic_models")["semantic_models/views/b.yaml"] == "b: 1\nc: 2\n"
    write_file(tmp_path, "semantic_models/views/b.yaml", "b: 3\nc: 2\n")
    assert (root / "views" / "b.yaml").read_bytes() == b"b: 3\r\nc: 2\r\n"
    write_file(tmp_path, "semantic_models/views/new.yml", "d: 1\n")
    assert (root / "views" / "new.yml").read_bytes() == b"d: 1\n"
    with pytest.raises(ProjectError):
        semantic_files(tmp_path, "missing")

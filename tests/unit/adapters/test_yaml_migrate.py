"""The migrate adapter locates filter entries, reads the files each migration takes, and outlines YAML."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.migrate import (
    agent_files,
    create_file,
    files_under,
    filter_sites,
    remove_file,
    semantic_files,
    uses_crlf,
    write_file,
)
from snowflake_semantic_tools.adapters.yaml.outline import outline


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


def test_filter_sites_read_a_single_filter_file() -> None:
    sites = filter_sites("filter:\n  name: done\n  expr: \"{{ ref('o', 's') }} = 'x'\"\n", "f.yml")
    assert [(site.name, site.last_line, site.indent) for site in sites] == [("done", 3, 2)]
    assert filter_sites("filter: [x]\n", "f.yml") == ()


def test_the_file_sets_each_migration_reads(tmp_path: Path) -> None:
    for path, text in {
        "agents/a/agent.yml": "name: a\n",
        "agents/a/evals/config.yml": "name: c\n",
        "agents/a/instructions/notes.yml": "x: 1\n",
        "tools/t.yml": "x: 1\n",
        "tools/notes.md": "---\n",
    }.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text, encoding="utf-8")
    agents, evals = agent_files(tmp_path, "agents")
    assert list(agents) == ["agents/a/agent.yml"] and list(evals) == ["agents/a/evals/config.yml"]
    assert list(files_under(tmp_path, "tools", frozenset((".md",)))) == ["tools/notes.md"]
    assert files_under(tmp_path, "missing", frozenset((".md",))) == {}


def test_create_and_remove_files(tmp_path: Path) -> None:
    create_file(tmp_path, "a/b.yml", "x: 1\n", crlf=True)
    assert (tmp_path / "a" / "b.yml").read_bytes() == b"x: 1\r\n" and uses_crlf(tmp_path, "a/b.yml")
    with pytest.raises(FileExistsError):
        create_file(tmp_path, "a/b.yml", "x: 2\n", crlf=False)
    remove_file(tmp_path, "a/b.yml")
    assert not (tmp_path / "a" / "b.yml").exists() and not uses_crlf(tmp_path, "a/b.yml")


def test_outline_keeps_positions_and_puts_templates_back() -> None:
    text = (
        "tables:\n  - {{ ref('a') }}\n  - x {{ ref('b') }}{{ ref('c') }} y\nflow: [{{ ref('d') }}]\n# {{ not a call\n"
    )
    root = outline(text)
    assert root is not None
    items = root.get("tables")
    assert items is not None and [item.text for item in items.items] == [
        "{{ ref('a') }}",
        "x {{ ref('b') }}{{ ref('c') }} y",
    ]
    assert items.items[1].line == 2 and items.items[1].col == 4
    assert root.get("flow").items[0].text == "{{ ref('d') }}"  # type: ignore[union-attr]
    assert outline("a: [\n") is None and outline("") is None
    assert outline('a: "\u2063"\n').get("a").text == "\u2063"  # type: ignore[union-attr]

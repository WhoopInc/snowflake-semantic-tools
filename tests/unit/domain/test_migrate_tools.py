"""`sst migrate refs` for tool files: columns as refs, body_file, stage imports, settings, owner."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.tools import (
    ToolInfo,
    columns_list,
    on_model,
    stage_path,
    tool_index,
    tool_references,
    tool_sites,
    tool_structure,
)
from tests.helpers.migrate_outline import applied, lines, outline

GROUPS = """tools:
  - group: g
    define:
      - name: search
        type: cortex_search_service
        on:
          table: "{{ ref('docs') }}"
          search_column: body
          attributes: [kind]
        id_column: doc_id
        attribute_columns:
          - "{{ ref('docs', 'kind') }}"
        columns_and_descriptions:
          doc_id:
            description: Id.
          # A comment between columns.
          kind:
          body: {searchable: true}
          empty: {}
        warehouse: null
        target_lag: 1 hour
      - name: proc
        type: procedure
        body_file: tools/sql/proc.sql
        execute_as: caller
        owner: me
        imports:
          - "@DB.SCH.EXPORTS/lib.zip"
          - "@OTHER.STAGE/x"
    reference:
      - name: remote_search
        type: cortex_search_service
        relations:
          dev: DB.SCH.REMOTE
        id_column: rid
        columns_and_descriptions:
          rid:
            description: Remote id.
      - name: exports
        type: stage
        relations:
          dev: DB.SCH.EXPORTS
"""


def test_sites_index_and_on_model() -> None:
    root = outline(GROUPS)
    assert [(site.node.get("name").text, site.defined) for site in tool_sites(root)] == [  # type: ignore[union-attr]
        ("search", True),
        ("proc", True),
        ("remote_search", False),
        ("exports", False),
    ]
    single = outline(
        "tool:\n  name: t\n  type: stage\n  on: \"{{ ref('m') }}\"\n"
        "tools:\n  - name: u\n    relations: {dev: X}\n  - type: x\n"
    )
    index = tool_index({"b.yml": root, "a.yml": single})
    assert index["t"] == ToolInfo("t", "stage", True, "m")
    assert index["u"].defined is False and index["u"].relations == ("X",) and index["u"].type == ""
    assert index["exports"].relations == ("DB.SCH.EXPORTS",)
    assert (
        on_model(outline('on: "{{ broken( }}"\n')) is None
        and on_model(outline("on: \"{{ ref('a', 'b') }}\"\n")) is None
    )
    assert list(tool_sites(outline("tools: {}\ntool: [x]\n"))) == []
    assert list(tool_sites(outline("tools:\n  - group: g\n    define: x\n"))) == []


def test_stage_paths_follow_the_declared_stage() -> None:
    index = {
        "exports": ToolInfo("exports", "stage", False, relations=("DB.SCH.EXPORTS",)),
        "drops": ToolInfo("drops", "stage", True),
        "other": ToolInfo("other", "procedure", True),
    }
    assert stage_path("@db.sch.exports/a/b", index) == "{{ tool('exports') }}/a/b"
    assert stage_path("@X.Y.DROPS", index) == "{{ tool('drops') }}"
    assert stage_path("@X.Y.NOPE/a", index) is None
    assert stage_path("{{ tool('drops') }}/a", index) is None and stage_path(None, index) is None
    assert stage_path("DB.SCH.EXPORTS", index) is None


def test_reference_pass_rewrites_defined_columns_body_and_imports() -> None:
    root = outline(GROUPS)
    index = tool_index({"tools/g.yml": root})
    changes = tool_references(lines(GROUPS), root, "tools/g.yml", index)
    text = applied(GROUPS, changes)
    assert "          search_column: \"{{ ref('docs', 'body') }}\"\n" in text
    assert "          attributes: [\"{{ ref('docs', 'kind') }}\"]\n" in text
    assert "        id_column: \"{{ ref('docs', 'doc_id') }}\"\n" in text
    assert (
        "        columns:\n"
        "          - column: \"{{ ref('docs', 'doc_id') }}\"\n"
        "            description: Id.\n"
        "          # A comment between columns.\n"
        "          - column: \"{{ ref('docs', 'kind') }}\"\n"
        "          - {column: \"{{ ref('docs', 'body') }}\", searchable: true}\n"
        "          - {column: \"{{ ref('docs', 'empty') }}\"}\n"
    ) in text
    assert "        body: \"{{ file('sql/proc.sql') }}\"\n" in text
    assert '          - "{{ tool(\'exports\') }}/lib.zip"\n          - "@OTHER.STAGE/x"\n' in text
    # A referenced member's columns stay bare, in a columns: list.
    assert "        id_column: rid\n        columns:\n          - column: rid\n" in text


def test_reference_pass_reports_what_it_cannot_rewrite() -> None:
    source = (
        "tools:\n"
        "  - name: a\n    type: cortex_search_service\n    search_column: body\n"
        "    columns_and_descriptions: {x: {}}\n"
        "  - name: b\n    type: cortex_search_service\n    on: \"{{ ref('m') }}\"\n"
        "    columns_and_descriptions:\n      x: text\n"
        "  - name: c\n    type: procedure\n    body_file: ../outside.sql\n"
        "  - name: d\n    type: procedure\n    body_file: \"{{ file('x') }}\"\n"
        "  - name: e\n    type: procedure\n    body_file:\n"
        "  - name: f\n    type: cortex_search_service\n    columns_and_descriptions:\n      x: {}\n"
        "  - name: g\n    type: cortex_search_service\n    on: \"{{ ref('m') }}\"\n"
        "    columns_and_descriptions:\n      x: {a: 1,\n          b: 2}\n"
    )
    changes = tool_references(lines(source), outline(source), "t.yml", {})
    reasons = [item.reason for item in changes.untouched]
    assert reasons == [
        "the member's on: names no model to ref the column of",
        "a flow mapping: rewrite it as columns: by hand",
        "the column's entry is not a mapping",
        "body_file names a path outside the project",
        "body_file names a path outside the project",
        "the member's on: names no model: rewrite it as columns: by hand",
        "the column's entry is not a mapping",
    ]
    entry = outline("columns_and_descriptions:\n  x:\n").entry("columns_and_descriptions")
    assert entry is not None
    bare = Changes()
    columns_list(lines("columns_and_descriptions:\n  x:\n"), entry, None, bare, as_ref=False)
    assert applied("columns_and_descriptions:\n  x:\n", bare) == "columns:\n  - column: x\n"


def test_structure_pass_moves_settings_and_owner() -> None:
    root = outline(GROUPS)
    text = applied(GROUPS, tool_structure(lines(GROUPS), root))
    assert "warehouse" not in text
    assert "        config:\n          target_lag: 1 hour\n" in text
    assert "        config:\n          execute_as: caller\n        meta:\n          owner: me\n" in text

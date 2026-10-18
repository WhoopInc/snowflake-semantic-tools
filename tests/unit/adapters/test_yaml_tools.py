from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml import tools as tools_loader
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolOwnership


def _dbt() -> DbtCatalog:
    return DbtCatalog(
        "v12",
        "1.11",
        "p",
        (
            DbtModel(
                "model.p.docs",
                "docs",
                "DB.S.DOCS",
                (),
                (),
                (
                    DbtColumn("body", None, "TEXT", None),
                    DbtColumn("category", None, "TEXT", None),
                    DbtColumn("doc_id", None, "TEXT", None),
                ),
            ),
            DbtModel("model.p.notes", "notes", "DB.S.NOTES", (), (), (DbtColumn("body", None, "TEXT", None),)),
        ),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def _load(project: Path, config: dict[str, object] | None = None) -> ToolCatalog:
    return load_tool_catalog(
        project, _dbt(), target_name="dev", declared_targets=frozenset(("dev", "prod")), config=config
    )


def _codes(catalog: ToolCatalog, *codes: str) -> list[Diagnostic]:
    return [item for item in catalog.diagnostics if item.code in codes]


SEARCH = """
tool:
  name: docs_search
  type: cortex_search_service
  on:
    table: "{{ ref('docs') }}"
    search_column: "{{ ref('docs', 'body') }}"
    attributes: ["{{ ref('docs', 'category') }}"]
  id_column: "{{ ref('docs', 'doc_id') }}"
  columns:
    - column: "{{ ref('docs', 'doc_id') }}"
      description: Document id.
      type: string
      filterable: true
  meta:
    owner: docs-team
"""


def test_a_tool_file_holds_one_member_and_a_tools_file_a_list(tmp_path: Path) -> None:
    _write(tmp_path / "tools" / "platform" / "docs_search.yml", SEARCH)
    _write(
        tmp_path / "tools" / "partner" / "lookups.yml",
        """
tools:
  - name: lookup
    type: procedure
    relations:
      dev: DB.S.LOOKUP
      prod: PROD.S.LOOKUP
""",
    )
    catalog = _load(tmp_path)
    assert not catalog.diagnostics.has_errors
    assert [member.name for member in catalog.members] == ["lookup", "docs_search"]
    [search] = catalog.managed
    assert search.ownership is ToolOwnership.DEFINE
    assert (search.on_model, search.search_column, search.attribute_columns) == ("docs", "body", ("category",))
    assert (search.id_column, [column.name for column in search.columns]) == ("doc_id", ["doc_id"])
    assert (search.folder, dict(search.meta)) == (("platform",), {"owner": "docs-team"})
    assert search.origin.line == 2
    [lookup] = catalog.references
    assert lookup.artifact_key is None and lookup.folder == ("partner",)
    resolved, diagnostics = catalog.resolve("LOOKUP")
    assert resolved is lookup and diagnostics == ()
    relation, diagnostics = catalog.relation(lookup)
    assert relation is not None and relation.sql == "DB.S.LOOKUP" and diagnostics == ()


def test_the_loader_reads_the_tool_roots_and_block_the_registry_declares() -> None:
    tool = ARTIFACT_REGISTRY.artifacts["tool"]
    roots = (tool.singular_root, tool.root_key)
    loaded = (tools_loader._ONE, tools_loader._MANY)
    assert loaded == roots
    assert set(roots) == tools_loader._FILE_KEYS
    assert tool.routed_block == tools_loader._TOOL.routed_block == "tools"


def test_a_member_s_settings_resolve_over_its_config_then_the_closest_folder_route(tmp_path: Path) -> None:
    _write(tmp_path / "tools" / "platform" / "docs" / "docs_search.yml", SEARCH)
    _write(
        tmp_path / "tools" / "partner" / "lookup.yml",
        """
tool:
  name: lookup
  type: procedure
  relations: {dev: DB.S.LOOKUP, prod: DB.S.LOOKUP}
  config:
    warehouse: LOOKUP_WH
""",
    )
    config: dict[str, object] = {
        "tools": {
            "+schema": "TOOLS",
            "+immutable": False,
            "platform": {"+schema": "PLATFORM", "docs": {"+target_lag": "1 day"}},
            "partner": {"+immutable": True},
        }
    }
    catalog = _load(tmp_path, config)
    [lookup, search] = catalog.members
    assert {key: (item.value, item.source) for key, item in search.settings.items()} == {
        "schema": ("PLATFORM", "route:platform"),
        "immutable": (False, "root"),
        "target_lag": ("1 day", "route:platform/docs"),
    }
    assert lookup.immutable and lookup.configured_warehouse == "LOOKUP_WH"
    assert lookup.settings["warehouse"].source == "config"
    # An immutable member may name one object for every target.
    assert _codes(catalog, "SST-REF023", "SST-VAL606") == []


def test_an_immutable_member_that_would_be_defined_is_reported(tmp_path: Path) -> None:
    _write(tmp_path / "tools" / "partner" / "docs_search.yml", SEARCH)
    catalog = _load(tmp_path, {"tools": {"partner": {"+immutable": True}}})
    [diagnostic] = _codes(catalog, "SST-VAL606")
    assert diagnostic.subject == "tool:docs_search"


def test_a_route_naming_no_folder_is_reported_even_without_a_tools_folder(tmp_path: Path) -> None:
    config: dict[str, object] = {"tools": {"vendor": {"+immutable": True}}}
    catalog = _load(tmp_path, config)
    assert [(item.code, item.subject) for item in catalog.diagnostics] == [("SST-CFG041", "config_route:vendor")]
    _write(tmp_path / "tools" / "platform" / "docs_search.yml", SEARCH)
    assert [item.code for item in _load(tmp_path, config).diagnostics] == ["SST-CFG041"]


def test_a_member_s_config_block_takes_only_the_tools_settings(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "lookup.yml",
        """
tool:
  name: lookup
  type: stage
  config:
    stage: X
    execute_as: everyone
    target_lag:
""",
    )
    codes = {item.code for item in _load(tmp_path).diagnostics}
    assert {"SST-CFG008", "SST-CFG053", "SST-CFG054"} <= codes


def test_tool_loader_validates_structure_targets_models_and_creation_keys(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "bad.yml",
        """
tools:
  - name: bad_search
    type: cortex_search_service
    on: "{{ ref('missing') }}"
    search_column: absent
  - name: bad_ref
    type: procedure
    on: "{{ ref('docs') }}"
    relations:
      prod: ONLY.TWO
      invented: DB.S.X
""",
    )
    codes = {diagnostic.code for diagnostic in _load(tmp_path).diagnostics}
    assert {"SST-VAL605", "SST-VAL608", "SST-REF018", "SST-REF019"}.issubset(codes)


def test_a_member_name_repeated_anywhere_in_the_project_is_one_error(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "one" / "a.yml",
        """
tools:
  - name: same
    type: mystery
    relations: {dev: DB.S.A}
  - name: same
    type: procedure
    signature:
      - {name: payload, type: object}
    relations: {dev: DB.S.B}
""",
    )
    _write(tmp_path / "tools" / "two" / "b.yml", "tool:\n  name: SAME\n  type: procedure\n  relations: {dev: DB.S.C}")
    catalog = _load(tmp_path)
    [repeated] = _codes(catalog, "SST-VAL001")
    assert (repeated.subject, len(repeated.related)) == ("tool:same", 2)
    assert "SST-VAL603" in {item.code for item in catalog.diagnostics}


def test_an_unreadable_tools_file_is_reported_and_the_rest_still_load(tmp_path: Path) -> None:
    _write(tmp_path / "tools" / "a_broken.yml", "tools: [\n")
    _write(tmp_path / "tools" / "b_dated.yml", "tools: []\nreviewed: 2024-13-01\n")
    _write(
        tmp_path / "tools" / "c" / "platform.yml", "tool:\n  name: lookup\n  type: procedure\n  relations: {dev: X.Y.Z}"
    )
    catalog = _load(tmp_path)
    located = sorted((item.code, item.context["file"]) for item in catalog.diagnostics)
    assert located == [("SST-LOD001", "tools/a_broken.yml"), ("SST-LOD001", "tools/b_dated.yml")]
    assert [member.name for member in catalog.references] == ["lookup"]


def test_a_tools_file_that_cannot_be_read_is_reported_as_unreadable_not_malformed(tmp_path: Path) -> None:
    locked = tmp_path / "tools" / "locked.yml"
    _write(locked, "tools: []\n")
    locked.chmod(0)
    try:
        catalog = _load(tmp_path)
    finally:
        locked.chmod(0o644)

    [diagnostic] = catalog.diagnostics
    assert (diagnostic.code, diagnostic.context["path"]) == ("SST-LOD018", "tools/locked.yml")
    assert diagnostic.message == "tools/locked.yml: could not read tools/locked.yml"


@pytest.mark.parametrize(
    ("text", "field", "expected"),
    [
        ("tool: {name: a, type: stage}\ntools: []", "root", "one of tool: and tools:"),
        ("other: 1", "tools", "a tool: mapping or a tools: list"),
        ("tools: {name: a}", "tools", "list"),
        ("tool: [a]", "tool", "mapping"),
        ("tools: [a]", "tools.0", "mapping"),
    ],
)
def test_a_tools_file_holds_one_tool_mapping_or_one_tools_list(
    tmp_path: Path, text: str, field: str, expected: str
) -> None:
    _write(tmp_path / "tools" / "t.yml", text)
    [diagnostic] = _codes(_load(tmp_path), "SST-PRS003")
    assert (diagnostic.context["field"], diagnostic.context["expected"]) == (field, expected)


def test_a_member_without_a_name_or_type_is_left_out(tmp_path: Path) -> None:
    _write(tmp_path / "tools" / "t.yml", "tools:\n  - description: x\n")
    catalog = _load(tmp_path)
    assert catalog.members == ()
    assert [item.context["field"] for item in _codes(catalog, "SST-PRS002")] == ["tools.0.name", "tools.0.type"]


def test_a_tool_group_and_a_moved_member_key_are_errors_that_say_where_to_write_now(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "t.yml",
        """
tools:
  - group: platform
    define: []
  - name: lookup
    type: procedure
    relations: {dev: X.Y.Z}
    warehouse: WH
    owner: team
""",
    )
    found = [
        (item.context["field"], item.origin.line if item.origin else None)
        for item in _codes(_load(tmp_path), "SST-PRS126")
    ]
    assert found == [("group", 2), ("define", 3), ("owner", 8), ("warehouse", 7)]


def test_a_near_miss_or_unknown_key_is_reported_where_it_is_written(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "t.yml",
        """
version: 2
tool:
  name: docs_search
  type: cortex_search_service
  on: "{{ ref('docs') }}"
  serch_column: "{{ ref('docs', 'body') }}"
  columns:
    - column: "{{ ref('docs', 'body') }}"
      colour: red
""",
    )
    found = [
        (item.code, item.message, item.origin.line if item.origin else None) for item in _load(tmp_path).diagnostics
    ]
    assert ("SST-PRS004", "tools/t.yml: unknown field 'version'", 1) in found
    assert ("SST-PRS022", "tool:docs_search: unknown field 'serch_column'; did you mean 'search_column'?", 6) in found
    assert ("SST-PRS004", "tool:docs_search: unknown field 'colour'", 9) in found


def _column_codes(tmp_path: Path, column: str, *, relations: bool = False) -> list[tuple[str, str]]:
    """The code and field of each problem reading a member whose search column is written `column`."""
    related = "\n  relations: {dev: DB.S.SEARCH}" if relations else "\n  on: \"{{ ref('docs') }}\""
    _write(
        tmp_path / "tools" / "t.yml",
        f"tool:\n  name: docs_search\n  type: cortex_search_service{related}\n  search_column: '{column}'",
    )
    catalog = _load(tmp_path)
    return [
        (item.code, str(item.context.get("field", "")))
        for item in catalog.diagnostics
        if item.code not in ("SST-VAL605", "SST-PRS002", "SST-VAL609", "SST-VAL617")
    ]


@pytest.mark.parametrize(
    ("column", "code"),
    [
        ("body", "SST-REF050"),
        ("{{ ref('notes', 'body') }}", "SST-VAL622"),
        ("{{ ref('docs') }}", "SST-REF015"),
        ("{{ var('docs') }}", "SST-REF041"),
        ("{{ ref('docs', 'body') }} and more", "SST-REF003"),
        ("{{ ref('docs', 'body')", "SST-LOD004"),
    ],
)
def test_a_defined_member_s_column_is_one_ref_of_its_on_model(tmp_path: Path, column: str, code: str) -> None:
    assert [found for found, _ in _column_codes(tmp_path, column)] == [code]


def test_a_referenced_member_s_column_is_written_bare(tmp_path: Path) -> None:
    assert _column_codes(tmp_path, "body", relations=True) == []
    assert _column_codes(tmp_path, "{{ ref('docs', 'body') }}", relations=True) == [("SST-REF008", "search_column")]


def test_each_columns_entry_is_a_mapping_naming_its_column(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "t.yml",
        """
tool:
  name: docs_search
  type: cortex_search_service
  on: "{{ ref('docs') }}"
  search_column: "{{ ref('docs', 'body') }}"
  columns:
    - plain
    - description: no column
    - column: category
    - column: "{{ ref('docs', 'category') }}"
""",
    )
    catalog = _load(tmp_path)
    assert [
        (item.code, item.context["field"]) for item in _codes(catalog, "SST-PRS003", "SST-PRS002", "SST-REF050")
    ] == [
        ("SST-PRS002", "columns.1.column"),
        ("SST-PRS003", "columns.0"),
        ("SST-REF050", "columns.2.column"),
    ]
    assert [column.name for column in catalog.members[0].columns] == ["category"]
    _write(tmp_path / "tools" / "t.yml", "tool:\n  name: a\n  type: stage\n  columns: {a: 1}\n  meta: [x]")
    assert [item.context["field"] for item in _codes(_load(tmp_path), "SST-PRS003")] == ["columns", "meta"]


def test_a_where_resolves_its_column_refs_to_bare_columns_and_its_vars(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "t.yml",
        """
tool:
  name: docs_search
  type: cortex_search_service
  on: "{{ ref('docs') }}"
  search_column: "{{ ref('docs', 'body') }}"
  where: "{{ ref('docs', 'category') }} = '{{ var('kind') }}'"
""",
    )
    catalog = _load(tmp_path, {"vars": {"kind": "public", "unset": None}})
    assert catalog.members[0].where == "CATEGORY = 'public'"
    _write(
        tmp_path / "tools" / "t.yml",
        """
tool:
  name: docs_search
  type: cortex_search_service
  on: "{{ ref('docs') }}"
  search_column: "{{ ref('docs', 'body') }}"
  where: "{{ ref('notes', 'body') }} IS NOT NULL"
""",
    )
    [diagnostic] = _codes(_load(tmp_path), "SST-VAL622")
    assert diagnostic.message == "tool member 'docs_search': where names model 'notes', not its on: model 'docs'"


def test_a_referenced_member_s_where_is_kept_as_written(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "t.yml",
        "tool:\n  name: s\n  type: cortex_search_service\n  relations: {dev: A.B.C}\n  where: \"{{ ref('x', 'y') }}\"",
    )
    assert _load(tmp_path).members[0].where == "{{ ref('x', 'y') }}"


def _body(tmp_path: Path, body: str, files: dict[str, bytes] | None = None) -> tuple[list[str], ToolCatalog]:
    """Load procedure `lookup` in tools/ops/ whose `body:` is `body`, beside `files` under tools/ops/."""
    for name, content in (files or {}).items():
        path = tmp_path / "tools" / "ops" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    _write(
        tmp_path / "tools" / "ops" / "lookup.yml",
        f"tool:\n  name: lookup\n  type: procedure\n  language: sql\n  returns: NUMBER\n  body: {body}",
    )
    catalog = _load(tmp_path)
    return [item.code for item in catalog.diagnostics], catalog


def test_a_body_is_read_from_the_file_it_names_beside_the_tool_file(tmp_path: Path) -> None:
    codes, catalog = _body(tmp_path, "\"{{ file('sql/lookup.sql') }}\"", {"sql/lookup.sql": b"RETURN 1"})
    assert codes == []
    assert (catalog.members[0].body_file, catalog.members[0].body) == ("tools/ops/sql/lookup.sql", "RETURN 1")


@pytest.mark.parametrize(
    ("body", "files", "code"),
    [
        ("RETURN 1", None, "SST-REF050"),
        ("\"{{ file('a.sql') }}{{ file('b.sql') }}\"", None, "SST-REF003"),
        ("\"{{ ref('a') }}\"", None, "SST-REF041"),
        ("\"{{ file('a.sql'\"", None, "SST-LOD004"),
        ("[1]", None, "SST-PRS003"),
        ("\"{{ file('../../../secret.sql') }}\"", None, "SST-REF027"),
        ("\"{{ file('/etc/passwd') }}\"", None, "SST-REF027"),
        ("\"{{ file('missing.sql') }}\"", None, "SST-REF014"),
        ("\"{{ file('empty.sql') }}\"", {"empty.sql": b"  \n"}, "SST-LOD019"),
        ("\"{{ file('latin.sql') }}\"", {"latin.sql": b"\xff\xfe"}, "SST-LOD006"),
    ],
)
def test_a_body_that_names_no_readable_file_is_reported(
    tmp_path: Path, body: str, files: dict[str, bytes] | None, code: str
) -> None:
    codes, _ = _body(tmp_path, body, files)
    assert code in codes and "SST-PRT009" not in codes


def test_a_body_s_ref_calls_become_the_target_s_relations_and_its_inputs(tmp_path: Path) -> None:
    sql = b"SELECT body FROM {{ ref('docs') }}\n-- '{{ not a call }}' stays\n"
    codes, catalog = _body(tmp_path, "\"{{ file('lookup.sql') }}\"", {"lookup.sql": sql})
    member = catalog.members[0]
    assert codes == []
    assert member.body == "SELECT body FROM DB.S.DOCS\n-- '{{ not a call }}' stays\n"
    assert dict(member.body_relations) == {"docs": "DB.S.DOCS"}


def test_a_body_call_that_cannot_resolve_is_reported_in_the_body_file(tmp_path: Path) -> None:
    sql = b"SELECT 1\nFROM {{ ref('nope') }}\n"
    _, catalog = _body(tmp_path, "\"{{ file('lookup.sql') }}\"", {"lookup.sql": sql})
    (diagnostic,) = _codes(catalog, "SST-REF001")
    assert diagnostic.origin == Origin("tools/ops/lookup.sql", 2, 6)
    assert catalog.members[0].body_relations == {}


def test_a_body_file_that_cannot_be_read_is_reported(tmp_path: Path) -> None:
    locked = tmp_path / "tools" / "ops" / "locked.sql"
    locked.parent.mkdir(parents=True)
    locked.write_text("RETURN 1", encoding="utf-8")
    locked.chmod(0)
    try:
        codes, catalog = _body(tmp_path, "\"{{ file('locked.sql') }}\"")
    finally:
        locked.chmod(0o644)
    assert "SST-LOD018" in codes and catalog.members[0].body is None

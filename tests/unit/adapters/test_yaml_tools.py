from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.tool import ToolOwnership


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
                ),
            ),
        ),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_tool_loader_preserves_defined_and_referenced_ownership(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "platform.yml",
        """
tools:
  - group: platform
    define:
      - name: docs_search
        type: cortex_search_service
        on:
          table: "{{ ref('docs') }}"
          search_column: body
          attributes: [category]
        warehouse: WH
    reference:
      - name: lookup
        type: procedure
        relations:
          dev: DB.S.LOOKUP
          prod: PROD.S.LOOKUP
""".strip(),
    )
    catalog = load_tool_catalog(
        tmp_path,
        _dbt(),
        target_name="dev",
        declared_targets=frozenset(("dev", "prod")),
    )
    assert not catalog.diagnostics.has_errors
    assert [member.name for member in catalog.managed] == ["docs_search"]
    assert [member.name for member in catalog.references] == ["lookup"]
    assert catalog.managed[0].ownership is ToolOwnership.DEFINE
    assert catalog.managed[0].on_model == "docs"
    assert catalog.managed[0].search_column == "body"
    assert catalog.managed[0].attribute_columns == ("category",)
    assert catalog.managed[0].artifact_key == "tool:docs_search"
    assert catalog.references[0].artifact_key is None
    resolved, diagnostics = catalog.resolve("platform", "lookup")
    assert resolved is catalog.references[0] and diagnostics == ()
    resolved, diagnostics = catalog.resolve("lookup")
    assert resolved is catalog.references[0] and diagnostics == ()
    relation, diagnostics = catalog.relation(catalog.references[0])
    assert relation is not None and relation.sql == "DB.S.LOOKUP" and diagnostics == ()


def test_tool_loader_validates_structure_targets_models_columns_and_immutability(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "bad.yml",
        """
tools:
  - group: locked
    immutable: true
    define:
      - name: bad_search
        type: cortex_search_service
        on: "{{ ref('missing') }}"
        search_column: absent
        relations: {dev: DB.S.X}
    reference:
      - name: bad_ref
        type: procedure
        on: "{{ ref('docs') }}"
        relations:
          prod: ONLY.TWO
          invented: DB.S.X
""".strip(),
    )
    catalog = load_tool_catalog(
        tmp_path,
        _dbt(),
        target_name="dev",
        declared_targets=frozenset(("dev", "prod")),
    )
    codes = {diagnostic.code for diagnostic in catalog.diagnostics}
    assert {
        "SST-VAL606",
        "SST-VAL605",
        "SST-VAL608",
        "SST-REF018",
        "SST-REF019",
    }.issubset(codes)


def test_tool_loader_reports_duplicate_names_unknown_types_and_object_signature(tmp_path: Path) -> None:
    _write(
        tmp_path / "tools" / "duplicates.yml",
        """
tools:
  - group: one
    reference:
      - name: same
        type: mystery
        relations: {dev: DB.S.A}
      - name: same
        type: procedure
        signature:
          - {name: payload, type: object, required: true}
        relations: {dev: DB.S.B}
  - group: two
    reference:
      - name: same
        type: procedure
        relations: {dev: DB.S.C}
""".strip(),
    )
    catalog = load_tool_catalog(
        tmp_path,
        _dbt(),
        target_name="dev",
        declared_targets=frozenset(("dev",)),
    )
    codes = [diagnostic.code for diagnostic in catalog.diagnostics]
    assert "SST-VAL601" in codes
    assert "SST-VAL602" in codes
    assert "SST-VAL603" in codes
    assert "SST-PRS032" in codes
    resolved, diagnostics = catalog.resolve("same")
    assert resolved is None and diagnostics[0].code == "SST-REF010"

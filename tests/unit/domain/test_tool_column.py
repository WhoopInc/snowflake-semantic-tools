"""The columns a tool member names: refs of its `on:` model when defined, bare when referenced."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.ref_fields import ref_field
from snowflake_semantic_tools.domain.resolve.template import ResolveContext, resolve_scalar
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column

SITE = ColumnSite("docs_search", "search_column", "tool:docs_search", "tool:docs_search", Origin("tools/t.yml", 2, 3))


def _code(value: str, *, referenced: bool = False, on_model: str | None = "docs") -> str | None:
    column, problem = tool_column(value, SITE, referenced=referenced, on_model=on_model)
    assert (column is None) is (problem is not None)
    return None if problem is None else problem.code


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("{{ ref('docs', 'body') }}", None),
        ("  {{ ref('docs', 'body') }}  ", None),
        ("body", "SST-REF050"),
        ("{{ ref('docs', 'body') }} {{ ref('docs', 'title') }}", "SST-REF003"),
        ("x {{ ref('docs', 'body') }}", "SST-REF003"),
        ("{{ var('body') }}", "SST-REF041"),
        ("{{ table('docs') }}", "SST-REF034"),
        ("{{ ref('docs') }}", "SST-REF015"),
        ("{{ ref('notes', 'body') }}", "SST-VAL622"),
        ("{{ ref('docs', 'body')", "SST-LOD004"),
    ],
)
def test_a_defined_member_s_column_is_one_ref_of_its_on_model(value: str, code: str | None) -> None:
    assert _code(value) == code


def test_a_defined_member_with_no_on_model_takes_a_ref_of_any_model() -> None:
    assert tool_column("{{ ref('notes', 'body') }}", SITE, referenced=False, on_model=None) == ("body", None)


def test_a_referenced_member_s_column_is_bare_and_takes_no_call() -> None:
    assert tool_column("body", SITE, referenced=True, on_model=None) == ("body", None)
    assert _code("{{ ref('docs', 'body') }}", referenced=True) == "SST-REF008"


def test_a_diagnostic_without_an_origin_names_the_template_and_the_site() -> None:
    site = ColumnSite("docs_search", "id_column", "tool:docs_search", "tool:docs_search", None)
    _, problem = tool_column("{{ ref('docs', 'id')", site, referenced=False, on_model="docs")
    assert problem is not None and problem.origin is not None and problem.origin.file == "<template>"


def test_a_column_ref_resolves_through_the_callback_a_field_gives() -> None:
    model = DbtModel("model.p.docs", "docs", "DB.S.DOCS", (), (), (DbtColumn("category", None, "TEXT", None),))
    context = ResolveContext(DbtCatalog("v12", None, None, (model,)))

    def bare(call: TemplateCall, column: str) -> str | Diagnostic:
        return column.upper()

    resolved, diagnostics = resolve_scalar(
        "{{ ref('docs', 'category') }} = 'x'",
        ref_field("tool.where"),
        Origin("tools/t.yml"),
        context,
        field="tool.where",
        column_value=bare,
    )
    assert (resolved.text, diagnostics) == ("CATEGORY = 'x'", ())

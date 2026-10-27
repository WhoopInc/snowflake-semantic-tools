"""SST-REF001: a one-argument `ref()` names a model the dbt manifest does not list."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.reference_project import reported, view_names, with_view
from tests.helpers.resolve_builders import resolved

VIEW = """  - name: v
    description: |-
      Use this view for questions about orders.
    tables:
{tables}
{extra}
"""
BAD_EXCLUDE = "    exclude_columns:\n      - \"{{ ref('customers', 'no_col') }}\""


def _view(tmp_path: Path, tables: tuple[str, ...], extra: str = "") -> Path:
    rows = "\n".join(f"      - \"{{{{ ref('{table}') }}}}\"" for table in tables)
    return with_view(tmp_path, VIEW.format(tables=rows, extra=extra))


def test_sst_ref001_fires() -> None:
    value, (diagnostic,) = resolved("{{ ref('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF001", Severity.ERROR)
    assert diagnostic.message == "{{ ref('missing') }} is not a model, snapshot or seed in the dbt manifest"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref001_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned


def test_sst_ref001_fires_for_a_view_table_beside_another_error_of_the_view(tmp_path: Path) -> None:
    # SST-VAL330 poisons the view before its build, which was the only place a table was checked.
    project = _view(tmp_path, ("customers", "nope"), BAD_EXCLUDE)
    [unknown] = reported(project, "SST-REF001")
    assert unknown.message == "{{ ref('nope') }} is not a model, snapshot or seed in the dbt manifest"
    assert unknown.subject == "semantic_view:v"
    assert unknown.origin is not None and unknown.origin.line is not None
    assert [item.subject for item in reported(project, "SST-VAL330")] == ["semantic_view:v"]
    assert "V" not in view_names(project)


def test_sst_ref001_fires_once_per_unknown_view_table(tmp_path: Path) -> None:
    assert len(reported(_view(tmp_path / "one", ("customers", "nope")), "SST-REF001")) == 1
    found = reported(_view(tmp_path / "two", ("nope", "customers", "gone")), "SST-REF001")
    assert [item.context["model"] for item in found] == ["nope", "gone"]
    assert all(item.severity is Severity.ERROR for item in found)


def test_sst_ref001_silent_for_known_view_tables(tmp_path: Path) -> None:
    project = _view(tmp_path, ("customers",))
    assert reported(project, "SST-REF001") == []
    assert "V" in view_names(project)


def test_sst_ref001_is_reported_at_the_tables_entry_not_at_a_word_of_the_description(tmp_path: Path) -> None:
    # GitHub #270: the finding was moved onto the description, which also names the model.
    view = VIEW.replace("questions about orders.", "questions about ghostmodel orders.")
    rows = "      - \"{{ ref('orders') }}\"\n      - \"{{ ref('ghostmodel') }}\""
    project = with_view(tmp_path, view.format(tables=rows, extra=""))
    [diagnostic] = [item for item in reported(project, "SST-REF001") if item.context["model"] == "ghostmodel"]
    assert diagnostic.origin is not None
    assert (diagnostic.origin.file, diagnostic.origin.line) == (
        "semantic_models/semantic_views/extra/semantic_views.yml",
        7,
    )

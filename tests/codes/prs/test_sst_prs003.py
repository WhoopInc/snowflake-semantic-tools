"""SST-PRS003: a field holds a value of the wrong type, here a metric's `using_relationships`.

A quoted `"false"` where a boolean is documented is text, which Python reads as true; the
regression cases show it is refused, on the dbt side and in SST's own YAML, rather than read.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.semantic_view import render, render_verified_query
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, edited_manifest, load, project_copy, set_column_meta
from tests.helpers.seam_projects import COUNTED, METRICS, SmallProject, metric_file


def test_sst_prs003_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(
            tmp_path,
            files={
                METRICS: "snowflake_metrics:\n  - name: total\n    description: T.\n"
                + COUNTED
                + "    using_relationships: x\n"
            },
        )
        .load()
        .diagnostics,
        "SST-PRS003",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: 'using_relationships' expects a list of strings, found str"
    assert diagnostic.subject == "metric:total"


def test_sst_prs003_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=metric_file(COUNTED)).load().diagnostics, "SST-PRS003") == []


def _customer_type_lines(project: Path, manifest: Path) -> tuple[list[str], list[str]]:
    """The PRS003 messages loading reports, and every rendered line naming `CUSTOMER_TYPE`."""
    loaded = load(project, manifest)
    lines = [line for view in loaded.views for line in str(render(view)).splitlines() if "CUSTOMER_TYPE AS" in line]
    return [item.message for item in loaded.diagnostics if item.code == "SST-PRS003"], lines


@pytest.mark.parametrize(
    ("meta", "field", "expected", "found", "enum"),
    [
        ({"is_enum": "false"}, "is_enum", "boolean", "str", False),
        ({"exclude": "false"}, "exclude", "boolean", "str", True),
        ({"access_modifier": False}, "access_modifier", "a string", "bool", True),
    ],
)
def test_a_dbt_column_meta_value_of_the_wrong_type_is_refused_not_read_as_true(
    tmp_path: Path, meta: dict[str, object], field: str, expected: str, found: str, enum: bool
) -> None:
    manifest = edited_manifest(
        tmp_path, lambda document: set_column_meta(document, "customers", "customer_type", **meta)
    )
    messages, lines = _customer_type_lines(project_copy(tmp_path), manifest)
    subject = "dbt_column:customers.customer_type"
    assert messages == [f"{subject}: 'meta.sst.{field}' expects {expected}, found {found}"]
    # Read as unset: a quoted is_enum is no enum, and a quoted exclude keeps the column in its views.
    assert lines
    assert all(("IS_ENUM" in line) is enum for line in lines)


def test_a_quoted_onboarding_flag_is_refused_and_does_not_publish_the_question(tmp_path: Path) -> None:
    project = edited(
        tmp_path,
        "semantic_models/verified_queries/revenue_by_location.yml",
        "use_as_onboarding_question: true",
        'use_as_onboarding_question: "false"',
    )
    loaded = load(project)
    assert [item.message for item in loaded.diagnostics if item.code == "SST-PRS003"] == [
        "verified_query:revenue_by_location: 'use_as_onboarding_question' expects boolean, found str"
    ]
    queries = [query for view in loaded.views for query in view.verified_queries if query.name == "REVENUE_BY_LOCATION"]
    assert queries
    assert all("ONBOARDING_QUESTION TRUE" not in str(render_verified_query(query)) for query in queries)


@pytest.mark.parametrize(("value", "found"), [("no", "bool"), ("[a]", "list"), ("'no'", None), ("3", None)])
def test_an_agent_tag_value_that_is_not_text_or_a_number_is_refused_not_published(
    tmp_path: Path, value: str, found: str | None
) -> None:
    project = edited(
        tmp_path,
        "agents/jaffle_analytics/agent.yml",
        "config:\n",
        f"config:\n  tags:\n    - name: \"{{{{ tag('cost_center') }}}}\"\n      value: {value}\n",
    )
    ddl = tmp_path / "ddl"
    exit_code, diagnostics = invoke_json(["compile", *common(project), "--emit-ddl", str(ddl)])
    messages = [item["message"] for item in diagnostics if item["code"] == "SST-PRS003"]
    if found is None:
        assert (exit_code, messages) == (0, [])
        return
    # An unquoted `no` is YAML's false, which published as text would set the tag to 'False'.
    assert exit_code == 1
    assert messages == [f"agent:jaffle_analytics_agent: 'config.tags[0].value' expects a string, found {found}"]
    assert not ddl.exists() or "'False'" not in "".join(path.read_text() for path in ddl.rglob("*.*"))

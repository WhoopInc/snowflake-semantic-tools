"""The semantic load leaves out what a diagnostic proves broken, reading the diagnostics in phase order."""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.adapters.yaml.semantic.poison import Poison
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.projects import load_project

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def _metric(name: str) -> MetricDef:
    return MetricDef(name=name, expr="COUNT(*)", description=None, synonyms=())


def _findings(diagnostics: Iterable[Diagnostic]) -> list[tuple[str, str | None]]:
    """The code and subject of each diagnostic that is not INFO, which only reports what attached where."""
    return [(item.code, item.subject) for item in diagnostics if item.severity is not Severity.INFO]


def _reference_copy(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target", "logs"))
    return project


def test_healthy_metrics_keep_order_and_compare_subjects_as_reported() -> None:
    metrics = (_metric("a"), _metric("Cycle"), _metric("B"), _metric("errored"))
    poison = Poison(metric_names=frozenset({"cycle"}), metric_subjects=frozenset({"metric:errored", "metric:b"}))
    # Names compare casefolded; subjects compare exactly as the metric checks reported them.
    assert [metric.name for metric in poison.healthy_metrics(metrics)] == ["a", "B"]


def test_with_members_grows_a_copy() -> None:
    poison = Poison(member_keys=frozenset({"metric:a"}))
    assert poison.with_members({"filter:b"}).member_keys == {"metric:a", "filter:b"}
    assert poison.member_keys == {"metric:a"}


def test_an_error_from_the_last_check_phase_still_keeps_its_view_unbuilt(tmp_path: Path) -> None:
    project = _reference_copy(tmp_path)
    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    text = views.read_text(encoding="utf-8")
    views.write_text(text.replace("'jaffle_question_scope'", "'no_such_instruction'"), encoding="utf-8")
    loaded = load_project(project, manifest_path=MANIFEST)
    # No view names jaffle_question_scope any more, so it is reported as referenced by nothing.
    assert _findings(loaded.diagnostics) == [
        ("SST-REF007", "semantic_view:jaffle_sales"),
        ("SST-VAL007", "custom_instruction:jaffle_question_scope"),
    ]
    assert sorted(view.fqn for view in loaded.views) == [
        "SST_REF_DEV.CORE.JAFFLE_MENU",
        "SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL",
    ]


def test_a_poisoned_metric_leaves_out_what_is_built_on_it_and_nothing_else(tmp_path: Path) -> None:
    project = _reference_copy(tmp_path)
    (project / "semantic_models" / "metrics" / "broken.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: broken_revenue\n"
        "    tables: [orders]\n"
        "    description: Names a column orders does not have.\n"
        "    expr: \"SUM({{ ref('orders', 'no_such_column') }})\"\n"
        "  - name: broken_revenue_per_order\n"
        "    derived: true\n"
        "    description: Built on the broken metric.\n"
        "    expr: \"DIV0({{ metric('broken_revenue') }}, {{ metric('order_count') }})\"\n",
        encoding="utf-8",
    )
    loaded = load_project(project, manifest_path=MANIFEST)
    assert _findings(loaded.diagnostics) == [("SST-REF002", "metric:broken_revenue")]
    assert len(loaded.views) == 3
    for view in loaded.views:
        names = {metric.name for metric in view.metrics}
        assert not names & {"BROKEN_REVENUE", "BROKEN_REVENUE_PER_ORDER"}
    sales = next(view for view in loaded.views if view.fqn == "SST_REF_DEV.JAFFLE.JAFFLE_SALES")
    assert {"ORDER_COUNT", "REVENUE_PER_ORDER"} <= {metric.name for metric in sales.metrics}

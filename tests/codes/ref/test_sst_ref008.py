"""SST-REF008: a template expression in a description, which is published as written."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, edited_fixture

FILE = "semantic_models/metrics/metrics.yml"
BEFORE = "      Total order value including tax, in cents."


def test_sst_ref008_fires(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, "      Total order value including tax, from {{ ref('orders') }}.")
    [diagnostic] = coded(project.diagnostics, "SST-REF008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total_revenue: 'description' does not accept template expressions"
    assert diagnostic.subject == "metric:total_revenue"


def test_sst_ref008_silent(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, "      Total order value including tax, from the orders model.")
    assert coded(project.diagnostics, "SST-REF008") == []

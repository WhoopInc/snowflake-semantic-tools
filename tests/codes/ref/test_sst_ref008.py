"""SST-REF008: a template expression in a description, which is published as written."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import common, validate_json
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load, member_file

FILE = member_file("metrics", "total_revenue")
BEFORE = "    Total order value including tax, in cents."


def test_sst_ref008_fires(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "    Total order value including tax, from {{ ref('orders') }}."))
    [diagnostic] = coded(project.diagnostics, "SST-REF008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total_revenue: 'description' does not accept template expressions"
    assert diagnostic.subject == "metric:total_revenue"


def test_sst_ref008_silent(tmp_path: Path) -> None:
    project = load(edited(tmp_path, FILE, BEFORE, "    Total order value including tax, from the orders model."))
    assert coded(project.diagnostics, "SST-REF008") == []


AGENT = "agents/jaffle_analytics/agent.yml"


@pytest.mark.parametrize(
    ("before", "after", "field"),
    [
        (
            "      - question: How many orders are open across all locations?",
            "      - question: \"{{ var('nope') }} revenue by month in 2025?\"",
            "spec.instructions.sample_questions[0].question",
        ),
        ("comment: |-\n  Covers order", "comment: |-\n  {{ ref('orders') }} covers order", "comment"),
        (
            "      description: |-\n        Customers and the orders",
            "      description: |-\n        Customers and the {{ x }} orders",
            "spec.tools[0].description",
        ),
        ("  display_name: Jaffle Analytics", "  display_name: \"{{ var('name') }}\"", "profile.display_name"),
    ],
)
def test_sst_ref008_fires_for_a_field_an_agent_publishes_as_written(
    tmp_path: Path, before: str, after: str, field: str
) -> None:
    project = edited(tmp_path, AGENT, before, after)
    specs = tmp_path / "specs"
    result = CliRunner().invoke(cli, ["compile", *common(project), "--emit-agent-spec", str(specs), "-o", "json"])
    [diagnostic] = [item for item in json.loads(result.output)["diagnostics"] if item["code"] == "SST-REF008"]
    assert diagnostic["message"] == f"agent:jaffle_analytics_agent: '{field}' does not accept template expressions"
    assert result.exit_code == 1
    # Refused, so the template is never published as the agent's text.
    assert not (specs / "jaffle_analytics_agent.json").exists()


def test_sst_ref008_fires_for_a_tool_members_description(tmp_path: Path) -> None:
    before = "    Full-text search over menu item DESCRIPTIONS"
    project = edited(tmp_path, "tools/platform/menu_docs_search.yml", before, "    Search {{ ref('menu') }}")
    code, diagnostics = validate_json(project)
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-REF008"]
    assert diagnostic["message"] == "tool:menu_docs_search: 'description' does not accept template expressions"
    assert code == 1

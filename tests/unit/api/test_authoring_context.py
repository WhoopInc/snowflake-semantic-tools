"""The names an editor offers for a call are the project's, as the session read it."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.api import ProjectSession, Variable
from tests.helpers.reference_project import DBT_MANIFEST, project_copy


@pytest.fixture
def session(tmp_path: Path) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST)


def test_every_dbt_model_is_offered_with_its_relation_and_columns(session: ProjectSession) -> None:
    context = session.authoring_context()

    names = [model.name for model in context.models]
    assert names == sorted(names) and "customers" in names
    customers = next(model for model in context.models if model.name == "customers")
    assert customers.relation.endswith("CUSTOMERS") and customers.columns
    assert all(column.name for column in customers.columns)


def test_the_variables_are_the_projects_then_the_one_sst_supplies(session: ProjectSession) -> None:
    variables = session.authoring_context().variables

    assert variables[-1] == Variable("sha_version", None, supplied=True)
    assert Variable("completed_state", "completed") in variables
    assert not any(item.supplied for item in variables[:-1])


def test_the_tags_are_the_ones_tags_declares(session: ProjectSession) -> None:
    assert session.authoring_context().tags == ("cost_center", "data_domain", "metric_tier")


def test_the_fields_follow_the_configuration_the_session_reads(session: ProjectSession) -> None:
    config = (session.project_dir / "sst_config.yml").read_text()
    edited = session.with_overlay(
        {"sst_config.yml": config.replace("snowflake:\n", "snowflake:\n  tool_types: [future_tool]\n", 1).encode()}
    )

    before = {item.path: item for item in session.authoring_context().fields}
    after = {item.path: item for item in edited.authoring_context().fields}
    assert "future_tool" not in before["agent.spec.tools.type"].choices
    assert "future_tool" in after["agent.spec.tools.type"].choices
    context = session.authoring_context()
    assert (context.setting_functions, context.target_keys) == (("var",), ("database", "schema", "warehouse"))
    assert {item.name for item in context.functions} >= {"ref", "semantic_view", "var"}

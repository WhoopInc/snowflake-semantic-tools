"""Render, import, render: every agent and CoWork profile of the reference project comes back byte for byte.

Each object is compiled, its payload staged as the live object's committed version, its files
removed, and `sst import` run over it against the rest of the project; the files it writes are
compiled again, and the payload must be the one the project compiled first. A key the import
cannot write, or writes otherwise, shows here as a difference, so the import is lossless for
everything SST renders. The project is run as committed, and with passthrough at every level
an import keeps unmodelled keys in.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent
from snowflake_semantic_tools.app.compile.cowork_profiles.compiled import CompiledCoworkProfile
from snowflake_semantic_tools.app.compile.extensions import consumed_extensions
from snowflake_semantic_tools.app.importing import ImportSetup, artifact_directories, project_index
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.cli.wiring.importing import write_imported
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from tests.helpers.import_fakes import FakeImportPort, run
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

TARGET = TargetIdentity(
    "dev", "ACCT", Identifier.parse("SST_REF_DEV"), Identifier.parse("JAFFLE"), "ANALYST", "SST_REF_WH"
)
# Passthrough at each level an import keeps unmodelled keys in: the spec's top level, inside
# three sections SST renders, and a search tool's tool_spec and resources.
_SPEC_PASSTHROUGH = {
    "thinking_effort": "high",
    "models": {"fallback": "claude-4-sonnet"},
    "instructions": {"system": "Be terse."},
    "orchestration": {"budget": {"steps": 3}},
}


def _with_passthrough(project: Path) -> None:
    path = project / "agents" / "jaffle_analytics" / "agent.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["spec"]["passthrough"] = _SPEC_PASSTHROUGH
    [search] = [tool for tool in document["spec"]["tools"] if tool.get("type") == "cortex_search"]
    search["passthrough"] = {"experimental_rerank": True}
    search["tool_spec_passthrough"] = {"x_hint": "fast"}
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _staged(compiled: CompiledAgent | CompiledCoworkProfile) -> FakeImportPort:
    """A Snowflake serving `compiled` as one committed version, with its comment, profile and alias."""
    model: Any = compiled.resolved.model
    profile: AgentProfile = model.profile if isinstance(compiled, CompiledAgent) else model.display
    database, schema, name = compiled.target.folded
    port = FakeImportPort()
    port.show(ShowRow(name, database, schema, "OWNER", "now", model.comment, object_type="AGENT"))
    shown = {key: value for key in ("display_name", "avatar", "color") if (value := getattr(profile, key))}
    port.show_rows[f"AGENT {compiled.target.sql}"] = {
        "name": name,
        "comment": model.comment,
        "profile": json.dumps(shown),
        "is_secure": "false",
    }
    port.agent_version_rows[compiled.target.sql] = (
        AgentVersion("VERSION$1", model.alias, True, None, compiled.payload),
    )
    return port


@pytest.mark.parametrize("passthrough", [False, True], ids=["as_committed", "with_passthrough"])
def test_each_agent_and_profile_imports_back_to_the_payload_it_compiled_to(tmp_path: Path, passthrough: bool) -> None:
    project = project_copy(tmp_path)
    if passthrough:
        _with_passthrough(project)
    paths = project_paths(project)
    first = compile_result(paths, "dev", DBT_MANIFEST)
    assert not first.diagnostics.has_errors
    objects = [item for item in first.compiled if isinstance(item, (CompiledAgent, CompiledCoworkProfile))]
    assert sorted(item.name for item in objects) == [
        "jaffle_analytics_agent",
        "jaffle_concierge",
        "jaffle_delivery_agent",
        "jaffle_minimal_agent",
    ]
    inputs = project_inputs(paths, "dev", DBT_MANIFEST)
    config = resolved_config(paths, "dev").tree
    extensions, _ = consumed_extensions(config, TARGET)
    for compiled in objects:
        # The rest of the project, as the import maps against it; this object is not yet in it.
        rest = dataclasses.replace(first, compiled=tuple(item for item in first.compiled if item is not compiled))
        index = project_index(
            rest, inputs.dbt_catalog(), inputs.tool_catalog(), "dev", artifact_directories(config), extensions
        )
        for source in compiled.source_files:
            (project / source).unlink()
        setup = ImportSetup(index, config, TARGET, "ANALYST", lambda path: (project / path).exists())
        report = run(_staged(compiled), compiled.artifact_type, compiled.target.sql, setup)
        [imported] = report.objects
        assert [item.message for item in (*report.diagnostics, *imported.diagnostics)] == []
        write_imported(project, imported.files)
    again = compile_result(paths, "dev", DBT_MANIFEST)
    assert not again.diagnostics.has_errors
    payloads = {
        item.target: item.payload for item in again.compiled if isinstance(item, (CompiledAgent, CompiledCoworkProfile))
    }
    for compiled in objects:
        assert json.loads(payloads[compiled.target]) == json.loads(compiled.payload), compiled.name
        assert payloads[compiled.target] == compiled.payload, compiled.name
    if passthrough:
        # The keys did reach the payload, so the comparison above covers them.
        [analytics] = [json.loads(item.payload) for item in objects if item.name == "jaffle_analytics_agent"]
        assert analytics["thinking_effort"] == "high" and analytics["instructions"]["system"] == "Be terse."
        assert analytics["models"]["fallback"] == "claude-4-sonnet"
        assert analytics["orchestration"]["budget"]["steps"] == 3
        assert [entry["tool_spec"].get("x_hint") for entry in analytics["tools"]].count("fast") == 1
        resources = analytics["tool_resources"].values()
        assert [resource.get("experimental_rerank") for resource in resources].count(True) == 1

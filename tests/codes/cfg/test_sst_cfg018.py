"""SST-CFG018: a declared tool member is referenced by nothing: no agent tool and no config value."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentTool
from snowflake_semantic_tools.domain.validate.config import config_tool_calls, unreferenced_tool_members
from tests.helpers.agent_context import docs_catalog
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_cfg018_fires() -> None:
    [diagnostic] = unreferenced_tool_members(docs_catalog(), ["other"])
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG018", Severity.WARNING)
    assert diagnostic.message == "tool member 'docs' is referenced by nothing"
    assert (diagnostic.subject, diagnostic.origin) == ("tool:docs", Origin("tools/p.yml", 1))


def test_sst_cfg018_silent() -> None:
    assert unreferenced_tool_members(docs_catalog(), ["Docs"]) == ()


def test_every_way_an_agent_or_the_config_names_a_member_counts() -> None:
    origin = Origin("agents/a.yml")
    by_backing = AgentTool("cortex_search", origin, name="search", backing="docs")
    by_name = AgentTool("agent_toolset", origin, name="docs")
    delegating = AgentTool("agent_toolset", origin, name="docs", agent_ref="other_agent")
    assert (by_backing.member_reference, by_name.member_reference, delegating.member_reference) == (
        "docs",
        "docs",
        None,
    )
    assert unreferenced_tool_members(docs_catalog(), [str(by_name.member_reference)]) == ()
    config = {"agents": {"+note": "{{ tool('docs') }}"}}
    assert config_tool_calls(config) == ("docs",)
    assert unreferenced_tool_members(docs_catalog(), config_tool_calls(config)) == ()


def test_sst_cfg018_silent_for_a_reference_member_an_agent_toolset_names(tmp_path: Path) -> None:
    # The reference project's delivery agent declares a `type: agent_toolset` tool by the member's name.
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli, ["validate", *common(project), "--no-snowflake-syntax-check", "--output", "json"], catch_exceptions=False
    )
    unused = [
        item["params"]["name"] for item in json.loads(result.stdout)["diagnostics"] if item["code"] == "SST-CFG018"
    ]
    assert "partner_delivery_agent" not in unused
    assert unused

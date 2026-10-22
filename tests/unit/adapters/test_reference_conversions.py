"""Each reference field converted to a template: the bare form is an error, the template resolves."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.project_source import _file_checksums
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.coco_desktop_profiles import load_coco_desktop_profile_catalog
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml, load_documents
from snowflake_semantic_tools.adapters.yaml.evals.config import parse_config
from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.semantic.nodes import _table_refs
from snowflake_semantic_tools.app.compile.agents.settled import settled_agent
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.coco_desktop_profile import CocoDesktopProfileCatalog
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.render.coco_desktop_profile import render_command
from snowflake_semantic_tools.domain.resolve.tags import declared_tags
from snowflake_semantic_tools.domain.validate.coco_desktop_profile import validate_coco_desktop_profile_catalog
from snowflake_semantic_tools.domain.validate.semantic.authored_keys import authored_key_diagnostics
from tests.helpers.compile_builders import skill as built_skill
from tests.helpers.projects import project_paths

TARGET = TargetIdentity("dev", "acct", Identifier.parse("DEV_DB"), Identifier.parse("SALES"))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _agent(tmp_path: Path, body: str) -> tuple[list[tuple[str, str | None]], AgentModel]:
    _write(tmp_path / "agents/a/agent.yml", f"name: a\n{body}")
    agents, diagnostics = load_agents(tmp_path)
    return [(item.code, item.context.get("field")) for item in diagnostics], agents[0]


@pytest.mark.parametrize(
    ("tool", "field", "function"),
    [
        ("{type: cortex_analyst_text_to_sql, semantic_view: sales}", "tools[0].semantic_view", "semantic_view"),
        ("{type: cortex_search, name: s, search_service: docs}", "tools[0].search_service", "tool"),
        ("{type: generic, name: g, identifier: lookup}", "tools[0].identifier", "tool"),
        ("{type: agent_toolset, name: d, identifier: partner}", "tools[0].identifier", "tool"),
        ("{type: agent_toolset, agent: other}", "tools[0].agent", "agent"),
    ],
)
def test_a_bare_agent_tool_reference_is_refused_naming_its_function(
    tmp_path: Path, tool: str, field: str, function: str
) -> None:
    found, _ = _agent(tmp_path, f"spec:\n  tools:\n    - {tool}\n")
    assert found == [("SST-REF050", field)]
    _write(tmp_path / "agents/a/agent.yml", "name: a\nspec:\n  tools:\n    - " + tool + "\n")
    _, diagnostics = load_agents(tmp_path)
    assert f"{{{{ {function}(...) }}}}" in diagnostics[0].message


def test_an_agent_toolset_names_its_member_with_identifier(tmp_path: Path) -> None:
    found, agent = _agent(
        tmp_path,
        "spec:\n  tools:\n    - type: agent_toolset\n      name: partner\n"
        "      identifier: \"{{ tool('partner_agent') }}\"\n",
    )
    assert found == []
    [tool] = agent.tools
    assert (tool.name, tool.backing, tool.member_reference) == ("partner", "partner_agent", "partner_agent")


def test_a_bare_name_on_an_agent_toolset_names_no_member(tmp_path: Path) -> None:
    _, agent = _agent(tmp_path, "spec:\n  tools:\n    - {type: agent_toolset, name: partner_agent}\n")
    assert agent.tools[0].member_reference is None


@pytest.mark.parametrize(
    ("skill", "found"),
    [
        ("{source: {type: project, path: sales}}", [("SST-REF050", "spec.skills[0].source.path")]),
        (
            "{name: close, source: {type: project, path: \"{{ plugin('kit') }}\"}}",
            [("SST-REF050", "spec.skills[0].name")],
        ),
        (
            "{source: {type: project, path: \"{{ skill('sales') }}\", version: \"{{ ref('v') }}\"}}",
            [("SST-REF041", "spec.skills[0].source.version")],
        ),
        ("{name: \"{{ skill('close') }}\", source: {type: project, path: \"{{ plugin('kit') }}\"}}", []),
        ("{source: {type: project, path: \"{{ skill('sales') }}\", version: \"{{ var('v') }}\"}}", []),
    ],
)
def test_an_agent_skill_names_its_skill_plugin_member_and_version_with_calls(
    tmp_path: Path, skill: str, found: list[tuple[str, str]]
) -> None:
    assert _agent(tmp_path, f"spec:\n  skills:\n    - {skill}\n")[0] == found


def test_a_plugin_member_is_kept_as_its_skill_name(tmp_path: Path) -> None:
    _, agent = _agent(
        tmp_path,
        "spec:\n  skills:\n    - name: \"{{ skill('close') }}\"\n"
        "      source: {type: project, path: \"{{ plugin('kit') }}\"}\n",
    )
    [skill] = agent.skills
    assert (skill.name, skill.path, skill.ref) == ("close", "kit", "plugin")


def test_an_agent_tag_names_a_declared_tag_by_its_tag_call(tmp_path: Path) -> None:
    _, loaded = _agent(
        tmp_path,
        "config:\n  tags:\n    - {name: \"{{ tag('cost_center') }}\", value: sales}\n"
        "    - {name: cost_center, value: bare}\n    - {name: \"{{ tag('ghost') }}\", value: gone}\n",
    )
    tags = declared_tags({"cost_center": {"fqn": "GOV.TAGS.COST_CENTER"}}, TARGET)
    model, problems = settled_agent(loaded, TARGET, tags)
    assert model.tags == (("GOV.TAGS.COST_CENTER", "sales"),)
    assert [(item.code, item.context.get("field")) for item in problems] == [
        ("SST-REF050", "config.tags[1].name"),
        ("SST-REF028", None),
    ]
    # Without a tags: block, no name resolves.
    _, none = settled_agent(loaded, TARGET)
    assert [item.code for item in none] == ["SST-REF028", "SST-REF050", "SST-REF028"]


@pytest.mark.parametrize("parse", [parse_dataset, parse_config], ids=["dataset", "config"])
def test_an_eval_file_names_no_agent(parse: Callable[[tuple[str, ParsedYaml], list[Diagnostic]], object]) -> None:
    text = "agent: sales\nname: n\nquestions: []\nmetrics: {system: []}\n"
    diagnostics: list[Diagnostic] = []
    parse(("agents/sales/evals/f.yml", parse_yaml_bytes(text.encode(), "agents/sales/evals/f.yml")), diagnostics)
    refused = [item for item in diagnostics if item.code == "SST-PRS126"]
    assert [(item.context["field"], item.origin.line if item.origin else None) for item in refused] == [("agent", 1)]


def _commands(tmp_path: Path, frontmatter: str) -> tuple[list[str], CocoDesktopProfileCatalog]:
    _write(tmp_path / "commands/sql/check.md", f"---\n{frontmatter}\n---\nCheck it.\n")
    catalog = load_coco_desktop_profile_catalog(
        tmp_path,
        coco_desktop_profiles_dir="coco_desktop_profiles",
        hooks_dir="hooks",
        mcp_servers_dir="mcp-servers",
        commands_dir="commands",
    )
    return [item.code for item in catalog.diagnostics], catalog


def test_a_command_skill_is_one_skill_call_rendered_to_its_name_on_publish(tmp_path: Path) -> None:
    codes, catalog = _commands(tmp_path, "skill: \"{{ skill('sql-author') }}\"")
    [command] = catalog.commands
    assert codes == [] and command.skill == "sql-author"
    assert render_command(command) == b"---\nskill: sql-author\n---\nCheck it.\n"
    known = {"sql-author": built_skill("sql-author")}
    assert [item.code for item in validate_coco_desktop_profile_catalog(catalog, known)] == []
    assert [item.code for item in validate_coco_desktop_profile_catalog(catalog, {})] == ["SST-REF032"]


@pytest.mark.parametrize(
    ("value", "code"), [("sql-author", "SST-REF050"), ("\"{{ plugin('sql-author') }}\"", "SST-REF041")]
)
def test_a_command_skill_that_is_not_one_skill_call_is_refused(tmp_path: Path, value: str, code: str) -> None:
    codes, catalog = _commands(tmp_path, f"skill: {value}")
    assert codes == [code] and catalog.commands[0].skill is None


def test_the_command_folder_joins_the_checksums(tmp_path: Path) -> None:
    _write(tmp_path / "commands/sql/check.md", "Check it.\n")
    assert "commands/sql/check.md" in _file_checksums(project_paths(tmp_path), {})


def test_a_non_additive_entry_names_its_column_by_ref_and_the_old_keys_are_errors(tmp_path: Path) -> None:
    _write(
        tmp_path / "semantic_models/metrics/m.yml",
        "metric:\n  name: m\n  expr: \"SUM({{ ref('s', 'cost') }})\"\n"
        "  non_additive_dimensions:\n    - {dimension: month, table: s}\n",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    assert sorted((item.code, item.context.get("field")) for item in authored_key_diagnostics(documents)) == [
        ("SST-PRS126", "non_additive_dimensions[0].dimension"),
        ("SST-PRS126", "non_additive_dimensions[0].table"),
    ]


def test_the_attachment_reader_takes_only_the_table_calls_the_build_takes() -> None:
    written = ["{{ ref('Orders') }}", "{{ source('raw', 'Events') }}", "customers", "{{ table('items') }}"]
    assert _table_refs(written) == ("orders", "raw.events")

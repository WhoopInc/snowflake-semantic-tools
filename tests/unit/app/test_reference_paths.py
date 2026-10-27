"""Stage paths, routine imports and agent tags compile from the template calls that name them."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.app.compile.agents.settled import settled_agent
from snowflake_semantic_tools.app.compile.tools import CompiledTool, CompileTools, ToolPlacement
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.resolve.tags import declared_tags
from tests.helpers.seam_projects import agent_context

ORIGIN = Origin("agents/a/agent.yml", 6)


def _member(name: str, kind: str, *, relations: dict[str, str] | None = None) -> ToolMember:
    ownership = ToolOwnership.REFERENCE if relations is not None else ToolOwnership.DEFINE
    return ToolMember(
        name,
        kind,
        ownership,
        Origin("tools/t.yml"),
        "tools/t.yml",
        description="Docs.",
        relations=MappingProxyType(relations or {}),
    )


SEARCH = _member("docs", "cortex_search_service", relations={"dev": "DB.S.DOCS"})
CATALOG = ToolCatalog(
    (
        SEARCH,
        _member("files", "stage"),
        _member("partner_files", "stage", relations={"dev": "PARTNER.SHARED.FILES"}),
        _member("lookup", "procedure", relations={"dev": "DB.S.LOOKUP"}),
    ),
    "dev",
    frozenset(("dev",)),
)


def _search(stage_path: str) -> AgentTool:
    return AgentTool(
        "cortex_search",
        ORIGIN,
        name="docs",
        backing="docs",
        description="Search docs.",
        stage_path=stage_path,
        relative_path_column="PATH",
    )


@pytest.mark.parametrize(
    ("stage_path", "rendered"),
    [
        ("{{ tool('files') }}/docs/", "@DB.S.FILES/docs/"),
        ("{{ tool('partner_files') }}/in/", "@PARTNER.SHARED.FILES/in/"),
        ("@EXT.SHARED.DROPS/in/", "@EXT.SHARED.DROPS/in/"),
    ],
)
def test_a_search_tool_stage_path_renders_where_its_stage_member_is(stage_path: str, rendered: str) -> None:
    resolved, diagnostics = resolve_tool(AgentModel("a", ORIGIN, ()), _search(stage_path), agent_context(CATALOG))
    assert diagnostics == ()
    assert resolved is not None and resolved.resources["stage_path"] == rendered


@pytest.mark.parametrize(
    ("stage_path", "code"), [("{{ tool('lookup') }}/x", "SST-REF020"), ("docs/{{ tool('files') }}", "SST-REF003")]
)
def test_a_search_tool_stage_path_that_names_no_stage_drops_the_tool(stage_path: str, code: str) -> None:
    resolved, diagnostics = resolve_tool(AgentModel("a", ORIGIN, ()), _search(stage_path), agent_context(CATALOG))
    assert resolved is None and code in [item.code for item in diagnostics]


def _routine(*imports: str) -> ToolMember:
    return ToolMember(
        "scorer",
        "function",
        ToolOwnership.DEFINE,
        Origin("tools/t.yml"),
        "tools/t.yml",
        language="python",
        runtime_version="3.11",
        handler="main",
        body_file="tools/scorer.py",
        body="def main(): return 1",
        returns="NUMBER",
        imports=imports,
    )


def _compile(*imports: str) -> CompileResult:
    catalog = ToolCatalog((_routine(*imports), *CATALOG.members[1:]), "dev", frozenset(("dev",)))
    return CompileTools(
        catalog, default=ToolPlacement("DB", "S", "WH", None, None, None), dbt_relations={}
    ).run_result()


def _scorer(result: CompileResult) -> CompiledTool | None:
    found = [item for item in result.compiled if isinstance(item, CompiledTool) and item.name == "scorer"]
    return found[0] if found else None


def test_a_routine_import_follows_its_stage_member_and_an_external_one_is_kept() -> None:
    compiled = _scorer(_compile("{{ tool('files') }}/libs/a.zip", "@EXT.SHARED.LIBS/b.zip"))
    assert compiled is not None
    assert "IMPORTS = ('@DB.S.FILES/libs/a.zip', '@EXT.SHARED.LIBS/b.zip')" in compiled.rendered_artifact.ddl
    assert compiled.member.imports == ("@DB.S.FILES/libs/a.zip", "@EXT.SHARED.LIBS/b.zip")


def test_a_routine_import_that_names_no_stage_keeps_the_routine_back() -> None:
    result = _compile("{{ tool('ghost') }}/a.zip", "{{ tool('lookup') }}/b.zip")
    assert _scorer(result) is None
    assert [item.code for item in result.diagnostics] == ["SST-REF010", "SST-REF020"]


TARGET = TargetIdentity("dev", "acct", Identifier.parse("DEV_DB"), Identifier.parse("SALES"))


def test_an_agent_tag_resolves_through_the_tags_block_and_a_bad_one_is_left_out() -> None:
    entries = [
        {"name": "{{ tag('cost_center') }}", "value": "sales"},
        {"name": "cost_center", "value": "bare"},
        {"name": "{{ tag('cost_center') }}"},
        "not a mapping",
    ]
    model = AgentModel("a", ORIGIN, (), settings=MappingProxyType({"tags": Setting("tags", entries, "config")}))
    tags = declared_tags({"cost_center": {"fqn": "GOV.TAGS.COST_CENTER"}}, TARGET)
    settled, problems = settled_agent(model, TARGET, tags)
    assert settled.tags == (("GOV.TAGS.COST_CENTER", "sales"),)
    assert [(item.code, item.subject) for item in problems] == [("SST-REF050", "agent:a")]

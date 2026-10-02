from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.app.compile.agents import (
    AgentCompileContext,
    CompileAgents,
    CompiledAgent,
    ExtensionPin,
    for_publication,
)
from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.model.agent import (
    AgentModel,
    AgentProfile,
    AgentSkill,
    AgentTool,
    ResolvedAgent,
    ResolvedAgentTool,
)
from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolColumn, ToolGroup, ToolMember, ToolOwnership
from tests.helpers.compile_builders import compiled_as

SEMANTICS = ExtensionPin("skill:semantics", QualifiedName.parse("DB.S.SEMANTICS"), "SST_ABCDEF012345", ("semantics",))
TOOLKIT = ExtensionPin(
    "plugin:toolkit",
    QualifiedName.parse("DB.S.TOOLKIT"),
    "SST_0123456789AB",
    ("semantics", "operations"),
    has_scripts=True,
)


def context(
    *,
    tools: ToolCatalog | None = None,
    agents: dict[str, QualifiedName] | None = None,
    warehouse: str | None = "WH",
) -> AgentCompileContext:
    return AgentCompileContext(
        semantic_views={"sales": QualifiedName.parse("DB.S.SALES")},
        tools=tools or ToolCatalog((), "dev", frozenset(("dev",))),
        agents=agents or {},
        extensions={"vendor-pack": QualifiedName.parse("DB.EXT.VENDOR_PACK")},
        variables={"sha_version": "0000000"},
        database="DB",
        schema="S",
        warehouse=warehouse,
        query_timeout=60,
        orchestration_model="model",
        budget_seconds=120,
        budget_tokens=16000,
        tool_not_accessible="reject",
        analytical_search=False,
        alias="promoted",
        allowed_models=frozenset(("model",)),
        skills={"semantics": SEMANTICS},
        plugins={"toolkit": TOOLKIT},
    )


def test_agent_compiler_resolves_tools_and_renders_complete_spec() -> None:
    backing = ToolMember(
        "platform",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        description="Use for one order. Do not use for populations.",
        warehouse="WH",
        relations=MappingProxyType({"dev": "DB.S.LOOKUP"}),
    )
    tools = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(backing,)),),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "sales_agent",
        Origin("agent.yml"),
        ("agent.yml",),
        profile=AgentProfile("Sales", "Icon", "blue"),
        tools=(
            AgentTool(
                "cortex_analyst_text_to_sql",
                Origin("agent.yml"),
                description="Use for sales. Do not use for support.",
                semantic_view="sales",
            ),
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="lookup",
                backing=("platform", "lookup"),
                input_schema=MappingProxyType(
                    {
                        "type": "object",
                        "properties": {"order_id": {"type": "string"}},
                        "required": ["order_id"],
                    }
                ),
            ),
        ),
        skills=(AgentSkill("semantics", "CORTEX_EXTENSION", "semantics", "", ref="skill"),),
    )
    result = CompileAgents((model,), DiagnosticBag(), context(tools=tools)).run_result()
    assert not result.diagnostics.has_errors
    assert len(result.compiled) == 1
    payload = compiled_as(result, CompiledAgent).payload
    assert '"name": "SALES"' in payload
    assert '"semantic_view": "DB.S.SALES"' in payload
    assert '"identifier": "DB.S.LOOKUP"' in payload
    assert '"path": "DB.S.SEMANTICS"' in payload
    assert '"version": "SST_ABCDEF012345"' in payload
    assert compiled_as(result, CompiledAgent).rendered_artifact.depends_on == ("semantic_view:sales", "skill:semantics")


def test_agent_compiler_validates_unknown_refs_names_alias_and_input_schema() -> None:
    model = AgentModel(
        "bad",
        Origin("agent.yml"),
        ("agent.yml",),
        orchestration_model="missing",
        alias="LIVE",
        tools=(
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="missing_tool",
                description="",
                backing=("missing", "tool"),
                input_schema=MappingProxyType(
                    {
                        "type": "object",
                        "properties": {"payload": {"type": "object"}},
                        "required": ["missing"],
                    }
                ),
            ),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context()).run_result()
    codes = {diagnostic.code for diagnostic in result.diagnostics}
    assert {"SST-REF010", "SST-PRS025", "SST-VAL543"}.issubset(codes)


def test_agent_compiler_rejects_delegation_cycle() -> None:
    first = AgentModel(
        "first",
        Origin("first.yml"),
        ("first.yml",),
        tools=(AgentTool("agent", Origin("first.yml"), name="second", agent_ref="second", description="Delegate."),),
    )
    second = AgentModel(
        "second",
        Origin("second.yml"),
        ("second.yml",),
        tools=(AgentTool("agent", Origin("second.yml"), name="first", agent_ref="first", description="Delegate."),),
    )
    agents = {
        "first": QualifiedName.parse("DB.S.FIRST"),
        "second": QualifiedName.parse("DB.S.SECOND"),
    }
    result = CompileAgents((first, second), DiagnosticBag(), context(agents=agents)).run_result()
    assert any(diagnostic.code == "SST-REF022" for diagnostic in result.diagnostics)
    assert result.compiled == ()


def test_agent_compiler_covers_all_tool_kinds_and_validation_branches() -> None:
    search = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        description="Use for docs.",
        relations=MappingProxyType({"dev": "DB.S.SEARCH"}),
    )
    wrong = ToolMember(
        "platform",
        "wrong",
        "stage",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.STAGE"}),
    )
    tools = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(search, wrong)),),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "branches",
        Origin("agent.yml"),
        ("agent.yml",),
        alias="LIVE",
        orchestration_model="missing-model",
        tool_not_accessible="bad",
        analytical_search=True,
        tools=(
            AgentTool("unknown", Origin("agent.yml"), name="unknown", description="unknown"),
            AgentTool("cortex_analyst_text_to_sql", Origin("agent.yml"), name="illegal", description="bad"),
            AgentTool(
                "cortex_analyst_text_to_sql",
                Origin("agent.yml"),
                description="missing semantic view",
                semantic_view="missing",
            ),
            AgentTool("cortex_search", Origin("agent.yml"), name="missing", description="bad"),
            AgentTool(
                "cortex_search",
                Origin("agent.yml"),
                name="wrong",
                description="bad",
                backing=("platform", "wrong"),
            ),
            AgentTool(
                "cortex_search",
                Origin("agent.yml"),
                name="search",
                backing=("platform", "search"),
                max_results=3,
            ),
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="generic",
                description="generic",
                backing=("platform", "search"),
                input_schema=MappingProxyType(
                    {
                        "type": "object",
                        "properties": {"bad": {"type": "object"}},
                        "required": ["missing"],
                    }
                ),
            ),
            AgentTool("agent", Origin("agent.yml"), name="missing_agent", agent_ref="missing", description="agent"),
            AgentTool(
                "mcp", Origin("agent.yml"), name="mcp", description="mcp", passthrough=MappingProxyType({"x": 1})
            ),
            AgentTool("web_search", Origin("agent.yml"), name="other", description="web"),
            AgentTool("code_execution", Origin("agent.yml"), name="", description=""),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context(tools=tools)).run_result()
    codes = {diagnostic.code for diagnostic in result.diagnostics}
    expected_codes = {
        "SST-RND012",
        "SST-VAL520",
        "SST-REF011",
        "SST-VAL521",
        "SST-REF020",
        "SST-REF012",
        "SST-VAL517",
        "SST-VAL518",
        "SST-PRS025",
        "SST-VAL543",
        "SST-VAL545",
    }
    assert expected_codes.issubset(codes), (expected_codes - codes, codes)

    no_search = AgentModel(
        "no_search",
        Origin("agent.yml"),
        ("agent.yml",),
        analytical_search=True,
    )
    no_search_result = CompileAgents((no_search,), DiagnosticBag(), context()).run_result()
    assert any(diagnostic.code == "SST-VAL546" for diagnostic in no_search_result.diagnostics)


def test_agent_compiler_duplicate_names_size_skills_and_publish_properties() -> None:
    long_text = "x" * 80_100
    model = AgentModel(
        "large",
        Origin("agent.yml"),
        ("agent.yml",),
        orchestration_instructions=long_text,
        tools=(
            AgentTool("data_to_chart", Origin("agent.yml"), name="same", description="chart"),
            AgentTool("code_execution", Origin("agent.yml"), name="same", description="code"),
            AgentTool("mcp", Origin("agent.yml"), name="Same", description="mcp"),
        ),
        skills=(
            AgentSkill("stage", "STAGE", "@stage", "LIVE"),
            AgentSkill("missing", "CORTEX_EXTENSION", "", ""),
            AgentSkill("vendor", "CORTEX_EXTENSION", "vendor-pack", "LIVE"),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context()).run_result()
    codes = {diagnostic.code for diagnostic in result.diagnostics}
    assert {"SST-VAL514", "SST-VAL515", "SST-VAL512", "SST-VAL538", "SST-VAL539", "SST-REF013"}.issubset(codes)

    valid = AgentModel("valid", Origin("agent.yml"), ("agent.yml",))
    compiled = compiled_as(CompileAgents((valid,), DiagnosticBag(), context()).run_result(), CompiledAgent)
    assert compiled.name == "valid"
    assert compiled.artifact_key == "agent:valid"
    assert compiled.artifact_type == "agent"
    assert compiled.source_files == ("agent.yml",)
    assert compiled.member_keys == ()
    assert compiled.referenced_models == ()
    assert compiled.dbt_relations == ()
    published = for_publication(
        compiled,
        stage=QualifiedName.parse("DB.S.STAGE"),
        git_sha="abcdef0",
    ).rendered_for_publish("a" * 64)
    assert published.upload_path == "@DB.S.STAGE/valid/abcdef0/agent_spec.yaml"


def test_agent_compiler_size_limit_and_inheritance_resolution() -> None:
    huge = AgentModel(
        "huge",
        Origin("agent.yml"),
        ("agent.yml",),
        orchestration_instructions="x" * 100_100,
    )
    result = CompileAgents((huge,), DiagnosticBag(), context()).run_result()
    assert any(diagnostic.code == "SST-VAL511" for diagnostic in result.diagnostics)

    inherited = AgentModel(
        "inherited",
        Origin("agent.yml"),
        ("agent.yml",),
        skills=(AgentSkill("vendor", "CORTEX_EXTENSION", "vendor-pack", "", version_var="release"),),
    )
    pinned = replace(context(), variables={"release": "V7"})
    result = CompileAgents((inherited,), DiagnosticBag(), pinned).run_result()
    assert '"path": "DB.EXT.VENDOR_PACK"' in compiled_as(result, CompiledAgent).payload
    assert '"version": "V7"' in compiled_as(result, CompiledAgent).payload


def test_agent_compiler_reports_duplicate_identity_and_display_name() -> None:
    first = AgentModel("duplicate", Origin("first.yml"), ("first.yml",), profile=AgentProfile("Shared"))
    second = AgentModel("DUPLICATE", Origin("second.yml"), ("second.yml",), profile=AgentProfile("shared"))
    result = CompileAgents((first, second), DiagnosticBag(), context()).run_result()
    assert {diagnostic.code for diagnostic in result.diagnostics} >= {"SST-VAL001", "SST-VAL549"}


def test_agent_compiler_resolves_reference_and_managed_tool_fallbacks() -> None:
    missing_search = ToolMember(
        "platform",
        "missing_search",
        "cortex_search_service",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({}),
    )
    managed_search = ToolMember(
        "platform",
        "managed_search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Search managed documents.",
        columns=(
            ToolColumn("DOCUMENT_ID", "Document id.", "VARCHAR", False, True),
            ToolColumn("DOCUMENT_NAME", "Document name.", "VARCHAR", True, False),
        ),
    )
    managed_procedure = ToolMember(
        "platform",
        "managed_procedure",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Run the managed procedure.",
    )
    referenced_agent = ToolMember(
        "platform",
        "delegated",
        "agent",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.DELEGATED"}),
    )
    broken_agent = replace(referenced_agent, name="broken", relations=MappingProxyType({}))
    managed_agent = replace(referenced_agent, name="managed_agent", ownership=ToolOwnership.DEFINE)
    tools = ToolCatalog(
        (
            ToolGroup(
                "platform",
                Origin("tools.yml"),
                "tools.yml",
                members=(
                    missing_search,
                    managed_search,
                    managed_procedure,
                    referenced_agent,
                    broken_agent,
                    managed_agent,
                ),
            ),
        ),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "resolution_branches",
        Origin("agent.yml"),
        ("agent.yml",),
        tools=(
            AgentTool(
                "cortex_search",
                Origin("agent.yml"),
                name="missing_search",
                backing=("platform", "missing_search"),
            ),
            AgentTool(
                "cortex_search",
                Origin("agent.yml"),
                name="managed_search",
                backing=("platform", "managed_search"),
            ),
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="managed_procedure",
                backing=("platform", "managed_procedure"),
                input_schema=MappingProxyType({"type": "object"}),
            ),
            AgentTool("agent", Origin("agent.yml"), name="delegated", description="Delegate."),
            AgentTool("agent", Origin("agent.yml"), name="broken", description="Delegate."),
            AgentTool("agent", Origin("agent.yml"), name="managed_agent", description="Delegate."),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context(tools=tools, warehouse=None)).run_result()
    codes = {diagnostic.code for diagnostic in result.diagnostics}
    assert {"SST-REF018", "SST-VAL527", "SST-VAL528"}.issubset(codes)

    valid_model = replace(
        model,
        tools=(
            replace(
                model.tools[1],
                description="Search managed documents.",
                columns_and_descriptions=MappingProxyType(
                    {
                        "DOCUMENT_ID": {"description": "Document id.", "type": "VARCHAR"},
                        "DOCUMENT_NAME": {"description": "Document name.", "type": "VARCHAR"},
                    }
                ),
            ),
            model.tools[2],
            model.tools[3],
        ),
    )
    valid = CompileAgents((valid_model,), DiagnosticBag(), context(tools=tools)).run_result()
    assert valid.compiled
    resolved = compiled_as(valid, CompiledAgent).resolved.tools
    search = next(tool for tool in resolved if tool.name == "managed_search")
    assert search.resources["search_service"] == "DB.S.MANAGED_SEARCH"
    assert search.resources["id_column"] == "DOCUMENT_ID"
    assert search.resources["title_column"] == "DOCUMENT_NAME"
    assert "columns_and_descriptions" in search.resources
    generic = next(tool for tool in resolved if tool.name == "managed_procedure")
    assert generic.resources["identifier"] == "DB.S.MANAGED_PROCEDURE"
    delegated = next(tool for tool in resolved if tool.name == "delegated")
    assert delegated.resources["identifier"] == "DB.S.DELEGATED"


def test_agent_compiler_validates_names_timeouts_and_scalar_input_schema() -> None:
    procedure = ToolMember(
        "platform",
        "procedure",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        description="Procedure.",
        warehouse="WH",
        relations=MappingProxyType({"dev": "DB.S.PROCEDURE"}),
    )
    tools = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(procedure,)),),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "validation_branches",
        Origin("agent.yml"),
        ("agent.yml",),
        tools=(
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="x" * 65,
                backing=("platform", "procedure"),
                query_timeout=0,
                input_schema=MappingProxyType(
                    {"type": "object", "properties": {"flag": {"type": "boolean"}}, "required": "flag"}
                ),
            ),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context(tools=tools)).run_result()
    assert {"SST-VAL513", "SST-PRS016"}.issubset({diagnostic.code for diagnostic in result.diagnostics})


def test_agent_compiler_covers_empty_environment_and_input_schema_edge_cases() -> None:
    procedure = ToolMember(
        "platform",
        "procedure",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        description="Procedure.",
        warehouse="WH",
        relations=MappingProxyType({"dev": "DB.S.PROCEDURE"}),
    )
    tools = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(procedure,)),),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "schema_edges",
        Origin("agent.yml"),
        ("agent.yml",),
        tools=(
            AgentTool(
                "generic",
                Origin("agent.yml"),
                name="procedure",
                backing=("platform", "procedure"),
                input_schema=MappingProxyType(
                    {
                        "type": "object",
                        "properties": {"raw": "string", "count": {"type": "integer"}},
                        "required": ["missing"],
                    }
                ),
            ),
        ),
    )
    result = CompileAgents((model,), DiagnosticBag(), context(tools=tools, warehouse=None)).run_result()
    assert {"SST-PRS032", "SST-PRS033"}.issubset({diagnostic.code for diagnostic in result.diagnostics})

    no_environment = AgentModel(
        "no_environment",
        Origin("agent.yml"),
        ("agent.yml",),
        tools=(
            AgentTool(
                "cortex_analyst_text_to_sql",
                Origin("agent.yml"),
                description="Query sales.",
                semantic_view="sales",
            ),
        ),
    )
    no_environment_context = replace(context(warehouse=None), query_timeout=None)
    compiled = compiled_as(
        CompileAgents((no_environment,), DiagnosticBag(), no_environment_context).run_result(), CompiledAgent
    )
    assert "execution_environment" not in compiled.resolved.tools[0].resources

    missing_schema = replace(
        model,
        name="missing_schema",
        tools=(replace(model.tools[0], input_schema=MappingProxyType({})),),
    )
    missing_schema_result = CompileAgents(
        (missing_schema,), DiagnosticBag(), context(tools=tools, warehouse=None)
    ).run_result()
    assert "SST-VAL526" in {diagnostic.code for diagnostic in missing_schema_result.diagnostics}


def test_agent_cycle_walk_handles_complete_nodes_and_acyclic_edges() -> None:
    origin = Origin("agent.yml")
    leaf = AgentModel("leaf", origin, ("agent.yml",))
    first = AgentModel(
        "first",
        origin,
        ("agent.yml",),
        tools=(AgentTool("agent", origin, name="leaf", agent_ref="leaf", description="Delegate."),),
    )
    second = AgentModel(
        "second",
        origin,
        ("agent.yml",),
        tools=(AgentTool("agent", origin, name="leaf", agent_ref="leaf", description="Delegate."),),
    )
    agents = {name: QualifiedName.parse(f"DB.S.{name}") for name in ("leaf", "first", "second")}
    result = CompileAgents((leaf, first, second), DiagnosticBag(), context(agents=agents)).run_result()
    assert len(result.compiled) == 3


def test_agent_search_resources_support_authored_columns_and_partial_environment() -> None:
    origin = Origin("agent.yml")
    backing = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        description="Search.",
        relations=MappingProxyType({"dev": "DB.S.SEARCH"}),
    )
    tools = ToolCatalog(
        (ToolGroup("platform", origin, "tools.yml", members=(backing,)),),
        "dev",
        frozenset(("dev",)),
    )
    model = AgentModel(
        "search_columns",
        origin,
        ("agent.yml",),
        tools=(
            AgentTool(
                "cortex_search",
                origin,
                name="search",
                description="Search.",
                backing=("platform", "search"),
                warehouse="LOCAL_WH",
                columns_and_descriptions=MappingProxyType({"BODY": {"description": "Body."}}),
            ),
        ),
    )
    compiled = compiled_as(
        CompileAgents((model,), DiagnosticBag(), context(tools=tools, warehouse=None)).run_result(), CompiledAgent
    )
    resources = compiled.resolved.tools[0].resources
    assert resources["columns_and_descriptions"] == {"BODY": {"description": "Body."}}
    assert resources["execution_environment"] == {
        "type": "warehouse",
        "warehouse": "LOCAL_WH",
        "query_timeout": 60,
    }

    timeout_only = replace(
        model,
        name="timeout_only",
        tools=(replace(model.tools[0], warehouse=None, query_timeout=9),),
    )
    timeout_compiled = compiled_as(
        CompileAgents(
            (timeout_only,),
            DiagnosticBag(),
            replace(context(tools=tools, warehouse=None), query_timeout=None),
        ).run_result(),
        CompiledAgent,
    )
    assert timeout_compiled.resolved.tools[0].resources["execution_environment"] == {
        "type": "warehouse",
        "query_timeout": 9,
    }


def test_compiled_agent_projection_properties_are_stable() -> None:
    model = AgentModel("projection", Origin("agent.yml"), ("agent.yml",))
    compiled = compiled_as(CompileAgents((model,), DiagnosticBag(), context()).run_result(), CompiledAgent)
    assert compiled.member_keys == ()
    assert compiled.referenced_models == ()
    assert compiled.dbt_relations == ()

    resolved = ResolvedAgent(
        model,
        (
            ResolvedAgentTool("generic", "first", "First.", depends_on=("tool:shared",)),
            ResolvedAgentTool("generic", "second", "Second.", depends_on=("tool:shared",)),
        ),
    )
    assert resolved.depends_on == ("tool:shared",)


def test_agent_artifact_programs_cover_temporary_alias_tags_and_metadata() -> None:
    model = AgentModel(
        "publish",
        Origin("agent.yml"),
        ("agent.yml",),
        comment="owner's agent",
        secure=True,
        profile=AgentProfile("Sales", "icon", "blue"),
        alias="promoted",
        tags=(("DB.S.TAG", "owner's"), ("MIXED_TAG", "value")),
    )
    compiled = compiled_as(CompileAgents((model,), DiagnosticBag(), context()).run_result(), CompiledAgent)
    published = for_publication(
        compiled,
        stage=QualifiedName.parse("DB.S.STAGE"),
        git_sha="abcdef0",
    ).rendered_for_publish("a" * 64)
    assert any("MODIFY VERSION" in str(statement) for statement in published.create_statements)
    assert any("SET TAG DB.S.TAG" in str(statement) for statement in published.create_statements)
    assert any("SET SECURE = TRUE" in str(statement) for statement in published.statements)
    assert "owner''s agent" in str(published.statements[-2])

    temporary = for_publication(
        compiled,
        stage=QualifiedName.parse("DB.S.STAGE"),
        git_sha="abcdef0",
        temporary=True,
    ).rendered_artifact
    assert temporary.upload_path is None
    assert str(temporary.statements[0]).startswith("CREATE OR REPLACE TEMPORARY AGENT")
    assert "WITH PROFILE" in str(temporary.statements[0])

    unsafe = replace(compiled, payload="contains $$ delimiter")
    with pytest.raises(ValueError, match="dollar-quote"):
        _ = for_publication(
            unsafe,
            stage=QualifiedName.parse("DB.S.STAGE"),
            git_sha="abcdef0",
            temporary=True,
        ).rendered_artifact


def test_skill_references_pin_owned_versions_and_check_consumed_ones() -> None:
    def skill(name: str, path: str, *, ref: str = "skill", version: str = "", var: str | None = None) -> AgentSkill:
        return AgentSkill(name, "CORTEX_EXTENSION", path, version, ref=ref, version_var=var)

    agent = AgentModel(
        "router",
        Origin("agent.yml"),
        ("agent.yml",),
        skills=(
            skill("semantics", "semantics"),
            skill("", "toolkit", ref="plugin"),
            skill("vendor", "vendor-pack", ref="extension", version="V2"),
        ),
    )
    result = CompileAgents((agent,), DiagnosticBag(), context()).run_result()
    assert [item.code for item in result.diagnostics] == ["SST-VAL814"]
    payload = compiled_as(result, CompiledAgent).payload
    assert '"path": "DB.S.TOOLKIT"' in payload and '"version": "SST_0123456789AB"' in payload
    assert '"path": "DB.EXT.VENDOR_PACK"' in payload and '"version": "V2"' in payload
    assert payload.count('"name":') == 2  # semantics and vendor; the plugin entry omits name
    assert compiled_as(result, CompiledAgent).rendered_artifact.depends_on == ("skill:semantics", "plugin:toolkit")

    broken = AgentModel(
        "broken",
        Origin("agent.yml"),
        ("agent.yml",),
        skills=(
            skill("ghost", "ghost"),
            skill("", "ghost-kit", ref="plugin"),
            skill("semantics", "semantics", ref="extension", version="V1"),
            skill("semantics", "semantics", version="V1"),
            skill("", "semantics"),
            skill("other", "semantics"),
            skill("stranger", "toolkit", ref="plugin"),
            skill("vendor", "vendor-pack", ref="extension", var="sha_version"),
            skill("unknown", "nowhere", ref="extension", version="V1"),
        ),
    )
    diagnostics = CompileAgents((broken,), DiagnosticBag(), context()).run_result().diagnostics
    assert [item.code for item in diagnostics] == [
        "SST-REF032",
        "SST-REF036",
        "SST-REF037",
        "SST-VAL838",
        "SST-VAL540",
        "SST-VAL840",
        "SST-VAL840",
        "SST-VAL839",
        "SST-REF013",
        "SST-VAL804",
        "SST-VAL804",
    ]


def test_references_to_declared_but_unpublished_extensions_name_the_cause() -> None:
    def skill(name: str, path: str, *, ref: str = "skill", version: str = "") -> AgentSkill:
        return AgentSkill(name, "CORTEX_EXTENSION", path, version, ref=ref)

    agent = AgentModel(
        "router",
        Origin("agent.yml"),
        ("agent.yml",),
        skills=(
            skill("draft", "draft"),
            skill("", "kit", ref="plugin"),
            skill("draft", "draft", ref="extension", version="V1"),
            skill("glossary", "partner-glossary", ref="extension", version="V1"),
        ),
    )
    unpublished = {
        "skill:draft": "it has errors",
        "plugin:kit": "skills.catalog is not configured",
        "extension:partner-glossary": "its skills.extensions entry cannot be qualified",
    }
    diagnostics = (
        CompileAgents((agent,), DiagnosticBag(), replace(context(), unpublished=unpublished)).run_result().diagnostics
    )
    assert [(item.code, item.context.get("reason")) for item in diagnostics[:4]] == [
        ("SST-VAL856", "it has errors"),
        ("SST-VAL856", "skills.catalog is not configured"),
        ("SST-REF037", None),
        ("SST-VAL856", "its skills.extensions entry cannot be qualified"),
    ]


def test_unreferenced_extensions_are_reported_only_in_projects_with_agents() -> None:
    assert CompileAgents((), DiagnosticBag(), context()).run_result().diagnostics == ()
    lonely = AgentModel("lonely", Origin("agent.yml"), ("agent.yml",))
    consumed = replace(context(), consumed=frozenset(("skill:semantics",)))
    diagnostics = CompileAgents((lonely,), DiagnosticBag(), consumed).run_result().diagnostics
    assert [(item.code, item.subject) for item in diagnostics] == [("SST-VAL804", "plugin:toolkit")]


def test_agent_tools_resolve_by_type_and_unknown_types_resolve_to_nothing() -> None:
    origin = Origin("agent.yml", 4, 1)
    model = AgentModel("router", origin, ("agent.yml",))

    unknown, unknown_diagnostics = resolve_tool(model, AgentTool("telepathy", origin, name="x"), context())
    assert unknown is None
    assert [(item.code, item.origin) for item in unknown_diagnostics] == [("SST-RND012", origin)]

    chart, chart_diagnostics = resolve_tool(model, AgentTool("data_to_chart", origin, description="Charts."), context())
    assert chart_diagnostics == ()
    assert chart is not None and (chart.name, dict(chart.resources)) == ("data_to_chart", {})

    mcp, _ = resolve_tool(
        model,
        AgentTool("mcp", origin, name="docs", description="Docs.", passthrough=MappingProxyType({"url": "u"})),
        context(),
    )
    assert mcp is not None and dict(mcp.resources) == {"url": "u"}


def test_agent_tools_without_a_reference_or_a_resolving_member_are_dropped() -> None:
    origin = Origin("agent.yml")
    model = AgentModel("delegates", origin, ("agent.yml",))

    assert resolve_tool(model, AgentTool("agent", origin, description="Neither."), context()) == (None, ())
    ghost, diagnostics = resolve_tool(model, AgentTool("agent", origin, name="ghost", description="Gone."), context())
    assert ghost is None
    assert [item.code for item in diagnostics] == ["SST-REF010"]


def test_analyst_environment_carries_a_warehouse_without_a_timeout() -> None:
    origin = Origin("agent.yml")
    model = AgentModel("analyst", origin, ("agent.yml",))
    authored = AgentTool("cortex_analyst_text_to_sql", origin, description="Sales.", semantic_view="SALES")

    tool, diagnostics = resolve_tool(model, authored, replace(context(), query_timeout=None))

    assert diagnostics == ()
    assert tool is not None
    assert tool.resources["execution_environment"] == {"type": "warehouse", "warehouse": "WH"}
    assert (tool.name, tool.depends_on) == ("SALES", ("semantic_view:sales",))

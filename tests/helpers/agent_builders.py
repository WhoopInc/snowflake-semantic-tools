"""Small agents, tool members and compile contexts for the per-code agent and tool tests.

Each builder fills only what the code under test does not care about, so a test states the
one field that makes its code fire, or stay quiet.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, CompileAgents, CompiledAgent
from snowflake_semantic_tools.app.compile.agents.cross import cross_artifact_result
from snowflake_semantic_tools.app.compile.agents.observe import ObserveLiveObjects
from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.project import POSITIONS
from snowflake_semantic_tools.app.compile.tools import CompiledTool, CompileTools
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalQuestion, EvalRunConfig, ResolvedEval
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import (
    ToolCatalog,
    ToolColumn,
    ToolGroup,
    ToolMember,
    ToolOwnership,
    ToolParameter,
)
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake

ORIGIN = Origin("agents/sales/agent.yml", 1, 1)
TOOLS_FILE = "tools/platform.yml"
TOOL_ORIGIN = Origin(TOOLS_FILE, 1, 1)
OBJECT_SCHEMA = MappingProxyType(
    {"type": "object", "properties": {"order_id": {"type": "string"}}, "required": ["order_id"]}
)
DOCS_COLUMNS = (
    ToolColumn("DOC_ID", "Document id.", "string", False, True),
    ToolColumn("DOC_NAME", "Document title.", "string", True, False),
)


def catalog(*members: ToolMember, immutable: bool = False) -> ToolCatalog:
    """Return a dev-target catalog holding `members` in one group, `platform`."""
    group = ToolGroup("platform", TOOL_ORIGIN, TOOLS_FILE, immutable=immutable, members=members)
    return ToolCatalog((group,), "dev", frozenset(("dev", "prod")))


def search_member(name: str = "docs_search", **fields: Any) -> ToolMember:
    """Return a defined search service member over `product_docs`."""
    values: dict[str, Any] = {
        "description": "Product documentation.",
        "on_model": "product_docs",
        "search_column": "BODY",
        "columns": DOCS_COLUMNS,
        "warehouse": "WH",
        "target_lag": "1 hour",
        **fields,
    }
    return ToolMember(
        "platform", name, "cortex_search_service", ToolOwnership.DEFINE, TOOL_ORIGIN, TOOLS_FILE, **values
    )


def procedure_member(name: str = "lookup", *, reference: bool = True, **fields: Any) -> ToolMember:
    """Return a procedure member: referenced on dev and prod, or defined from a body file."""
    values: dict[str, Any] = {"description": "Looks up one order.", "warehouse": "WH", **fields}
    if reference:
        values.setdefault("relations", MappingProxyType({"dev": "DB.DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}))
    else:
        values.setdefault("body_file", "tools/lookup.sql")
        values.setdefault("body", "BEGIN RETURN 'x'; END")
        values.setdefault("language", "sql")
        values.setdefault("returns", "VARCHAR")
    ownership = ToolOwnership.REFERENCE if reference else ToolOwnership.DEFINE
    return ToolMember("platform", name, "procedure", ownership, TOOL_ORIGIN, TOOLS_FILE, **values)


def parameter(name: str, sql_type: str) -> ToolParameter:
    """Return one required signature parameter."""
    return ToolParameter(name, sql_type, True)


def analyst_tool(**fields: Any) -> AgentTool:
    """Return an Analyst tool over the `sales` semantic view."""
    values: dict[str, Any] = {"description": "Sales questions. Not for support.", "semantic_view": "sales", **fields}
    keys = ("type", "description", "semantic_view", *(key for key in fields if key != "declared_keys"))
    values.setdefault("declared_keys", tuple(sorted(set(keys))))
    return AgentTool("cortex_analyst_text_to_sql", ORIGIN, **values)


def search_tool(name: str = "docs", **fields: Any) -> AgentTool:
    """Return a Cortex Search tool over the `docs_search` member."""
    values: dict[str, Any] = {"name": name, "description": "Product documents.", "backing": ("docs_search",), **fields}
    keys = (
        "type",
        "name",
        "description",
        "search_service",
        *(key for key in fields if key not in ("backing", "declared_keys")),
    )
    values.setdefault("declared_keys", tuple(sorted(set(keys))))
    return AgentTool("cortex_search", ORIGIN, **values)


def generic_tool(name: str = "lookup", **fields: Any) -> AgentTool:
    """Return a generic tool over the `lookup` member, with an object input schema."""
    values: dict[str, Any] = {
        "name": name,
        "description": "One order's tier.",
        "backing": ("lookup",),
        "input_schema": OBJECT_SCHEMA,
        **fields,
    }
    values.setdefault("declared_keys", ("description", "identifier", "input_schema", "name", "type"))
    return AgentTool("generic", ORIGIN, **values)


def builtin_tool(tool_type: str = "data_to_chart", **fields: Any) -> AgentTool:
    """Return a built-in tool, described."""
    values: dict[str, Any] = {"description": f"The {tool_type} built-in.", **fields}
    values.setdefault("declared_keys", ("description", "type"))
    return AgentTool(tool_type, ORIGIN, **values)


def agent(name: str = "sales_agent", *tools: AgentTool, **fields: Any) -> AgentModel:
    """Return an agent with `tools`, its orchestration model pinned to one the context allows."""
    values: dict[str, Any] = {"orchestration_model": "claude", **fields}
    return AgentModel(name, ORIGIN, ("agents/sales/agent.yml",), tools=tools, **values)


def context(tools: ToolCatalog | None = None, **fields: Any) -> AgentCompileContext:
    """Return a compile context with the `sales` view, the given tools, and quiet defaults."""
    base = AgentCompileContext(
        semantic_views={"sales": QualifiedName.parse("DB.S.SALES")},
        tools=tools or catalog(),
        agents={},
        extensions={"vendor_pack": QualifiedName.parse("DB.EXT.VENDOR_PACK")},
        variables={"sha_version": "0000000"},
        database="DB",
        schema="S",
        warehouse="WH",
        query_timeout=None,
        orchestration_model="claude",
        budget_seconds=None,
        budget_tokens=None,
        tool_not_accessible=None,
        analytical_search=None,
        alias=None,
        allowed_models=frozenset(("claude", "auto")),
    )
    return replace(base, **fields)


def compile_agents(*models: AgentModel, tools: ToolCatalog | None = None, **fields: Any) -> CompileResult:
    """Compile `models` against `context(tools, **fields)`."""
    return CompileAgents(models, DiagnosticBag(), context(tools, **fields)).run_result()


def compile_tools(members: Iterable[ToolMember], **materializations: str) -> CompileResult:
    """Compile defined members over `product_docs`, its relation `DB.MARTS.PRODUCT_DOCS`."""
    return CompileTools(
        catalog(*members),
        database="DB",
        schema="S",
        warehouse="WH",
        target_lag="1 hour",
        embedding_model=None,
        execute_as="caller",
        dbt_relations={"product_docs": "DB.MARTS.PRODUCT_DOCS"},
        dbt_materializations=dict(materializations),
    ).run_result()


def compiled_agents(result: CompileResult) -> tuple[CompiledAgent, ...]:
    """Return the compiled agents of a result, in order."""
    return tuple(item for item in result.compiled if isinstance(item, CompiledAgent))


def compiled_tools(result: CompileResult) -> tuple[CompiledTool, ...]:
    """Return the compiled tools of a result, in order."""
    return tuple(item for item in result.compiled if isinstance(item, CompiledTool))


def found(diagnostics: Iterable[Diagnostic], code: str) -> list[Diagnostic]:
    """Return the diagnostics carrying `code`, in order."""
    return [item for item in diagnostics if item.code == code]


def evaluation(model: AgentModel, *questions: str, tier: str | None = "blocking") -> ResolvedEval:
    """Return an eval of `model` asking `questions`, run at `tier`."""
    base = resolved_eval()
    rows = tuple(EvalQuestion(ORIGIN, question, None) for question in questions)
    return replace(
        base,
        agent=model,
        dataset=replace(base.dataset, agent=model.name, questions=rows),
        config=replace(base.config, agent=model.name, run=EvalRunConfig(tier=tier)),
    )


def cross(*results: CompileResult, evals: tuple[ResolvedEval, ...] = ()) -> tuple[Diagnostic, ...]:
    """Run the cross-artifact checks over compiled `results` and `evals`, as the project compiler does."""
    return tuple(cross_artifact_result(results, EvalCatalog(evals, ()), POSITIONS).diagnostics)


def observe(port: FakeSnowflake, *results: CompileResult) -> tuple[Diagnostic, ...]:
    """Run the connected agent and tool checks over compiled `results` against `port`, target `dev`."""
    merged = CompileResult(tuple(item for result in results for item in result.compiled))
    return ObserveLiveObjects(port, target="dev").run(merged)


def dbt_catalog() -> DbtCatalog:
    """Return a manifest holding `product_docs`, with the columns `search_member` names."""
    columns = tuple(DbtColumn(name, None, "VARCHAR", None) for name in ("BODY", "DOC_ID", "DOC_NAME"))
    model = DbtModel("model.jaffle.product_docs", "product_docs", "DB.MARTS.PRODUCT_DOCS", (), (), columns)
    return DbtCatalog("v12", None, None, (model,))


def group(name: str, *members: ToolMember, immutable: bool = False) -> ToolGroup:
    """Return a group named `name` holding `members`, each moved into it."""
    moved = tuple(replace(member, group=name) for member in members)
    return ToolGroup(name, TOOL_ORIGIN, TOOLS_FILE, immutable=immutable, members=moved)

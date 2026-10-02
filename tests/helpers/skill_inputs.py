"""Skills, plugins, Desktop profiles and their parts, for the per-code skill and profile tests.

`skill` is one valid skill folder, `month-close`, whose SKILL.md references the one file it
ships; `plugin` bundles skills by name; `profile` is the `analyst` Desktop profile carrying
`month-close`. `write` lays a project tree on disk for the loader-level checks. `compile_agents`
compiles agents against the project's published extensions: `month-close`, a skill, and
`finance-kit`, a plugin carrying it and its `close.py` script. `publish_extensions` plans and
applies compiled skills against a recorded Snowflake, the way `sst apply` does.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, CompileAgents, ExtensionPin
from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill, AgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ChangeSet
from snowflake_semantic_tools.domain.model.profile import (
    CommandFile,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.model.tool import ToolCatalog
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, State
from tests.helpers.app_ports import FixedClock, InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.recorded_snowflake import RecordedSnowflake

SKILL_MD = "---\nname: {name}\ndescription: Does things.\n---\n# Close\nRead reference/steps.md.\n"


def skill(name: str = "month-close", files: dict[str, bytes | str] | None = None, **fields: object) -> Skill:
    """A valid skill: SKILL.md and `reference/steps.md`, which SKILL.md references."""
    authored: dict[str, bytes | str] = {"SKILL.md": SKILL_MD.format(name=name), "reference/steps.md": "Steps.\n"}
    authored.update(files or {})
    entries = tuple(
        sorted(
            (
                SkillFile(path, value.encode("utf-8") if isinstance(value, str) else value)
                for path, value in authored.items()
            ),
            key=lambda item: item.path,
        )
    )
    value = Skill(
        name,
        f"skills/finance/{name}",
        name,
        "Does things.",
        "# Close\n",
        entries,
        Origin(f"skills/finance/{name}/SKILL.md", 1),
    )
    return replace(value, **fields)  # type: ignore[arg-type]


def plugin(name: str = "finance-kit", members: tuple[str, ...] = ("month-close",), **fields: object) -> Plugin:
    value = Plugin(
        name,
        f"plugins/{name}",
        f"plugins/{name}/plugin.yml",
        "Finance skills.",
        "Data",
        members,
        Origin(f"plugins/{name}/plugin.yml", 1),
    )
    return replace(value, **fields)  # type: ignore[arg-type]


def skill_catalog(*skills: Skill, plugins: tuple[Plugin, ...] = ()) -> SkillCatalog:
    return SkillCatalog(skills or (skill(),), plugins)


def profile(name: str = "analyst", **fields: object) -> DesktopProfile:
    value = DesktopProfile(
        name,
        f"profiles/{name}",
        "Analyst.",
        "Data",
        ("month-close",),
        (),
        (),
        None,
        Origin(f"profiles/{name}/profile.yml", 1),
        (f"profiles/{name}/profile.yml",),
    )
    return replace(value, **fields)  # type: ignore[arg-type]


def shared(**fields: object) -> SharedProfile:
    value = SharedProfile("Shared rules.\n", (), (), Origin("profiles/shared/profile.yml", 1))
    return replace(value, **fields)  # type: ignore[arg-type]


def hook(name: str = "sql-safety", script: str = "check.sh") -> HookDefinition:
    return HookDefinition(
        name,
        f"hooks/{name}",
        "PreToolUse",
        "bash",
        SkillFile(script, b"#!/bin/sh\n"),
        Origin(f"hooks/{name}/hook.yml", 1),
        matcher="snowflake_sql_execute",
    )


def mcp(name: str = "dbt", servers: dict[str, object] | None = None) -> McpConfig:
    value = servers if servers is not None else {"dbt": {"command": "dbt-mcp", "env": {"TOKEN": "${DBT_TOKEN}"}}}
    return McpConfig(name, f"mcp-servers/{name}/mcp.json", value, Origin(f"mcp-servers/{name}/mcp.json", 1))


def command(path: str = "sql/check", content: str = "Run the check.\n") -> CommandFile:
    return CommandFile(path, content.encode("utf-8"), f"commands/{path}.md", Origin(f"commands/{path}.md", 1))


def profile_catalog(*profiles: DesktopProfile, **fields: object) -> ProfileCatalog:
    value = ProfileCatalog(profiles or (profile(),))
    return replace(value, **fields)  # type: ignore[arg-type]


def write(root: Path, files: Mapping[str, str | bytes]) -> Path:
    """Write each file below `root`, creating its folders, and return `root`."""
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
    return root


SKILL_PIN = ExtensionPin(
    "skill:month-close", QualifiedName.parse("DB.S.MONTH_CLOSE"), "SST_ABCDEF012345", ("month-close",)
)
PLUGIN_PIN = ExtensionPin(
    "plugin:finance-kit",
    QualifiedName.parse("DB.S.FINANCE_KIT"),
    "SST_0123456789AB",
    ("month-close",),
    (("month-close", "close.py"),),
)


def agent(*skills: AgentSkill, code_execution: bool = False) -> AgentModel:
    tools = (AgentTool("code_execution", Origin("agents/router/agent.yml"), name="run"),) if code_execution else ()
    return AgentModel(
        "router", Origin("agents/router/agent.yml"), ("agents/router/agent.yml",), skills=skills, tools=tools
    )


def skill_ref(name: str, path: str, *, ref: str = "skill", version: str = "", var: str | None = None) -> AgentSkill:
    return AgentSkill(name, "CORTEX_EXTENSION", path, version, ref=ref, version_var=var)


def compile_agents(*agents: AgentModel, unpublished: dict[str, str] | None = None) -> CompileResult:
    context = AgentCompileContext(
        semantic_views={},
        tools=ToolCatalog((), "dev", frozenset(("dev",))),
        agents={},
        extensions={},
        variables={"sha_version": "0000000"},
        database="DB",
        schema="S",
        warehouse="WH",
        query_timeout=60,
        orchestration_model="model",
        budget_seconds=None,
        budget_tokens=None,
        tool_not_accessible=None,
        analytical_search=None,
        alias=None,
        allowed_models=frozenset(("model",)),
        skills={"month-close": SKILL_PIN},
        plugins={"finance-kit": PLUGIN_PIN},
        unpublished=unpublished or {},
    )
    return CompileAgents(agents, DiagnosticBag(), context).run_result()


CATALOG_CHANNEL = CatalogChannel("DB", "S", QualifiedName.parse("DB.S.SKILL_BUNDLES"))


def compile_extensions(catalog: SkillCatalog) -> dict[str, CompiledExtension]:
    result = CompileSkills(catalog, CATALOG_CHANNEL).run_result()
    assert not result.diagnostics.has_errors, result.diagnostics
    return {item.artifact_key: item for item in result.compiled if isinstance(item, CompiledExtension)}


def empty_state() -> State:
    return State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType({}))


def publish_extensions(
    port: RecordedSnowflake, compiled: dict[str, CompiledExtension], previous: State, *, include_prune: bool = False
) -> tuple[ChangeSet, State]:
    """Plan and apply the compiled extensions; return the plan and the state apply left."""
    releases = {key: item.release for key, item in compiled.items()}
    handlers = {kind: ExtensionLifecycleHandler(port, releases, kind) for kind in ("skill", "plugin")}
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
        rendered, manifest, previous, target(), fetched_at="now", include_prune=include_prune
    )
    store = InMemoryStateStore(previous)
    ApplyArtifacts(
        port, store, FixedClock(), state_table=QualifiedName.parse("DB.S.SST_STATE"), lifecycle_handlers=handlers
    ).run(changeset, previous, ApplyOptions())
    return changeset, store.state or previous

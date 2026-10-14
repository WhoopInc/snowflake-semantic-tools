"""Skills, plugins, semantic views, agents, and compile results small enough to build inline.

Shared by the compile, plan, smoke, and golden tests. `CHANNELS` is the part of a configuration
that publishes every skill and plugin as an extension, and every profile through a stage.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import TypeVar

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillFile
from tests.helpers.project_inputs import InMemoryProjectInputs
from tests.helpers.sql_values import authored

_Compiled = TypeVar("_Compiled")


def compiled_as(result: CompileResult, kind: type[_Compiled], index: int = 0) -> _Compiled:
    """Return the compiled artifact at `index`, asserted to be a `kind`.

    `CompileResult.compiled` holds the `CompiledArtifact` protocol; a test reading a kind's own
    fields narrows it here instead of casting, so a compile that built the wrong kind fails.
    """
    compiled = result.compiled[index]
    assert isinstance(compiled, kind), f"compiled[{index}] is {type(compiled).__name__}, not {kind.__name__}"
    return compiled


CHANNELS: Mapping[str, object] = {
    "skills": {"+bundle_stage": "BUNDLES"},
    "plugins": {"+bundle_stage": "BUNDLES"},
    "profiles": {"+stage": "PROFILES"},
}


def skill(name: str, description: str | None = "Close the month.") -> Skill:
    frontmatter = f"name: {name}\n" + (f"description: {description}\n" if description else "")
    content = f"---\n{frontmatter}---\nBody.\n".encode()
    return Skill(
        name, f"skills/{name}", name, description, "Body.\n", (SkillFile("SKILL.md", content),), Origin("SKILL.md")
    )


def plugin(name: str, *members: str) -> Plugin:
    manifest = f"plugins/{name}/plugin.yml"
    return Plugin(name, f"plugins/{name}", manifest, "Kit.", "Data", members, Origin(manifest))


def view(name: str) -> SemanticView:
    return SemanticView(
        fqn=f"DB.SCH.{name}",
        tables=(Table(logical_name="T", fqn="DB.SCH.T", primary_key=("ID",)),),
        columns=(Column(table="T", name="C", kind=ColumnKind.DIMENSION, expr=authored("T.C")),),
    )


def own_settings(**values: object) -> Mapping[str, Setting]:
    """An agent's settings as its own `config:` block sets them."""
    return MappingProxyType({key: Setting(key, value, "config") for key, value in values.items()})


def agent(name: str, *skills: str, enabled: bool = True, model: str = "auto") -> AgentModel:
    """An agent pinning `skills`; a `model` other than `auto` is set in its own `config:`."""
    references = tuple(AgentSkill("", "CORTEX_EXTENSION", item, "", ref="skill") for item in skills)
    settings = own_settings(orchestration_model=model) if model != "auto" else own_settings()
    return AgentModel(
        name,
        Origin("agent.yml"),
        ("agent.yml",),
        skills=references,
        enabled=enabled,
        orchestration_model=model,
        settings=settings,
    )


def compiled(*views: str, tree: dict[str, object] | None = None, agents: tuple[str, ...] = ()) -> CompileResult:
    inputs = InMemoryProjectInputs(
        tree=tree or {},
        views=SemanticViewProject(tuple(view(name) for name in views or ("SALES",))),
        agent_models=tuple(agent(name) for name in agents),
    )
    return CompileProject(inputs).run()


def with_diagnostics(result: CompileResult, *diagnostics: object) -> CompileResult:
    """`result` reporting exactly `diagnostics`."""
    return replace(result, diagnostics=DiagnosticBag(diagnostics))  # type: ignore[arg-type]

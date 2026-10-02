"""Skills, plugins, semantic views, agents, and compile results small enough to build inline.

Shared by the compile, plan, smoke, and golden tests. `CHANNELS` is a `skills:` block that
publishes through both channels, the catalog and a stage.
"""

from __future__ import annotations

from typing import TypeVar

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillFile
from tests.helpers.project_inputs import InMemoryProjectInputs

_Compiled = TypeVar("_Compiled")


def compiled_as(result: CompileResult, kind: type[_Compiled], index: int = 0) -> _Compiled:
    """Return the compiled artifact at `index`, asserted to be a `kind`.

    `CompileResult.compiled` holds the `CompiledArtifact` protocol; a test reading a kind's own
    fields narrows it here instead of casting, so a compile that built the wrong kind fails.
    """
    compiled = result.compiled[index]
    assert isinstance(compiled, kind), f"compiled[{index}] is {type(compiled).__name__}, not {kind.__name__}"
    return compiled


CHANNELS = {"catalog": {"+bundle_stage": "BUNDLES"}, "stage": {"+stage": "PROFILES"}}


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
        columns=(Column(table="T", name="C", kind=ColumnKind.DIMENSION, expr="T.C"),),
    )


def agent(name: str, *skills: str, enabled: bool = True, model: str = "auto") -> AgentModel:
    references = tuple(AgentSkill(item, "CORTEX_EXTENSION", item, "", ref="skill") for item in skills)
    return AgentModel(
        name, Origin("agent.yml"), ("agent.yml",), skills=references, enabled=enabled, orchestration_model=model
    )


def compiled(*views: str, tree: dict[str, object] | None = None, agents: tuple[str, ...] = ()) -> CompileResult:
    inputs = InMemoryProjectInputs(
        tree=tree or {},
        views=SemanticViewProject(tuple(view(name) for name in views or ("SALES",))),
        agent_models=tuple(agent(name) for name in agents),
    )
    return CompileProject(inputs).run()

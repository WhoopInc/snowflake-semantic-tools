"""Immutable Cortex Agent authoring and resolved specification values."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from .artifact_key import artifact_key
from .diagnostic import DiagnosticBag, Origin

BUILTIN_AGENT_TOOLS = frozenset(("data_to_chart", "web_search", "code_execution"))
KNOWN_AGENT_TOOL_TYPES = frozenset(
    (
        "cortex_analyst_text_to_sql",
        "cortex_search",
        *BUILTIN_AGENT_TOOLS,
        "generic",
        "mcp",
        "agent",
    )
)
RESERVED_AGENT_ALIASES = frozenset(("LIVE", "FIRST", "LAST", "DEFAULT"))


@dataclass(frozen=True, slots=True)
class AgentProfile:
    display_name: str | None = None
    avatar: str | None = None
    color: str | None = None


@dataclass(frozen=True, slots=True)
class AgentTool:
    type: str
    origin: Origin
    name: str | None = None
    description: str | None = None
    semantic_view: str | None = None
    backing: tuple[str, ...] = ()
    agent_ref: str | None = None
    warehouse: str | None = None
    query_timeout: int | None = None
    max_results: int | None = None
    title_column: str | None = None
    id_column: str | None = None
    stage_path: str | None = None
    relative_path_column: str | None = None
    filter: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    columns_and_descriptions: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    input_schema: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tool_spec_passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class AgentSkill:
    """One `skills:` entry of an agent.

    `ref` names the resolver its `source.path` used: `skill` or `plugin` for extensions this
    project publishes, `extension` for a consumed one. `name` is empty when omitted, which only
    a plugin allows.
    """

    name: str
    source_type: str
    path: str
    version: str
    ref: str = "extension"
    version_var: str | None = None


@dataclass(frozen=True, slots=True)
class AgentEvalFiles:
    origin: Origin
    dataset: str | None = None
    config: str | None = None


@dataclass(frozen=True, slots=True)
class AgentModel:
    name: str
    origin: Origin
    source_files: tuple[str, ...]
    comment: str | None = None
    secure: bool = False
    profile: AgentProfile = AgentProfile()
    orchestration_model: str = "auto"
    budget_seconds: int | None = None
    budget_tokens: int | None = None
    tool_not_accessible: str | None = None
    analytical_search: bool | None = None
    orchestration_instructions: str | None = None
    response_instructions: str | None = None
    sample_questions: tuple[str, ...] = ()
    tools: tuple[AgentTool, ...] = ()
    skills: tuple[AgentSkill, ...] = ()
    alias: str | None = None
    enabled: bool = True
    meta: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tags: tuple[tuple[str, str], ...] = ()
    passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    evals: AgentEvalFiles | None = None

    @property
    def key(self) -> str:
        return artifact_key("agent", self.name.casefold())


@dataclass(frozen=True, slots=True)
class ResolvedAgentTool:
    type: str
    name: str
    description: str
    resources: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    input_schema: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    depends_on: tuple[str, ...] = ()
    tool_spec_passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class ResolvedAgent:
    model: AgentModel
    tools: tuple[ResolvedAgentTool, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()
    external_dependencies: tuple[str, ...] = ()
    skill_dependencies: tuple[str, ...] = ()

    @property
    def depends_on(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    *(dependency for tool in self.tools for dependency in tool.depends_on),
                    *self.skill_dependencies,
                )
            )
        )

    @property
    def agent_facing_tool_names(self) -> frozenset[str]:
        return frozenset(tool.name for tool in self.tools)

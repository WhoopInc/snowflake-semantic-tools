"""Immutable Cortex Agent authoring and resolved specification values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key

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
    """An agent's `profile:` block: its display name, avatar, and color, each None when unset."""

    display_name: str | None = None
    avatar: str | None = None
    color: str | None = None


@dataclass(frozen=True, slots=True)
class AgentTool:
    """One `spec.tools:` entry of an agent, as authored, before it resolves.

    Attributes:
        type: The tool type as written; resolution reports one it does not know.
        name: None when unset, as an Analyst tool must leave it.
        semantic_view: The name an Analyst tool's `{{ semantic_view('<name>') }}` gives.
        backing: The arguments of the `{{ tool(...) }}` call that is the whole of
            `search_service:` or `identifier:`: a group and a member name, or a member name
            alone; () when there is no such call.
        agent_ref: The name a delegating tool's `{{ agent('<name>') }}` gives.
        passthrough: Keys added last to a search or generic tool's resources, so they override
            the computed ones; an MCP tool's resources are exactly these.
        tool_spec_passthrough: Keys added last to the rendered `tool_spec`, so they override it.
        declared_keys: Every key the entry writes, sorted; validation reports one the type does
            not take.
    """

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
    declared_keys: tuple[str, ...] = ()

    @property
    def member_reference(self) -> tuple[str, ...]:
        """The tool member this tool resolves to, as `ToolCatalog.resolve` takes it; () for none.

        That is its `{{ tool(...) }}` backing, or, for an `agent` tool that delegates to no agent of
        this project, its own name: such a tool resolves the member of that name.
        """
        if self.backing:
            return self.backing
        if self.type == "agent" and not self.agent_ref and self.name:
            return (self.name,)
        return ()


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
    """An agent's `evals:` block: where its eval dataset and config files are.

    Attributes:
        dataset: The dataset file, relative to the project root; None when it is missing or
            cannot be used, which loading reports.
        config: The config file, relative to the project root; None as for `dataset`.
    """

    origin: Origin
    dataset: str | None = None
    config: str | None = None


@dataclass(frozen=True, slots=True)
class AgentModel:
    """One agent as its `agent.yml` declares it, before its tools and skills resolve.

    Compile fills what the agent leaves unset from the `agents:` defaults, as the folder routes
    over its file override them: the orchestration model, budget, `tool_not_accessible`,
    `analytical_search`, alias, `secure`, and tags.

    Attributes:
        source_files: The `agent.yml` and each instruction file it reads, relative to the
            project root, in the order they were read.
        orchestration_model: "auto" when unset.
        secure: None when unset, which compile resolves from `agents.+secure`, else False.
        analytical_search: None when unset, which is not the same as False.
        orchestration_instructions: The instruction text, or the content of the file its
            `{{ file('<path>') }}` names; None when unset or the file cannot be used.
        response_instructions: As `orchestration_instructions`, for the response instructions.
        enabled: False leaves the agent out of the compiled project.
        meta: Free-form metadata; it counts toward the agent's definition fingerprint.
        tags: `(tag name, value)` pairs, in authored order; none takes `agents.+tags`.
        passthrough: Keys added last to the rendered spec, so they override the computed ones.
        evals: None when the agent declares no `evals:`.
        deprecated: The agent is retired from use; no alias may still point at a version of it.
        budget_tokens_documented: A comment on or just above `budget.tokens` says it bounds
            orchestration, so the budget is not read as a spend ceiling.
        folder: The agent file's directory below `project.agents_dir`, one segment per directory;
            the `agents:` folder routes along it apply to the agent.
    """

    name: str
    origin: Origin
    source_files: tuple[str, ...]
    comment: str | None = None
    secure: bool | None = None
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
    deprecated: bool = False
    budget_tokens_documented: bool = False
    folder: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """The agent's artifact key: `agent:` and its casefolded name."""
        return artifact_key("agent", self.name.casefold())


@dataclass(frozen=True, slots=True)
class ResolvedAgentTool:
    """One agent tool after resolution, ready to render into the spec's `tools` and `tool_resources`.

    Attributes:
        name: The name the agent calls the tool by.
        resources: The tool's `tool_resources` entry; never rendered for a built-in tool, nor
            when empty.
        depends_on: The artifact keys of the project artifacts it uses, such as the semantic
            view an Analyst tool queries; a referenced tool member adds none.
        tool_spec_passthrough: Keys added last to the rendered `tool_spec`, so they override it.
        member: The tool member the tool resolved through; None for a tool that names none.
        external: The object the tool calls is one SST does not publish: a referenced tool
            member, or an agent outside the project. Connected validation looks it up.
    """

    type: str
    name: str
    description: str
    resources: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    input_schema: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    depends_on: tuple[str, ...] = ()
    tool_spec_passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    member: str | None = None
    external: bool = False


@dataclass(frozen=True, slots=True)
class ResolvedAgent:
    """An agent whose tools and skills have resolved, with what resolving them reported.

    Attributes:
        model: The agent with its inherited defaults filled and its skills resolved.
        tools: The tools that resolved, in authored order; one that could not is absent.
        skill_dependencies: The artifact keys of this project's skills and plugins it pins,
            first reference first, each once.
    """

    model: AgentModel
    tools: tuple[ResolvedAgentTool, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()
    external_dependencies: tuple[str, ...] = ()
    skill_dependencies: tuple[str, ...] = ()

    @property
    def depends_on(self) -> tuple[str, ...]:
        """The artifact keys the agent publishes after: its tools' dependencies, then its skills'.

        Each key appears once, where it first occurs; `external_dependencies` is not included.
        """
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
        """The names the agent knows its resolved tools by."""
        return frozenset(tool.name for tool in self.tools)

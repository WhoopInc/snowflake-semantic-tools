"""Immutable Cortex Agent authoring and resolved specification values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.setting import Setting

BUILTIN_AGENT_TOOLS = frozenset(("data_to_chart", "web_search", "code_execution"))
KNOWN_AGENT_TOOL_TYPES = frozenset(
    (
        "cortex_analyst_text_to_sql",
        "cortex_search",
        *BUILTIN_AGENT_TOOLS,
        "generic",
        "agent_toolset",
    )
)
RESERVED_AGENT_ALIASES = frozenset(("LIVE", "FIRST", "LAST", "DEFAULT"))
# The types a search tool's column descriptor may give a column, lower-cased.
SEARCH_COLUMN_TYPES = frozenset(("string", "datetime"))


@dataclass(frozen=True, slots=True)
class AgentProfile:
    """An agent's `profile:` block: its display name, avatar, and color, each None when unset.

    Attributes:
        display_name: The name shown for the agent; None when unset.
        avatar: The agent's avatar; None when unset.
        color: The agent's color; None when unset.
    """

    display_name: str | None = None
    avatar: str | None = None
    color: str | None = None


@dataclass(frozen=True, slots=True)
class AgentTool:
    """One `spec.tools:` entry of an agent, as authored, before it resolves.

    Authored keys: `authored_schema.AGENT_TOOL`.

    Attributes:
        type: The tool type as written; resolution reports one it does not know.
        origin: Where the entry was read from.
        name: None when unset, as an Analyst tool must leave it.
        description: The tool's description as written; None when unset.
        semantic_view: The name an Analyst tool's `{{ semantic_view('<name>') }}` gives.
        backing: The member name the `{{ tool('<name>') }}` call that is the whole of
            `search_service:` or `identifier:` gives, an agent toolset's over an `agent`
            member included; None when there is no such call.
        title_column, id_column, relative_path_column: A search tool's own columns, as written:
            each a `{{ ref('<model>', '<column>') }}` over a defined member's `on:` model, or a
            bare column of a referenced member; resolution reads them against its member.
        columns: A search tool's own `columns:` entries, as written, which replace its
            member's; resolution reads each against its member as it reads the columns above.
        agent_ref: The name an agent toolset's `{{ agent('<name>') }}` gives.
        warehouse: The warehouse the tool runs on, as written; None when unset.
        query_timeout: The tool's own query timeout, in seconds; None when unset, so the compile context's applies.
            Validation reports one that is zero or less.
        max_results: The most results a search tool returns; None when unset.
        stage_path: A search tool's stage path, as written; resolution resolves it. None when unset.
        filter: A search tool's filter mapping as written; empty when none.
        input_schema: A generic tool's input schema as written; empty when none. Resolution requires
            its `type` to be `object` and checks it against the backing member's signature.
        passthrough: Keys added last to a search or generic tool's resources, so they override
            the computed ones; an MCP tool's resources are exactly these.
        tool_spec_passthrough: Keys merged last into the rendered `tool_spec`, as `merge_passthrough` merges.
        declared_keys: Every key the entry writes, sorted; validation reports one the type does
            not take.
    """

    type: str
    origin: Origin
    name: str | None = None
    description: str | None = None
    semantic_view: str | None = None
    backing: str | None = None
    agent_ref: str | None = None
    warehouse: str | None = None
    query_timeout: int | None = None
    max_results: int | None = None
    title_column: str | None = None
    id_column: str | None = None
    stage_path: str | None = None
    relative_path_column: str | None = None
    filter: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    columns: tuple[object, ...] = ()
    input_schema: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tool_spec_passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    declared_keys: tuple[str, ...] = ()

    @property
    def member_reference(self) -> str | None:
        """The name of the tool member this tool resolves to, as `ToolCatalog.resolve` takes it; None for none.

        That is its `{{ tool('<name>') }}` backing, written as `search_service:` or `identifier:`.
        """
        return self.backing


@dataclass(frozen=True, slots=True)
class AgentSkill:
    """One `skills:` entry of an agent.

    `ref` names the resolver its `source.path` used: `skill` or `plugin` for extensions this
    project publishes, `extension` for a consumed one. `name` is empty when omitted, which only
    a plugin allows.

    Attributes:
        name: The skill's name; empty when omitted.
        source_type: The `source.type` value as written, such as `STAGE` or `CORTEX_EXTENSION`.
        path: The name the `source.path` call gives, or empty when it is not a call.
        version: The literal `source.version`; empty when unset or given by a variable.
        ref: The function the `source.path` call used; "extension" when there is no call.
        version_var: The variable a templated `source.version` names; None for a literal version.
    """

    name: str
    source_type: str
    path: str
    version: str
    ref: str = "extension"
    version_var: str | None = None


@dataclass(frozen=True, slots=True)
class AgentEvalFiles:
    """An agent's `evals:` block, or a CoWork profile's: the eval dataset and config, by name.

    Attributes:
        origin: Where the `evals:` block was read from.
        dataset: The name `{{ eval_dataset('<name>') }}` gives; None when the value is missing
            or not that one call, which loading reports.
        config: The name `{{ eval_config('<name>') }}` gives; None as for `dataset`.
        folder: The agent's, or profile's, `evals/` folder, relative to the project root,
            which holds every dataset and config the names resolve against.
    """

    origin: Origin
    dataset: str | None = None
    config: str | None = None
    folder: str = ""


@dataclass(frozen=True, slots=True)
class AgentModel:
    """One agent as its `agent.yml` declares it, before its tools and skills resolve.

    Loading resolves the agent's `settings` -- its own `config:`, then the `agents:` folder
    routes over its file, then the block root -- and its `enabled` from them. Compile then sets
    the fields below that a setting gives, rendered for the target: the orchestration model,
    budget, `tool_not_accessible`, `analytical_search`, alias, `secure`, `allow_auto_model`, and
    tags. Until it does, each holds its default.

    Authored keys: `authored_schema.AGENT`.

    Attributes:
        name: The agent's name.
        origin: Where the agent file was read from.
        source_files: The `agent.yml` and each instruction file it reads, relative to the
            project root, in the order they were read.
        orchestration_model: "auto" when no setting names one.
        allow_auto_model: The agent keeps `auto` deliberately, so SST-VAL560 does not warn; False
            unless a setting says true.
        comment: The agent's `comment:` as written; None when unset.
        secure: None until compile resolves it; then False when no setting says true.
        profile: The agent's `profile:` block; every value None when it has none.
        budget_seconds, budget_tokens: The orchestration budget a setting gives; None when unset.
        tool_not_accessible: The value a `tool_not_accessible` setting gives; None when unset.
        analytical_search: None when unset, which is not the same as False.
        orchestration_instructions: The instruction text, or the content of the file its
            `{{ file('<path>') }}` names; None when unset or the file cannot be used.
        response_instructions: As `orchestration_instructions`, for the response instructions.
        sample_questions: The agent's sample questions, in authored order; empty when none.
        tools: The agent's `spec.tools:` entries as authored, in order.
        skills: The agent's `skills:` entries; compile replaces them with the resolved ones.
        alias: The alias a setting gives the agent; None when unset.
        enabled: False leaves the agent out of the compiled project.
        meta: Free-form metadata; it counts toward the agent's definition fingerprint.
        tags: `(tag name, value)` pairs, in the order the resolved `tags` setting lists them.
        passthrough: Keys merged last into the rendered spec, mapping into mapping (`merge_passthrough`).
        evals: None when the agent declares no `evals:`.
        deprecated: The agent is retired from use; no alias may still point at a version of it.
        budget_tokens_documented: A comment on or just above `config.budget_tokens` says it
            bounds orchestration, so the budget is not read as a spend ceiling.
        folder: The agent file's directory below `project.agents_dir`, one segment per directory;
            the `agents:` folder routes along it apply to the agent.
        settings: Each `agents:` setting the agent resolved, by key without its `+`, with where
            its value came from; compile adds the defaults it applies and renders the location.
    """

    name: str
    origin: Origin
    source_files: tuple[str, ...]
    comment: str | None = None
    secure: bool | None = None
    profile: AgentProfile = AgentProfile()
    orchestration_model: str = "auto"
    allow_auto_model: bool = False
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
    settings: Mapping[str, Setting] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def key(self) -> str:
        """The agent's artifact key: `agent:` and its casefolded name."""
        return artifact_key("agent", self.name.casefold())


@dataclass(frozen=True, slots=True)
class ResolvedAgentTool:
    """One agent tool after resolution, ready to render into the spec's `tools` and `tool_resources`.

    Attributes:
        type: The tool type.
        name: The name the agent calls the tool by.
        description: The tool's description.
        resources: The tool's `tool_resources` entry; never rendered for a built-in tool, nor
            when empty.
        input_schema: The tool's input schema; empty when it has none.
        depends_on: The artifact keys of the project artifacts it uses, such as the semantic
            view an Analyst tool queries; a referenced tool member adds none.
        tool_spec_passthrough: Keys merged last into the rendered `tool_spec`, as `merge_passthrough` merges.
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
        diagnostics: What resolving the tools, agent-wide rules, and skills reported.
        external_dependencies: Artifact keys kept apart from `depends_on`; empty by default.
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

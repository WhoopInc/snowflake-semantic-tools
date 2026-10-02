"""What the agent compiler resolves against: the project around the agents it compiles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog


@dataclass(frozen=True, slots=True)
class AgentCompileContext:
    """Everything outside the agents that compiling them reads, and the `agents:` defaults.

    Attributes:
        semantic_views: The views that compiled, by casefolded name; an Analyst tool over
            any other view is SST-REF011.
        agents: Every enabled agent's published name, by casefolded name.
        extensions: `skills.extensions` entries, by casefolded name.
        variables: What `{{ var('<name>') }}` resolves to in a skill's version.
        database, schema: Where agents publish, and where a tool an agent names from a
            `define:` member is taken to be.
        warehouse, query_timeout: The execution environment a tool gets when it sets none.
        orchestration_model, budget_seconds, budget_tokens, tool_not_accessible,
            analytical_search, alias: What an agent inherits when it sets none; an
            orchestration model of `auto` also inherits.
        allowed_models: `snowflake.orchestration_models`, the gate for SST-VAL543.
        skills, plugins: What `skill()` and `plugin()` references pin to, by name.
    """

    semantic_views: Mapping[str, QualifiedName]
    tools: ToolCatalog
    agents: Mapping[str, QualifiedName]
    extensions: Mapping[str, QualifiedName]
    variables: Mapping[str, str]
    database: str
    schema: str
    warehouse: str | None
    query_timeout: int | None
    orchestration_model: str
    budget_seconds: int | None
    budget_tokens: int | None
    tool_not_accessible: str | None
    analytical_search: bool | None
    alias: str | None
    allowed_models: frozenset[str]
    skills: Mapping[str, ExtensionPin] = field(default_factory=lambda: MappingProxyType({}))
    plugins: Mapping[str, ExtensionPin] = field(default_factory=lambda: MappingProxyType({}))
    # Skills a plugin or profile already consumes, so SST-VAL804 does not report them.
    consumed: frozenset[str] = frozenset()
    # Declared extensions with no version to pin -- `skill:<name>`, `plugin:<name>`,
    # or `extension:<name>` -- mapped to the reason, so a reference to one names
    # the cause instead of reporting the name as undeclared.
    unpublished: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    # `snowflake.allow_unknown_keys`: whether a key SST does not model renders with a warning.
    allow_unknown_keys: bool = True
    # `snowflake.profile.avatar_allowlist`; None when it is not configured (SST-VAL548).
    avatar_allowlist: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class ExtensionPin:
    """An extension this project publishes, as an agent reference resolves it.

    Attributes:
        key: The extension's artifact key, `skill:<name>` or `plugin:<name>`.
        alias: The published version an agent reference is pinned to.
        members: The skills the extension carries: the skill itself, or a plugin's members.
        scripts: Each script a carried skill ships, as the skill and the path inside it;
            only an agent with a code_execution tool can run one (SST-VAL814).
    """

    key: str
    target: QualifiedName
    alias: str
    members: tuple[str, ...] = ()
    scripts: tuple[tuple[str, str], ...] = ()

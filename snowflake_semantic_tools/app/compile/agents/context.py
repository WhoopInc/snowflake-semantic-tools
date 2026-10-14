"""What the agent compiler resolves against: the project around the agents it compiles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolCatalog


@dataclass(frozen=True, slots=True)
class AgentCompileContext:
    """Everything outside the agents that compiling them reads, and the tools' execution environment.

    Compiling a project gives each agent its own context, with the warehouse and query timeout
    its settings resolve to; the rest is shared.

    Attributes:
        semantic_views: The views that compiled, by casefolded name; an Analyst tool over
            any other view is SST-REF011.
        agents: Every enabled agent's published name, by casefolded name, each in the location
            the folder routes over its file resolve to.
        extensions: `skills.extensions` entries, by casefolded name.
        variables: What `{{ var('<name>') }}` resolves to in a skill's version: the project's
            `vars:`, and `sha_version`.
        database, schema: The `agents:` block's own location: where an agent `agents` does not
            name publishes, and where a `define:` member `tool_locations` does not place is
            taken to be.
        tool_locations: Each `define:` member's database and schema, by casefolded member
            name: the `tools:` block's location, or its group's `tools.<group>` override.
        warehouse, query_timeout: The execution environment a tool gets when it sets none: the
            agent's resolved `warehouse`, else the target's, and its `query_timeout`.
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
    # `snowflake.tool_types`: tool types this project adds to the ones SST renders (SST-RND012).
    extra_tool_types: frozenset[str] = frozenset()
    tool_locations: Mapping[str, tuple[str, str]] = field(default_factory=lambda: MappingProxyType({}))

    def agent_target(self, name: str) -> QualifiedName:
        """Return where the agent named `name` publishes: its entry in `agents`, else the block's location."""
        return self.agents.get(name.casefold()) or QualifiedName.from_parts(self.database, self.schema, name)

    def tool_target(self, name: str) -> QualifiedName:
        """Return where the `define:` member named `name` publishes.

        That is its `tool_locations` entry, else the block's location.

        Raises:
            ValueError: a part of the name is not a valid identifier.
        """
        database, schema = self.tool_locations.get(name.casefold(), (self.database, self.schema))
        return QualifiedName.from_parts(database, schema, name)


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

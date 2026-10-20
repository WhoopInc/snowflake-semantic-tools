"""Immutable Snowflake CoWork profile authoring values, before the agents a profile names resolve.

CoWork profiles are a Snowflake private preview (PrPr) feature. A profile is a Cortex Agent whose
one router tool hands each question to one of the agents it lists; these values follow the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentProfile, AgentSkill
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.setting import Setting

COWORK_PROFILES_DOC = "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles"
COWORK_ROUTING_DOC = "https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing"
# The tools a profile may carry beside its router; every other tool belongs on an agent.
COWORK_PROFILE_TOOLS = frozenset(("web_search", "code_execution"))
# The name SST gives the profile's one `agent_router` tool, and its `tool_resources` entry.
ROUTER_NAME = "router"
# The only schema CoWork lists profiles from.
COWORK_PROFILE_SCHEMA = ("SNOWFLAKE_INTELLIGENCE", "PROFILES")


@dataclass(frozen=True, slots=True)
class CoworkSubagent:
    """One `spec.subagents:` entry of a CoWork profile: an agent it may route a question to.

    Attributes:
        reference: The template function the entry's `agent:` calls: `agent` for an agent this
            project publishes, `tool` for an `agent` tool member.
        name: The name that call gives, as written.
        display_name: None when unset, which leaves the agent's own to apply.
        description: None when unset, as for `display_name`.
        sample_questions: None when the entry writes none, so the agent's own apply; `()` when
            it writes an empty list, so the subagent offers none.
    """

    reference: str
    name: str
    origin: Origin
    display_name: str | None = None
    description: str | None = None
    sample_questions: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class CoworkProfileTool:
    """One `spec.tools:` entry of a CoWork profile, as authored.

    Attributes:
        type: The tool type as written; validation reports one a profile does not carry.
        name: None when unset.
    """

    type: str
    name: str | None
    origin: Origin


@dataclass(frozen=True, slots=True)
class CoworkProfileModel:
    """One CoWork profile as its `cowork_profile.yml` declares it, before its subagents resolve.

    Loading resolves the profile's `settings` -- its own `config:`, then the `cowork_profiles:`
    folder routes over its file, then the block root -- and its `enabled` from them. The fields
    a setting gives (the orchestration model, alias, and tags) hold their defaults until compile
    sets them.

    Attributes:
        source_files: The `cowork_profile.yml` and each instruction file it reads, relative to
            the project root, in the order they were read.
        display: The `display:` block, read as an agent's `profile:` block is.
        orchestration_model: "auto" when no setting names one.
        orchestration_instructions: The instruction text, or the content of the file its
            `{{ file('<path>') }}` names; None when unset or the file cannot be used.
        response_instructions: As `orchestration_instructions`, for the response instructions.
        subagents: In authored order; an entry that could not be read is absent.
        enabled: False leaves the profile out of the compiled project.
        meta: Free-form metadata; it counts toward the profile's definition fingerprint.
        tags: `(tag name, value)` pairs, in the order the resolved `tags` setting lists them.
        passthrough: Keys added last to the rendered spec, so they override the computed ones.
        deprecated: The profile is retired from use; no alias may still point at a version of it.
        folder: The profile file's directory below `project.cowork_profiles_dir`, one segment per
            directory; the `cowork_profiles:` folder routes along it apply to the profile.
        settings: Each `cowork_profiles:` setting the profile resolved, by key without its `+`,
            with where its value came from.
    """

    name: str
    origin: Origin
    source_files: tuple[str, ...]
    comment: str | None = None
    display: AgentProfile = AgentProfile()
    orchestration_model: str = "auto"
    orchestration_instructions: str | None = None
    response_instructions: str | None = None
    sample_questions: tuple[str, ...] = ()
    subagents: tuple[CoworkSubagent, ...] = ()
    tools: tuple[CoworkProfileTool, ...] = ()
    skills: tuple[AgentSkill, ...] = ()
    alias: str | None = None
    enabled: bool = True
    meta: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tags: tuple[tuple[str, str], ...] = ()
    passthrough: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    deprecated: bool = False
    folder: tuple[str, ...] = ()
    settings: Mapping[str, Setting] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def key(self) -> str:
        """The profile's artifact key: `cowork_profile:` and its casefolded name."""
        return artifact_key("cowork_profile", self.name.casefold())

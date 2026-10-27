"""Immutable Cortex Code agent profile values: as authored, and once their skills are pinned.

A Cortex Code agent profile is a Snowflake private preview (PrPr) feature. A profile is an AGENT
object in CORTEX_CODE.CONFIG whose specification holds only an orchestration model, response
instructions, and skills; the Cortex Code CLI, Desktop, and Snowsight list the profiles the
current role can use, and a role that sees several uses the one named DEFAULT.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.setting import Setting

# The folder of the layer every profile carries, which is never a profile itself.
SHARED_PROFILE = "shared"
# The profile Cortex Code applies to a role that sees more than one.
DEFAULT_PROFILE = "default"
# The only schema Cortex Code lists agent profiles from.
COCO_PROFILE_SCHEMA = ("CORTEX_CODE", "CONFIG")
# A `+name_suffix` keeps the profile's AGENT name a plain identifier.
NAME_SUFFIX_PATTERN = r"[A-Za-z0-9_]{0,64}"
# The keys agent profiles cannot carry, each with what to do instead.
REJECTED_COCO_PROFILE_KEYS: Mapping[str, str] = MappingProxyType(
    {
        "owner_team": "an agent profile has no owner field; say who owns it in description",
        "hooks": "Cortex Code agent profiles cannot carry hooks",
        "mcp_servers": "Cortex Code agent profiles cannot carry MCP servers",
        "commands": "Cortex Code agent profiles cannot carry commands; make each one a skill",
        "allowed_roles": "grant USAGE on the profile's AGENT outside SST",
    }
)


def owned_entries(ref: str, names: tuple[str, ...]) -> tuple[AgentSkill, ...]:
    """Return one skill entry per name, as an agent's `{{ <ref>('<name>') }}` source path authors it."""
    return tuple(AgentSkill(name="", source_type="CORTEX_EXTENSION", path=name, version="", ref=ref) for name in names)


def listed_by_cortex_code(target: QualifiedName) -> bool:
    """Report whether Cortex Code lists a profile published at `target`: in CORTEX_CODE.CONFIG.

    The database and schema are compared as Snowflake resolves them; a profile anywhere else is a
    rehearsal of one.
    """
    return (target.database.folded, target.schema.folded) == COCO_PROFILE_SCHEMA


@dataclass(frozen=True, slots=True)
class SharedCocoProfile:
    """The `shared/` layer every profile carries: instructions first, then rules, then skills.

    Attributes:
        prompt: The layer's `AGENTS.md`; None when it has none or it cannot be read.
        rules: `(file name, text)` for each `rules/*.md`, in file-name order.
        skills: The skill names its `coco_profile.yml` lists, as authored.
        source_files: The manifest, `AGENTS.md`, then the rules, each when present.
    """

    prompt: str | None
    rules: tuple[tuple[str, str], ...]
    skills: tuple[str, ...]
    origin: Origin
    source_files: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CocoProfileModel:
    """One agent profile as its folder declares it, before its skills are pinned.

    Loading resolves the profile's `settings` -- its own `config:`, then the `coco_profiles:`
    route of its folder, then the block root -- and its `enabled` from them; compile sets
    `published_name` from `+name_suffix`.

    Attributes:
        comment: The `description:`, which becomes the AGENT's comment.
        orchestration_model: The `model:`; None leaves `models.orchestration` unset.
        prompt: The folder's `AGENTS.md`; None when it has none or it cannot be read.
        skills: Each `skills:` entry as a `skill()` entry, then each `plugins:` entry as a
            `plugin()` entry, as an agent's skills are authored.
        source_files: The manifest, then `AGENTS.md` when present, relative to the project root.
        settings: Each `coco_profiles:` setting the profile resolved, by key without its `+`.
        published_name: The AGENT's name: the profile's, with its `+name_suffix`.
    """

    name: str
    origin: Origin
    source_files: tuple[str, ...]
    comment: str | None = None
    orchestration_model: str | None = None
    prompt: str | None = None
    skills: tuple[AgentSkill, ...] = ()
    enabled: bool = True
    settings: Mapping[str, Setting] = field(default_factory=lambda: MappingProxyType({}))
    published_name: str = ""

    @property
    def key(self) -> str:
        """The profile's artifact key: `coco_profile:` and its casefolded name."""
        return artifact_key("coco_profile", self.name.casefold())


@dataclass(frozen=True, slots=True)
class CocoProfileCatalog:
    """Every profile a project declares, in folder order, and the shared layer they carry."""

    profiles: tuple[CocoProfileModel, ...]
    shared: SharedCocoProfile | None


@dataclass(frozen=True, slots=True)
class ResolvedCocoProfile:
    """A profile whose skills are pinned and whose instructions are assembled, ready to render.

    Attributes:
        model: The profile with every skill entry, its own and the shared layer's, pinned as an
            agent's are.
        instructions: The response instructions: the shared `AGENTS.md`, its rules, then the
            profile's `AGENTS.md`; empty when none has text.
        skill_dependencies: The artifact keys of this project's skills and plugins it pins,
            first reference first, each once.
    """

    model: CocoProfileModel
    instructions: str
    skill_dependencies: tuple[str, ...] = ()

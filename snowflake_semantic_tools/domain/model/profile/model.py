"""CoCo Desktop profile records: what a profile names, and the release it compiles to.

A profile is one folder under `profiles_dir`; `shared/` is the reserved layer every profile
carries. Hooks, MCP configs, and commands are defined once each and named by the profiles
that use them. A `ProfileRelease` is the compiled form: content-addressed `StageTree`s plus
the one registry row whose pointers name them.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

from ..artifact_key import artifact_key
from ..diagnostic import DiagnosticBag, Origin
from ..skill import BundleEntry, SkillFile, bundle_digest

SHARED_PROFILE = "shared"
DESKTOP_REGISTRY = "CORTEX_CODE.CONFIG.PROFILE_REGISTRY"
TREE_HASH_CHARACTERS = 12
# Keys the pipeline's profile.yml carried that SST refuses, with the reason.
REJECTED_PROFILE_KEYS: Mapping[str, str] = {
    "allowed_roles": "access is managed outside SST; Desktop never reads the column",
    "active": "delete the profile and apply with --prune to deactivate it",
    "env_vars": "environment variables change every user's local environment, so SST does not publish them",
    "settings_overrides": "settings overrides change every user's local settings, so SST does not publish them",
}
# Frontmatter a Desktop command may carry; Desktop ignores anything else.
COMMAND_FRONTMATTER_KEYS = frozenset(("description", "allowed-tools", "skill", "hidden"))


@dataclass(frozen=True, slots=True)
class HookDefinition:
    """One command hook, `<hooks_dir>/<name>/`: Desktop runs `command` on the script for `event`.

    Attributes:
        script: The one script the hook runs, by its path inside the hook folder.
        matcher: The matcher the hook is grouped under within its event; None for the group with none.
        timeout: Copied into the hook's registry entry; None leaves the key out.
        interactive: Copied into the hook's registry entry; None leaves the key out.
    """

    name: str
    directory: str
    event: str
    command: str
    script: SkillFile
    origin: Origin
    matcher: str | None = None
    timeout: int | None = None
    interactive: bool | None = None
    description: str | None = None

    @property
    def tree_path(self) -> str:
        """Place the script in a profile's hooks tree: `<name>/<script path>`."""
        return f"{self.name}/{self.script.path}"


@dataclass(frozen=True, slots=True)
class McpConfig:
    """One MCP config, `<mcp_servers_dir>/<name>/mcp.json`, and the servers it defines.

    Attributes:
        file: The `mcp.json`, relative to the project root.
        servers: The `mcpServers` object as written: each server name to its definition.
    """

    name: str
    file: str
    servers: Mapping[str, object]
    origin: Origin


@dataclass(frozen=True, slots=True)
class CommandFile:
    """One slash command, `<commands_dir>/<path>`, which Desktop loads as `/<name>`.

    A profile names it by its path without `.md`; Desktop joins nested folders with
    `:`, so `sql/check.md` is `sql/check` here and `/sql:check` in Desktop.
    """

    path: str
    content: bytes
    file: str
    origin: Origin
    skill: str | None = None

    @property
    def name(self) -> str:
        """Name the command as a profile does: its path without `.md`."""
        return self.path.removesuffix(".md")

    @property
    def key(self) -> str:
        """Name the command as an artifact key, `command:<name>`."""
        return artifact_key("command", self.name)


@dataclass(frozen=True, slots=True)
class DesktopProfile:
    """One profile folder: the skills, plugins, commands, hooks, and MCP configs it names.

    Attributes:
        name: The folder name, which is also the registry row's CONFIG_NAME.
        prompt: The folder's own `AGENTS.md`; None when it has none.
        source_files: The profile's own files, relative to the project root.
    """

    name: str
    directory: str
    description: str | None
    owner_team: str | None
    skills: tuple[str, ...]
    mcp_servers: tuple[str, ...]
    hooks: tuple[str, ...]
    prompt: str | None
    origin: Origin
    source_files: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()
    plugins: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """Name the profile as an artifact key, `profile:<name>`."""
        return artifact_key("profile", self.name)


@dataclass(frozen=True, slots=True)
class SharedProfile:
    """`profiles_dir/shared/`: the prompt, rules, skills, and commands every profile carries.

    Attributes:
        rules: (file name, text) for each `rules/*.md`; the prompt joins them in name order.
    """

    prompt: str | None
    rules: tuple[tuple[str, str], ...]
    skills: tuple[str, ...]
    origin: Origin
    source_files: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProfileCatalog:
    """Every profile of a project, the shared layer, and the definitions profiles name.

    Attributes:
        shared: The `shared/` folder; None when the project has none.
        diagnostics: What loading reported. An error here marks its subject as broken, so a
            profile naming it is not also told it is unknown.
    """

    profiles: tuple[DesktopProfile, ...] = ()
    shared: SharedProfile | None = None
    hooks: tuple[HookDefinition, ...] = ()
    mcp_configs: tuple[McpConfig, ...] = ()
    diagnostics: DiagnosticBag = DiagnosticBag()
    commands: tuple[CommandFile, ...] = ()


@dataclass(frozen=True, slots=True)
class StageTree:
    """One content-addressed tree: `<kind>/<scope>/<H>/...`, H being its own digest."""

    kind: str
    scope: str
    entries: tuple[BundleEntry, ...]

    @property
    def digest(self) -> str:
        """Address the tree by its content, as a bundle is."""
        return bundle_digest(self.entries)

    @property
    def prefix(self) -> str:
        """Locate the tree below the stage root: `<kind>/<scope>/<12 hex digest characters>/`."""
        return f"{self.kind}/{self.scope}/{self.digest[:TREE_HASH_CHARACTERS]}/"

    @property
    def paths(self) -> tuple[str, ...]:
        """List the entries' paths inside the tree, in entry order."""
        return tuple(entry.path for entry in self.entries)


@dataclass(frozen=True, slots=True)
class ProfileRelease:
    """Everything one profile publishes: its trees and the registry row naming them.

    Attributes:
        version: The row's VERSION: the version prefix, then a digest of the other columns.
        trees: In upload order: skills, prompt, MCP config, hooks, commands, plugins.
        source_files: Every authored file the release is built from, in first-use order.
    """

    name: str
    version: str
    trees: tuple[StageTree, ...]
    row: Mapping[str, object]
    source_files: tuple[str, ...]

    @property
    def key(self) -> str:
        """Name the profile as an artifact key, `profile:<name>`."""
        return artifact_key("profile", self.name)

    def document(self) -> str:
        """Canonical JSON of the row and the trees: the payload plan and goldens compare."""
        value = {
            "row": dict(self.row),
            "trees": [
                {
                    "prefix": tree.prefix,
                    "digest": tree.digest,
                    "files": [
                        {"path": entry.path, "sha256": entry.sha256, "size": entry.size} for entry in tree.entries
                    ],
                }
                for tree in self.trees
            ],
        }
        return json.dumps(value, indent=2, sort_keys=True) + "\n"

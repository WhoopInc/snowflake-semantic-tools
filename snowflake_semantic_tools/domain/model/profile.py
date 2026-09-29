"""CoCo Desktop profiles: content-addressed stage trees and one registry row each.

A profile publishes as trees below the fixed `by_type` prefixes Desktop reads --
`skills/`, `prompts/`, `mcp/`, `hooks/`, `commands/`, `plugins/` -- each named by
the digest of its own content, then one registry row whose pointers name those
trees. Uploading a new tree never touches a tree a live row points at, so the row
write is the atomic switch and the previous trees remain for rollback.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256

from .diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from .skill import BundleEntry, Plugin, Skill, SkillFile, build_plugin_bundle, bundle_digest
from .stage_path import ALLOWED_DESCRIPTION, unsafe_segment

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
_NAME = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")
_PLACEHOLDER = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*\}$")
_CREDENTIAL_KEY = re.compile(r"(?i)(token|secret|password|passwd|api[_-]?key|private[_-]?key)")


@dataclass(frozen=True, slots=True)
class HookDefinition:
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


@dataclass(frozen=True, slots=True)
class McpConfig:
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
        return self.path.removesuffix(".md")

    @property
    def key(self) -> str:
        return f"command:{self.name}"

    @property
    def desktop_name(self) -> str:
        return "/" + self.name.replace("/", ":")


@dataclass(frozen=True, slots=True)
class DesktopProfile:
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
        return f"profile:{self.name}"


@dataclass(frozen=True, slots=True)
class SharedProfile:
    """`profiles_dir/shared/`: the prompt, rules, skills, and commands every profile carries."""

    prompt: str | None
    rules: tuple[tuple[str, str], ...]
    skills: tuple[str, ...]
    origin: Origin
    source_files: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProfileCatalog:
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
        return bundle_digest(self.entries)

    @property
    def prefix(self) -> str:
        return f"{self.kind}/{self.scope}/{self.digest[:TREE_HASH_CHARACTERS]}/"

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(entry.path for entry in self.entries)


@dataclass(frozen=True, slots=True)
class ProfileRelease:
    """Everything one profile publishes: its trees and the registry row naming them."""

    name: str
    version: str
    trees: tuple[StageTree, ...]
    row: Mapping[str, object]
    source_files: tuple[str, ...]

    @property
    def key(self) -> str:
        return f"profile:{self.name}"

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


def assemble_prompt(shared: SharedProfile | None, profile: DesktopProfile) -> str | None:
    """Shared `AGENTS.md`, then shared rules in name order, then the profile's own `AGENTS.md`."""
    parts = [
        *((shared.prompt or "",) if shared is not None else ()),
        *(text for _, text in (sorted(shared.rules) if shared is not None else ())),
        profile.prompt or "",
    ]
    kept = [part.strip() for part in parts if part.strip()]
    return "\n\n".join(kept) + "\n" if kept else None


def validate_profile_catalog(
    catalog: ProfileCatalog,
    skills: Mapping[str, Skill],
    plugins: Mapping[str, Plugin] | None = None,
) -> DiagnosticBag:
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    # A hook, MCP, or command file that failed to load already carries its own
    # error; it is defined, so a profile naming it is not also told it is unknown.
    broken = {item.subject for item in catalog.diagnostics if item.severity is Severity.ERROR}
    hooks = {hook.name for hook in catalog.hooks}
    configs = {config.name: config for config in catalog.mcp_configs}
    commands = {command.name for command in catalog.commands}
    plugins = plugins or {}
    shared_skills = set(catalog.shared.skills) if catalog.shared is not None else set()
    shared_commands = set(catalog.shared.commands) if catalog.shared is not None else set()
    if catalog.shared is not None:
        for name in catalog.shared.skills:
            if name not in skills:
                diagnostics.append(
                    D(
                        "SST-VAL844",
                        origin=catalog.shared.origin,
                        subject="profile:shared",
                        artifact="shared",
                        name=name,
                    )
                )
        for name in catalog.shared.commands:
            if name not in commands and f"command:{name}" not in broken:
                diagnostics.append(
                    D(
                        "SST-VAL858",
                        origin=catalog.shared.origin,
                        subject="profile:shared",
                        artifact="shared",
                        name=name,
                    )
                )
    for profile in catalog.profiles:
        subject = profile.key
        if profile.name == SHARED_PROFILE or not _NAME.fullmatch(profile.name):
            diagnostics.append(
                D(
                    "SST-VAL801",
                    origin=profile.origin,
                    subject=subject,
                    artifact=subject,
                    detail=f"profile name '{profile.name}' must be lowercase letters, digits, '-' or '_', and not 'shared'",
                )
            )
        for name in profile.skills:
            if name not in skills:
                diagnostics.append(
                    D("SST-VAL844", origin=profile.origin, subject=subject, artifact=profile.name, name=name)
                )
            elif name in shared_skills:
                diagnostics.append(
                    D(
                        "SST-VAL845",
                        origin=profile.origin,
                        subject=subject,
                        artifact=profile.name,
                        kind="skill",
                        name=name,
                    )
                )
        for name in profile.commands:
            if name not in commands:
                if f"command:{name}" not in broken:
                    diagnostics.append(
                        D("SST-VAL858", origin=profile.origin, subject=subject, artifact=profile.name, name=name)
                    )
            elif name in shared_commands:
                diagnostics.append(
                    D(
                        "SST-VAL845",
                        origin=profile.origin,
                        subject=subject,
                        artifact=profile.name,
                        kind="command",
                        name=name,
                    )
                )
        for name in profile.plugins:
            if name not in plugins:
                diagnostics.append(
                    D("SST-VAL860", origin=profile.origin, subject=subject, artifact=profile.name, name=name)
                )
        for name in profile.hooks:
            if name not in hooks and f"hook:{name}" not in broken:
                diagnostics.append(
                    D("SST-VAL846", origin=profile.origin, subject=subject, artifact=profile.name, name=name)
                )
        servers: dict[str, str] = {}
        for name in profile.mcp_servers:
            config = configs.get(name)
            if config is None:
                if f"mcp:{name}" not in broken:
                    diagnostics.append(
                        D("SST-VAL847", origin=profile.origin, subject=subject, artifact=profile.name, name=name)
                    )
                continue
            for server, value in config.servers.items():
                if not isinstance(value, Mapping):
                    diagnostics.append(
                        D("SST-VAL848", origin=config.origin, subject=subject, artifact=profile.name, name=server)
                    )
                other = servers.get(server)
                if other is not None:
                    diagnostics.append(
                        D(
                            "SST-VAL849",
                            origin=config.origin,
                            subject=subject,
                            artifact=profile.name,
                            name=server,
                            a=other,
                            b=config.name,
                        )
                    )
                servers[server] = config.name
    for config in catalog.mcp_configs:
        for server, key in _literal_credentials(config.servers):
            diagnostics.append(
                D(
                    "SST-VAL850",
                    origin=config.origin,
                    subject=f"mcp:{config.name}",
                    artifact=config.name,
                    name=server,
                    key=key,
                )
            )
    for hook in catalog.hooks:
        path = f"{hook.name}/{hook.script.path}"
        unsafe = unsafe_segment(path)
        if unsafe is not None:
            diagnostics.append(
                D(
                    "SST-VAL857",
                    origin=hook.origin,
                    subject=f"hook:{hook.name}",
                    artifact=f"hook:{hook.name}",
                    value=path,
                    found=unsafe,
                    expected=ALLOWED_DESCRIPTION,
                )
            )
    for command in catalog.commands:
        unsafe = unsafe_segment(command.path)
        if unsafe is not None:
            diagnostics.append(
                D(
                    "SST-VAL857",
                    origin=command.origin,
                    subject=command.key,
                    artifact=command.key,
                    value=command.path,
                    found=unsafe,
                    expected=ALLOWED_DESCRIPTION,
                )
            )
    return DiagnosticBag(diagnostics)


def _literal_credentials(servers: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    found: list[tuple[str, str]] = []

    def walk(server: str, value: object, key: str) -> None:
        if isinstance(value, Mapping):
            for child_key, child in value.items():
                walk(server, child, str(child_key))
        elif isinstance(value, list):
            for item in value:
                walk(server, item, key)
        elif isinstance(value, str) and _CREDENTIAL_KEY.search(key) and value and not _PLACEHOLDER.fullmatch(value):
            found.append((server, key))

    for server, value in servers.items():
        walk(server, value, "")
    return tuple(found)


def _skill_tree(scope: str, names: tuple[str, ...], skills: Mapping[str, Skill]) -> StageTree:
    entries = tuple(
        sorted(
            (
                BundleEntry(f"{name}/{item.path}", item.content)
                for name in dict.fromkeys(names)
                if name in skills
                for item in skills[name].files
            ),
            key=lambda entry: entry.path,
        )
    )
    return StageTree("skills", scope, entries)


def _command_tree(scope: str, names: tuple[str, ...], commands: Mapping[str, CommandFile]) -> StageTree:
    entries = tuple(
        sorted(
            (
                BundleEntry(commands[name].path, commands[name].content)
                for name in dict.fromkeys(names)
                if name in commands
            ),
            key=lambda entry: entry.path,
        )
    )
    return StageTree("commands", scope, entries)


def _plugin_tree(
    scope: str, names: tuple[str, ...], plugins: Mapping[str, Plugin], skills: Mapping[str, Skill]
) -> tuple[StageTree, tuple[str, ...]]:
    """Each plugin's own bundle under `<plugin>/`, byte for byte what its extension publishes."""
    entries: list[BundleEntry] = []
    included: list[str] = []
    for name in dict.fromkeys(names):
        plugin = plugins.get(name)
        bundle = build_plugin_bundle(plugin, skills)[0] if plugin is not None else None
        if bundle is None:
            continue
        entries.extend(BundleEntry(f"{name}/{entry.path}", entry.content) for entry in bundle.entries)
        included.append(name)
    return StageTree("plugins", scope, tuple(sorted(entries, key=lambda entry: entry.path))), tuple(included)


def build_profile(
    profile: DesktopProfile,
    catalog: ProfileCatalog,
    skills: Mapping[str, Skill],
    *,
    stage: str,
    version_prefix: str,
    plugins: Mapping[str, Plugin] | None = None,
) -> ProfileRelease:
    """Render the nested trees and the registry row one profile publishes."""
    shared = catalog.shared
    shared_names = shared.skills if shared is not None else ()
    own = tuple(name for name in profile.skills if name not in shared_names)
    plugins = plugins or {}
    trees: list[StageTree] = []
    skill_repos: list[dict[str, str]] = []
    for tree in (_skill_tree(SHARED_PROFILE, shared_names, skills), _skill_tree(profile.name, own, skills)):
        if tree.entries:
            trees.append(tree)
            skill_repos.append({"snowflake_stage": f"@{stage}/{tree.prefix}"})
    prompt_pointer: dict[str, str] | None = None
    prompt = assemble_prompt(shared, profile)
    if prompt is not None:
        tree = StageTree("prompts", profile.name, (BundleEntry("AGENTS.md", prompt.encode("utf-8")),))
        trees.append(tree)
        prompt_pointer = {"snowflake_stage": f"@{stage}/{tree.prefix}AGENTS.md"}
    mcp_pointer: dict[str, str] = {}
    configs = {config.name: config for config in catalog.mcp_configs}
    merged: dict[str, object] = {}
    for name in profile.mcp_servers:
        config = configs.get(name)
        if config is not None:
            merged.update(config.servers)
    if merged:
        content = json.dumps({"mcpServers": merged}, indent=2, sort_keys=True) + "\n"
        tree = StageTree("mcp", profile.name, (BundleEntry("mcp.json", content.encode("utf-8")),))
        trees.append(tree)
        mcp_pointer = {"snowflake_stage": f"@{stage}/{tree.prefix}mcp.json"}
    hooks_by_name = {hook.name: hook for hook in catalog.hooks}
    selected = tuple(hooks_by_name[name] for name in dict.fromkeys(profile.hooks) if name in hooks_by_name)
    events: dict[str, list[dict[str, object]]] = {}
    if selected:
        tree = StageTree(
            "hooks",
            profile.name,
            tuple(
                sorted(
                    (BundleEntry(f"{hook.name}/{hook.script.path}", hook.script.content) for hook in selected),
                    key=lambda entry: entry.path,
                )
            ),
        )
        trees.append(tree)
        for hook in selected:
            entry: dict[str, object] = {
                "type": "command",
                "command": hook.command,
                "source": {"snowflake_stage": f"@{stage}/{tree.prefix}{hook.name}/{hook.script.path}"},
            }
            if hook.timeout is not None:
                entry["timeout"] = hook.timeout
            if hook.interactive is not None:
                entry["interactive"] = hook.interactive
            groups = events.setdefault(hook.event, [])
            group = next((item for item in groups if item.get("matcher") == hook.matcher), None)
            if group is None:
                group = {"hooks": []} if hook.matcher is None else {"matcher": hook.matcher, "hooks": []}
                groups.append(group)
            hook_list = group["hooks"]
            assert isinstance(hook_list, list)
            hook_list.append(entry)
    commands = {command.name: command for command in catalog.commands}
    shared_commands = shared.commands if shared is not None else ()
    own_commands = tuple(name for name in profile.commands if name not in shared_commands)
    command_repos: list[dict[str, str]] = []
    for tree in (
        _command_tree(SHARED_PROFILE, shared_commands, commands),
        _command_tree(profile.name, own_commands, commands),
    ):
        if tree.entries:
            trees.append(tree)
            command_repos.append({"snowflake_stage": f"@{stage}/{tree.prefix}"})
    plugin_tree, included = _plugin_tree(profile.name, profile.plugins, plugins, skills)
    plugin_pointers: list[str] = []
    if plugin_tree.entries:
        trees.append(plugin_tree)
        plugin_pointers = [f"@{stage}/{plugin_tree.prefix}{name}/" for name in included]
    row: dict[str, object] = {
        "CONFIG_NAME": profile.name,
        "DESCRIPTION": profile.description,
        "OWNER_TEAM": profile.owner_team,
        "SKILL_REPOS": skill_repos,
        "SYSTEM_PROMPT_REPO": prompt_pointer,
        "MCP_SERVERS": mcp_pointer,
        "HOOKS": events,
        "PLUGINS": plugin_pointers,
        "COMMAND_REPOS": command_repos,
        "ENV_VARS": {},
        "SETTINGS_OVERRIDES": {},
    }
    digest = sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    row["VERSION"] = f"{version_prefix}{digest[:TREE_HASH_CHARACTERS].upper()}"
    sources = tuple(
        dict.fromkeys(
            (
                *profile.source_files,
                *(shared.source_files if shared is not None else ()),
                *(path for name in (*shared_names, *own) if name in skills for path in skills[name].source_files),
                *(f"{hook.directory}/{hook.script.path}" for hook in selected),
                *(configs[name].file for name in profile.mcp_servers if name in configs),
                *(commands[name].file for name in (*shared_commands, *own_commands) if name in commands),
                *(
                    path
                    for name in included
                    for path in (
                        plugins[name].manifest_file,
                        *(
                            file
                            for member in plugins[name].members
                            if member in skills
                            for file in skills[member].source_files
                        ),
                    )
                ),
            )
        )
    )
    return ProfileRelease(profile.name, str(row["VERSION"]), tuple(trees), row, sources)


def unreached_skills(
    skills: Mapping[str, Skill],
    catalog: ProfileCatalog,
    *,
    catalog_channel: bool,
    plugins: Mapping[str, Plugin] | None = None,
) -> tuple[Diagnostic, ...]:
    """`K215`: a skill no channel publishes, when the catalog channel is off."""
    if catalog_channel:
        return ()
    plugins = plugins or {}
    reached = set(catalog.shared.skills) if catalog.shared is not None else set()
    for profile in catalog.profiles:
        reached.update(profile.skills)
        reached.update(member for name in profile.plugins if name in plugins for member in plugins[name].members)
    return tuple(
        D("SST-VAL830", origin=skill.origin, subject=skill.key, artifact=skill.name)
        for name, skill in sorted(skills.items())
        if name not in reached
    )

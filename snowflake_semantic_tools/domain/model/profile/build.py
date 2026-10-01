"""Render one Desktop profile: its content-addressed stage trees, its registry row, and its sources.

Each input a profile carries becomes its own tree below a fixed `by_type` prefix Desktop
reads -- `skills/`, `prompts/`, `mcp/`, `hooks/`, `commands/`, `plugins/` -- named by the
digest of its own content, and the row points at the trees by stage path. Uploading a new
tree never touches one a live row points at, so the row write is the atomic switch and the
previous trees remain for rollback. The row's VERSION is a digest of every other column.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from hashlib import sha256

from snowflake_semantic_tools.domain.model.profile.model import (
    SHARED_PROFILE,
    TREE_HASH_CHARACTERS,
    CommandFile,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    ProfileRelease,
    SharedProfile,
    StageTree,
)
from snowflake_semantic_tools.domain.model.skill import BundleEntry, Plugin, Skill, build_plugin_bundle


def assemble_prompt(shared: SharedProfile | None, profile: DesktopProfile) -> str | None:
    """Join shared `AGENTS.md`, then shared rules in name order, then the profile's own `AGENTS.md`.

    Blank parts are dropped and the rest stripped; None when nothing is left.
    """
    parts = [
        *((shared.prompt or "",) if shared is not None else ()),
        *(text for _, text in (sorted(shared.rules) if shared is not None else ())),
        profile.prompt or "",
    ]
    kept = [part.strip() for part in parts if part.strip()]
    return "\n\n".join(kept) + "\n" if kept else None


def build_profile(
    profile: DesktopProfile,
    catalog: ProfileCatalog,
    skills: Mapping[str, Skill],
    *,
    stage: str,
    version_prefix: str,
    plugins: Mapping[str, Plugin] | None = None,
) -> ProfileRelease:
    """Render the nested trees and the registry row one profile publishes.

    A name the catalog does not define is left out here; validation reports it. A tree with
    no files is not published, and its column holds an empty value instead of a pointer.

    Returns:
        The release, its trees in upload order: skills (shared, then the profile's own), prompt,
        MCP config, hooks, commands (shared, then own), plugins.
    """
    plugins = plugins or {}
    chosen = _Selection.of(profile, catalog)
    trees = _Trees(stage)
    # Upload order, which is not the row's column order.
    skill_repos = _repositories(
        trees,
        _skill_tree(SHARED_PROFILE, chosen.shared_skills, skills),
        _skill_tree(profile.name, chosen.own_skills, skills),
    )
    prompt_repo = _prompt_repository(trees, catalog.shared, profile)
    mcp_servers = _mcp_repository(trees, profile.name, chosen.configs)
    hook_events = _hook_events(trees, profile.name, chosen.hooks)
    command_repos = _repositories(
        trees,
        _command_tree(SHARED_PROFILE, chosen.shared_commands, chosen.commands),
        _command_tree(profile.name, chosen.own_commands, chosen.commands),
    )
    plugin_repos, included = _plugin_repositories(trees, profile, plugins, skills)
    row: dict[str, object] = {
        "CONFIG_NAME": profile.name,
        "DESCRIPTION": profile.description,
        "OWNER_TEAM": profile.owner_team,
        "SKILL_REPOS": skill_repos,
        "SYSTEM_PROMPT_REPO": prompt_repo,
        "MCP_SERVERS": mcp_servers,
        "HOOKS": hook_events,
        "PLUGINS": plugin_repos,
        "COMMAND_REPOS": command_repos,
        "ENV_VARS": {},
        "SETTINGS_OVERRIDES": {},
    }
    row["VERSION"] = _version(row, version_prefix)
    sources = _source_files(profile, catalog.shared, chosen, skills, tuple(plugins[name] for name in included))
    return ProfileRelease(profile.name, str(row["VERSION"]), tuple(trees.published), row, sources)


@dataclass(frozen=True, slots=True)
class _Selection:
    """What one profile takes from the catalog, resolved once so its trees and sources agree.

    Attributes:
        own_skills: The profile's skills the shared layer does not already carry.
        own_commands: The profile's commands the shared layer does not already carry.
        hooks: The named hooks that are defined, each once, in the profile's order.
        configs: The named MCP configs that are defined, in the profile's order.
    """

    shared_skills: tuple[str, ...]
    own_skills: tuple[str, ...]
    shared_commands: tuple[str, ...]
    own_commands: tuple[str, ...]
    hooks: tuple[HookDefinition, ...]
    configs: tuple[McpConfig, ...]
    commands: Mapping[str, CommandFile]

    @classmethod
    def of(cls, profile: DesktopProfile, catalog: ProfileCatalog) -> _Selection:
        """Split `profile`'s skills and commands from the shared layer's, and resolve its hooks and configs."""
        shared = catalog.shared
        shared_skills = shared.skills if shared is not None else ()
        shared_commands = shared.commands if shared is not None else ()
        hooks = {hook.name: hook for hook in catalog.hooks}
        configs = {config.name: config for config in catalog.mcp_configs}
        return cls(
            shared_skills=shared_skills,
            own_skills=tuple(name for name in profile.skills if name not in shared_skills),
            shared_commands=shared_commands,
            own_commands=tuple(name for name in profile.commands if name not in shared_commands),
            hooks=tuple(hooks[name] for name in dict.fromkeys(profile.hooks) if name in hooks),
            configs=tuple(configs[name] for name in profile.mcp_servers if name in configs),
            commands={command.name: command for command in catalog.commands},
        )


@dataclass(slots=True)
class _Trees:
    """The trees one profile publishes, in upload order, and the stage pointers into them."""

    stage: str
    published: list[StageTree] = field(default_factory=list)

    def add(self, tree: StageTree, path: str = "") -> str:
        """Publish `tree` after the ones before it, and point at `path` inside it."""
        self.published.append(tree)
        return self.pointer(tree, path)

    def pointer(self, tree: StageTree, path: str = "") -> str:
        """Locate `path` inside `tree` on the stage: `@<stage>/<kind>/<scope>/<H>/<path>`."""
        return f"@{self.stage}/{tree.prefix}{path}"


def _repositories(trees: _Trees, *candidates: StageTree) -> list[dict[str, str]]:
    """Publish each candidate tree that has files, and point one repository entry at its root."""
    return [{"snowflake_stage": trees.add(tree)} for tree in candidates if tree.entries]


def _prompt_repository(trees: _Trees, shared: SharedProfile | None, profile: DesktopProfile) -> dict[str, str] | None:
    """Publish the assembled prompt as `AGENTS.md` and point at it; None when there is no prompt."""
    prompt = assemble_prompt(shared, profile)
    if prompt is None:
        return None
    tree = StageTree("prompts", profile.name, (BundleEntry("AGENTS.md", prompt.encode("utf-8")),))
    return {"snowflake_stage": trees.add(tree, "AGENTS.md")}


def _mcp_repository(trees: _Trees, scope: str, configs: tuple[McpConfig, ...]) -> dict[str, str]:
    """Publish the configs merged into one `mcp.json` and point at it; empty when no server is defined.

    A later config's server replaces an earlier one's of the same name; validation reports the clash.
    """
    merged: dict[str, object] = {}
    for config in configs:
        merged.update(config.servers)
    if not merged:
        return {}
    content = json.dumps({"mcpServers": merged}, indent=2, sort_keys=True) + "\n"
    tree = StageTree("mcp", scope, (BundleEntry("mcp.json", content.encode("utf-8")),))
    return {"snowflake_stage": trees.add(tree, "mcp.json")}


def _hook_events(trees: _Trees, scope: str, hooks: tuple[HookDefinition, ...]) -> dict[str, list[dict[str, object]]]:
    """Publish the hook scripts as one tree, and group the hooks by event, then by matcher.

    Events and matchers keep the order their first hook is named in, hooks the profile's order.
    """
    if not hooks:
        return {}
    scripts = sorted((BundleEntry(hook.tree_path, hook.script.content) for hook in hooks), key=lambda entry: entry.path)
    tree = StageTree("hooks", scope, tuple(scripts))
    trees.add(tree)
    grouped: dict[str, dict[str | None, list[dict[str, object]]]] = {}
    for hook in hooks:
        matchers = grouped.setdefault(hook.event, {})
        matchers.setdefault(hook.matcher, []).append(_hook_entry(hook, trees.pointer(tree, hook.tree_path)))
    return {
        event: [_hook_group(matcher, entries) for matcher, entries in matchers.items()]
        for event, matchers in grouped.items()
    }


def _hook_entry(hook: HookDefinition, source: str) -> dict[str, object]:
    """Render one command hook as the row carries it, with its script at `source`."""
    entry: dict[str, object] = {"type": "command", "command": hook.command, "source": {"snowflake_stage": source}}
    if hook.timeout is not None:
        entry["timeout"] = hook.timeout
    if hook.interactive is not None:
        entry["interactive"] = hook.interactive
    return entry


def _hook_group(matcher: str | None, hooks: list[dict[str, object]]) -> dict[str, object]:
    return {"hooks": hooks} if matcher is None else {"matcher": matcher, "hooks": hooks}


def _plugin_repositories(
    trees: _Trees, profile: DesktopProfile, plugins: Mapping[str, Plugin], skills: Mapping[str, Skill]
) -> tuple[list[str], tuple[str, ...]]:
    """Publish the profile's plugins as one tree and point at each plugin's folder in it.

    Returns:
        The pointers, and the names of the plugins the tree carries, in the profile's order.
    """
    tree, included = _plugin_tree(profile.name, profile.plugins, plugins, skills)
    if not tree.entries:
        return [], included
    trees.add(tree)
    return [trees.pointer(tree, f"{name}/") for name in included], included


def _version(row: Mapping[str, object], prefix: str) -> str:
    """Name the row's content: `prefix`, then 12 hex characters of its canonical JSON's digest."""
    digest = sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:TREE_HASH_CHARACTERS].upper()}"


def _source_files(
    profile: DesktopProfile,
    shared: SharedProfile | None,
    chosen: _Selection,
    skills: Mapping[str, Skill],
    plugins: tuple[Plugin, ...],
) -> tuple[str, ...]:
    """List every authored file the release is built from, once each, in first-use order."""
    commands = chosen.commands
    return tuple(
        dict.fromkeys(
            (
                *profile.source_files,
                *(shared.source_files if shared is not None else ()),
                *_skill_sources((*chosen.shared_skills, *chosen.own_skills), skills),
                *(f"{hook.directory}/{hook.script.path}" for hook in chosen.hooks),
                *(config.file for config in chosen.configs),
                *(commands[name].file for name in (*chosen.shared_commands, *chosen.own_commands) if name in commands),
                *(
                    path
                    for plugin in plugins
                    for path in (plugin.manifest_file, *_skill_sources(plugin.members, skills))
                ),
            )
        )
    )


def _skill_sources(names: Iterable[str], skills: Mapping[str, Skill]) -> Iterator[str]:
    """Yield the authored files of each named skill that is defined, in name order."""
    return (path for name in names if name in skills for path in skills[name].source_files)


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

"""Read Desktop profiles, the shared prompt, hooks, and MCP configs from disk."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_strings,
    checked_text,
    optional_int,
    project_relative,
    refused,
    report_unknown_keys,
    unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.skills import _published
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.profile import (
    COMMAND_FRONTMATTER_KEYS,
    REJECTED_PROFILE_KEYS,
    SHARED_PROFILE,
    CommandFile,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from snowflake_semantic_tools.domain.model.skill import SkillFile

PROFILE_KEYS = frozenset(("name", "description", "owner_team", "skills", "mcp_servers", "hooks", "commands", "plugins"))
SHARED_KEYS = frozenset(("skills", "commands"))
HOOK_KEYS = frozenset(
    ("name", "event", "type", "command", "matcher", "timeout", "interactive", "description", "script")
)
HOOK_FILES = ("hook.yml", "hook.yaml")
PROFILE_FILES = ("profile.yml", "profile.yaml")


def load_profile_catalog(
    project_dir: Path,
    *,
    profiles_dir: str,
    hooks_dir: str,
    mcp_servers_dir: str,
    commands_dir: str = "commands",
) -> ProfileCatalog:
    """Read a project's Desktop profiles, the shared layer, and the hooks, MCP configs and commands.

    Each published folder directly under `profiles_dir` is a profile, except `shared/`, the
    layer every profile carries; each folder under `hooks_dir` or `mcp_servers_dir` is one hook
    or MCP config; and every `*.md` at any depth below `commands_dir` is a command. Folders and
    files are read in name order, hidden entries and caches are skipped, and a missing
    directory contributes nothing. A profile, hook or MCP config that cannot be read is
    reported and left out, and so is anything reached through a symbolic link.

    Raises:
        OSError: A manifest, script, prompt, rule or command file cannot be read.

    Diagnostics:
        SST-VAL801: when a profile folder holds no manifest or both spellings, or its manifest's
            `name` is not the folder's; or `shared/` holds both spellings.
        SST-VAL851: when a profile declares a key SST refuses.
        SST-PRS004: when a manifest, or a command's frontmatter, holds a key SST does not read.
        SST-PRS003: when a profile or `shared/` field has the wrong type.
        SST-VAL852: when a hook folder is incomplete or ambiguous.
        SST-VAL853: when an MCP config folder has no `mcp.json` or an unusable one.
        SST-VAL859: when a command is not UTF-8, or its frontmatter is malformed or holds a value
            of the wrong type.
        SST-LOD006: when a manifest, an `AGENTS.md` prompt or a shared rule is not UTF-8; the
            prompt or rule is left out.
        SST-LOD001: when a manifest is not valid YAML.
        SST-LOD004: when a template in a manifest is malformed.
        SST-LOD005: when a manifest writes a key twice in one mapping.
        SST-LOD003: when a manifest holds only whitespace or comments.
        SST-LOD008: when a manifest holds more than one document.
        SST-LOD002: when a manifest's root is not a mapping.
        SST-PRT009: when a folder or file that would be read is a symbolic link, or lies in a
            linked folder; it is left out.
    """
    diagnostics: list[Diagnostic] = []
    profiles: list[DesktopProfile] = []
    shared: SharedProfile | None = None
    root = project_dir / profiles_dir
    for folder in _folders(project_dir, root, diagnostics):
        if folder.name == SHARED_PROFILE:
            shared = _load_shared(project_dir, folder, diagnostics)
            continue
        profile = _load_profile(project_dir, folder, diagnostics)
        if profile is not None:
            profiles.append(profile)
    hooks = [
        hook
        for folder in _folders(project_dir, project_dir / hooks_dir, diagnostics)
        if (hook := _load_hook(project_dir, folder, diagnostics)) is not None
    ]
    configs = [
        config
        for folder in _folders(project_dir, project_dir / mcp_servers_dir, diagnostics)
        if (config := _load_mcp(project_dir, folder, diagnostics)) is not None
    ]
    commands = _load_commands(project_dir, project_dir / commands_dir, diagnostics)
    return ProfileCatalog(tuple(profiles), shared, tuple(hooks), tuple(configs), DiagnosticBag(diagnostics), commands)


def _folders(project_dir: Path, root: Path, diagnostics: list[Diagnostic]) -> tuple[Path, ...]:
    """Return the published folders directly under `root`, in name order; a linked one is reported."""
    if not root.is_dir():
        return ()
    return tuple(
        path
        for path in sorted(root.iterdir())
        if path.is_dir() and _published(path, root) and not refused(project_dir, path, diagnostics)
    )


def _manifest(folder: Path, names: tuple[str, ...]) -> tuple[Path | None, str | None]:
    found = [folder / name for name in names if (folder / name).is_file()]
    if len(found) > 1:
        return None, f"has both {names[0]} and {names[1]}"
    return (found[0], None) if found else (None, f"has no {names[0]}")


def _parse(project_dir: Path, path: Path, subject: str, diagnostics: list[Diagnostic]) -> dict[str, Any] | None:
    if refused(project_dir, path, diagnostics, subject=subject):
        return None
    file = project_relative(project_dir, path)
    try:
        parsed = parse_yaml_bytes(path.read_bytes(), file)
    except ProjectError as exc:
        found = exc.diagnostics or (D("SST-LOD001", origin=Origin(file), file=file, line=1, col=1, detail=str(exc)),)
        diagnostics.extend(replace(item, subject=subject) for item in found)
        return None
    diagnostics.extend(replace(item, subject=subject) for item in parsed.diagnostics)
    return dict(parsed.tree)


def _names(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[str, ...]:
    return checked_strings(
        value, diagnostics, field=field, artifact=subject, origin=origin, subject=subject, expected="a list of names"
    )


def _text(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    return checked_text(value, diagnostics, field=field, artifact=subject, origin=origin, subject=subject)


def _prompt(project_dir: Path, path: Path, subject: str, diagnostics: list[Diagnostic]) -> str | None:
    return _read_text(project_dir, path, subject, diagnostics) if path.is_file() else None


def _read_text(project_dir: Path, path: Path, subject: str, diagnostics: list[Diagnostic]) -> str | None:
    """Return a file's UTF-8 text; None, reporting SST-PRT009 or SST-LOD006, if linked or not UTF-8."""
    if refused(project_dir, path, diagnostics, subject=subject):
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        file = project_relative(project_dir, path)
        diagnostics.append(D("SST-LOD006", origin=Origin(file), subject=subject, file=file, offset=exc.start))
        return None


def _load_profile(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> DesktopProfile | None:
    """Read one profile folder; None when it has no single manifest or the manifest does not parse.

    The profile is named after its folder, whatever its manifest's `name` says. A problem in the
    manifest is reported at its first line, and one that stops it parsing with the codes
    `parse_yaml_bytes` lists. `prompt` is the folder's `AGENTS.md`, and the source files are the
    manifest, then that prompt when there is one.

    Raises:
        OSError: The manifest or `AGENTS.md` cannot be read.

    Diagnostics:
        SST-VAL801: when the folder holds no `profile.yml` or both spellings, or the manifest's
            `name` is not the folder's.
        SST-VAL851: when an unread key is one SST refuses, which names the reason.
        SST-PRS004: when any other key is not one SST reads.
        SST-PRS003: when a text field is not a string, or a name list not a list of strings.
        SST-LOD006: when `AGENTS.md` is not UTF-8; the profile then has no prompt.
    """
    subject = artifact_key("profile", folder.name)
    manifest, problem = _manifest(folder, PROFILE_FILES)
    directory = project_relative(project_dir, folder)
    if manifest is None:
        diagnostics.append(
            D(
                "SST-VAL801",
                origin=Origin(directory),
                subject=subject,
                artifact=subject,
                detail=f"profile folder {problem}",
            )
        )
        return None
    tree = _parse(project_dir, manifest, subject, diagnostics)
    if tree is None:
        return None
    origin = Origin(project_relative(project_dir, manifest), 1)
    for key in unknown_keys(tree, PROFILE_KEYS):
        if key in REJECTED_PROFILE_KEYS:
            diagnostics.append(
                D(
                    "SST-VAL851",
                    origin=origin,
                    subject=subject,
                    artifact=folder.name,
                    key=key,
                    reason=REJECTED_PROFILE_KEYS[key],
                )
            )
        else:
            diagnostics.append(D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=key))
    declared = tree.get("name")
    if declared is not None and declared != folder.name:
        diagnostics.append(
            D(
                "SST-VAL801",
                origin=origin,
                subject=subject,
                artifact=subject,
                detail=f"profile name '{declared}' does not match the folder name",
            )
        )
    prompt_path = folder / "AGENTS.md"
    sources = [project_relative(project_dir, manifest)]
    if prompt_path.is_file():
        sources.append(project_relative(project_dir, prompt_path))
    return DesktopProfile(
        name=folder.name,
        directory=directory,
        description=_text(tree.get("description"), "description", subject, origin, diagnostics),
        owner_team=_text(tree.get("owner_team"), "owner_team", subject, origin, diagnostics),
        skills=_names(tree.get("skills"), "skills", subject, origin, diagnostics),
        mcp_servers=_names(tree.get("mcp_servers"), "mcp_servers", subject, origin, diagnostics),
        hooks=_names(tree.get("hooks"), "hooks", subject, origin, diagnostics),
        prompt=_prompt(project_dir, prompt_path, subject, diagnostics),
        origin=origin,
        source_files=tuple(sources),
        commands=_names(tree.get("commands"), "commands", subject, origin, diagnostics),
        plugins=_names(tree.get("plugins"), "plugins", subject, origin, diagnostics),
    )


def _load_shared(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> SharedProfile:
    """Read the `shared/` layer every profile carries: its prompt, rules, skills and commands.

    The manifest is optional: with none, both spellings, or one that does not parse, the layer
    lists no skills or commands. The rules are its published `rules/*.md` files, by file name
    in name order. The source files are the manifest, `AGENTS.md`, then the rules, each when
    present.

    Raises:
        OSError: A file of the layer cannot be read.

    Diagnostics:
        SST-VAL801: when the folder holds both `profile.yml` and `profile.yaml`.
        SST-PRS004: when the manifest holds a key other than `skills` and `commands`.
        SST-PRS003: when `skills` or `commands` is not a list of strings.
        SST-LOD006: when `AGENTS.md` or a rule is not UTF-8; it is left out of the layer.
    """
    subject = "profile:shared"
    origin = Origin(project_relative(project_dir, folder))
    sources: list[str] = []
    skills: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()
    manifest, problem = _manifest(folder, PROFILE_FILES)
    if manifest is None and problem is not None and "both" in problem:
        diagnostics.append(
            D("SST-VAL801", origin=origin, subject=subject, artifact=subject, detail=f"shared folder {problem}")
        )
    if manifest is not None:
        origin = Origin(project_relative(project_dir, manifest), 1)
        sources.append(project_relative(project_dir, manifest))
        tree = _parse(project_dir, manifest, subject, diagnostics) or {}
        report_unknown_keys(tree, SHARED_KEYS, diagnostics, artifact=subject, origin=origin, subject=subject)
        skills = _names(tree.get("skills"), "skills", subject, origin, diagnostics)
        commands = _names(tree.get("commands"), "commands", subject, origin, diagnostics)
    prompt_path = folder / "AGENTS.md"
    if prompt_path.is_file():
        sources.append(project_relative(project_dir, prompt_path))
    rules_dir = folder / "rules"
    rules = tuple(
        (path.name, text)
        for path in sorted(rules_dir.glob("*.md"))
        if path.is_file() and _published(path, rules_dir)
        if (text := _read_text(project_dir, path, subject, diagnostics)) is not None
    )
    sources.extend(project_relative(project_dir, rules_dir / name) for name, _ in rules)
    return SharedProfile(
        _prompt(project_dir, prompt_path, subject, diagnostics), rules, skills, origin, tuple(sources), commands
    )


def _load_commands(project_dir: Path, root: Path, diagnostics: list[Diagnostic]) -> tuple[CommandFile, ...]:
    """Every `*.md` below the commands directory, as Desktop scans a command repository."""
    if not root.is_dir():
        return ()
    commands: list[CommandFile] = []
    for path in sorted(root.rglob("*.md")):
        if not _published(path, root) or refused(project_dir, path, diagnostics) or not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        file = project_relative(project_dir, path)
        content = path.read_bytes()
        command = CommandFile(relative, content, file, Origin(file, 1))
        _check_command(command, diagnostics)
        commands.append(command)
    return tuple(commands)


def _check_command(command: CommandFile, diagnostics: list[Diagnostic]) -> None:
    """Frontmatter is optional; when present it must be a mapping Desktop can read."""

    def invalid(detail: str, line: int = 1) -> None:
        diagnostics.append(
            D(
                "SST-VAL859",
                origin=Origin(command.file, line),
                subject=command.key,
                artifact=command.name,
                detail=detail,
            )
        )

    try:
        text = command.content.decode("utf-8")
    except UnicodeDecodeError:
        invalid("is not UTF-8")
        return
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return
    closing = next((index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---"), None)
    if closing is None:
        invalid("frontmatter opens with --- but never closes")
        return
    try:
        value: Any = bounded_yaml.safe_load("\n".join(lines[1:closing]))
    except yaml.YAMLError as exc:
        invalid(f"frontmatter is not valid YAML: {getattr(exc, 'problem', exc)}")
        return
    if value is None:
        return
    if not isinstance(value, dict):
        invalid(f"frontmatter is a {type(value).__name__}, not a mapping")
        return
    report_unknown_keys(
        value,
        COMMAND_FRONTMATTER_KEYS,
        diagnostics,
        artifact=command.key,
        origin=Origin(command.file, 1),
        subject=command.key,
    )
    for key in ("description", "skill"):
        if key in value and not isinstance(value[key], str):
            invalid(f"'{key}' must be a string")
    if "hidden" in value and not isinstance(value["hidden"], bool):
        invalid("'hidden' must be true or false")
    tools = value.get("allowed-tools")
    if "allowed-tools" in value and not (
        isinstance(tools, str) or (isinstance(tools, list) and all(isinstance(item, str) for item in tools))
    ):
        invalid("'allowed-tools' must be a string or a list of strings")


def _load_hook(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> HookDefinition | None:
    """Read one hook folder into a command hook and the script it runs; None when that cannot be done.

    The manifest is `hook.yml` or `hook.yaml`. Its `script:` names the script among the folder's
    other published files; without it, the folder must hold exactly one such file. A `script`,
    `matcher`, `timeout`, `interactive` or `description` of the wrong type is ignored, unreported.

    Raises:
        OSError: The manifest or the script cannot be read.

    Diagnostics:
        SST-VAL852: when the folder holds no manifest or both spellings, the hook is not of type
            `command`, it declares no `event` or `command`, or its script is absent or ambiguous.
        SST-PRT009: when the manifest or a script is a symbolic link; it is left out.
        SST-PRS004: when the manifest holds a key SST does not read.
    """
    name = folder.name
    subject = f"hook:{name}"
    directory = project_relative(project_dir, folder)
    manifest, problem = _manifest(folder, HOOK_FILES)
    if manifest is None:
        diagnostics.append(
            D("SST-VAL852", origin=Origin(directory), subject=subject, artifact=name, detail=f"folder {problem}")
        )
        return None
    tree = _parse(project_dir, manifest, subject, diagnostics)
    if tree is None:
        return None
    origin = Origin(project_relative(project_dir, manifest), 1)
    report_unknown_keys(tree, HOOK_KEYS, diagnostics, artifact=subject, origin=origin, subject=subject)
    event = tree.get("event")
    command = tree.get("command")
    if tree.get("type", "command") != "command":
        diagnostics.append(
            D(
                "SST-VAL852",
                origin=origin,
                subject=subject,
                artifact=name,
                detail="only command hooks can carry a script",
            )
        )
        return None
    if not isinstance(event, str) or not isinstance(command, str) or not event or not command:
        diagnostics.append(
            D("SST-VAL852", origin=origin, subject=subject, artifact=name, detail="declares no event or command")
        )
        return None
    scripts = [
        path
        for path in sorted(folder.iterdir())
        if path.name not in HOOK_FILES
        and _published(path, folder)
        and path.is_file()
        and not refused(project_dir, path, diagnostics, subject=subject)
    ]
    declared = tree.get("script")
    if isinstance(declared, str):
        chosen = [path for path in scripts if path.name == declared]
        if not chosen:
            diagnostics.append(
                D("SST-VAL852", origin=origin, subject=subject, artifact=name, detail=f"script '{declared}' is absent")
            )
            return None
    elif len(scripts) != 1:
        detail = "has no script" if not scripts else f"has {len(scripts)} candidate scripts and no script: key"
        diagnostics.append(D("SST-VAL852", origin=origin, subject=subject, artifact=name, detail=detail))
        return None
    else:
        chosen = scripts
    timeout = tree.get("timeout")
    interactive = tree.get("interactive")
    matcher = tree.get("matcher")
    return HookDefinition(
        name=name,
        directory=directory,
        event=event,
        command=command,
        script=SkillFile(chosen[0].name, chosen[0].read_bytes()),
        origin=origin,
        matcher=matcher if isinstance(matcher, str) and matcher else None,
        timeout=optional_int(timeout),
        interactive=interactive if isinstance(interactive, bool) else None,
        description=tree.get("description") if isinstance(tree.get("description"), str) else None,
    )


def _load_mcp(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> McpConfig | None:
    """Read one MCP config folder's `mcp.json`; None when it is absent or cannot be used.

    The document must be one JSON object whose only key is `mcpServers`, an object of server
    definitions, which are kept as written.

    Raises:
        OSError: `mcp.json` exists and cannot be read.

    Diagnostics:
        SST-VAL853: when the folder has no `mcp.json`, it is not UTF-8 JSON within the size and
            nesting `adapters.json_files` bounds, or it is not one `mcpServers` object.
        SST-PRT009: when `mcp.json` is a symbolic link; the config is left out.
    """
    name = folder.name
    subject = f"mcp:{name}"
    path = folder / "mcp.json"
    file = project_relative(project_dir, path)
    origin = Origin(file, 1)
    if not path.is_file():
        diagnostics.append(
            D(
                "SST-VAL853",
                origin=Origin(project_relative(project_dir, folder)),
                subject=subject,
                artifact=name,
                detail="folder has no mcp.json",
            )
        )
        return None
    if refused(project_dir, path, diagnostics, subject=subject):
        return None
    try:
        document = read_json_file(path)
    except JsonFileError as exc:
        diagnostics.append(
            D("SST-VAL853", origin=origin, subject=subject, artifact=name, detail=f"mcp.json cannot be used: {exc}")
        )
        return None
    servers = document.get("mcpServers") if isinstance(document, dict) else None
    if not isinstance(document, dict) or not isinstance(servers, dict) or set(document) != {"mcpServers"}:
        diagnostics.append(
            D(
                "SST-VAL853",
                origin=origin,
                subject=subject,
                artifact=name,
                detail="must hold exactly one mcpServers object",
            )
        )
        return None
    return McpConfig(name, file, servers, origin)

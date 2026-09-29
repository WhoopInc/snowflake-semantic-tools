"""Read Desktop profiles, the shared prompt, hooks, and MCP configs from disk."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.profile import (
    REJECTED_PROFILE_KEYS,
    SHARED_PROFILE,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from ...domain.model.skill import SkillFile
from ..project import ProjectError
from .loader import _parse_yaml_bytes
from .skills import _published

PROFILE_KEYS = frozenset(("name", "description", "owner_team", "skills", "mcp_servers", "hooks"))
HOOK_KEYS = frozenset(
    ("name", "event", "type", "command", "matcher", "timeout", "interactive", "description", "script")
)
HOOK_FILES = ("hook.yml", "hook.yaml")
PROFILE_FILES = ("profile.yml", "profile.yaml")


def load_profile_catalog(
    project_dir: Path, *, profiles_dir: str, hooks_dir: str, mcp_servers_dir: str
) -> ProfileCatalog:
    diagnostics: list[Diagnostic] = []
    profiles: list[DesktopProfile] = []
    shared: SharedProfile | None = None
    root = project_dir / profiles_dir
    for folder in _folders(root):
        if folder.name == SHARED_PROFILE:
            shared = _load_shared(project_dir, folder, diagnostics)
            continue
        profile = _load_profile(project_dir, folder, diagnostics)
        if profile is not None:
            profiles.append(profile)
    hooks = [
        hook
        for folder in _folders(project_dir / hooks_dir)
        if (hook := _load_hook(project_dir, folder, diagnostics)) is not None
    ]
    configs = [
        config
        for folder in _folders(project_dir / mcp_servers_dir)
        if (config := _load_mcp(project_dir, folder, diagnostics)) is not None
    ]
    return ProfileCatalog(tuple(profiles), shared, tuple(hooks), tuple(configs), DiagnosticBag(diagnostics))


def _folders(root: Path) -> tuple[Path, ...]:
    if not root.is_dir():
        return ()
    return tuple(sorted(path for path in root.iterdir() if path.is_dir() and _published(path, root)))


def _relative(project_dir: Path, path: Path) -> str:
    return path.relative_to(project_dir).as_posix()


def _manifest(folder: Path, names: tuple[str, ...]) -> tuple[Path | None, str | None]:
    found = [folder / name for name in names if (folder / name).is_file()]
    if len(found) > 1:
        return None, f"has both {names[0]} and {names[1]}"
    return (found[0], None) if found else (None, f"has no {names[0]}")


def _parse(project_dir: Path, path: Path, subject: str, diagnostics: list[Diagnostic]) -> dict[str, Any] | None:
    file = _relative(project_dir, path)
    try:
        parsed = _parse_yaml_bytes(path.read_bytes(), file)
    except ProjectError as exc:
        found = exc.diagnostics or (D("SST-INT902", detail=str(exc)),)
        diagnostics.extend(replace(item, subject=subject) for item in found)
        return None
    return dict(parsed.tree)


def _names(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        diagnostics.append(
            D(
                "SST-PRS003",
                origin=origin,
                subject=subject,
                artifact=subject,
                field=field,
                expected="a list of names",
                found=type(value).__name__,
            )
        )
        return ()
    return tuple(value)


def _text(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        diagnostics.append(
            D(
                "SST-PRS003",
                origin=origin,
                subject=subject,
                artifact=subject,
                field=field,
                expected="a string",
                found=type(value).__name__,
            )
        )
        return None
    return value.strip() or None


def _prompt(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.is_file() else None


def _load_profile(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> DesktopProfile | None:
    subject = f"profile:{folder.name}"
    manifest, problem = _manifest(folder, PROFILE_FILES)
    directory = _relative(project_dir, folder)
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
    origin = Origin(_relative(project_dir, manifest), 1)
    for key in sorted(set(tree) - PROFILE_KEYS):
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
    sources = [_relative(project_dir, manifest)]
    if prompt_path.is_file():
        sources.append(_relative(project_dir, prompt_path))
    return DesktopProfile(
        name=folder.name,
        directory=directory,
        description=_text(tree.get("description"), "description", subject, origin, diagnostics),
        owner_team=_text(tree.get("owner_team"), "owner_team", subject, origin, diagnostics),
        skills=_names(tree.get("skills"), "skills", subject, origin, diagnostics),
        mcp_servers=_names(tree.get("mcp_servers"), "mcp_servers", subject, origin, diagnostics),
        hooks=_names(tree.get("hooks"), "hooks", subject, origin, diagnostics),
        prompt=_prompt(prompt_path),
        origin=origin,
        source_files=tuple(sources),
    )


def _load_shared(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> SharedProfile:
    subject = "profile:shared"
    origin = Origin(_relative(project_dir, folder))
    sources: list[str] = []
    skills: tuple[str, ...] = ()
    manifest, problem = _manifest(folder, PROFILE_FILES)
    if manifest is None and problem is not None and "both" in problem:
        diagnostics.append(
            D("SST-VAL801", origin=origin, subject=subject, artifact=subject, detail=f"shared folder {problem}")
        )
    if manifest is not None:
        origin = Origin(_relative(project_dir, manifest), 1)
        sources.append(_relative(project_dir, manifest))
        tree = _parse(project_dir, manifest, subject, diagnostics) or {}
        for key in sorted(set(tree) - {"skills"}):
            diagnostics.append(D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=key))
        skills = _names(tree.get("skills"), "skills", subject, origin, diagnostics)
    prompt_path = folder / "AGENTS.md"
    if prompt_path.is_file():
        sources.append(_relative(project_dir, prompt_path))
    rules_dir = folder / "rules"
    rules = tuple(
        (path.name, path.read_text(encoding="utf-8"))
        for path in sorted(rules_dir.glob("*.md"))
        if path.is_file() and _published(path, rules_dir)
    )
    sources.extend(_relative(project_dir, rules_dir / name) for name, _ in rules)
    return SharedProfile(_prompt(prompt_path), rules, skills, origin, tuple(sources))


def _load_hook(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> HookDefinition | None:
    name = folder.name
    subject = f"hook:{name}"
    directory = _relative(project_dir, folder)
    manifest, problem = _manifest(folder, HOOK_FILES)
    if manifest is None:
        diagnostics.append(
            D("SST-VAL852", origin=Origin(directory), subject=subject, artifact=name, detail=f"folder {problem}")
        )
        return None
    tree = _parse(project_dir, manifest, subject, diagnostics)
    if tree is None:
        return None
    origin = Origin(_relative(project_dir, manifest), 1)
    for key in sorted(set(tree) - HOOK_KEYS):
        diagnostics.append(D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=key))
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
        if path.is_file() and path.name not in HOOK_FILES and _published(path, folder)
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
        timeout=timeout if isinstance(timeout, int) and not isinstance(timeout, bool) else None,
        interactive=interactive if isinstance(interactive, bool) else None,
        description=tree.get("description") if isinstance(tree.get("description"), str) else None,
    )


def _load_mcp(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> McpConfig | None:
    name = folder.name
    subject = f"mcp:{name}"
    path = folder / "mcp.json"
    file = _relative(project_dir, path)
    origin = Origin(file, 1)
    if not path.is_file():
        diagnostics.append(
            D(
                "SST-VAL853",
                origin=Origin(_relative(project_dir, folder)),
                subject=subject,
                artifact=name,
                detail="folder has no mcp.json",
            )
        )
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        diagnostics.append(
            D("SST-VAL853", origin=origin, subject=subject, artifact=name, detail=f"is not valid JSON: {exc}")
        )
        return None
    servers = document.get("mcpServers") if isinstance(document, dict) else None
    if not isinstance(servers, dict) or set(document) != {"mcpServers"}:
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

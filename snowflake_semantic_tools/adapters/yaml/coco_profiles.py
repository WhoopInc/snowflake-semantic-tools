"""Read Cortex Code agent profiles and their shared layer from disk.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature. Each published folder
directly under `project.coco_profiles_dir` is one profile, except `shared/`, the layer every
profile carries: a `coco_profile.yml` naming skills and plugins, and an optional `AGENTS.md`
that becomes the profile's response instructions.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_text,
    member_names,
    project_relative,
    refused,
    report_unknown_keys,
    unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.adapters.yaml.skills import _published
from snowflake_semantic_tools.adapters.yaml.text_checks import read_authored_text
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.coco_profile import (
    REJECTED_COCO_PROFILE_KEYS,
    SHARED_PROFILE,
    CocoProfileCatalog,
    CocoProfileModel,
    SharedCocoProfile,
    owned_entries,
)
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings, setting_value
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

COCO_PROFILE_KEYS = frozenset(("name", "description", "model", "skills", "plugins", "config"))
SHARED_KEYS = frozenset(("skills",))
COCO_PROFILE_FILES = ("coco_profile.yml", "coco_profile.yaml")
# The folder SST read CoCo Desktop profiles from before agent profiles replaced them.
RETIRED_PROFILES_DIR = "coco_desktop_profiles"
# A profile publishes as an unquoted AGENT name, so `default` becomes the DEFAULT Cortex Code applies.
_PROFILE_NAME = re.compile(r"[a-z][a-z0-9_]*")
_KIND = "coco_profile"


def load_coco_profile_catalog(
    project_dir: Path, *, coco_profiles_dir: str = "coco_profiles", config: Mapping[str, Any] | None = None
) -> tuple[CocoProfileCatalog, DiagnosticBag]:
    """Read every profile folder under `coco_profiles_dir`, and `shared/`, in name order.

    Hidden folders and caches are skipped, and a missing directory holds nothing. A profile that
    cannot be read is reported and left out, and so is anything reached through a symbolic link.
    Each profile's settings resolve through its own `config:`, the `coco_profiles:` route of its
    folder, then the block root; one whose `enabled` resolves to false is kept, disabled.

    Args:
        config: The run's resolved configuration, whose `coco_profiles:` folder routes are
            checked against the folders; None checks no route and resolves each profile's
            settings from its `config:` alone.

    Raises:
        OSError: A manifest, `AGENTS.md`, or rule cannot be read.

    Diagnostics:
        SST-VAL895: the project still has a `coco_desktop_profiles/` folder, which nothing reads.
        SST-CFG041: a `coco_profiles:` folder route names no folder under the directory.
        Every code `_load_profile` lists, for each profile folder.
        Every code `_load_shared` lists, for `shared/`.
    """
    diagnostics: list[Diagnostic] = []
    retired = project_dir / RETIRED_PROFILES_DIR
    if retired.is_dir() and coco_profiles_dir != RETIRED_PROFILES_DIR:
        diagnostics.append(
            D("SST-VAL895", origin=Origin(RETIRED_PROFILES_DIR), subject=None, folder=RETIRED_PROFILES_DIR)
        )
    if config is not None:
        diagnostics.extend(routed_diagnostics(config, project_dir, _KIND))
    block = config.get("coco_profiles") if config is not None else None
    profiles: list[CocoProfileModel] = []
    shared: SharedCocoProfile | None = None
    for folder in _folders(project_dir, project_dir / coco_profiles_dir, diagnostics):
        if folder.name == SHARED_PROFILE:
            shared = _load_shared(project_dir, folder, diagnostics)
            continue
        profile = _load_profile(project_dir, folder, block, diagnostics)
        if profile is not None:
            profiles.append(profile)
    return CocoProfileCatalog(tuple(profiles), shared), DiagnosticBag(diagnostics)


def _folders(project_dir: Path, root: Path, diagnostics: list[Diagnostic]) -> tuple[Path, ...]:
    """Return the published folders directly under `root`, in name order; a linked one is reported."""
    if not root.is_dir():
        return ()
    return tuple(
        path
        for path in sorted(root.iterdir())
        if path.is_dir() and _published(path, root) and not refused(project_dir, path, diagnostics)
    )


def _manifest(folder: Path) -> tuple[Path | None, str | None]:
    found = [folder / name for name in COCO_PROFILE_FILES if (folder / name).is_file()]
    if len(found) > 1:
        return None, f"has both {COCO_PROFILE_FILES[0]} and {COCO_PROFILE_FILES[1]}"
    return (found[0], None) if found else (None, f"has no {COCO_PROFILE_FILES[0]}")


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


def _load_profile(
    project_dir: Path, folder: Path, block: object, diagnostics: list[Diagnostic]
) -> CocoProfileModel | None:
    """Read one profile folder; None when its name is unusable, or it has no single manifest that parses.

    The profile is named after its folder. A problem in the manifest is reported at its first
    line. Its `skills:` become `skill()` entries and its `plugins:` `plugin()` entries, in that
    order, each to be pinned as an agent's are.

    Raises:
        OSError: The manifest or `AGENTS.md` cannot be read.

    Diagnostics:
        SST-VAL801: the folder name is not lowercase letters, digits and `_` starting with a
            letter; the folder holds no `coco_profile.yml` or both spellings; or the manifest's
            `name` is not the folder's.
        SST-VAL851: a key is one agent profiles cannot carry, which names the reason.
        SST-PRS004: any other key is not one SST reads.
        SST-PRS003: a text field is not a string, or a member list not a list of strings.
        Every code `member_names` lists, for an entry that is not one call of its key's function.
        Every code `read_authored_text` lists, for an `AGENTS.md` it refuses.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG008: as `item_config_diagnostics` reports
            the manifest's `config:` block.
    """
    subject = artifact_key(_KIND, folder.name.casefold())
    directory = project_relative(project_dir, folder)
    if not _PROFILE_NAME.fullmatch(folder.name):
        detail = f"profile name '{folder.name}' is not lowercase letters, digits and '_', starting with a letter"
        diagnostics.append(D("SST-VAL801", origin=Origin(directory), subject=subject, artifact=subject, detail=detail))
        return None
    manifest, problem = _manifest(folder)
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
    for key in unknown_keys(tree, COCO_PROFILE_KEYS):
        reason = REJECTED_COCO_PROFILE_KEYS.get(key)
        if reason is not None:
            diagnostics.append(
                D("SST-VAL851", origin=origin, subject=subject, artifact=folder.name, key=key, reason=reason)
            )
        else:
            diagnostics.append(D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=key))
    diagnostics.extend(
        item_config_diagnostics("coco_profiles", tree.get("config"), subject=subject, origin_of=lambda key: origin)
    )
    declared = tree.get("name")
    if declared is not None and declared != folder.name:
        detail = f"profile name '{declared}' does not match the folder name"
        diagnostics.append(D("SST-VAL801", origin=origin, subject=subject, artifact=subject, detail=detail))
    prompt_path = folder / "AGENTS.md"
    sources = [project_relative(project_dir, manifest)]
    if prompt_path.is_file():
        sources.append(project_relative(project_dir, prompt_path))
    skills = _members(tree.get("skills"), "skills", subject, origin, diagnostics)
    plugins = _members(tree.get("plugins"), "plugins", subject, origin, diagnostics)
    settings = resolve_settings(block, (folder.name,), tree.get("config"))
    return CocoProfileModel(
        name=folder.name,
        origin=origin,
        source_files=tuple(sources),
        comment=_text(tree.get("description"), "description", subject, origin, diagnostics),
        orchestration_model=_text(tree.get("model"), "model", subject, origin, diagnostics),
        prompt=_read_text(project_dir, prompt_path, subject, diagnostics) if prompt_path.is_file() else None,
        skills=(*owned_entries("skill", skills), *owned_entries("plugin", plugins)),
        enabled=setting_value(settings, "enabled") is not False,
        settings=settings,
        published_name=folder.name,
    )


def _load_shared(project_dir: Path, folder: Path, diagnostics: list[Diagnostic]) -> SharedCocoProfile:
    """Read the `shared/` layer every profile carries: its prompt, its rules, and its skills.

    The manifest is optional: with none, both spellings, or one that does not parse, the layer
    lists no skills. The rules are its published `rules/*.md` files, in name order.

    Raises:
        OSError: A file of the layer cannot be read.

    Diagnostics:
        SST-VAL801: the folder holds both `coco_profile.yml` and `coco_profile.yaml`.
        SST-PRS004: the manifest holds a key other than `skills`.
        SST-PRS003: `skills` is not a list of strings.
        Every code `member_names` lists, for an entry that is not one `skill()` call.
        Every code `read_authored_text` lists, for an `AGENTS.md` or a rule it refuses; it is
            left out of the layer.
    """
    subject = artifact_key(_KIND, SHARED_PROFILE)
    origin = Origin(project_relative(project_dir, folder))
    sources: list[str] = []
    skills: tuple[str, ...] = ()
    manifest, problem = _manifest(folder)
    if manifest is None and problem is not None and "both" in problem:
        diagnostics.append(
            D("SST-VAL801", origin=origin, subject=subject, artifact=subject, detail=f"shared folder {problem}")
        )
    if manifest is not None:
        origin = Origin(project_relative(project_dir, manifest), 1)
        sources.append(project_relative(project_dir, manifest))
        tree = _parse(project_dir, manifest, subject, diagnostics) or {}
        report_unknown_keys(tree, SHARED_KEYS, diagnostics, artifact=subject, origin=origin, subject=subject)
        skills = _members(tree.get("skills"), "skills", subject, origin, diagnostics)
    prompt_path = folder / "AGENTS.md"
    prompt = None
    if prompt_path.is_file():
        sources.append(project_relative(project_dir, prompt_path))
        prompt = _read_text(project_dir, prompt_path, subject, diagnostics)
    rules_dir = folder / "rules"
    rules = tuple(
        (path.name, text)
        for path in sorted(rules_dir.glob("*.md"))
        if path.is_file() and _published(path, rules_dir)
        if (text := _read_text(project_dir, path, subject, diagnostics)) is not None
    )
    sources.extend(project_relative(project_dir, rules_dir / name) for name, _ in rules)
    return SharedCocoProfile(prompt, rules, skills, origin, tuple(sources))


def _members(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[str, ...]:
    """Read a `skills:` or `plugins:` list, each entry one `skill()` or `plugin()` call, as the names."""
    return member_names(
        value, f"coco_profile.{field}", diagnostics, field=field, artifact=subject, origin=origin, subject=subject
    )


def _text(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    return checked_text(value, diagnostics, field=field, artifact=subject, origin=origin, subject=subject)


def _read_text(project_dir: Path, path: Path, subject: str, diagnostics: list[Diagnostic]) -> str | None:
    """Return a file's text; None, reporting why, when it is linked or `read_authored_text` refuses it.

    Diagnostics:
        SST-PRT009: the file, or a folder above it, is a symbolic link.
        Every code `read_authored_text` lists.
    """
    if refused(project_dir, path, diagnostics, subject=subject):
        return None
    text = read_authored_text(path.read_bytes(), project_relative(project_dir, path), subject=subject)
    if isinstance(text, Diagnostic):
        diagnostics.append(text)
        return None
    return text

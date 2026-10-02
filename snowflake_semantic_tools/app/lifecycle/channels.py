"""Compare what the two publication channels serve live for each skill a Desktop profile ships.

A skill reaches agents through its catalog extension, flattened, and Desktop through a
profile's stage tree, nested. Each channel publishes and fails on its own, so a deploy that
wrote one and not the other leaves them serving different files until the next good run.
Flattening alone explains a renamed path; any other difference is reported.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension, ExtensionRelease
from snowflake_semantic_tools.app.desktop_contract import desktop_view
from snowflake_semantic_tools.app.lifecycle.profiles import ProfilePublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


def channel_divergence(port: ProfilePublicationPort, result: CompileResult) -> tuple[Diagnostic, ...]:
    """Warn, profile by profile, of each skill whose live catalog and stage files differ beyond flattening.

    The catalog side is the extension's default version; the stage side is the skill's folder
    in the trees the profile's live registry row points at. A read that fails reports nothing
    here: the handlers' own observation reports it.

    Diagnostics:
        SST-VAL829: the two file sets differ by more than flattening's renames, by `value` files.
    """
    releases = {
        item.release.bundle.name: item.release
        for item in result.compiled
        if isinstance(item, CompiledExtension) and item.release.extension_type == "SKILL"
    }
    found: dict[tuple[str, int], Diagnostic] = {}
    for profile in (item for item in result.compiled if isinstance(item, CompiledProfile)):
        try:
            served = _stage_skills(port, profile)
            for name, stage_files in sorted(served.items()):
                release = releases.get(name)
                catalog_files = _catalog_files(port, release, name) if release is not None else None
                if release is None or catalog_files is None:
                    continue
                renames = dict(release.bundle.renames)
                differ = {renames.get(path, path) for path in stage_files} ^ catalog_files
                if differ:
                    # Profiles that ship the skill from one tree report it once.
                    found.setdefault(
                        (name, len(differ)),
                        D("SST-VAL829", subject=release.key, artifact=name, value=f"{len(differ)} file(s)"),
                    )
        except SnowflakePortError:
            continue
    return tuple(found.values())


def _stage_skills(port: ProfilePublicationPort, profile: CompiledProfile) -> Mapping[str, frozenset[str]]:
    """Each skill folder in the trees the profile's live row points at, as its files' authored paths."""
    registry = profile.channel.registry
    row = port.read_profile_row(registry, profile.name) if port.table_columns(registry) is not None else None
    if row is None:
        return {}
    files: dict[str, set[str]] = {}
    for pointer in _skill_pointers(desktop_view(row).get("SKILL_REPOS")):
        for path in port.list_location(pointer):
            name, _, rest = path.partition("/")
            if rest:
                files.setdefault(name, set()).add(rest)
    return {name: frozenset(paths) for name, paths in files.items()}


def _skill_pointers(value: object) -> Iterator[str]:
    for item in value if isinstance(value, list) else ():
        stage = item.get("snowflake_stage") if isinstance(item, Mapping) else None
        if isinstance(stage, str):
            yield stage if stage.endswith("/") else f"{stage}/"


def _catalog_files(port: ProfilePublicationPort, release: ExtensionRelease, name: str) -> frozenset[str] | None:
    """The skill's files in the extension's default version, below `skills/<name>/`.

    Returns:
        Their published paths in the skill's folder; None when there is no extension, or no default version.
    """
    if port.observe_extension(release.target) is None:
        return None
    default = next((item for item in port.extension_versions(release.target) if item.is_default), None)
    if default is None:
        return None
    folder = f"skills/{name}/"
    return frozenset(
        path.removeprefix(folder) for path in port.list_location(default.location) if path.startswith(folder)
    )

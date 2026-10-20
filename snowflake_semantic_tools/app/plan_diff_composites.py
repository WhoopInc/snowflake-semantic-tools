"""Read what a published skill, plugin, profile or eval holds, and build what SST renders for one.

Each artifact type has a desired builder, from what compiled, and a live read, from a session:
- a skill or plugin: the files of the version state records SST published, by its alias, else
  of the version the catalog serves -- the effective one, else the default -- and the
  extension's comment;
- a profile: its registry row and the files of each tree the row points into;
- an eval: the config file and the questions of what state records it last published.

Each file is read with one GET; a file a read returns nothing for, or a version, row or record
that is not there, is a `SnowflakePortError`, which leaves the artifact uncompared.
"""

from __future__ import annotations

from typing import TypeVar

from snowflake_semantic_tools.app.compile import CompiledArtifact
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.desktop_contract import desktop_view, stage_pointers
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.plan.definitions.composite import (
    eval_definition,
    extension_definition,
    payload_questions,
    profile_definition,
    tree_root,
)
from snowflake_semantic_tools.domain.plan.member_diff import Definition
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation, ExtensionVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry


def desired_extension(compiled: CompiledArtifact) -> Definition:
    """The bundle and comment a skill or plugin releases."""
    release = compiled_as(compiled, CompiledExtension).release
    return extension_definition({entry.path: entry.content for entry in release.bundle.entries}, release.comment)


def live_extension(port: SnowflakePort, name: QualifiedName, recorded: AppliedEntry | None = None) -> Definition:
    """The files of the version of the extension `name` state records, else of the one served.

    The definition holds the extension's comment too. The recorded version is the one carrying
    the alias `recorded` names: what SST last published there, which agents pin. The catalog
    may serve another, such as a certified one, so the served version is read only when state
    records no alias, or one no version carries.
    """
    extension = port.observe_extension(name)
    versions = port.extension_versions(name)
    alias = dict(recorded.component_fingerprints).get("alias", "").casefold() if recorded is not None else ""
    chosen = next((item for item in versions if alias and (item.alias or "").casefold() == alias), None)
    version = chosen or served_extension(extension, versions)
    if extension is None or version is None:
        raise SnowflakePortError(f"{name.sql} serves no version to compare")
    location = version.location if version.location.endswith("/") else f"{version.location}/"
    files = {path: _read(port, f"{location}{path}") for path in port.list_location(location)}
    return extension_definition(files, extension.comment)


def served_extension(
    extension: ExtensionObservation | None, versions: tuple[ExtensionVersion, ...]
) -> ExtensionVersion | None:
    """Return the version the catalog serves: the effective one, else the default, else the last.

    None when it lists none.
    """
    by_name = {version.name.upper(): version for version in versions}
    by_alias = {(version.alias or "").upper(): version for version in versions if version.alias}
    effective = (extension.effective_version or "").upper() if extension is not None else ""
    if effective and (effective in by_name or effective in by_alias):
        return by_name.get(effective) or by_alias[effective]
    default = next((version for version in versions if version.is_default), None)
    if default is not None or not versions:
        return default
    return max(versions, key=_number)


def desired_profile(compiled: CompiledArtifact) -> Definition:
    """The registry row and trees a profile releases."""
    release = compiled_as(compiled, CompiledProfile).release
    trees = {
        f"{tree.kind}/{tree.scope}/": {entry.path: entry.content for entry in tree.entries} for tree in release.trees
    }
    return profile_definition(desktop_view(release.row), trees)


def live_profile(port: SnowflakePort, compiled: CompiledArtifact) -> Definition:
    """The registry row the profile's channel holds for it, and the trees that row points into."""
    profile = compiled_as(compiled, CompiledProfile)
    row = port.read_profile_row(profile.channel.registry, profile.release.name)
    if row is None:
        raise SnowflakePortError(f"{profile.channel.registry.sql} holds no row for profile {profile.release.name}")
    view = desktop_view(row)
    roots = dict(root for pointer in stage_pointers(view) if (root := tree_root(pointer)) is not None)
    trees = {
        tree: {path: _read(port, f"{location}{path}") for path in port.list_location(location)}
        for tree, location in roots.items()
    }
    return profile_definition(view, trees)


def desired_eval(compiled: CompiledArtifact) -> Definition:
    """The config and questions an eval renders."""
    rendered = compiled_as(compiled, CompiledEval).rendered
    return eval_definition(rendered.config_yaml, payload_questions(rendered.dataset_payload))


def live_eval(port: SnowflakePort, config_folder: str | None, recorded: AppliedEntry | None) -> Definition:
    """The config and questions state records an eval last published, from `config_folder` and its table.

    The questions are read only when state records the source table; else the definition holds
    the config alone.
    """
    if recorded is None:
        raise SnowflakePortError("state records no earlier publish of the eval to compare")
    published = dict(recorded.component_fingerprints).get("config")
    if config_folder is None or published is None:
        raise SnowflakePortError("state records no config the eval published")
    config = _read(port, f"{config_folder}/{published}.yaml").decode("utf-8", errors="replace")
    table = next(
        (item.qualified_name for item in recorded.applied_resources if item.object_type.upper() == "TABLE"), None
    )
    if table is None:
        return eval_definition(config, None)
    result = port.query(sql("SELECT INPUT_QUERY, GROUND_TRUTH FROM {table}", table=qname(QualifiedName.parse(table))))
    return eval_definition(config, tuple((str(row[0]), row[1]) for row in result.rows))


ArtifactT = TypeVar("ArtifactT", bound=CompiledArtifact)


def compiled_as(compiled: CompiledArtifact, kind: type[ArtifactT]) -> ArtifactT:
    """Return `compiled` as the artifact class `kind`.

    Raises:
        TypeError: it is not one: the caller paired an artifact with another type's read.
    """
    if not isinstance(compiled, kind):
        raise TypeError(f"{compiled.artifact_key} did not compile as a {kind.__name__}")
    return compiled


def _read(port: SnowflakePort, path: str) -> bytes:
    content = port.read_staged_file(path)
    if content is None:
        raise SnowflakePortError(f"{path} could not be read")
    return content


def _number(version: ExtensionVersion) -> int:
    number = version.name.upper().removeprefix("VERSION$")
    return int(number) if number.isdigit() else -1

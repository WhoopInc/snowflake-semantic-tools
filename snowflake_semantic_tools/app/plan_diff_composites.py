"""Read what a planned update of a skill, plugin, profile or eval replaces, to compare by member.

Each read returns the live definition and the desired one, as `plan_diff` compares them:
- a skill or plugin, the files of the version the catalog serves -- the default, or the latest
  certified -- and the extension's comment;
- a profile, its registry row and the files of each tree the row points at;
- an eval, the config file and the questions of what state records it last published.

Each file is read with one GET; a file a read returns nothing for, or a version, row or record
that is not there, is a `SnowflakePortError`, which leaves the update uncompared (SST-PLN041).
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledArtifact
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.desktop_contract import desktop_view, stage_pointers
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Change
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


def extension_sides(port: SnowflakePort, compiled: CompiledArtifact, change: Change) -> tuple[Definition, Definition]:
    """A skill's or plugin's served version and comment, against the bundle and comment it releases."""
    if not isinstance(compiled, CompiledExtension):
        raise TypeError(f"{compiled.artifact_key} did not compile as a CompiledExtension")
    release = compiled.release
    desired = extension_definition({entry.path: entry.content for entry in release.bundle.entries}, release.comment)
    extension = port.observe_extension(release.target)
    served = served_extension(extension, port.extension_versions(release.target))
    if extension is None or served is None:
        raise SnowflakePortError(f"{release.target.sql} serves no version to compare")
    location = served.location if served.location.endswith("/") else f"{served.location}/"
    files = {path: _read(port, f"{location}{path}") for path in port.list_location(location)}
    return extension_definition(files, extension.comment), desired


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


def profile_sides(port: SnowflakePort, compiled: CompiledArtifact, change: Change) -> tuple[Definition, Definition]:
    """A profile's registry row and the trees it points at, against the row and trees it releases."""
    if not isinstance(compiled, CompiledProfile):
        raise TypeError(f"{compiled.artifact_key} did not compile as a CompiledProfile")
    release = compiled.release
    trees = {
        f"{tree.kind}/{tree.scope}/": {entry.path: entry.content for entry in tree.entries} for tree in release.trees
    }
    desired = profile_definition(desktop_view(release.row), trees)
    row = port.read_profile_row(compiled.channel.registry, release.name)
    if row is None:
        raise SnowflakePortError(f"{compiled.channel.registry.sql} holds no row for profile {release.name}")
    view = desktop_view(row)
    live_trees: dict[str, dict[str, bytes]] = {}
    roots = dict(root for pointer in stage_pointers(view) if (root := tree_root(pointer)) is not None)
    for tree, location in roots.items():
        live_trees[tree] = {path: _read(port, f"{location}{path}") for path in port.list_location(location)}
    return profile_definition(view, live_trees), desired


def eval_sides(
    port: SnowflakePort, compiled: CompiledArtifact, change: Change, recorded: AppliedEntry | None
) -> tuple[Definition, Definition]:
    """The config and questions state records the eval last published, against those it renders.

    The questions are compared only when the source table state records can be read back; else
    the config alone is.
    """
    if not isinstance(compiled, CompiledEval):
        raise TypeError(f"{compiled.artifact_key} did not compile as a CompiledEval")
    rendered = compiled.rendered
    if recorded is None:
        raise SnowflakePortError("state records no earlier publish of the eval to compare")
    observation = change.composite_observation
    published = dict(recorded.component_fingerprints).get("config")
    if observation is None or not observation.config_path or published is None:
        raise SnowflakePortError("state records no config the eval published")
    folder = observation.config_path.rpartition("/")[0]
    live_config = _read(port, f"{folder}/{published}.yaml").decode("utf-8", errors="replace")
    table = next(
        (item.qualified_name for item in recorded.applied_resources if item.object_type.upper() == "TABLE"), None
    )
    if table is None:
        return eval_definition(live_config, ()), eval_definition(rendered.config_yaml, ())
    result = port.query(sql("SELECT INPUT_QUERY, GROUND_TRUTH FROM {table}", table=qname(QualifiedName.parse(table))))
    live = tuple((str(row[0]), row[1]) for row in result.rows)
    return eval_definition(live_config, live), eval_definition(
        rendered.config_yaml, payload_questions(rendered.dataset_payload)
    )


def _read(port: SnowflakePort, path: str) -> bytes:
    content = port.read_staged_file(path)
    if content is None:
        raise SnowflakePortError(f"{path} could not be read")
    return content


def _number(version: ExtensionVersion) -> int:
    number = version.name.upper().removeprefix("VERSION$")
    return int(number) if number.isdigit() else -1

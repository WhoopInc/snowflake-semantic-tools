"""Where a project publishes, for every target its profile declares, and which setting says so.

Each routed block of `sst_config.yml` -- `agents:`, `skills:` and the rest -- sets defaults at its
root and overrides them in folder routes, and each value may differ by target. `locations` reads
the configuration once per declared target, as a run for that target reads it, and reports each
route folder's settings with the step that set them, and the database and schema an artifact
placed there publishes in. An artifact's own folder is `ObjectSummary.route_folder`; the closest
route folder it starts with is the one that applies, since routes nest as folders do.

Nothing here compiles, connects, or reads a credential: a target's own database and schema are
read from `profiles.yml` alone, and one that names a variable this process lacks reads as None.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.adapters.dbt.profiles import TargetPlace, declared_targets, target_places
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.importing.placement import folded_part, route_folders
from snowflake_semantic_tools.domain.model.config_schema import (
    Setting,
    block_settings,
    config_block,
    resolve_settings,
    route_root,
    setting_value,
    target_text,
)
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY

# The keys whose values decide where an artifact publishes, in the order a name reads them.
LOCATION_KEYS = ("database", "schema")
# The block that places the state table: it takes `+` keys at its root, and no folder routes.
STATE_BLOCK = "state"
# Route roots an eval's routes may also name a folder under, as `sst` checks them.
_ALSO_UNDER: Mapping[str, tuple[str, ...]] = {"eval": ("cowork_profile",)}


@dataclass(frozen=True, slots=True)
class TargetLocation:
    """One target the profile declares, and where it publishes what no setting places.

    Attributes:
        name: The `profiles.yml` target name, such as `prod`.
        database: The target's own database, as `profiles.yml` sets it; None when it cannot be read.
        schema: The target's own schema, likewise.
        unreadable: Why its database or schema cannot be read, such as an unset `env_var()`; None
            when both can.
    """

    name: str
    database: str | None
    schema: str | None
    unreadable: str | None


@dataclass(frozen=True, slots=True)
class LocationKey:
    """One `+` key a block's root and routes may set.

    Attributes:
        key: The key without its `+`, as an artifact's `config:` writes it.
        summary: What it sets, as the configuration reference says.
        default: The default the reference prints; None when it has none.
        location: Whether it decides where an artifact publishes: `database` or `schema`.
    """

    key: str
    summary: str
    default: str | None
    location: bool


@dataclass(frozen=True, slots=True)
class Placement:
    """Where an artifact in one folder publishes on one target, as Snowflake resolves the names.

    Attributes:
        database: The database; None when it is the target's own and that cannot be read.
        schema: The schema, likewise.
    """

    database: str | None
    schema: str | None


@dataclass(frozen=True, slots=True)
class RouteFolder:
    """A block's root, or one folder a route names, and what it resolves to on each target.

    Attributes:
        folder: The folder below the block's route root, one segment per directory; empty for
            the root.
        exists: Whether the folder is a directory; a route to one that is not is SST-CFG041.
        settings: By target name, each key the folder resolves, without its `+`, with the
            step that set it: `root`, or `route:<a/b>`. Target conditionals are resolved for
            that target; `{{ target.* }}` is left as written. A key no step sets is absent, and
            takes its `LocationKey.default`.
        placement: By target name, where an artifact here publishes; empty for a block whose
            keys place nothing, such as `evals:`.
    """

    folder: tuple[str, ...]
    exists: bool
    settings: Mapping[str, Mapping[str, Setting]]
    placement: Mapping[str, Placement]


@dataclass(frozen=True, slots=True)
class BlockLocations:
    """One block of `sst_config.yml` that places or configures a type of artifact.

    Attributes:
        block: The block's name, such as `agents`.
        artifact_type: The registry artifact type it configures; None for `state`, which places
            SST's own state table.
        root: The directory its routes start from, relative to the project; empty for `state`.
        routed: Whether it takes folder routes; `state` does not.
        keys: The `+` keys it takes, in the configuration reference's order.
        folders: Its root, then each folder a route names, shallowest first.
    """

    block: str
    artifact_type: str | None
    root: str
    routed: bool
    keys: tuple[LocationKey, ...]
    folders: tuple[RouteFolder, ...]


@dataclass(frozen=True, slots=True)
class Locations:
    """Where every routed block of a project publishes, on every target its profile declares.

    Attributes:
        session_target: The target the session compiles for.
        default_target: The profile's default `target:`; None when it sets none.
        targets: Every target the profile declares, in its order.
        target_lists: Each variable of `vars:` that lists targets, such as `production_targets`,
            with the names it lists; a conditional's `in var(...)` arm reads one.
        blocks: Each routed block, in the registry's order, then `state`.
    """

    session_target: str
    default_target: str | None
    targets: tuple[TargetLocation, ...]
    target_lists: Mapping[str, tuple[str, ...]]
    blocks: tuple[BlockLocations, ...]


def project_locations(paths: ProjectPaths, session_target: str | None) -> Locations:
    """Read where the project in `paths` publishes, for every target its profile declares.

    Args:
        session_target: The target the session compiles for; None takes the profile's default.

    Raises:
        ProjectError: the configuration or `profiles.yml` cannot be read, as a run reports it.
    """
    _, declared, default = declared_targets(paths)
    places = target_places(paths)
    names = tuple(name for name in places if name in declared)
    session = session_target or default or ""
    trees = {name: resolved_config(paths, name).tree for name in names}
    # Routes are the block's mappings, which no conditional removes, so any target's tree has them.
    shape = resolved_config(paths, session_target).tree
    blocks = [
        _block(paths, shape, trees, places, artifact.routed_block, name)
        for name, artifact in ARTIFACT_REGISTRY.artifacts.items()
        if artifact.routed_block
    ]
    blocks.append(_block(paths, shape, trees, places, STATE_BLOCK, None))
    return Locations(
        session_target=session,
        default_target=default,
        targets=tuple(
            TargetLocation(name, places[name].database, places[name].schema, places[name].problem) for name in names
        ),
        target_lists=_target_lists(shape.get("vars"), frozenset(names)),
        blocks=tuple(blocks),
    )


def _block(
    paths: ProjectPaths,
    shape: Mapping[str, object],
    trees: Mapping[str, Mapping[str, object]],
    places: Mapping[str, TargetPlace],
    block: str,
    artifact_type: str | None,
) -> BlockLocations:
    """Read one block's keys, root and route folders; `artifact_type` None reads `state:`.

    A block places artifacts when it takes both location keys; `evals:` takes neither, so its
    folders carry no placement. A route SST-CFG041 reports, or one below it, is not there.
    """
    specs = block_settings(block)
    keys = tuple(LocationKey(key, spec.summary, spec.default, key in LOCATION_KEYS) for key, spec in specs.items())
    places_artifacts = all(key in specs for key in LOCATION_KEYS)
    folders: tuple[tuple[str, ...], ...] = ((),)
    missing: frozenset[str] = frozenset()
    root = ""
    if artifact_type is not None:
        artifact = ARTIFACT_REGISTRY.artifacts[artifact_type]
        root = route_root(shape, artifact)
        folders = route_folders(shape.get(block))
        found = routed_diagnostics(
            shape, paths.project_dir, artifact_type, also_under=_ALSO_UNDER.get(artifact_type, ())
        )
        missing = frozenset(str(item.subject).removeprefix("config_route:") for item in found)
    return BlockLocations(
        block=block,
        artifact_type=artifact_type,
        root=root,
        routed=artifact_type is not None,
        keys=keys,
        folders=tuple(
            _folder(paths, root, folder, missing, block, trees, places if places_artifacts else None)
            for folder in folders
        ),
    )


def _folder(
    paths: ProjectPaths,
    root: str,
    folder: tuple[str, ...],
    missing: frozenset[str],
    block: str,
    trees: Mapping[str, Mapping[str, object]],
    places: Mapping[str, TargetPlace] | None,
) -> RouteFolder:
    # A route under one SST-CFG041 reports is not walked, so it is missing too.
    absent = any(".".join(folder[: depth + 1]) in missing for depth in range(len(folder)))
    exists = not absent and (paths.project_dir / root / "/".join(folder)).is_dir()
    settings = {name: resolve_settings(tree.get(block), folder) for name, tree in trees.items()}
    placement = {} if places is None else {name: _placement(settings[name], places[name]) for name in settings}
    return RouteFolder(
        folder=folder,
        exists=exists,
        settings=MappingProxyType(settings),
        placement=MappingProxyType(placement),
    )


def _placement(settings: Mapping[str, Setting], own: TargetPlace) -> Placement:
    """Where an artifact with `settings` publishes: each part set and not empty, else the target's."""

    def part(key: str, fallback: str | None) -> str | None:
        value = setting_value(settings, key)
        if isinstance(value, str) and "{{ target." in value:
            if own.database is None or own.schema is None:
                return None
            value = target_text(value, DbtTarget(database=own.database, schema=own.schema), None)
        text = fallback if value is None or str(value) == "" else str(value)
        return None if text is None else folded_part(text)

    return Placement(part("database", own.database), part("schema", own.schema))


def _target_lists(declared_vars: object, targets: frozenset[str]) -> Mapping[str, tuple[str, ...]]:
    """Each variable that lists only declared target names, in `vars:` order."""
    lists = {
        name: tuple(value)
        for name, value in config_block(declared_vars).items()
        if isinstance(value, list) and value and all(isinstance(item, str) and item in targets for item in value)
    }
    return MappingProxyType(lists)

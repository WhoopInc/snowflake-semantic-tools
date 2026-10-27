"""Where an imported object's files go: the folder whose route publishes it where it already is.

A file compiles to the database and schema its folder routes resolve to on the target, over
the target's own, so an import writes the object under a folder that resolves to the
object's live location; adopt then finds the compiled artifact at the live object's name. Of
the folders that do, one already holding artifacts of the type there is taken, the most used,
and otherwise the shallowest a route names, the type's root first. When none does, the
object's file names its location in `config:` itself (SST-IMP014).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.config_schema import (
    config_block,
    resolve_settings,
    setting_value,
    target_text,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity


@dataclass(frozen=True, slots=True)
class Destination:
    """The folder an imported object goes in, below its type's directory.

    Attributes:
        folder: One segment per directory; empty for the type's root.
        overridden: No folder resolves to the live location, so the file's `config:` names it.
    """

    folder: tuple[str, ...]
    overridden: bool = False


def placement(block: object, folder: tuple[str, ...], target: TargetIdentity) -> tuple[str, str]:
    """Return the database and schema an artifact in `folder` publishes in, folded, as compile does."""
    settings = resolve_settings(block, folder)
    database = target_text(setting_value(settings, "database"), target, None) or target.database.folded
    schema = target_text(setting_value(settings, "schema"), target, None) or target.schema.folded
    return _folded(database), _folded(schema)


def inherited(block: object, folder: tuple[str, ...], target: TargetIdentity) -> Mapping[str, object]:
    """Return each setting an artifact in `folder` inherits from its routes, templates rendered."""
    return {
        key: target_text(setting.value, target, None) if isinstance(setting.value, str) else setting.value
        for key, setting in resolve_settings(block, folder).items()
    }


def route_folders(block: object) -> tuple[tuple[str, ...], ...]:
    """Return every folder a routed block names, the root first, then shallowest first."""
    found = [(), *_routes(config_block(block), ())]
    return tuple(sorted(dict.fromkeys(found), key=lambda folder: (len(folder), folder)))


def destination(
    block: object,
    target: TargetIdentity,
    live: QualifiedName,
    used: tuple[tuple[str, ...], ...],
) -> Destination:
    """Return the folder that publishes `live` where it is on `target`.

    Args:
        block: The type's routed block, as the configuration renders it for the target.
        used: The folders that already hold artifacts of the type in `live`'s schema, most used first.
    """
    location = live.folded[:2]
    for folder in used:
        if placement(block, folder, target) == location:
            return Destination(folder)
    for folder in route_folders(block):
        if placement(block, folder, target) == location:
            return Destination(folder)
    return Destination((), overridden=True)


def _routes(block: Mapping[str, object], trail: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for key, value in block.items():
        if isinstance(key, str) and not key.startswith("+") and isinstance(value, Mapping):
            folder = (*trail, key)
            yield folder
            yield from _routes(config_block(value), folder)


def _folded(value: str) -> str:
    """Return a rendered location part as Snowflake resolves it."""
    try:
        return Identifier.parse(value).folded
    except ValueError:
        return value.upper()

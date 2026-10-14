"""Resolve the settings of one artifact through the one precedence chain.

Every key of a routed block resolves the same way, closest first:

1. the artifact's own `config:` block, which writes the key without its `+`;
2. the closest folder route over the artifact's file that sets `+key`;
3. the block root's `+key`;
4. the consumer's built-in default.

An absent key inherits from the next step down. A key written with no value is an error that
`domain.validate.settings` and `domain.validate.config` report (SST-CFG054); here it reads as
absent, so the run that reports it still has a value to work with. This module only folds
values; it never checks them, and it renders no `{{ target.* }}` template.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.config_schema.keys import CHILDREN, ConfigKey, KeyStatus
from snowflake_semantic_tools.domain.model.config_schema.values import config_block
from snowflake_semantic_tools.domain.model.setting import (
    CONFIG_SOURCE,
    DEFAULT_SOURCE,
    ROOT_SOURCE,
    ROUTE_PREFIX,
    Setting,
)

__all__ = [
    "CONFIG_SOURCE",
    "DEFAULT_SOURCE",
    "ROOT_SOURCE",
    "ROUTE_PREFIX",
    "Setting",
    "block_settings",
    "resolve_settings",
    "setting_value",
    "with_defaults",
]


def block_settings(block: str) -> Mapping[str, ConfigKey]:
    """Return the `+` keys of one routed block, by their name without the `+`, in table order.

    These are the keys a folder route may set and, unless one is root-only, the keys an
    artifact's `config:` may set; removed keys are left out.
    """
    return MappingProxyType(
        {
            name[1:]: spec
            for name, spec in CHILDREN.get(block, {}).items()
            if name.startswith("+") and spec.status is KeyStatus.CURRENT
        }
    )


def resolve_settings(block: object, folder: Sequence[str], item_config: object = None) -> Mapping[str, Setting]:
    """Resolve each key an artifact in `folder` sets, closest step of the precedence chain first.

    The block's root `+` keys come first; each folder route along `folder` overrides only the
    keys it sets, so the closest folder wins, and the walk stops at the first directory with no
    route; the artifact's own `config:` overrides them all. A key no step sets is absent, so
    the consumer applies its built-in default. A null value at any step reads as absent: it is
    an error reported elsewhere, never a value.

    Args:
        block: The routed block as parsed, such as `agents:`; anything but a mapping is empty.
        folder: The artifact file's directory below the block's root, one segment per directory.
        item_config: The artifact's `config:` block as parsed; anything but a mapping is empty.

    Returns:
        Each key that resolved, without its `+`, with where its value came from.
    """
    resolved: dict[str, Setting] = {}
    cursor = config_block(block)
    _fold(resolved, cursor, ROOT_SOURCE)
    trail: list[str] = []
    for part in folder:
        child = cursor.get(part)
        if not isinstance(child, dict):
            break
        cursor = config_block(child)
        trail.append(part)
        _fold(resolved, cursor, ROUTE_PREFIX + "/".join(trail))
    for key, value in config_block(item_config).items():
        if value is not None and not key.startswith("+"):
            resolved[key] = Setting(key, value, CONFIG_SOURCE)
    return MappingProxyType(resolved)


def _fold(resolved: dict[str, Setting], block: Mapping[str, object], source: str) -> None:
    for key, value in block.items():
        if key.startswith("+") and value is not None:
            resolved[key[1:]] = Setting(key[1:], value, source)


def setting_value(settings: Mapping[str, Setting], key: str) -> object:
    """Return the value a key resolved to; None when no step of the chain set it."""
    found = settings.get(key)
    return None if found is None else found.value


def with_defaults(settings: Mapping[str, Setting], defaults: Mapping[str, object]) -> tuple[Setting, ...]:
    """Return the resolved settings, then each built-in default for a key nothing set, sorted by key.

    A default of None names no value, and is left out; this is what an artifact reports as
    its settings, so `sst list --long` can say where each came from.
    """
    filled = dict(settings)
    for key, value in defaults.items():
        if key not in filled and value is not None:
            filled[key] = Setting(key, value, DEFAULT_SOURCE)
    return tuple(filled[key] for key in sorted(filled))

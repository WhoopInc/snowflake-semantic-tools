"""Serialize a plain value as YAML, for `--output yaml`."""

from __future__ import annotations

import yaml


def dump_yaml(value: object) -> str:
    """Return `value` -- dicts, lists, and scalars -- as block-style YAML with keys in the order given."""
    return yaml.safe_dump(value, sort_keys=False, default_flow_style=False, allow_unicode=True)

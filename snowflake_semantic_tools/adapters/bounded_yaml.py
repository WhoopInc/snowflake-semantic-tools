"""Parsing YAML with a nesting bound, so a crafted file is refused as YAML rather than a crash.

PyYAML composes nested collections by recursion, so a file nesting them a few thousand levels
deep raises `RecursionError` from inside the parser. `BoundedSafeLoader` is `yaml.SafeLoader`
that refuses a node nested deeper than `MAX_YAML_DEPTH` with a `yaml.composer.ComposerError` at
its mark: a `yaml.YAMLError`, which every reader already reports with its own code for a file
that is not valid YAML. Every YAML document SST parses with PyYAML goes through it.
"""

from __future__ import annotations

import re
from typing import Any

import yaml

# Far beyond the nesting of any configuration, profile, or semantic file, and far within
# Python's recursion limit for the composer and for code that walks the value.
MAX_YAML_DEPTH = 200
# What `safe_load` and `compose` raise on text that is not YAML, for a caller that may not import
# PyYAML itself.
YAMLError = yaml.YAMLError


class BoundedSafeLoader(yaml.SafeLoader):
    """`yaml.SafeLoader`, refusing a node nested deeper than `MAX_YAML_DEPTH`."""

    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self._depth = 0

    def compose_node(self, parent: yaml.Node | None, index: int) -> yaml.Node | None:
        """Compose the next node, as `yaml.SafeLoader` does, unless it is nested past the bound."""
        self._depth += 1
        try:
            if self._depth > MAX_YAML_DEPTH:
                mark = self.peek_event().start_mark  # type: ignore[no-untyped-call]
                raise yaml.composer.ComposerError(
                    None, None, f"collections nest deeper than {MAX_YAML_DEPTH} levels", mark
                )
            return super().compose_node(parent, index)
        finally:
            self._depth -= 1


def safe_load(text: str) -> Any:
    """Return the one document in `text` as `yaml.safe_load` does, within the nesting bound.

    Raises:
        yaml.YAMLError: the text is not YAML, holds more than one document, or nests deeper
            than `MAX_YAML_DEPTH`.
    """
    loader = BoundedSafeLoader(text)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def compose(text: str) -> yaml.Node | None:
    """Return the one document in `text` as `yaml.compose` does with a safe loader, within the bound.

    Raises:
        yaml.YAMLError: the text is not YAML, holds more than one document, or nests deeper
            than `MAX_YAML_DEPTH`.
    """
    loader = BoundedSafeLoader(text)
    try:
        return loader.get_single_node()
    finally:
        loader.dispose()


_BOOL = "tag:yaml.org,2002:bool"
_TIMESTAMP = "tag:yaml.org,2002:timestamp"


class _CoreSchemaLoader(BoundedSafeLoader):
    """`BoundedSafeLoader` reading plain scalars by YAML 1.2's core schema, as Snowflake writes them.

    YAML 1.1 reads a plain `2016-09-01 00:00:00.000` as a timestamp and `yes` as true; 1.2 reads
    both as text, and only `true` and `false` as booleans.
    """


_CoreSchemaLoader.yaml_implicit_resolvers = {
    first: [(tag, pattern) for tag, pattern in resolvers if tag not in (_BOOL, _TIMESTAMP)]
    for first, resolvers in BoundedSafeLoader.yaml_implicit_resolvers.items()
}
_CoreSchemaLoader.add_implicit_resolver(_BOOL, re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF"))


def safe_load_core(text: str) -> Any:
    """Return the one document in `text` read by YAML 1.2's core schema, within the nesting bound.

    For YAML Snowflake writes, such as `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW`'s, where a plain
    timestamp or `no` is text.

    Raises:
        yaml.YAMLError: the text is not YAML, holds more than one document, or nests deeper
            than `MAX_YAML_DEPTH`.
    """
    loader = _CoreSchemaLoader(text)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()

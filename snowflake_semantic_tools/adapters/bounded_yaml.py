"""Parsing YAML with a nesting bound, so a crafted file is refused as YAML rather than a crash.

PyYAML composes nested collections by recursion, so a file nesting them a few thousand levels
deep raises `RecursionError` from inside the parser. `BoundedSafeLoader` is `yaml.SafeLoader`
that refuses a node nested deeper than `MAX_YAML_DEPTH` with a `yaml.composer.ComposerError` at
its mark: a `yaml.YAMLError`, which every reader already reports with its own code for a file
that is not valid YAML. Every YAML document SST parses with PyYAML goes through it.
"""

from __future__ import annotations

from typing import Any

import yaml

# Far beyond the nesting of any configuration, profile, or semantic file, and far within
# Python's recursion limit for the composer and for code that walks the value.
MAX_YAML_DEPTH = 200


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

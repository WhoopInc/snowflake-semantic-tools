"""One resolved setting of an artifact, and where its value came from.

A data type only: `domain.model.config_schema.settings` resolves settings, and the artifact
models carry them, so this module imports nothing of either.
"""

from __future__ import annotations

from dataclasses import dataclass

# Where a setting's value came from, as `Setting.source` names it.
CONFIG_SOURCE = "config"
ROOT_SOURCE = "root"
DEFAULT_SOURCE = "default"
ROUTE_PREFIX = "route:"


@dataclass(frozen=True, slots=True)
class Setting:
    """One resolved setting of an artifact, and the step of the precedence chain that set it.

    Attributes:
        key: The key without its `+`, as `config:` writes it.
        value: The value as written, never None; a template in it is not rendered.
        source: `config` for the artifact's own `config:`; `route:<a/b>` for the closest folder
            route that sets it, named by its directories below the block root; `root` for the
            block's own `+key`; `default` for a built-in default a consumer applied.
    """

    key: str
    value: object
    source: str

    def as_dict(self) -> dict[str, object]:
        """Return the setting as the manifest records it: its value and its source."""
        return {"value": self.value, "source": self.source}

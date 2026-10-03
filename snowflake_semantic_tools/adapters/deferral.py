"""Which target a run defers to, and where that target's dbt manifest is read from.

`--defer-target` (else `$SST_DEFER_TARGET`, else `defer.target`) names a `profiles.yml` target
whose relations the project's dbt objects resolve to, while the run itself publishes to its own
target; `--no-defer` turns deferral off whatever is configured. The deferred target's manifest is
`manifest.json` under `defer.state_path`, else under `target/sst/defer/<target>`. With
`defer.auto_compile`, SST produces it by running dbt for that target into that directory;
otherwise it is read as supplied.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import declared_targets
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.domain.diagnostics import D, Origin
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool

# Where a deferred target's manifest is kept when `defer.state_path` does not say.
DEFAULT_STATE_DIR = Path("target") / "sst" / "defer"


@dataclass(frozen=True, slots=True)
class Deferral:
    """The target a run defers to, and how its manifest is had.

    Attributes:
        target: The `profiles.yml` target whose relations dbt objects resolve to.
        state_dir: The directory holding that target's `manifest.json`.
        produce: Whether SST runs dbt for the target to write it there (`defer.auto_compile`).
    """

    target: str
    state_dir: Path
    produce: bool

    @property
    def manifest(self) -> Path:
        """Return the deferred target's dbt manifest."""
        return self.state_dir / "manifest.json"


def resolve_deferral(files: ProjectPaths) -> Deferral | None:
    """Return the run's deferral, or None when it defers to no target.

    Raises:
        ProjectError: the target is not declared in `profiles.yml` (SST-CFG010), or the profile
            cannot be read.

    Diagnostics:
        SST-CFG010: the deferred target is absent from the profile; raised.
    """
    if files.defer_disabled:
        return None
    config = resolved_config(files)
    defer = config_block(config.tree.get("defer"))
    configured = defer.get("target")
    target = files.defer_target or (configured if isinstance(configured, str) and configured else None)
    if target is None:
        return None
    profile, declared, _ = declared_targets(files)
    if target not in declared:
        diagnostic = D(
            "SST-CFG010", origin=Origin(config.file), subject="config:defer.target", target=target, profile=profile
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    state_path = defer.get("state_path")
    state_dir = Path(state_path) if isinstance(state_path, str) and state_path else DEFAULT_STATE_DIR / target
    return Deferral(
        target,
        state_dir if state_dir.is_absolute() else files.project_dir / state_dir,
        produce=config_bool(defer.get("auto_compile")) is True,
    )

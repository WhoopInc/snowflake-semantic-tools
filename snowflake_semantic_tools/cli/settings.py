"""Typed readers for the `sst_config.yml` settings the commands consult themselves.

Everything else in the file reaches the use cases through `ProjectInputs`.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_int, configured_dir


def project_config(project_dir: Path) -> dict[str, object]:
    """Return the project's `sst_config.yml` as a plain tree."""
    return dict(load_project_config(project_dir).tree)


def semantic_models_dir(project_dir: Path) -> str:
    """Return `project.semantic_models_dir`, or `semantic_models` when it is not set."""
    return configured_dir(project_config(project_dir), "semantic_models_dir", "semantic_models")


def apply_parallelism(project_dir: Path) -> int:
    """Return how many changes of one wave apply runs at once: `skills.+threads` (1..16), else 4."""
    return config_int(config_block(project_config(project_dir).get("skills")).get("+threads")) or 4


def validation_settings(project_dir: Path, *, strict: bool | None, connected: bool | None) -> tuple[bool, bool]:
    """Return whether to validate strictly, and against Snowflake: each flag given, else `validation:`."""
    return project_inputs(project_dir, None, None).validation_defaults().resolve(strict, connected)

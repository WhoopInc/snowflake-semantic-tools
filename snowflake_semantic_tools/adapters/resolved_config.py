"""The run's configuration: read once per target, with its templates resolved, and read from there.

Every reader of `sst_config.yml` -- the CLI's settings, the severity policy, the use cases through
`ProjectInputs`, the manifest's checksums -- goes through `resolved_config`, so no two of them can
see the same key two ways. The file is parsed and checked once for each target a run resolves it
against, and the result is kept on the run's `ProjectPaths`.

One read stays outside it by construction: which `profiles.yml` profile a project uses,
`project.target_profile`, is read as written, since the target a conditional compares against is
found through that profile.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.adapters.dbt.profiles import declared_targets
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.ports.project import ProjectConfig
from snowflake_semantic_tools.domain.resolve.config import has_target_conditional, render_config


def resolved_config(files: ProjectPaths, target_name: str | None = None) -> ProjectConfig:
    """Return the checked configuration with its target conditionals and `var()` calls resolved.

    The target is `target_name`, else the run's `files.target_name`; the profile's default target
    is read from `profiles.yml` only when a conditional needs it. The first call for a target reads
    the file, and every later one returns that value.

    Raises:
        ProjectError: the file cannot be read or parsed, or a needed profile cannot be resolved.
    """
    target = target_name if target_name is not None else files.target_name
    key = (target, files.semantic_models_dir)
    cached = files.resolved.get(key)
    if cached is None:
        cached = load_project_config(files, lambda tree: _render(files, target, tree))
        files.resolved[key] = cached
    return cached


def _render(files: ProjectPaths, target: str | None, tree: Mapping[str, Any]) -> tuple[dict[str, Any], DiagnosticBag]:
    name = target
    if name is None and has_target_conditional(tree):
        name = declared_targets(files)[2]
    return render_config(tree, target_name=name, file=files.config_name)

"""Read `sst_config.yml` once, with source positions, and check its declared shape."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from ..domain.model.config_schema import CONFIG_FILE, validate_config
from ..domain.model.diagnostic import D, DiagnosticBag, Origin
from .project import ProjectError
from .yaml.loader import _parse_yaml_bytes

# Blocks and directory keys whose artifacts are compiled from a dbt project.
DBT_ONLY_KEYS = (
    "semantic_views",
    "agents",
    "tools",
    "evals",
    "project.semantic_models_dir",
    "project.agents_dir",
    "project.tools_dir",
    "project.eval_metrics_dir",
)

# Every `project.*_dir` key. A configured one that does not exist is an error: the
# loaders would find nothing there, and `--prune` would remove what it published.
PROJECT_DIR_KEYS = (
    "semantic_models_dir",
    "agents_dir",
    "eval_metrics_dir",
    "tools_dir",
    "skills_dir",
    "plugins_dir",
    "profiles_dir",
    "hooks_dir",
    "mcp_servers_dir",
    "commands_dir",
)


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    tree: Mapping[str, Any]
    diagnostics: DiagnosticBag
    has_dbt_project: bool


def load_project_config(project_dir: Path) -> ProjectConfig:
    """Parse the config file, apply the deprecated `deploy:` alias, and validate it."""
    has_dbt_project = (project_dir / "dbt_project.yml").is_file()
    path = project_dir / CONFIG_FILE
    if not path.is_file():
        return ProjectConfig(MappingProxyType({}), DiagnosticBag(), has_dbt_project)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProjectError(f"cannot read {path}: {exc}") from exc
    parsed = _parse_yaml_bytes(raw, CONFIG_FILE)
    positions = {
        key: (position.line, position.col) for key, position in parsed.line_index.items() if isinstance(key, tuple)
    }
    diagnostics = list(validate_config(parsed.tree, positions=positions))
    tree = dict(parsed.tree)
    if "deploy" in tree:
        if "apply" in tree:
            diagnostics.append(
                D(
                    "SST-CFG043",
                    origin=Origin(CONFIG_FILE, *positions.get(("deploy",), (None, None))),
                    subject="config:deploy",
                    key="deploy",
                    reason="apply: is also set; delete deploy:",
                )
            )
            del tree["deploy"]
        else:
            tree["apply"] = tree.pop("deploy")
    project = tree.get("project")
    dbt_only_dirs: set[str] = set()
    if not has_dbt_project:
        for dotted in DBT_ONLY_KEYS:
            head, _, tail = dotted.partition(".")
            present = head in tree if not tail else isinstance(project, dict) and tail in project
            if present:
                dbt_only_dirs.add(tail)
                diagnostics.append(
                    D(
                        "SST-CFG046",
                        origin=Origin(CONFIG_FILE, *positions.get(tuple(dotted.split(".")), (None, None))),
                        subject=f"config:{dotted}",
                        key=dotted,
                    )
                )
    if isinstance(project, dict):
        for key in PROJECT_DIR_KEYS:
            value = project.get(key)
            if key in dbt_only_dirs or not isinstance(value, str) or not value:
                continue
            if not (project_dir / value).is_dir():
                diagnostics.append(
                    D(
                        "SST-CFG047",
                        origin=Origin(CONFIG_FILE, *positions.get(("project", key), (None, None))),
                        subject=f"config:project.{key}",
                        key=f"project.{key}",
                        value=value,
                    )
                )
    return ProjectConfig(MappingProxyType(tree), DiagnosticBag(diagnostics), has_dbt_project)


def config_tree(project_dir: Path) -> dict[str, Any]:
    """The normalized config mapping, for callers that only read values."""
    return dict(load_project_config(project_dir).tree)

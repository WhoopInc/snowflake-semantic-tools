"""Read `sst_config.yml` once, with source positions, and check its declared shape."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

import yaml

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, validate_config
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.ports.project import ProjectConfig

# Blocks and directory keys whose artifacts are compiled from a dbt project.
DBT_ONLY_KEYS = (
    "semantic_views",
    "agents",
    "tools",
    "evals",
    "enrichment",
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


def load_project_config(project_dir: Path) -> ProjectConfig:
    """Parse the config file and validate it."""
    has_dbt_project = (project_dir / "dbt_project.yml").is_file()
    path = project_dir / CONFIG_FILE
    if not path.is_file():
        return ProjectConfig(MappingProxyType({}), DiagnosticBag(), has_dbt_project)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProjectError(f"cannot read {path}: {exc}") from exc
    parsed = parse_yaml_bytes(raw, CONFIG_FILE)
    positions = {
        key: (position.line, position.col) for key, position in parsed.line_index.items() if isinstance(key, tuple)
    }
    diagnostics = list(validate_config(parsed.tree, positions=positions))
    tree = dict(parsed.tree)
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


def read_config_document(project_dir: Path) -> object | None:
    """Return `sst_config.yml` parsed as plain YAML, or None when the file does not exist.

    This is the document as written, with no templates neutralized and no positions: the
    manifest's config checksum hashes exactly this value, so it must stay a plain parse.
    An empty file reads as `{}`.
    """
    path = project_dir / CONFIG_FILE
    if not path.is_file():
        return None
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

"""Read the configuration file a run resolved, once, with source positions, and check its shape.

Which file that is was decided by `adapters.locations.locate_project`; nothing here looks for one.
"""

from __future__ import annotations

from types import MappingProxyType

import yaml

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.ports.project import ProjectConfig
from snowflake_semantic_tools.domain.validate.config import unstated_policy, validate_config

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


def load_project_config(files: ProjectPaths) -> ProjectConfig:
    """Parse the resolved configuration file and validate it; an empty tree when there is none.

    Raises:
        ProjectError: the file cannot be read (SST-PRT009), is not valid YAML (SST-CFG002), or
            fails to load as one YAML mapping, as `parse_yaml_bytes` raises.

    Diagnostics:
        SST-CFG002: the file is not valid YAML; raised.
        SST-PRT009: the file cannot be read; raised.
        SST-CFG032: an explicit configuration file shadows a discovered one.
        SST-CFG046: dbt-only configuration is present without a dbt project.
        SST-CFG047: a `project.*_dir` key names no directory.
        SST-CFG031: `validation.snowflake_syntax_check` is not stated.
        Each code `validate_config` reports.
    """
    project_dir = files.project_dir
    has_dbt_project = (project_dir / "dbt_project.yml").is_file()
    if files.config_file is None:
        return ProjectConfig(MappingProxyType({}), DiagnosticBag(), has_dbt_project)
    name = files.config_name
    parsed = _parse_config(files)
    positions = {
        key: (position.line, position.col) for key, position in parsed.line_index.items() if isinstance(key, tuple)
    }
    diagnostics = [
        *files.diagnostics,
        *parsed.diagnostics,
        *validate_config(parsed.tree, positions=positions, file=name),
        *unstated_policy(parsed.tree, file=name),
    ]
    tree = dict(parsed.tree)
    if "deploy" in tree and "apply" not in tree:
        # The deprecated spelling is read as the block it was renamed to; SST-CFG200 says so.
        tree["apply"] = tree["deploy"]
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
                        origin=Origin(name, *positions.get(tuple(dotted.split(".")), (None, None))),
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
                        origin=Origin(name, *positions.get(("project", key), (None, None))),
                        subject=f"config:project.{key}",
                        key=f"project.{key}",
                        value=value,
                    )
                )
    return ProjectConfig(MappingProxyType(tree), DiagnosticBag(diagnostics), has_dbt_project, name)


def _parse_config(files: ProjectPaths) -> ParsedYaml:
    """Read and parse the configuration file, reporting a YAML syntax error as SST-CFG002."""
    assert files.config_file is not None
    try:
        raw = files.config_file.read_bytes()
    except OSError as exc:
        diagnostic = D("SST-PRT009", subject=f"config:{files.config_name}", path=files.config_name, detail=str(exc))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    try:
        return parse_yaml_bytes(raw, files.config_name)
    except ProjectError as exc:
        syntax = [item for item in exc.diagnostics if item.code == "SST-LOD001"]
        if not syntax:
            raise
        context = syntax[0].context
        diagnostic = D(
            "SST-CFG002",
            origin=Origin(files.config_name, context["line"], context["col"]),
            subject=f"config:{files.config_name}",
            path=files.config_name,
            detail=str(context["detail"]),
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc


def read_config_document(files: ProjectPaths) -> object | None:
    """Return the configuration file parsed as plain YAML, or None when the run has none.

    This is the document as written, with no templates neutralized and no positions: the
    manifest's config checksum hashes exactly this value, so it must stay a plain parse.
    An empty file reads as `{}`.
    """
    if files.config_file is None:
        return None
    return yaml.safe_load(files.config_file.read_text(encoding="utf-8")) or {}

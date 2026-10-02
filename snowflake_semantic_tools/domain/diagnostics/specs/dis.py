"""Discovery codes (DIS): finding a project's files, and which artifact type owns each.

Discovery runs before anything is built from a file, and decides only where files are and
which type claims them; what a file contains is the load and parse phases' concern.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Discovery"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-DIS001",
        Severity.ERROR,
        "Semantic path does not exist",
        "{path} does not exist",
        "create the directory, or correct project.semantic_models_dir",
    ),
    spec(
        "SST-DIS002",
        Severity.ERROR,
        "Semantic path is not a directory",
        "{path} is not a directory",
        "point project.semantic_models_dir at a directory",
    ),
    spec(
        "SST-DIS003",
        Severity.WARNING,
        "No candidate files found",
        "no candidate files under {path}",
        "check project.semantic_models_dir and the file-name conventions",
    ),
    spec("SST-DIS004", Severity.ERROR, "File not readable", "{path} is not readable", "fix the file permissions"),
    spec(
        "SST-DIS005",
        Severity.ERROR,
        "Symlink loop or depth limit exceeded",
        "{path} exceeds the traversal depth limit",
        "remove the symlink loop, or flatten the tree",
    ),
    spec(
        "SST-DIS006",
        Severity.ERROR,
        "Two paths collide under case folding",
        "{a} and {b} collide under case folding",
        "rename one file",
    ),
    spec(
        "SST-DIS007",
        Severity.ERROR,
        "File claimed by two artifact types",
        "{path} is claimed by {types}",
        "move the file, or give the types' directories distinct paths",
    ),
    spec(
        "SST-DIS008",
        Severity.WARNING,
        "File matches no artifact type",
        "{path} matches no registered artifact type",
        "remove the file, or give it a root key a registered type owns",
    ),
    spec(
        "SST-DIS009",
        Severity.ERROR,
        "model-paths directory does not exist",
        "dbt model-paths entry {path} does not exist",
        "correct model-paths in dbt_project.yml",
    ),
    spec(
        "SST-DIS010",
        Severity.WARNING,
        "Selector matched nothing",
        "--select {selector} matched no artifact",
        "widen the selector, or check the artifact name",
    ),
    spec("SST-DIS200", Severity.INFO, "Artifact ownership assigned", "{path} assigned to {type}", None),
    spec("SST-DIS201", Severity.INFO, "Files excluded by selector", "{count} files excluded by --exclude", None),
)

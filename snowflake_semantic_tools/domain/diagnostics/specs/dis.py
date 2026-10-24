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
        "create the directory, or correct project.semantic_models_dir or --semantic",
        condition="the semantic-model directory (`project.semantic_models_dir`, or `--semantic`) does not exist",
    ),
    spec(
        "SST-DIS002",
        Severity.ERROR,
        "Semantic path is not a directory",
        "{path} is not a directory",
        "point project.semantic_models_dir or --semantic at a directory",
        condition="the semantic-model directory (`project.semantic_models_dir`, or `--semantic`) is not a directory",
    ),
    spec(
        "SST-DIS003",
        Severity.WARNING,
        "No candidate files found",
        "no candidate files under {path}",
        "check project.semantic_models_dir, --semantic, and validation.exclude_dirs",
        condition="the walk of the semantic-model directory found no YAML file",
    ),
    spec(
        "SST-DIS004",
        Severity.ERROR,
        "File not readable",
        "{path} is not readable",
        "fix the file permissions",
        condition="a discovered file is not readable by the current user; it is left out",
    ),
    spec(
        "SST-DIS005",
        Severity.ERROR,
        "Symlink loop or depth limit exceeded",
        "{path} is not walked: it links back into the tree, or lies more than 32 directories deep",
        "remove the symlink loop, or flatten the tree",
        condition=(
            "a symlink points back into the tree or at itself, or a directory is more than 32 levels "
            "deep; it is not walked"
        ),
        note="The message does not say which of the two applies.",
    ),
    spec(
        "SST-DIS006",
        Severity.ERROR,
        "Two paths collide under case folding",
        "{a} and {b} collide under case folding",
        "rename one file",
        condition="two discovered paths differ only by case; the later one is left out",
    ),
    spec(
        "SST-DIS007",
        Severity.ERROR,
        "Directory claimed by two artifact types",
        "{path} is claimed by {types}",
        "give each artifact type its own directory, neither inside the other",
        condition="two artifact types' configured directories are one directory, or one contains the other",
    ),
    spec(
        "SST-DIS008",
        Severity.WARNING,
        "File matches no artifact type",
        "{path} matches no registered artifact type",
        "remove the file, or add its root key to a type",
        condition="a YAML document holds keys and none of them is a registered artifact type's root key",
    ),
    spec(
        "SST-DIS009",
        Severity.ERROR,
        "model-paths directory does not exist",
        "dbt model-paths entry {path} does not exist",
        "correct model-paths in dbt_project.yml",
        condition="a `model-paths` entry in dbt_project.yml is not a directory",
    ),
    spec(
        "SST-DIS010",
        Severity.WARNING,
        "Selector matched nothing",
        "--select {selector} matched no artifact",
        "widen the selector, or check the artifact name",
        condition="a `--select` value matches no artifact; `state:` selectors are not checked",
    ),
    spec(
        "SST-DIS011",
        Severity.WARNING,
        "Exclusion matched nothing",
        "--exclude {selector} matched no artifact",
        "check the artifact name, or drop the exclusion",
        condition=(
            "an `--exclude` value matches no artifact, so it leaves nothing out; `state:` selectors are not checked"
        ),
    ),
    spec(
        "SST-DIS012",
        Severity.ERROR,
        "Nothing to smoke test",
        "nothing to smoke test: the selection compiles no artifact `sst test` can probe",
        "add a semantic view, or widen --select and --exclude",
        condition=(
            "`sst test` runs on a selection that compiles no artifact with a smoke probe -- no "
            "semantic view, tool, agent, or CoWork profile -- so it would probe nothing"
        ),
    ),
    spec(
        "SST-DIS013",
        Severity.ERROR,
        "Selection matched no dbt model",
        "{selector} matched no dbt model",
        "check the PATH or selector against model-paths and the model names",
        condition=(
            "`sst enrich` is given a PATH or `--select` value and no dbt model of the project matches "
            "any of them, so there is nothing to enrich; one per value"
        ),
    ),
    spec(
        "SST-DIS200",
        Severity.INFO,
        "Artifact ownership assigned",
        "{path} assigned to {type}",
        None,
        condition="a YAML document is assigned to the artifact type whose root key it holds; shown with --show-info",
    ),
    spec(
        "SST-DIS201",
        Severity.INFO,
        "Files excluded by selector",
        "{count} files excluded by --exclude",
        None,
        condition="`--exclude` left artifacts out of the run; the count is of their distinct source files",
    ),
)

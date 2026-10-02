"""The reference fixture with one authored file edited, loaded for the diagnostics it reports.

The per-code semantic tests change the smallest span of a real project that makes a code fire,
so a code is shown to fire through the loader a user runs, not only through a helper.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from tests.helpers.cli_projects import MANIFEST, project_copy
from tests.helpers.projects import load_project

FILTERS = "semantic_models/filters/filters.yml"
METRICS = "semantic_models/metrics/metrics.yml"
INSTRUCTIONS = "semantic_models/custom_instructions/custom_instructions.yml"
QUERIES = "semantic_models/verified_queries/verified_queries.yml"
VIEWS = "semantic_models/semantic_views/semantic_views.yml"


def edited(tmp_path: Path, path: str, old: str, new: str) -> Path:
    """Copy the fixture and replace the first `old` in `path` with `new`; `old` must be there."""
    project = project_copy(tmp_path)
    target = project / path
    text = target.read_text(encoding="utf-8")
    assert old in text, f"{old!r} is not in {path}"
    target.write_text(text.replace(old, new, 1), encoding="utf-8")
    return project


def appended(tmp_path: Path, path: str, text: str) -> Path:
    """Copy the fixture and append `text` to `path`."""
    project = project_copy(tmp_path)
    target = project / path
    target.write_text(target.read_text(encoding="utf-8") + text, encoding="utf-8")
    return project


def diagnostics(project: Path) -> tuple[Diagnostic, ...]:
    """Load the project's semantic views and return every diagnostic the load reports."""
    return tuple(load_project(project, manifest_path=MANIFEST).diagnostics)


def reported(project: Path, code: str) -> list[Diagnostic]:
    """Return the diagnostics of `code` loading the project reports, in order."""
    return [item for item in diagnostics(project) if item.code == code]

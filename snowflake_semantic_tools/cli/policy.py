"""The project policy every result is held to: severity overrides, and the strict-adoption notice.

Both read the configuration file once the body has run. `diagnostics.severity_overrides`
changes the severity a code reports at, and so the exit code of a command whose exit follows its
diagnostics. The first validate, plan, or apply of a project that declares `validation.strict:
true` with no baseline says, once, how many warnings now block.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool
from snowflake_semantic_tools.domain.validate.config import severity_overrides

# The commands whose result a strict project's warnings now block.
_STRICT_COMMANDS = frozenset(("validate", "plan", "apply"))
_STRICT_NOTICE = "strict-enforced"


def with_policy(
    command: str,
    result_diagnostics: DiagnosticBag,
    exit_code: int,
    *,
    gated: bool,
    promoted: int,
    paths: ProjectPaths,
    baselined: bool,
) -> tuple[DiagnosticBag, int]:
    """Return the diagnostics and exit code once the project's severity policy is applied.

    Args:
        gated: Whether the exit code follows the diagnostics, so an override can change it.
        promoted: How many warnings `--strict` made errors, which the notice reports.
        baselined: Whether the run read a baseline; the notice is for a project without one.

    Diagnostics:
        SST-CFG037: the project declares `validation.strict: true` and has no baseline; once.
    """
    if paths.config_file is None:
        return result_diagnostics, exit_code
    tree = load_project_config(paths).tree
    overrides = severity_overrides(tree)
    diagnostics = DiagnosticBag(
        dataclasses.replace(item, severity=overrides[item.code]) if item.code in overrides else item
        for item in result_diagnostics
    )
    if gated and overrides and exit_code in (OK, ERROR):
        exit_code = ERROR if diagnostics.has_errors else OK
    strict = config_bool(config_block(tree.get("validation")).get("strict"))
    marker = target_dir(paths.project_dir) / _STRICT_NOTICE
    if command in _STRICT_COMMANDS and strict and not baselined and not marker.exists():
        notice = D(
            "SST-CFG037",
            origin=Origin(paths.config_name),
            subject="config:validation.strict",
            count=promoted,
        )
        diagnostics = DiagnosticBag((notice, *diagnostics))
        _remember(paths.project_dir, marker)
    return diagnostics, exit_code


def _remember(project_dir: Path, marker: Path) -> None:
    """Record that the strict-adoption notice was given, so it is given once per project."""
    write_within(project_dir, marker, "validation.strict is enforced; this notice is given once\n")

"""Hold a command's result to the project's policy and its baseline, as `app` decides them.

The decisions are `app.policy` and `app.baseline`; this module passes them the run's resolved
configuration and clock, maps what they decide onto exit codes, and keeps the one file the
strict-adoption notice needs, which records that it was given.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app.baseline import gate_baseline
from snowflake_semantic_tools.app.policy import hold_to_policy
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline
from snowflake_semantic_tools.domain.ports.clock import ClockPort

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
        SST-CFG037: as `app.policy.hold_to_policy` gives it.
    """
    if paths.config_file is None:
        return result_diagnostics, exit_code
    marker = target_dir(paths.project_dir) / _STRICT_NOTICE
    held = hold_to_policy(
        command,
        result_diagnostics,
        resolved_config(paths),
        gated=gated,
        promoted=promoted,
        baselined=baselined,
        notice_due=not marker.exists(),
    )
    if held.blocks is not None and exit_code in (OK, ERROR):
        exit_code = ERROR if held.blocks else OK
    if held.notice_given:
        _remember(paths.project_dir, marker)
    return held.diagnostics, exit_code


def with_baseline(
    diagnostics: DiagnosticBag, exit_code: int, baseline: Baseline, *, gated: bool, clock: ClockPort
) -> tuple[DiagnosticBag, int, frozenset[str]]:
    """Return the diagnostics, exit code and baselined fingerprints once `baseline` holds the run.

    A gated run whose every error the baseline holds exits 0; a passing run whose baseline has
    expired exits 1.
    """
    gate = gate_baseline(diagnostics, baseline, clock=clock)
    if gated and exit_code == ERROR and gate.forgiven:
        exit_code = OK
    if exit_code == OK and gate.expired:
        exit_code = ERROR
    return gate.diagnostics, exit_code, gate.baselined


def _remember(project_dir: Path, marker: Path) -> None:
    """Record that the strict-adoption notice was given, so it is given once per project."""
    write_within(project_dir, marker, "validation.strict is enforced; this notice is given once\n")

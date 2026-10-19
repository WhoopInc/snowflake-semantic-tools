"""Hold a command's result to the project's policy and its baseline, as `app` decides them.

The decisions are `app.policy` and `app.baseline`; this module passes them the run's resolved
configuration and clock, and maps what they decide onto exit codes.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.locations import LEGACY_SST_TARGET, ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app.baseline import gate_baseline
from snowflake_semantic_tools.app.policy import STRICT_COMMANDS, hold_to_policy
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool
from snowflake_semantic_tools.domain.ports.clock import ClockPort


def ran_under_1_0(project_dir: Path) -> bool:
    """Report whether the project has an SST 1.0 manifest, which only a 1.0 run writes.

    0.3 wrote `target/sst_manifest.json`, so this file is the recorded fact that a 1.0 run
    happened: in `sst_target/`, or in `target/sst/`, where 1.0 wrote it before it moved. Read
    before a command runs, it says the same thing on every run until one compiles.
    """
    return any(
        (directory / "manifest.json").is_file()
        for directory in (target_dir(project_dir), project_dir / LEGACY_SST_TARGET)
    )


def with_policy(
    command: str,
    result_diagnostics: DiagnosticBag,
    exit_code: int,
    *,
    gated: bool,
    promoted: int,
    paths: ProjectPaths,
    baselined: bool,
    strict: bool | None = None,
    first_run: bool = True,
) -> tuple[DiagnosticBag, int]:
    """Return the diagnostics and exit code once the project's severity policy is applied.

    Args:
        gated: Whether the exit code follows the diagnostics, so an override can change it.
        promoted: How many warnings `--strict` made errors, which the notice reports.
        baselined: Whether the run read a baseline; the notice is for a project without one.
        strict: `--strict` or `--no-strict` as given; None when neither was.
        first_run: Whether the project had not run under 1.0 when the command began, as
            `ran_under_1_0` says.

    Diagnostics:
        SST-CFG037: as `app.policy.hold_to_policy` gives it.
    """
    if paths.config_file is None:
        return result_diagnostics, exit_code
    held = hold_to_policy(
        command,
        result_diagnostics,
        resolved_config(paths),
        gated=gated,
        promoted=promoted,
        baselined=baselined,
        notice_due=first_run,
        strict=strict,
    )
    if held.blocks is not None and exit_code in (OK, ERROR):
        exit_code = ERROR if held.blocks else OK
    return held.diagnostics, exit_code


def run_is_strict(command: str, paths: ProjectPaths, strict: bool | None) -> bool:
    """Report whether a run is strict: a strict command, under `--strict`, else `validation.strict`."""
    if command not in STRICT_COMMANDS:
        return False
    if strict is not None:
        return strict
    if paths.config_file is None:
        return False
    return config_bool(config_block(resolved_config(paths).tree.get("validation")).get("strict")) is True


def with_baseline(
    diagnostics: DiagnosticBag,
    exit_code: int,
    baseline: Baseline,
    *,
    gated: bool,
    clock: ClockPort,
    strict: bool = False,
) -> tuple[DiagnosticBag, int, frozenset[str], int]:
    """Return the run once `baseline` holds it: diagnostics, exit code, what it holds, and promoted.

    A gated run whose every error the baseline holds exits 0; a passing run, or a plan with
    changes, whose baseline has expired -- or under `strict` is nearing expiry -- exits 1.

    Args:
        strict: Whether the run is strict, as `run_is_strict` says; the baseline's own
            warnings are then errors too.
    """
    gate = gate_baseline(diagnostics, baseline, clock=clock, strict=strict)
    if gated and exit_code == ERROR and gate.forgiven:
        exit_code = OK
    if exit_code in (OK, CHANGES) and gate.expired:
        exit_code = ERROR
    return gate.diagnostics, exit_code, gate.baselined, gate.promoted

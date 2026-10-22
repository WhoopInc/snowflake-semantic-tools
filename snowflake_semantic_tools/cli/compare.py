"""`sst test --compare-models` and `--refresh-cost`: compare orchestration models on published evals.

A comparison runs the selected evals on committed versions of their published agents and CoWork
profiles, one per candidate model, and writes its report, `compare.json` and a page beside it,
to `--compare-report`, else `sst_target/compare/<eval>_<UTC time>/`. `--refresh-cost` reads
such a `compare.json` back, reads its runs' usage again, and rewrites the report in place. The
`sst.compare/1` object is the command's `--output json` data.
"""

from __future__ import annotations

import importlib
import json
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, read_json_file
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.snowflake.connector import ConnectorPool
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare import CompareOutcome, RunEvalComparison, requested_models
from snowflake_semantic_tools.app.evals.compare_refresh import refresh_cost
from snowflake_semantic_tools.app.evals.compare_subjects import compare_subjects
from snowflake_semantic_tools.app.evals.compare_writer import COMPARE_JSON, CompareReportWriter
from snowflake_semantic_tools.app.evals.progress import EvalProgress
from snowflake_semantic_tools.app.evals.suite import EvalGateRefused
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.output import render_policy
from snowflake_semantic_tools.cli.render.compare import comparison_lines
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.runner import CommandResult
from snowflake_semantic_tools.cli.wiring.manifest import current_manifest
from snowflake_semantic_tools.cli.wiring.project import (
    closed_on_error,
    connect,
    open_connector,
    state_store,
    target_dir,
)
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.compare import PENDING, cost_states
from snowflake_semantic_tools.domain.evaluate.compare_json import CompareReport, report_json
from snowflake_semantic_tools.domain.model.config_schema import config_block

# The renderer of the page beside compare.json, imported only when it is installed.
_HTML_RENDERER = "snowflake_semantic_tools.adapters.report.compare_html"
_REPORT_LIMIT = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class CompareFlags:
    """The command line of one comparison.

    Attributes:
        models: `--compare-models`' list as given; empty for the flag alone, which compares the
            candidates each eval's settings name.
        runs: `--compare-runs`; None leaves each eval's `compare_runs` setting.
        report_dir: `--compare-report`; None writes under `sst_target/compare/`.
    """

    models: str
    runs: int | None = None
    report_dir: Path | None = None
    break_stale_lock: bool = False


def given_models(value: str) -> tuple[str, ...] | None:
    """Read `--compare-models`' list: each comma-separated name once; None for the flag alone.

    Raises:
        SstUsageError: the list names no model, such as `,`.
    """
    if not value.strip():
        return None
    names = tuple(dict.fromkeys(name.strip() for name in value.split(",") if name.strip()))
    if not names:
        raise SstUsageError("--compare-models names no model; give a comma-separated list, or no value")
    return names


def run_comparison(
    paths: ProjectPaths,
    target_name: str | None,
    results: tuple[CompileResult, CompileResult],
    inputs: YamlProjectInputs,
    evals: tuple[CompiledEval, ...],
    flags: CompareFlags,
    progress: EvalProgress,
) -> CommandResult:
    """Compare the candidates on each selected eval, then write and show the report; exit 1 on a lifecycle error.

    `results` is the whole compile, which holds every eval's subject, and the selection. A
    comparison never fails on scores: only a refusal, or a version it could not commit or
    restore, exits 1, as does another run holding the target's lock.

    Raises:
        SstUsageError: a model `--compare-models` names is `auto` or not allowed, or an eval has
            no candidate (SST-VAL770, SST-VAL771, SST-VAL772).
        ProjectError: the compiled manifest is stale.
    """
    full, _ = results
    config = inputs.config().tree
    requests = requested_models(
        evals, given=given_models(flags.models), runs=flags.runs, allowed=_allowed_models(config)
    )
    if not isinstance(requests, Mapping):
        raise SstUsageError(requests.message, diagnostic=requests)
    manifest = current_manifest(paths.project_dir, full, inputs, before="evals")
    directory = flags.report_dir or _default_directory(paths, evals)
    profile, port = connect(paths, target_name)
    params = profile.connection_params
    workers = max(1, sum((1 + len(request.candidates)) * request.runs for request in requests.values()))
    subjects = compare_subjects(
        full,
        evals,
        manifest,
        apply_config=config_block(config.get("apply")),
        target=profile.identity,
        git_sha=inputs.git_sha(),
        dirty=bool(inputs.git_dirty()),
    )
    with closed_on_error(port), ConnectorPool(workers, lambda: open_connector(params)) as pool:
        outcome = RunEvalComparison(
            port,
            inputs,
            state_store(paths, profile.target_name),
            SystemClock(),
            actor=profile.identity.role or "",
            host=socket.gethostname(),
            sessions=pool,
            progress=progress,
        ).run(
            evals,
            subjects,
            requests,
            manifest,
            target=profile.identity,
            state_table=profile.state_table,
            report_path=_shown(paths, directory / COMPARE_JSON),
            break_stale_lock=flags.break_stale_lock,
        )
    port.close()
    if isinstance(outcome, EvalGateRefused):
        return CommandResult(ERROR, outcome.diagnostics, {"suite": "evals", "error": outcome.reason})
    return _reported(paths, outcome, directory)


def refresh_comparison(paths: ProjectPaths, target_name: str | None, path: Path) -> CommandResult:
    """Read a comparison's usage again and rewrite its report in place; start no run.

    Raises:
        ProjectError: the file cannot be read or is not a report SST wrote (SST-MAN032), exit 4.
    """
    try:
        document = read_json_file(path, max_bytes=_REPORT_LIMIT)
    except (OSError, JsonFileError) as exc:
        refused = D("SST-MAN032", path=str(path), detail=str(exc).splitlines()[0] if str(exc) else "it cannot be read")
        raise ProjectError(refused.message, diagnostics=(refused,)) from exc
    _, port = connect(paths, target_name)
    with closed_on_error(port):
        refreshed = refresh_cost(port, document, path=str(path), now=SystemClock().now_iso())
    port.close()
    if not isinstance(refreshed, tuple):
        raise ProjectError(refreshed.message, diagnostics=(refreshed,))
    report, notices = refreshed
    return _written(paths, report, DiagnosticBag(notices), path.parent, passed=True)


def compare_report_writer() -> CompareReportWriter:
    """Return the writer of a comparison's report: compare.json, with the HTML page where it renders.

    Until `adapters/report/compare_html.py` is installed, only compare.json is written; this
    guard goes once it is.
    """
    try:
        module = importlib.import_module(_HTML_RENDERER)
    except ModuleNotFoundError:
        return JsonReportWriter()
    return RenderedReportWriter(module.write_compare_report)


class JsonReportWriter:
    """Write the report object alone, as `compare.json`."""

    def write(self, directory: Path, report: Mapping[str, object]) -> Path:
        path = directory / COMPARE_JSON
        write_within(directory, path, _json(report))
        return path


class RenderedReportWriter:
    """Write the report through the HTML renderer, which writes compare.json and report.html."""

    def __init__(self, write: Callable[[Path, Mapping[str, object]], Path]) -> None:
        self._write = write

    def write(self, directory: Path, report: Mapping[str, object]) -> Path:
        return self._write(directory, report)


def _reported(paths: ProjectPaths, outcome: CompareOutcome, directory: Path) -> CommandResult:
    if outcome.report is None:
        return CommandResult(ERROR, outcome.diagnostics, {"suite": "evals"})
    return _written(paths, outcome.report, outcome.diagnostics, directory, passed=outcome.passed)


def _written(
    paths: ProjectPaths, report: CompareReport, diagnostics: DiagnosticBag, directory: Path, *, passed: bool
) -> CommandResult:
    """Write the report, then return it as the command's data and its human output."""
    document = report_json(report)
    opened = compare_report_writer().write(directory, document)
    files = tuple(dict.fromkeys(_shown(paths, item) for item in (opened, directory / COMPARE_JSON)))
    winners = [item.verdict.model for item in report.evals if item.verdict.model]
    headline = f"compared models on {_count(len(report.evals), 'eval')}" + (
        f": {', '.join(winners)} won" if winners else ": no winner"
    )
    pending = any(cost_states(item)[PENDING] for item in report.evals)
    return CommandResult(
        OK if passed else ERROR,
        diagnostics,
        document,
        human=lambda: _print(report, files),
        headline=headline,
        next_steps=(f"run `sst test --refresh-cost {files[-1]}` in about an hour for the cost still pending",)
        if pending
        else None,
    )


def _print(report: CompareReport, files: tuple[str, ...]) -> None:
    policy = render_policy()
    style = Style(policy.terminal, stdout=True)
    for line in comparison_lines(report, style, files=files):
        click.echo(line, color=True if policy.terminal.color_out else None)


def _allowed_models(config: Mapping[str, object]) -> frozenset[str]:
    """Return `snowflake.orchestration_models`, casefolded; empty when it is not set."""
    raw = config_block(config.get("snowflake")).get("orchestration_models")
    return frozenset(str(item).casefold() for item in raw) if isinstance(raw, list) else frozenset()


def _default_directory(paths: ProjectPaths, evals: tuple[CompiledEval, ...]) -> Path:
    """Name the report's directory: `sst_target/compare/<eval>_<UTC time>`, `evals` for several."""
    name = evals[0].name if len(evals) == 1 else "evals"
    return target_dir(paths.project_dir) / "compare" / f"{name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def _shown(paths: ProjectPaths, path: Path) -> str:
    """Show a path relative to the project when it is inside it."""
    try:
        return path.resolve().relative_to(paths.project_dir.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _json(report: Mapping[str, object]) -> str:
    return json.dumps(report, indent=2, ensure_ascii=False) + "\n"


def _count(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"

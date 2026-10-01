"""SST 1.0 composition root and Milestone 2 command surface."""

from __future__ import annotations

import dataclasses
import inspect
import json
import shutil
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping, NoReturn, cast

import click

from .._version import __version__ as VERSION
from ..adapters.clock import SystemClock
from ..adapters.dbt.profiles import ProfileTarget, load_profile_target
from ..adapters.errors import ProjectError
from ..adapters.fs.golden import GoldenFileStore
from ..adapters.fs.local import STATE_FILE_GLOB, ManifestFileStore, PlanFileStore, StateFileStore, state_file
from ..adapters.project_source import YamlProjectInputs
from ..adapters.snowflake.connector import SnowflakeConnector
from ..adapters.snowflake.eval_state import SnowflakeEvalStateStore
from ..adapters.yaml.config import load_project_config
from ..adapters.yaml.migrate import filter_sites, semantic_files, write_file
from ..app.apply import ApplyArtifacts
from ..app.compile import CompileResult
from ..app.compile.project import CompileProject
from ..app.evals.run import EvalSuiteResult, eval_suite_json
from ..app.evals.suite import EvalGateOutcome, EvalGateRefused, EvalGateRequest, RunEvalGate, compiled_evals
from ..app.golden import CompareGoldens
from ..app.listing import list_artifacts
from ..app.manifest import manifest_for, stale_manifest
from ..app.migrate_refs import MigrateRefs
from ..app.partial import partial_refusal, partial_split
from ..app.plan import PlanReady, PlanRefused, PlanScope, PreparePlan
from ..app.smoke import SmokePublished
from ..app.validate import ValidateArtifacts
from ..domain.model.artifact_key import artifact_key, split_artifact_key
from ..domain.model.config_schema import config_block, config_int
from ..domain.model.config_schema import configured_dir as _project_dir_value
from ..domain.model.diagnostic import ERROR_REGISTRY, D, Diagnostic, DiagnosticBag, Severity, render_diagnostic
from ..domain.model.identifier import Identifier, QualifiedName
from ..domain.model.lifecycle import Action, ApplyOptions, ApplyOutcome, Change, ChangeSet, FailurePolicy
from ..domain.model.registry import SEMANTIC_REGISTRY
from ..domain.ports.snowflake import SnowflakePortError
from ..domain.render.reference_docs import CommandDoc, OptionDoc, reference_pages
from ..domain.state import Manifest, SavedPlan

OK = 0
ERROR = 1
CHANGES = 2
USAGE = 3
CONFIG = 4
CONNECTION = 5
INTERRUPTED = 130

_INVOCATION: dict[str, object] = {}
_ARTIFACT_SUBJECTS = frozenset(SEMANTIC_REGISTRY.artifacts) | {"profile"}


class SstUsageError(click.UsageError):
    exit_code = USAGE


class SstGroup(click.Group):
    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            raise SstUsageError(str(exc), ctx) from exc

    def invoke(self, ctx: click.Context) -> Any:
        """Run the command; an interrupt outside a guarded action still exits 130 rather than click's 1."""
        try:
            return super().invoke(ctx)
        except (click.exceptions.Abort, KeyboardInterrupt, EOFError) as exc:
            click.echo("Aborted.", err=True)
            raise click.exceptions.Exit(INTERRUPTED) from exc

    def main(
        self,
        args: Sequence[str] | None = None,
        prog_name: str | None = None,
        complete_var: str | None = None,
        standalone_mode: bool = True,
        **extra: Any,
    ) -> Any:
        arguments = list(args if args is not None else sys.argv[1:])
        _INVOCATION.clear()
        _INVOCATION.update(
            {
                "argv": [prog_name or "sst", *arguments],
                "started_at": SystemClock().now_iso(),
                "started_monotonic": time.monotonic(),
            }
        )
        json_requested = any(
            value == "--output=json"
            or value == "--output"
            and index + 1 < len(arguments)
            and arguments[index + 1] == "json"
            for index, value in enumerate(arguments)
        )
        try:
            result = super().main(
                args=arguments,
                prog_name=prog_name,
                complete_var=complete_var,
                standalone_mode=False if json_requested else standalone_mode,
                **extra,
            )
        except click.UsageError as exc:
            if json_requested:
                command = next((value for value in arguments if value in self.commands), "")
                click.echo(
                    json.dumps(
                        _json_envelope(
                            command,
                            DiagnosticBag(),
                            exit_code=USAGE,
                            status="error",
                            data={"error": str(exc)},
                        ),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                if standalone_mode:
                    raise SystemExit(USAGE) from exc
                return USAGE
            raise
        if json_requested and standalone_mode:
            raise SystemExit(result if isinstance(result, int) else OK)
        return result


@click.group(
    cls=SstGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(version=VERSION, prog_name="sst")
@click.option("--output", "global_output", type=click.Choice(["human", "json"]), default=None)
@click.option(
    "--project-dir",
    "global_project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
)
@click.pass_context
def cli(ctx: click.Context, global_output: str | None, global_project_dir: Path | None) -> None:
    """Snowflake Semantic Tools 1.0."""
    defaults = dict(ctx.default_map or {})
    for command_name in cli.commands:
        command_defaults = dict(defaults.get(command_name, {}))
        if global_output is not None:
            command_defaults["output"] = global_output
        if global_project_dir is not None:
            command_defaults["project_dir"] = global_project_dir
        defaults[command_name] = command_defaults
    ctx.default_map = defaults


for _click_error in (
    click.UsageError,
    click.BadParameter,
    click.NoSuchOption,
    click.MissingParameter,
):
    _click_error.exit_code = USAGE


def _target_dir(project_dir: Path) -> Path:
    return project_dir / "target" / "sst"


def _compiled_manifest(project_dir: Path) -> Manifest:
    """The manifest `sst compile` wrote; plan, apply, list, and the suites read it."""
    path = _target_dir(project_dir) / "manifest.json"
    manifest = ManifestFileStore(path).read()
    if manifest is None:
        diagnostic = D("SST-MAN001", path=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return manifest


def _project_inputs(project_dir: Path, target_name: str | None, manifest_path: Path | None) -> YamlProjectInputs:
    """Bind the project's files as the inputs every use case reads.

    The commit is asked for through this module's `_git_sha`, looked up when a use case
    needs it, so replacing `_git_sha` here changes the commit every use case sees.
    """
    return YamlProjectInputs(
        project_dir,
        target_name=target_name,
        manifest_path=manifest_path,
        git_sha=lambda: _git_sha(project_dir),
    )


def _compile_result(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: str | None = None,
) -> CompileResult:
    """Compile the project from its files; with `selected`, keep only what that one selector names."""
    result = CompileProject(_project_inputs(project_dir, target_name, manifest_path)).run()
    return _selected_result(project_dir, result, selected)


def _selected_result(project_dir: Path, result: CompileResult, selected: str | None) -> CompileResult:
    if selected is None:
        return result
    selected_types, selected_keys = _selection((selected,))
    compiled = tuple(
        item
        for item in result.compiled
        if (selected_types is None or item.artifact_type in selected_types)
        and (selected_keys is None or item.artifact_key in selected_keys)
    )
    if not compiled:
        raise ProjectError(f"selector {selected!r} matched no artifact in {project_dir}")
    return dataclasses.replace(result, compiled=compiled)


def _config(project_dir: Path) -> dict[str, object]:
    return dict(load_project_config(project_dir).tree)


def _git_sha(project_dir: Path) -> str:
    import subprocess

    completed = subprocess.run(
        ["git", "-C", str(project_dir), "rev-parse", "--short=7", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and value else "WORKTREE"


def _selection(values: tuple[str, ...]) -> tuple[frozenset[str] | None, frozenset[str] | None]:
    if not values:
        return None, None
    keys: set[str] = set()
    types: set[str] = set()
    for value in values:
        if "," in value:
            raise SstUsageError("commas are not accepted in selectors; pass space-separated selectors")
        if ":" in value:
            prefix, name = split_artifact_key(value)
            if prefix == "type":
                if name not in SEMANTIC_REGISTRY.artifacts:
                    raise SstUsageError(f"unknown artifact type {name!r}")
                types.add(name)
                continue
            if prefix not in SEMANTIC_REGISTRY.artifacts or not name:
                raise SstUsageError(f"unsupported selector {value!r}")
            keys.add(artifact_key(prefix, name.casefold()))
            continue
        keys.add(artifact_key("semantic_view", value.casefold()))
    return (frozenset(types) if types else None), (frozenset(keys) if keys else None)


def _build_manifest(project_dir: Path, result: CompileResult, manifest_path: Path | None) -> Manifest:
    """Build the manifest `result` publishes, reading what it records about the project's files now."""
    return manifest_for(result, _project_inputs(project_dir, None, manifest_path).manifest_sources())


def _effective_validation_settings(
    project_dir: Path,
    *,
    strict: bool | None,
    connected: bool | None,
) -> tuple[bool, bool]:
    return _project_inputs(project_dir, None, None).validation_defaults().resolve(strict, connected)


def _diagnostic_json(value: Diagnostic) -> dict[str, object]:
    origin = getattr(value, "origin", None)
    spec = ERROR_REGISTRY[value.code]
    subject_parts = split_artifact_key(value.subject) if value.subject and ":" in value.subject else None
    return {
        "code": value.code,
        "severity": value.severity.name.lower(),
        "declared_severity": spec.severity.name.lower(),
        "promoted_from": (spec.severity.name.lower() if value.severity is not spec.severity else None),
        "baselined": False,
        "message": value.message,
        "suggestion": spec.suggestion,
        "phase": value.phase,
        "help_url": value.help_url,
        "fingerprint": value.fingerprint[:16],
        "params": dict(value.context),
        "artifact": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] in _ARTIFACT_SUBJECTS
            else None
        ),
        "member": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] not in _ARTIFACT_SUBJECTS
            else None
        ),
        "location": (
            {
                "file": origin.file,
                "line": origin.line,
                "column": origin.col,
                "end_line": None,
                "end_column": None,
            }
            if origin is not None
            else None
        ),
        "related": [{"file": item.file, "line": item.line, "column": item.col} for item in value.related],
        "internal_detail": None,
        "caused_by": value.caused_by,
    }


def _json_envelope(
    command: str,
    diagnostics: DiagnosticBag,
    *,
    exit_code: int | None = None,
    status: str | None = None,
    artifact_count: int = 0,
    promoted: int = 0,
    data: object | None = None,
) -> dict[str, object]:
    errors = diagnostics.count(Severity.ERROR)
    warnings = diagnostics.count(Severity.WARNING)
    info = diagnostics.count(Severity.INFO)
    resolved_exit = exit_code if exit_code is not None else ERROR if errors else OK
    project_dir = _invocation_option("--project-dir")
    target = _invocation_option("--target")
    started_at = _INVOCATION.get("started_at")
    started_monotonic = _INVOCATION.get("started_monotonic")
    raw_argv = _INVOCATION.get("argv")
    invocation_argv = [str(value) for value in raw_argv] if isinstance(raw_argv, list) else ["sst", command]
    duration = max(time.monotonic() - started_monotonic, 0.0) if isinstance(started_monotonic, float) else 0.0
    return {
        "tool": "sst",
        "sst_version": VERSION,
        "schema_version": 2,
        "command": command,
        "status": status
        or ("error" if resolved_exit not in (OK, CHANGES) else "changes" if resolved_exit == CHANGES else "ok"),
        "exit_code": resolved_exit,
        "invocation": {
            "argv": invocation_argv,
            "target": target,
            "project_dir": str(Path(project_dir or ".").resolve()),
            "config_file": (
                str((Path(project_dir or ".") / "sst_config.yml").resolve())
                if (Path(project_dir or ".") / "sst_config.yml").is_file()
                else None
            ),
            "started_at": started_at,
            "duration_s": round(duration, 6),
        },
        "diagnostics": [_diagnostic_json(diagnostic) for diagnostic in diagnostics],
        "summary": {
            "error": errors,
            "warning": warnings,
            "info": info,
            "promoted": promoted,
            "suppressed_cascade": sum(
                diagnostic.caused_by is not None and diagnostic.severity is Severity.INFO for diagnostic in diagnostics
            ),
            "baselined": 0,
        },
        "data": data if data is not None else {},
    }


def _invocation_option(name: str) -> str | None:
    argv = _INVOCATION.get("argv", ())
    if not isinstance(argv, list):
        return None
    for index, value in enumerate(argv):
        if value == name and index + 1 < len(argv):
            return str(argv[index + 1])
        if isinstance(value, str) and value.startswith(f"{name}="):
            return value.split("=", 1)[1]
    return None


def _emit_json(envelope: dict[str, object], exit_code: int) -> None:
    click.echo(json.dumps(envelope, sort_keys=True, separators=(",", ":")))
    raise click.exceptions.Exit(exit_code)


def _interrupted(command: str, output: str, cause: BaseException) -> NoReturn:
    """End a run the user interrupted or declined to confirm with exit 130, never an internal error."""
    if output == "json":
        envelope = _json_envelope(
            command, DiagnosticBag(), exit_code=INTERRUPTED, status="error", data={"error": "interrupted"}
        )
        click.echo(json.dumps(envelope, sort_keys=True, separators=(",", ":")))
    else:
        click.echo("Aborted.", err=True)
    raise click.exceptions.Exit(INTERRUPTED) from cause


def _render_diagnostics(diagnostics: DiagnosticBag) -> None:
    for diagnostic in diagnostics:
        click.echo(render_diagnostic(diagnostic), err=True)


def _guarded(action: Callable[[], None], *, command: str, output: str) -> None:
    try:
        action()
    except click.exceptions.Exit:
        raise
    except click.UsageError:
        raise
    except (click.exceptions.Abort, KeyboardInterrupt, EOFError) as exc:
        _interrupted(command, output, exc)
    except SnowflakePortError as exc:
        diagnostics = DiagnosticBag((exc.diagnostic,) if exc.diagnostic is not None else ())
        if output == "json":
            _emit_json(
                _json_envelope(
                    command,
                    diagnostics,
                    exit_code=CONNECTION,
                    status="error",
                    data={"error": str(exc)},
                ),
                CONNECTION,
            )
        if diagnostics:
            _render_diagnostics(diagnostics)
            raise click.exceptions.Exit(CONNECTION) from exc
        raise click.ClickException(str(exc)) from exc
    except (ProjectError, ValueError, OSError, json.JSONDecodeError) as exc:
        diagnostics = DiagnosticBag(getattr(exc, "diagnostics", ()))
        if output == "json":
            _emit_json(
                _json_envelope(
                    command,
                    diagnostics,
                    exit_code=CONFIG,
                    status="error",
                    data={"error": str(exc)},
                ),
                CONFIG,
            )
        _render_diagnostics(diagnostics)
        if not diagnostics:
            click.echo(f"error: {exc}", err=True)
        raise click.exceptions.Exit(CONFIG) from exc
    except Exception as exc:
        diagnostics = DiagnosticBag((D("SST-INT902", subject=command, detail=str(exc)),))
        if output == "json":
            _emit_json(
                _json_envelope(
                    command,
                    diagnostics,
                    exit_code=ERROR,
                    status="error",
                    data={"error": str(exc)},
                ),
                ERROR,
            )
        _render_diagnostics(diagnostics)
        raise click.exceptions.Exit(ERROR) from exc


def _connect(project_dir: Path, target_name: str | None) -> tuple[ProfileTarget, SnowflakeConnector]:
    profile = load_profile_target(project_dir, target_name)
    port = SnowflakeConnector(profile.connection_params)
    try:
        live_account = port.current_account_locator()
        live_role = port.current_role()
        if profile.identity.role and live_role.upper() != profile.identity.role.upper():
            raise SnowflakePortError(
                f"connected role {live_role!r} differs from configured role {profile.identity.role!r}"
            )
        identity = dataclasses.replace(
            profile.identity,
            account_locator=live_account,
            role=live_role,
        )
        resolved = ProfileTarget(
            profile_name=profile.profile_name,
            target_name=profile.target_name,
            connection_params=profile.connection_params,
            identity=identity,
            state_table=profile.state_table,
        )
        return resolved, port
    except Exception:
        port.close()
        raise


@contextmanager
def _closed_on_error(port: SnowflakeConnector) -> Iterator[None]:
    """Close `port` if the block raises; a block that completes leaves the connection to its owner."""
    try:
        yield
    except BaseException:
        # Also on an interrupt or a declined prompt: nothing else will close the session.
        port.close()
        raise


def _change_json(change: Change) -> dict[str, object]:
    rendered = change.rendered
    observed = change.observed
    return {
        "artifact_key": change.key,
        "artifact_type": change.artifact_type,
        "action": change.action.value,
        "reason": change.reason.value,
        "target": (rendered.target.sql if rendered is not None else observed.qualified_name.sql if observed else None),
        "fingerprint": rendered.fingerprint if rendered is not None else None,
        "previous_marker": (observed.marker.text if observed is not None and observed.marker is not None else None),
        "depends_on": list(change.depends_on),
        "order": change.order,
        "component_fingerprints": (dict(rendered.component_fingerprints) if rendered is not None else {}),
        "physical_resources": (
            [
                {"object_type": object_type, "qualified_name": name.sql}
                for object_type, name in rendered.physical_resources
            ]
            if rendered is not None
            else []
        ),
        "prune_executable": change.prune_executable,
        "report_only": change.action is Action.PRUNE and not change.prune_executable,
    }


def _print_plan(changeset: ChangeSet) -> None:
    counts = {action: sum(change.action is action for change in changeset.changes) for action in Action}
    report_only = len(changeset.report_only)
    click.echo(
        "Plan: "
        f"{counts[Action.CREATE]} to create, {counts[Action.UPDATE]} to update, "
        f"{counts[Action.PRUNE] - report_only} to prune, {counts[Action.NOOP]} unchanged, "
        f"{counts[Action.BLOCKED]} blocked"
        + (f", {report_only} report-only (SST never removes these)." if report_only else ".")
    )
    markers = {
        Action.CREATE: "+",
        Action.UPDATE: "~",
        Action.PRUNE: "-",
        Action.NOOP: "=",
        Action.BLOCKED: "!",
    }
    for change in changeset.changes:
        target = (
            change.rendered.target.sql
            if change.rendered
            else change.observed.qualified_name.sql if change.observed else "-"
        )
        alias = dict(change.rendered.component_fingerprints).get("alias") if change.rendered else None
        click.echo(
            f"{markers[change.action]} {change.key} {change.action.value.upper()} {target} {change.reason.value}"
            + (f" {alias}" if alias else "")
            + (" (report only)" if change.action is Action.PRUNE and not change.prune_executable else "")
        )


def _print_eval_results(result: EvalSuiteResult) -> None:
    for eval_result in result.evals:
        click.echo(f"{eval_result.eval_key}:")
        for attempt in eval_result.attempts:
            click.echo(
                f"  attempt {attempt.attempt}: {attempt.run_name} {attempt.terminal_status} "
                f"duration_ms={attempt.cost.duration_ms} tokens={attempt.cost.total_tokens} "
                f"agent_input={attempt.cost.total_input_tokens} agent_output={attempt.cost.total_output_tokens} "
                f"llm_calls={attempt.cost.llm_call_count}"
            )
            attempt_payload = eval_suite_json(
                dataclasses.replace(result, evals=(dataclasses.replace(eval_result, attempts=(attempt,)),))
            )
            eval_payloads = cast(list[dict[str, object]], attempt_payload["evals"])
            attempt_payloads = cast(list[dict[str, object]], eval_payloads[0]["attempts"])
            summaries = cast(list[dict[str, object]], attempt_payloads[0]["metric_summaries"])
            for summary in summaries:
                click.echo(
                    f"    {summary['metric_name']}: passed={summary['passed_count']}/{summary['record_count']} "
                    f"average={summary['average_score']}"
                )
            if attempt.status_details:
                click.echo("    status_details: " + "; ".join(attempt.status_details))
            if attempt.retrieval_error:
                click.echo(f"    retrieval_error: {attempt.retrieval_error}")


def _write_plan_sql(project_dir: Path, changeset: ChangeSet, sql_out: Path | None) -> Path:
    output = sql_out or _target_dir(project_dir) / "sql"
    output.mkdir(parents=True, exist_ok=True)
    for change in changeset.changes:
        if change.rendered is None or change.action not in (
            Action.CREATE,
            Action.UPDATE,
        ):
            continue
        suffix = _artifact_suffix(change.rendered.render_dialect)
        path = output / f"{change.artifact_type}__{split_artifact_key(change.key)[1].replace('/', '_')}{suffix}"
        content = (
            change.rendered.content
            if suffix in (".json", ".yaml")
            else ";\n\n".join(change.rendered.statements) + ";\n"
        )
        path.write_text(content, encoding="utf-8")
    return output


@dataclasses.dataclass(frozen=True)
class _PlanRequest:
    """What `sst plan` or `sst apply` was asked to plan, as the command line gave it.

    Attributes:
        strict, connected: `--strict` and `--snowflake-syntax-check`; None defers to `validation:`.
    """

    project_dir: Path
    target_name: str | None
    manifest_path: Path | None
    selected: tuple[str, ...]
    excluded: tuple[str, ...]
    prune: bool
    partial: bool
    strict: bool | None
    connected: bool | None

    def following(self, saved: SavedPlan | None) -> _PlanRequest:
        """Return the request with a saved plan's selection in place of the flags'; itself without one."""
        if saved is None:
            return self
        return dataclasses.replace(self, selected=saved.selected, excluded=saved.excluded, prune=saved.include_prune)


@dataclasses.dataclass(frozen=True)
class _PlanSession:
    """A ready plan and what the CLI wired for it: the live target, the open connection, the state file.

    Whoever holds the session closes `port`.
    """

    ready: PlanReady
    profile: ProfileTarget
    port: SnowflakeConnector
    state_store: StateFileStore


def _plan_scope(request: _PlanRequest) -> PlanScope:
    """Resolve `--select`, `--exclude`, and `--prune` into what a plan covers and may prune.

    An excluded type leaves the selected types, or every type when none is selected. With
    `--prune`, an excluded key leaves the selected keys, which must then be given.

    Raises:
        SstUsageError: a selector does not parse, or `--prune` excludes keys without `--select`.
    """
    prune_types, prune_keys = _selection(request.selected)
    excluded_types, excluded_keys = _selection(request.excluded)
    if excluded_types is not None:
        prune_types = (
            frozenset(SEMANTIC_REGISTRY.artifacts) - excluded_types
            if prune_types is None
            else frozenset(prune_types - excluded_types)
        )
    if request.prune and excluded_keys is not None:
        if prune_keys is None:
            raise SstUsageError("--prune with --exclude requires --select so the prune scope is explicit")
        prune_keys = frozenset(prune_keys - excluded_keys)
    return PlanScope(request.selected, prune_types, prune_keys, excluded_types, excluded_keys, request.prune)


def _plan_runtime(request: _PlanRequest) -> _PlanSession | PlanRefused:
    """Compile and decide what to plan offline, then connect and plan; a refusal when nothing can be.

    A ready plan comes with its open connection, which the caller closes. Once connected, the
    connection is closed here whenever planning raises or is refused.

    Raises:
        SstUsageError: the selectors cannot be resolved, as `_plan_scope` says.
        ProjectError: the selectors matched nothing, or the compiled manifest is stale.
    """
    scope = _plan_scope(request)
    project_dir = request.project_dir
    full_result = _compile_result(project_dir, request.target_name, request.manifest_path)
    prepare = PreparePlan(_project_inputs(project_dir, request.target_name, request.manifest_path), SystemClock())
    candidates = prepare.select(
        full_result,
        _compiled_manifest(project_dir),
        scope,
        partial=request.partial,
        strict=request.strict,
        connected=request.connected,
        project=str(project_dir),
    )
    if isinstance(candidates, PlanRefused):
        if candidates.reason is not None:
            raise ProjectError(candidates.reason)
        return candidates
    profile, port = _connect(project_dir, request.target_name)
    with _closed_on_error(port):
        state_store = StateFileStore(state_file(_target_dir(project_dir), profile.target_name))
        outcome = prepare.run(candidates, port, state_store, target=profile.identity, state_table=profile.state_table)
    if isinstance(outcome, PlanRefused):
        port.close()
        return outcome
    return _PlanSession(outcome, profile, port, state_store)


def _fail(command: str, diagnostics: DiagnosticBag, output: str) -> NoReturn:
    """Report the errors that stopped a command, and exit 1."""
    if output == "json":
        _emit_json(_json_envelope(command, diagnostics, exit_code=ERROR), ERROR)
    _render_diagnostics(diagnostics)
    raise click.exceptions.Exit(ERROR)


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("."),
    show_default=True,
)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def init(project_dir: Path, output: str) -> None:
    """Create a minimal SST project scaffold without overwriting files."""

    def action() -> None:
        created: list[str] = []
        config = project_dir / "sst_config.yml"
        if not config.exists():
            project_dir.mkdir(parents=True, exist_ok=True)
            config.write_text(
                "project:\n  semantic_models_dir: semantic_models\n\n"
                "validation:\n  strict: false\n  snowflake_syntax_check: true\n\n"
                'state:\n  +database: "{{ target.database }}"\n'
                '  +schema: "{{ target.schema }}"\n  +table: SST_STATE\n',
                encoding="utf-8",
            )
            created.append("sst_config.yml")
        views = project_dir / "semantic_models" / "semantic_views"
        views.mkdir(parents=True, exist_ok=True)
        if output == "json":
            _emit_json(_json_envelope("init", DiagnosticBag(), data={"created": created}), OK)
        click.echo(f"initialized SST project; created {len(created)} file(s)")

    _guarded(action, command="init", output=output)


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--target", "target_name")
@click.option("--test-connection", is_flag=True)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def debug(project_dir: Path, target_name: str | None, test_connection: bool, output: str) -> None:
    """Show resolved project, profile, target, and optional connection identity."""

    def action() -> None:
        profile = load_profile_target(project_dir, target_name)
        data: dict[str, object] = {
            "project_dir": str(project_dir.resolve()),
            "profile": profile.profile_name,
            "target": profile.target_name,
            "database": profile.identity.database.sql,
            "schema": profile.identity.schema.sql,
            "state_table": profile.state_table.sql,
            "authentication": profile.authentication,
        }
        if test_connection:
            port = SnowflakeConnector(profile.connection_params)
            try:
                data["current_role"] = port.current_role()
                data["current_account"] = port.current_account_locator()
            finally:
                port.close()
        diagnostics = DiagnosticBag(profile.diagnostics)
        if output == "json":
            _emit_json(_json_envelope("debug", diagnostics, data=data), OK)
        _render_diagnostics(diagnostics)
        for key, value in data.items():
            click.echo(f"{key}: {value}")

    _guarded(action, command="debug", output=output)


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--emit-ddl", "emit_ddl_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--print-ddl", is_flag=True)
@click.option("--ddl-output-dir", type=click.Path(file_okay=False, path_type=Path), hidden=True)
@click.option("--manifest-output", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--select", "selected")
@click.option("--partial", is_flag=True)
@click.option("--target", "target_name")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def compile(
    project_dir: Path,
    emit_ddl_dir: Path | None,
    print_ddl: bool,
    ddl_output_dir: Path | None,
    manifest_output: Path | None,
    selected: str | None,
    partial: bool,
    target_name: str | None,
    manifest_path: Path | None,
    output: str,
) -> None:
    """Compile semantic views and write the canonical manifest."""

    def action() -> None:
        full_result = _compile_result(project_dir, target_name, manifest_path)
        split = partial_split(full_result) if partial and not full_result.success else None
        if not full_result.success and split is None:
            refusal = partial_refusal(full_result) if partial else None
            failed = DiagnosticBag((*full_result.diagnostics, *((refusal,) if refusal else ())))
            if output == "json":
                _emit_json(
                    _json_envelope(
                        "compile",
                        failed,
                        artifact_count=len(full_result.compiled),
                    ),
                    ERROR,
                )
            _render_diagnostics(failed)
            raise click.exceptions.Exit(ERROR)
        # `--partial` writes the manifest for what can publish and still exits 1.
        exit_code = OK
        shown = full_result.diagnostics
        if split is not None:
            full_result = split.healthy
            exit_code = ERROR
            shown = DiagnosticBag((*full_result.diagnostics, *split.notices))
        default_manifest = _target_dir(project_dir) / "manifest.json"
        full_manifest = _build_manifest(project_dir, full_result, manifest_path)
        ManifestFileStore(default_manifest).write(full_manifest)
        result = full_result
        destination = default_manifest
        manifest = full_manifest
        if selected is not None:
            selected_types, selected_keys = _selection((selected,))
            compiled = tuple(
                item
                for item in full_result.compiled
                if (selected_types is None or item.artifact_type in selected_types)
                and (selected_keys is None or item.artifact_key in selected_keys)
            )
            if not compiled:
                raise ProjectError(f"selector {selected!r} matched no artifact in {project_dir}")
            result = dataclasses.replace(full_result, compiled=compiled)
            if manifest_output is not None:
                destination = manifest_output
                manifest = _build_manifest(project_dir, result, manifest_path)
                ManifestFileStore(destination).write(manifest)
        elif manifest_output is not None and manifest_output != default_manifest:
            destination = manifest_output
            ManifestFileStore(destination).write(manifest)
        if output == "json":
            artifacts = [
                {
                    "artifact_key": item.artifact_key,
                    "fingerprint": item.rendered_artifact.fingerprint,
                    "target": item.rendered_artifact.target.sql,
                }
                for item in result.compiled
            ]
            data: dict[str, object] = {
                "manifest_path": str(destination),
                "manifest_id": manifest.manifest_id,
                "artifacts": artifacts,
            }
            if split is not None:
                data["partial"] = {"excluded": list(split.excluded)}
            _emit_json(
                _json_envelope("compile", shown, exit_code=exit_code, artifact_count=len(result.compiled), data=data),
                exit_code,
            )
        if split is not None:
            _render_diagnostics(shown)
        if print_ddl:
            click.echo("\n\n".join(item.rendered_artifact.content for item in result.compiled))
        elif (output_dir := emit_ddl_dir or ddl_output_dir) is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            for item in result.compiled:
                suffix = _artifact_suffix(item.rendered_artifact.render_dialect)
                (output_dir / f"{item.name.casefold()}{suffix}").write_text(
                    (
                        item.rendered_artifact.content
                        if suffix in (".json", ".yaml")
                        else item.rendered_artifact.content.rstrip() + ";\n"
                    ),
                    encoding="utf-8",
                )
            click.echo(f"wrote {len(result.compiled)} artifact payload file(s) to {output_dir}")
        else:
            partial_note = f" (partial: {len(split.excluded)} excluded)" if split is not None else ""
            click.echo(f"compiled {len(result.compiled)} artifact(s){partial_note}; manifest {destination}")
        if exit_code:
            raise click.exceptions.Exit(exit_code)

    _guarded(action, command="compile", output=output)


def _artifact_suffix(render_dialect: str) -> str:
    if render_dialect in ("json", "bundle_json", "profile_json"):
        return ".json"
    if render_dialect == "eval_yaml":
        return ".yaml"
    return ".sql"


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--target", "target_name")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--strict/--no-strict", default=None)
@click.option("--snowflake-syntax-check/--no-snowflake-syntax-check", default=None)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def validate(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    output: str,
) -> None:
    """Validate every offline rule, with optional connected checks."""

    def action() -> None:
        compiled = _compile_result(project_dir, target_name, manifest_path)
        effective_strict, effective_connected = _effective_validation_settings(
            project_dir,
            strict=strict,
            connected=snowflake_syntax_check,
        )
        port = None
        if effective_connected:
            _, port = _connect(project_dir, target_name)
        try:
            result = ValidateArtifacts(port).run(
                compiled,
                strict=effective_strict,
                connected=effective_connected,
            )
        finally:
            if port is not None:
                port.close()
        exit_code = OK if result.success else ERROR
        if output == "json":
            _emit_json(
                _json_envelope(
                    "validate",
                    result.diagnostics,
                    exit_code=exit_code,
                    artifact_count=len(result.rendered),
                    promoted=result.promoted,
                ),
                exit_code,
            )
        _render_diagnostics(result.diagnostics)
        if exit_code:
            raise click.exceptions.Exit(exit_code)
        click.echo(
            f"validated {len(result.rendered)} artifact(s): "
            f"{result.diagnostics.count(Severity.ERROR)} errors, "
            f"{result.diagnostics.count(Severity.WARNING)} warnings"
        )

    _guarded(action, command="validate", output=output)


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--target", "target_name")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--select", "selected", multiple=True)
@click.option("--exclude", "excluded", multiple=True)
@click.option("--prune", is_flag=True)
@click.option("--partial", is_flag=True)
@click.option("--plan-out", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--no-plan-out", is_flag=True)
@click.option("--sql-out", type=click.Path(file_okay=False, path_type=Path))
@click.option("--no-detailed-exitcode", is_flag=True)
@click.option("--strict/--no-strict", default=None)
@click.option("--snowflake-syntax-check/--no-snowflake-syntax-check", default=None)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def plan(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    prune: bool,
    partial: bool,
    plan_out: Path | None,
    no_plan_out: bool,
    sql_out: Path | None,
    no_detailed_exitcode: bool,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    output: str,
) -> None:
    """Observe live Snowflake state and compute a non-writing plan."""

    if plan_out is not None and no_plan_out:
        raise SstUsageError("--plan-out and --no-plan-out are mutually exclusive")
    _refuse_partial_prune(partial, prune)
    request = _PlanRequest(
        project_dir, target_name, manifest_path, selected, excluded, prune, partial, strict, snowflake_syntax_check
    )

    def action() -> None:
        session = _plan_runtime(request)
        if isinstance(session, PlanRefused):
            _fail("plan", session.diagnostics, output)
        saved, destination, sql_path = _save_plan(request, session, plan_out, no_plan_out, sql_out)
        plan_path = None if no_plan_out else destination
        _report_plan(request, session.ready, saved, plan_path, sql_path, no_detailed_exitcode, output)

    _guarded(action, command="plan", output=output)


def _save_plan(
    request: _PlanRequest, session: _PlanSession, plan_out: Path | None, no_plan_out: bool, sql_out: Path | None
) -> tuple[SavedPlan, Path, Path]:
    """Save the plan unless `--no-plan-out`, write each change's statements, then close the connection.

    Returns:
        The saved plan, the path it is saved at or would have been, and the statements' directory.
    """
    changeset = session.ready.changeset
    try:
        saved = SavedPlan.from_changeset(
            changeset,
            selected=request.selected,
            excluded=request.excluded,
            include_prune=request.prune,
            partial=request.partial,
        )
        destination = plan_out or _target_dir(request.project_dir) / "plan.json"
        if not no_plan_out:
            PlanFileStore(destination).write(saved)
        sql_path = _write_plan_sql(request.project_dir, changeset, sql_out)
    finally:
        session.port.close()
    return saved, destination, sql_path


def _report_plan(
    request: _PlanRequest,
    ready: PlanReady,
    saved: SavedPlan,
    plan_path: Path | None,
    sql_path: Path,
    no_detailed_exitcode: bool,
    output: str,
) -> None:
    """Report a plan and exit: 1 on an error or a blocked change, 2 with writes pending, else 0.

    With `--no-detailed-exitcode`, pending writes exit 0. With `--partial`, what was left out
    is reported ahead of the plan's own diagnostics.
    """
    changeset = ready.changeset
    shown = (
        DiagnosticBag((*ready.result.diagnostics, *changeset.diagnostics)) if request.partial else changeset.diagnostics
    )
    if changeset.blocked or shown.has_errors:
        exit_code = ERROR
    elif changeset.writes:
        exit_code = OK if no_detailed_exitcode else CHANGES
    else:
        exit_code = OK
    if output == "json":
        data: dict[str, object] = {
            "manifest_id": ready.manifest.manifest_id,
            "plan_id": saved.plan_id,
            "plan_path": None if plan_path is None else str(plan_path),
            "sql_path": str(sql_path),
            "changes": [_change_json(change) for change in changeset.changes],
            "report_only": [change.key for change in changeset.report_only],
        }
        if request.partial:
            data["partial"] = {"excluded": _partial_excluded(ready.result.diagnostics)}
        _emit_json(
            _json_envelope("plan", shown, exit_code=exit_code, artifact_count=len(changeset.changes), data=data),
            exit_code,
        )
    _render_diagnostics(shown)
    _print_plan(changeset)
    if exit_code:
        raise click.exceptions.Exit(exit_code)


def _refuse_partial_prune(partial: bool, prune: bool) -> None:
    # An artifact left out for errors looks orphaned, so pruning could remove a live
    # object whose source is only broken; the combination is refused outright.
    if partial and prune:
        raise SstUsageError("--partial cannot be combined with --prune")


def _partial_excluded(diagnostics: DiagnosticBag) -> list[str]:
    return [str(item.subject) for item in diagnostics if item.code == "SST-PLN032"]


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--target", "target_name")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--select", "selected", multiple=True)
@click.option("--exclude", "excluded", multiple=True)
@click.option("--plan", "plan_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--prune", is_flag=True)
@click.option("--partial", is_flag=True)
@click.option("--yes", "confirmed", is_flag=True)
@click.option("--fail-fast", is_flag=True)
@click.option("--break-stale-lock", is_flag=True)
@click.option("--sql-out", type=click.Path(file_okay=False, path_type=Path))
@click.option("--strict/--no-strict", default=None)
@click.option("--snowflake-syntax-check/--no-snowflake-syntax-check", default=None)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def apply(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    plan_path: Path | None,
    prune: bool,
    partial: bool,
    confirmed: bool,
    fail_fast: bool,
    break_stale_lock: bool,
    sql_out: Path | None,
    strict: bool | None,
    snowflake_syntax_check: bool | None,
    output: str,
) -> None:
    """Apply a current reviewed plan; smoke probes never run here."""

    if output == "json" and not confirmed:
        raise SstUsageError("--output json apply requires --yes")
    if prune and not confirmed:
        raise SstUsageError("--prune requires --yes")
    _refuse_partial_prune(partial, prune)
    request = _PlanRequest(
        project_dir, target_name, manifest_path, selected, excluded, prune, partial, strict, snowflake_syntax_check
    )

    def action() -> None:
        saved = _saved_plan(plan_path, request)
        planned = request.following(saved)
        session = _plan_runtime(planned)
        if isinstance(session, PlanRefused):
            _fail("apply", session.diagnostics, output)
        with _closed_on_error(session.port):
            options = _confirmed_options(
                planned,
                session,
                saved,
                plan_path,
                sql_out=sql_out,
                confirmed=confirmed,
                fail_fast=fail_fast,
                break_stale_lock=break_stale_lock,
            )
        _apply_and_report(planned, session, options, output)

    _guarded(action, command="apply", output=output)


def _saved_plan(plan_path: Path | None, request: _PlanRequest) -> SavedPlan | None:
    """Read the plan `--plan` names, refusing selection flags that disagree with it; None without one.

    Raises:
        ProjectError: no saved plan is at the path.
        SstUsageError: `--select`, `--exclude`, `--prune`, or `--partial` disagrees with the saved plan.
    """
    if plan_path is None:
        return None
    saved = PlanFileStore(plan_path).read()
    if saved is None:
        raise ProjectError(f"no saved plan at {plan_path}")
    if request.selected and request.selected != saved.selected:
        raise SstUsageError("--select conflicts with the saved plan selection")
    if request.excluded and request.excluded != saved.excluded:
        raise SstUsageError("--exclude conflicts with the saved plan selection")
    if request.prune and not saved.include_prune:
        raise SstUsageError("--prune conflicts with a non-pruning saved plan")
    # Publishing a partial result is always explicit, in both directions.
    if saved.partial and not request.partial:
        raise SstUsageError("the saved plan is partial; apply it with --partial")
    if request.partial and not saved.partial:
        raise SstUsageError("--partial conflicts with a saved plan that is not partial")
    return saved


def _confirmed_options(
    request: _PlanRequest,
    session: _PlanSession,
    saved: SavedPlan | None,
    plan_path: Path | None,
    *,
    sql_out: Path | None,
    confirmed: bool,
    fail_fast: bool,
    break_stale_lock: bool,
) -> ApplyOptions:
    """Check a saved plan still applies, write the statements, and confirm; return how to apply.

    Asks before applying a plan that writes, unless `--yes` was given.

    Raises:
        ProjectError: the saved plan cannot be applied, as `SavedPlan.check_applicable` says.
        click.exceptions.Abort: the user declined to apply.
    """
    changeset = session.ready.changeset
    current = SavedPlan.from_changeset(
        changeset,
        selected=request.selected,
        excluded=request.excluded,
        include_prune=request.prune,
        partial=request.partial,
    )
    if saved is not None:
        mismatch = saved.check_applicable(current, source=str(plan_path))
        if mismatch is not None:
            raise ProjectError(mismatch.message, diagnostics=mismatch.diagnostics)
    _write_plan_sql(request.project_dir, changeset, sql_out)
    if changeset.writes and not confirmed:
        _print_plan(changeset)
        click.confirm("Apply this plan?", abort=True)
    return ApplyOptions(
        # skills.+threads bounds each wave's concurrency (1..16, default 4).
        parallelism=config_int(config_block(_config(request.project_dir).get("skills")).get("+threads")) or 4,
        on_failure=(FailurePolicy.STOP_ALL if fail_fast else FailurePolicy.STOP_DEPENDENTS),
        allow_prune=request.prune,
        break_stale_lock=break_stale_lock,
    )


def _apply_and_report(request: _PlanRequest, session: _PlanSession, options: ApplyOptions, output: str) -> None:
    """Apply the plan, close the connection, and report each outcome; exit 1 unless everything applied.

    A partial apply publishes the healthy changes and still exits 1 while errors remain.
    """
    ready = session.ready
    try:
        apply_result = ApplyArtifacts(
            session.port,
            session.state_store,
            SystemClock(),
            state_table=session.profile.state_table,
            git_sha=_git_sha(request.project_dir),
            actor=session.profile.identity.role or "",
            lifecycle_handlers=ready.lifecycle_handlers,
        ).run(ready.changeset, ready.state, options)
    finally:
        session.port.close()
    left_out = ready.result.diagnostics if request.partial else DiagnosticBag()
    shown = DiagnosticBag((*left_out, *apply_result.diagnostics))
    exit_code = OK if apply_result.success and not left_out.has_errors else ERROR
    if output == "json":
        data: dict[str, object] = {
            "run_id": apply_result.run_id,
            "state_written": apply_result.state_written,
            "outcomes": [_outcome_json(outcome) for outcome in apply_result.outcomes],
        }
        if request.partial:
            data["partial"] = {"excluded": _partial_excluded(ready.result.diagnostics)}
        _emit_json(
            _json_envelope("apply", shown, exit_code=exit_code, artifact_count=len(apply_result.outcomes), data=data),
            exit_code,
        )
    _render_diagnostics(shown)
    for outcome in apply_result.outcomes:
        click.echo(f"{outcome.status.value}: {outcome.key} ({outcome.action.value})")
    if exit_code:
        raise click.exceptions.Exit(exit_code)


def _outcome_json(outcome: ApplyOutcome) -> dict[str, object]:
    return {
        "artifact_key": outcome.key,
        "action": outcome.action.value,
        "status": outcome.status.value,
        "attempts": outcome.attempts,
        "duration_ms": outcome.duration_ms,
        "grant_check": outcome.grants.value,
        "error": (outcome.error.message if outcome.error else None),
        "component_fingerprints": dict(outcome.component_fingerprints),
    }


@cli.command(name="list")
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def list_command(project_dir: Path, output: str) -> None:
    """List compiled artifacts and cached application status."""

    def action() -> None:
        manifest = _compiled_manifest(project_dir)
        states = tuple(sorted(_target_dir(project_dir).glob(STATE_FILE_GLOB)))
        state = StateFileStore(states[0]).read_local() if len(states) == 1 else None
        summaries = list_artifacts(manifest, state)
        data = [dataclasses.asdict(item) for item in summaries]
        if output == "json":
            _emit_json(
                _json_envelope("list", DiagnosticBag(), artifact_count=len(data), data=data),
                OK,
            )
        for item in summaries:
            click.echo(f"{item.key} {item.status} {item.target} {item.fingerprint}")

    _guarded(action, command="list", output=output)


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def clean(project_dir: Path, output: str) -> None:
    """Remove local SST build artifacts only; never touch Snowflake."""

    def action() -> None:
        path = _target_dir(project_dir)
        existed = path.exists()
        shutil.rmtree(path, ignore_errors=True)
        if output == "json":
            _emit_json(
                _json_envelope(
                    "clean",
                    DiagnosticBag(),
                    data={"removed": str(path), "existed": existed},
                ),
                OK,
            )
        click.echo(f"removed {path}" if existed else f"nothing to remove at {path}")

    _guarded(action, command="clean", output=output)


@cli.group()
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """Rewrite a 0.3 project into the 1.0 dialect."""
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in ("refs",)}


@migrate.command(name="refs")
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--write", "write_files", is_flag=True, help="Rewrite files in place instead of reporting.")
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def migrate_refs_command(project_dir: Path, write_files: bool, output: str) -> None:
    """Rewrite legacy table()/column() globals to ref(), and label boolean filters.

    Dry-run by default: exit 2 when rewrites are pending, 0 when there are none,
    and 1 when a table() call sits where no rewrite is safe.
    """

    def action() -> None:
        config = _config(project_dir)
        files = semantic_files(project_dir, _project_dir_value(config, "semantic_models_dir", "semantic_models"))
        report = MigrateRefs(files, filter_sites).run()
        if write_files:
            for item in report.changed:
                write_file(project_dir, item.path, item.result.text)
        if report.untouched:
            exit_code = ERROR
        elif report.changed and not write_files:
            exit_code = CHANGES
        else:
            exit_code = OK
        if output == "json":
            _emit_json(
                _json_envelope(
                    "migrate refs",
                    DiagnosticBag(),
                    exit_code=exit_code,
                    status="error" if exit_code == ERROR else None,
                    artifact_count=len(report.changed),
                    data={
                        "written": write_files and bool(report.changed),
                        "files": [
                            {
                                "path": item.path,
                                "rewrites": item.counts(),
                                "untouched": [
                                    {
                                        "line": entry.line,
                                        "column": entry.col,
                                        "text": entry.text,
                                        "reason": entry.reason,
                                    }
                                    for entry in item.result.untouched
                                ],
                            }
                            for item in report.files
                            if item.result.changed or item.result.untouched
                        ],
                    },
                ),
                exit_code,
            )
        for item in report.files:
            if item.result.changed:
                counts = ", ".join(f"{value} {kind}" for kind, value in item.counts().items() if value)
                click.echo(f"{'rewrote' if write_files else 'would rewrite'} {item.path}: {counts}")
            for entry in item.result.untouched:
                click.echo(
                    f"{item.path}:{entry.line}:{entry.col}: left {entry.text} unchanged: {entry.reason}", err=True
                )
        if not report.changed and not report.untouched:
            click.echo("no legacy references found")
        if exit_code:
            raise click.exceptions.Exit(exit_code)

    _guarded(action, command="migrate refs", output=output)


@cli.command(name="test")
@click.option(
    "--project-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("."),
)
@click.option("--suite", type=click.Choice(["golden", "smoke", "evals"]), required=True)
@click.option("--target", "target_name")
@click.option(
    "--manifest",
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--golden-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("expected/ddl"),
)
@click.option("--fail-fast", is_flag=True)
@click.option("--capture-baseline", "capture_baseline_requested", is_flag=True)
@click.option("--reason")
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def test_command(
    project_dir: Path,
    suite: str,
    target_name: str | None,
    manifest_path: Path | None,
    golden_dir: Path,
    fail_fast: bool,
    capture_baseline_requested: bool,
    reason: str | None,
    output: str,
) -> None:
    """Run exact offline goldens or separate connected smoke probes."""

    def action() -> None:
        result = _compile_result(project_dir, target_name, manifest_path)
        if not result.success:
            _fail("test", result.diagnostics, output)
        inputs = _project_inputs(project_dir, target_name, manifest_path)
        if suite == "golden":
            _run_golden(project_dir, golden_dir, result, inputs, output)
        elif suite == "evals":
            request = EvalGateRequest(fail_fast, capture_baseline_requested, reason)
            _run_evals(project_dir, target_name, result, inputs, request, output)
        else:
            _run_smoke(project_dir, target_name, result, inputs, fail_fast, output)

    _guarded(action, command="test", output=output)


def _run_golden(
    project_dir: Path, golden_dir: Path, result: CompileResult, inputs: YamlProjectInputs, output: str
) -> None:
    """Compare every compiled payload with its committed golden, offline; exit 1 on any failure.

    A relative `--golden-dir` is taken from the project directory.
    """
    resolved = golden_dir if golden_dir.is_absolute() else project_dir / golden_dir
    report = CompareGoldens(GoldenFileStore(resolved), inputs.git_sha).run(result)
    exit_code = OK if report.passed else ERROR
    if output == "json":
        _emit_json(
            _json_envelope(
                "test",
                DiagnosticBag(),
                exit_code=exit_code,
                artifact_count=len(result.compiled),
                data={"suite": "golden", "failures": list(report.failures)},
            ),
            exit_code,
        )
    if not report.passed:
        click.echo("golden suite failed:\n" + "\n\n".join(report.failures), err=True)
        raise click.exceptions.Exit(ERROR)
    click.echo(f"golden suite passed for {len(result.compiled)} artifact(s)")


def _current_manifest(project_dir: Path, result: CompileResult, inputs: YamlProjectInputs, before: str) -> Manifest:
    """Return the manifest `result` publishes, refusing the run when `sst compile` wrote another.

    Raises:
        ProjectError: no compiled manifest exists (SST-MAN001), or it is stale.
    """
    compiled = _compiled_manifest(project_dir)
    current = manifest_for(result, inputs.manifest_sources())
    stale = stale_manifest(compiled, current, before=before)
    if stale is not None:
        raise ProjectError(stale)
    return current


def _run_smoke(
    project_dir: Path,
    target_name: str | None,
    result: CompileResult,
    inputs: YamlProjectInputs,
    fail_fast: bool,
    output: str,
) -> None:
    """Probe the published objects once SST is proven to own them; exit 1 when a check or probe fails."""
    manifest = _current_manifest(project_dir, result, inputs, before="smoke")
    profile, port = _connect(project_dir, target_name)
    try:
        state_store = StateFileStore(state_file(_target_dir(project_dir), profile.target_name))
        smoke = SmokePublished(port, state_store).run(
            result,
            manifest,
            target=profile.identity,
            state_table=profile.state_table,
            fail_fast=fail_fast,
        )
    finally:
        port.close()
    exit_code = OK if smoke.success else ERROR
    if output == "json":
        _emit_json(
            _json_envelope(
                "test",
                smoke.diagnostics,
                exit_code=exit_code,
                artifact_count=len(result.compiled),
                data={"suite": "smoke", "attempted": len(smoke.attempted)},
            ),
            exit_code,
        )
    _render_diagnostics(smoke.diagnostics)
    if exit_code:
        raise click.exceptions.Exit(exit_code)
    click.echo(f"smoke suite passed: {len(smoke.attempted)} probe(s)")


def _run_evals(
    project_dir: Path,
    target_name: str | None,
    result: CompileResult,
    inputs: YamlProjectInputs,
    request: EvalGateRequest,
    output: str,
) -> None:
    """Run the evals against their published agents and gate them; exit 1 unless the run passes.

    Raises:
        SstUsageError: `--capture-baseline` and `--reason` are not given together.
        ProjectError: the project compiles no eval, its compiled manifest is stale, or another
            operation holds the target's state lock.
    """
    if request.capture_baseline and not request.reason:
        raise SstUsageError("--capture-baseline requires --reason")
    if request.reason and not request.capture_baseline:
        raise SstUsageError("--reason requires --capture-baseline")
    evals = compiled_evals(result)
    if not evals:
        raise ProjectError("no eval artifacts matched the project")
    manifest = _current_manifest(project_dir, result, inputs, before="evals")
    profile, port = _connect(project_dir, target_name)
    with _closed_on_error(port):
        state_store = StateFileStore(state_file(_target_dir(project_dir), profile.target_name))
        eval_store = SnowflakeEvalStateStore(port, _eval_state_table(profile.state_table))
        outcome = RunEvalGate(port, inputs, state_store, eval_store, SystemClock()).run(
            evals, manifest, request, target=profile.identity, state_table=profile.state_table
        )
    port.close()
    if isinstance(outcome, EvalGateRefused):
        raise ProjectError(outcome.reason)
    _report_evals(outcome, len(evals), output)


def _eval_state_table(state_table: QualifiedName) -> QualifiedName:
    """Name the table eval baselines and gates are kept in: beside the state table, suffixed `_EVALS`."""
    return QualifiedName(state_table.database, state_table.schema, Identifier.parse(f"{state_table.name.folded}_EVALS"))


def _report_evals(outcome: EvalGateOutcome, artifact_count: int, output: str) -> None:
    """Report an eval run, its diagnostics and then every attempt; exit 1 unless it passed."""
    exit_code = OK if outcome.passed else ERROR
    if output == "json":
        _emit_json(
            _json_envelope(
                "test",
                outcome.diagnostics,
                exit_code=exit_code,
                artifact_count=artifact_count,
                data=dict(outcome.data),
            ),
            exit_code,
        )
    _render_diagnostics(outcome.diagnostics)
    if outcome.suite is not None:
        _print_eval_results(outcome.suite)
    if exit_code:
        raise click.exceptions.Exit(exit_code)


@cli.command()
@click.option("--project-dir", type=click.Path(file_okay=False, path_type=Path), default=Path("."))
@click.option("--check", is_flag=True)
@click.option("--output", type=click.Choice(["human", "json"]), default="human")
def docs(project_dir: Path, check: bool, output: str) -> None:
    """Write the generated reference pages under docs/reference/.

    The artifact, error-code, configuration, and command-line references are
    rendered from the engine's own registries, so they cannot drift from what the
    engine accepts. With --check, nothing is written and the command exits 1 when
    a committed page differs.
    """

    def action() -> None:
        pages = reference_pages(_command_docs(cli), _option_docs(cli), EXIT_CODE_DOCS)
        drifted = [
            path
            for path, text in sorted(pages.items())
            if not (project_dir / path).is_file() or (project_dir / path).read_text(encoding="utf-8") != text
        ]
        if not check:
            for path in drifted:
                (project_dir / path).parent.mkdir(parents=True, exist_ok=True)
                (project_dir / path).write_text(pages[path], encoding="utf-8")
        exit_code = ERROR if check and drifted else OK
        if output == "json":
            data = {"pages": sorted(pages), "drifted": drifted, "written": [] if check else drifted}
            _emit_json(_json_envelope("docs", DiagnosticBag(), exit_code=exit_code, data=data), exit_code)
        for path in drifted:
            click.echo(f"out of date: {path}" if check else f"wrote {path}")
        if exit_code:
            click.echo("run `sst docs` to regenerate the reference pages")
            raise click.exceptions.Exit(exit_code)
        click.echo(f"{len(pages)} reference page(s) current")

    _guarded(action, command="docs", output=output)


EXIT_CODE_DOCS: tuple[tuple[int, str, str], ...] = (
    (OK, "OK", "Success. For `sst plan`, nothing to change."),
    (ERROR, "ERROR", "Errors were reported, or an apply, a test suite, or a check failed."),
    (CHANGES, "CHANGES", "`sst plan` found changes, or `sst migrate refs` found rewrites to make."),
    (USAGE, "USAGE", "The command line is invalid."),
    (CONFIG, "CONFIG", "The project, its configuration, or a saved plan cannot be used."),
    (CONNECTION, "CONNECTION", "Snowflake could not be reached."),
    (INTERRUPTED, "INTERRUPTED", "The run was interrupted."),
)

# One description per flag, shared by every command that takes it, so `--help`
# and the generated CLI reference say the same thing everywhere. A command whose
# flag means something narrower overrides it in `_COMMAND_OPTION_HELP`.
_OPTION_HELP: Mapping[str, str] = {
    "--project-dir": "Project root: the directory that holds `sst_config.yml`.",
    "--target": "Target from `profiles.yml`; defaults to the profile's own default target.",
    "--manifest": "Read this dbt `manifest.json` instead of running `dbt parse`.",
    "--output": "`human` for readable text, or `json` for one machine-readable envelope.",
    "--select": "Only these artifacts: a semantic view name, `type:<type>`, or `<type>:<name>`.",
    "--exclude": "Leave these artifacts out; same forms as `--select`.",
    "--strict": "Promote every warning to an error. Defaults to `validation.strict`.",
    "--snowflake-syntax-check": (
        "Compile expressions against Snowflake. Defaults to `validation.snowflake_syntax_check`."
    ),
    "--prune": (
        "Also act on managed artifacts whose source was deleted, as far as each type "
        "allows: drop, deactivate, or report."
    ),
    "--partial": (
        "Go ahead with every artifact that has no errors and depends on nothing that does; "
        "still exits 1 while errors remain. Cannot be combined with `--prune`."
    ),
    "--sql-out": "Also write the statements for each change into this directory.",
    "--fail-fast": "Stop at the first failure instead of continuing.",
}
_COMMAND_OPTION_HELP: Mapping[tuple[str, str], str] = {
    ("sst", "--output"): "Default `--output` for the command that follows.",
    ("sst", "--project-dir"): "Default `--project-dir` for the command that follows.",
    ("sst apply", "--plan"): "Apply this saved plan. It must still match the compiled project.",
    ("sst apply", "--yes"): "Apply without asking for confirmation.",
    ("sst apply", "--break-stale-lock"): "Take over a state lock left behind by a run that no longer exists.",
    ("sst compile", "--emit-ddl"): "Write each semantic view's rendered DDL into this directory.",
    ("sst compile", "--print-ddl"): "Print the rendered DDL to stdout.",
    ("sst compile", "--ddl-output-dir"): "Same as `--emit-ddl`.",
    ("sst compile", "--manifest-output"): "Also write the SST manifest here; with `--select`, only the selection.",
    ("sst compile", "--select"): "Only this artifact: a semantic view name, `type:<type>`, or `<type>:<name>`.",
    ("sst debug", "--test-connection"): "Also connect to Snowflake and report the session's role and account.",
    ("sst docs", "--check"): "Write nothing; exit 1 when a committed reference page is out of date.",
    ("sst plan", "--plan-out"): "Write the saved plan here instead of `target/sst/plan.json`.",
    ("sst plan", "--no-plan-out"): "Do not write a saved plan.",
    ("sst plan", "--no-detailed-exitcode"): "Exit 0 when changes are pending, instead of 2.",
    ("sst test", "--suite"): (
        "`golden` compares outputs with committed goldens offline; `smoke` probes deployed "
        "objects; `evals` runs agent evaluations."
    ),
    ("sst test", "--golden-dir"): "Directory of the semantic view DDL goldens; the other goldens sit beside it.",
    ("sst test", "--capture-baseline"): "Record this eval run as the new baseline. Requires `--reason`.",
    ("sst test", "--reason"): "Why the baseline is changing; stored with it.",
    ("sst test", "--fail-fast"): "Stop at the first failing golden, probe, or eval.",
}


def _document_options(command: click.Command, path: str = "sst") -> None:
    """Give every option declared without help text its shared description."""
    for param in command.params:
        if isinstance(param, click.Option) and not param.help:
            flag = param.opts[0]
            param.help = _COMMAND_OPTION_HELP.get((path, flag)) or _OPTION_HELP.get(flag)
    for name, child in getattr(command, "commands", {}).items():
        _document_options(child, f"{path} {name}")


def _command_docs(group: click.Group, prefix: str = "sst") -> tuple[CommandDoc, ...]:
    """The visible command tree, each group followed by its subcommands."""
    documented: list[CommandDoc] = []
    for name in sorted(group.commands):
        command = group.commands[name]
        if command.hidden:
            continue
        path = f"{prefix} {name}"
        description = inspect.cleandoc(command.help or "")
        if isinstance(command, click.Group):
            children = tuple(f"{path} {child}" for child in sorted(command.commands))
            documented.append(CommandDoc(path, description, subcommands=children))
            documented.extend(_command_docs(command, path))
        else:
            documented.append(CommandDoc(path, description, _option_docs(command)))
    return tuple(documented)


def _option_docs(command: click.Command) -> tuple[OptionDoc, ...]:
    documented: list[OptionDoc] = []
    for param in command.params:
        if not isinstance(param, click.Option) or param.hidden:
            continue
        if param.is_flag:
            value = None
        elif isinstance(param.type, click.Choice):
            value = "|".join(str(choice) for choice in param.type.choices)
        elif isinstance(param.type, click.Path):
            value = "DIRECTORY" if not param.type.file_okay else "FILE" if not param.type.dir_okay else "PATH"
        else:
            value = param.type.name.upper()
        default = param.default
        shown = str(default) if isinstance(default, (str, int, Path)) and not isinstance(default, bool) else None
        documented.append(
            OptionDoc(
                " / ".join((*param.opts, *param.secondary_opts)),
                value,
                None if param.is_flag else shown,
                param.help or "",
                multiple=param.multiple,
                required=param.required,
            )
        )
    return tuple(documented)


_document_options(cli)


def main() -> None:
    """Run `sst`; `python -m snowflake_semantic_tools.cli.main` enters here."""
    cli(standalone_mode=True)


if __name__ == "__main__":
    main()

"""SST 1.0 composition root and Milestone 2 command surface."""

from __future__ import annotations

import dataclasses
import difflib
import json
import shutil
import sys
import time
from collections.abc import Callable, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import click

from ..adapters.clock import SystemClock
from ..adapters.dbt.manifest import load_manifest_catalog
from ..adapters.fs.local import ManifestFileStore, PlanFileStore, StateFileStore
from ..adapters.profile import ProfileTarget, load_profile_target
from ..adapters.project import ProjectError
from ..adapters.snowflake.connector import SnowflakeConnector
from ..adapters.snowflake.eval_state import SnowflakeEvalStateStore
from ..adapters.yaml.agents import load_agents
from ..adapters.yaml.documents import discover_yaml, load_documents
from ..adapters.yaml.loader import _parse_yaml_bytes
from ..adapters.yaml.project_source import YamlProjectSource
from ..app.agent_compile import AgentCompileContext, CompileAgents, CompiledAgent, for_publication
from ..app.apply import ApplyArtifacts
from ..app.compile import CompileArtifacts, CompileResult, CompileSemanticViews
from ..app.eval_compile import CompiledEval, CompileEvals
from ..app.eval_gate import capture_baseline, evaluate_gate, persist_gate
from ..app.eval_lifecycle import EvalLifecycleConfig, EvalLifecycleHandler
from ..app.eval_run import (
    EvalRunOptions,
    EvalSuiteResult,
    RunEvalSuite,
    empty_eval_suite_json,
    eval_suite_json,
    validate_eval_publication,
)
from ..app.listing import list_artifacts
from ..app.manifest import build_manifest
from ..app.plan import PlanArtifacts
from ..app.smoke import RunSmokeSuite
from ..app.state import read_state
from ..app.tool_compile import CompileTools
from ..app.validate import ValidateArtifacts
from ..domain.model.diagnostic import ERROR_REGISTRY, D, Diagnostic, DiagnosticBag, Severity
from ..domain.model.identifier import Identifier, QualifiedName
from ..domain.model.lifecycle import (
    Action,
    ApplyOptions,
    Change,
    ChangeSet,
    FailurePolicy,
    OutcomeStatus,
    OwnershipMarker,
)
from ..domain.model.registry import SEMANTIC_REGISTRY
from ..domain.ports.snowflake import SnowflakePortError
from ..domain.state.model import Manifest, SavedPlan, State, canonical_json

VERSION = "1.0.0.dev0"
OK = 0
ERROR = 1
CHANGES = 2
USAGE = 3
CONFIG = 4
CONNECTION = 5
INTERRUPTED = 130

_INVOCATION: dict[str, object] = {}


class SstUsageError(click.UsageError):
    exit_code = USAGE


class SstGroup(click.Group):
    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            raise SstUsageError(str(exc), ctx) from exc

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


def _source(project_dir: Path, target_name: str | None, manifest_path: Path | None) -> YamlProjectSource:
    return YamlProjectSource(
        project_dir,
        target_name=target_name,
        manifest_path=manifest_path,
        invoke_dbt=manifest_path is None,
    )


def _target_dir(project_dir: Path) -> Path:
    return project_dir / "target" / "sst"


def _compile_result(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: str | None = None,
) -> CompileResult:
    source = _source(project_dir, target_name, manifest_path)
    semantic = CompileSemanticViews(source).run_result()
    profile = load_profile_target(project_dir, target_name)
    config = _config(project_dir)
    dbt_path = manifest_path or project_dir / "target" / "manifest.json"
    dbt = load_manifest_catalog(dbt_path)
    tool_catalog = source.load_tools()
    tool_defaults = _config_map(config.get("tools"))
    tools = CompileTools(
        tool_catalog,
        database=_target_config_text(tool_defaults.get("+database"), profile, profile.identity.database.folded)
        or profile.identity.database.folded,
        schema=_target_config_text(tool_defaults.get("+schema"), profile, profile.identity.schema.folded)
        or profile.identity.schema.folded,
        warehouse=_target_config_text(tool_defaults.get("+warehouse"), profile, profile.identity.warehouse),
        target_lag=_config_text(tool_defaults.get("+target_lag"), None),
        embedding_model=_config_text(tool_defaults.get("+embedding_model"), None),
        execute_as=_config_text(tool_defaults.get("+execute_as"), "caller"),
        dbt_relations={model.name: model.relation_name for model in dbt.models},
    ).run_result()
    agent_models, agent_diagnostics = load_agents(
        project_dir, agents_dir=_project_dir_value(config, "agents_dir", "agents")
    )
    agent_defaults = _config_map(config.get("agents"))
    snowflake_config = _config_map(config.get("snowflake"))
    skills_config = _config_map(config.get("skills"))
    raw_models = snowflake_config.get("orchestration_models")
    allowed_models = (
        frozenset(str(value) for value in raw_models) if isinstance(raw_models, list) else frozenset(("auto",))
    )
    extensions = {
        skill.name.replace("-", "_").casefold(): QualifiedName.from_parts(
            profile.identity.database.folded,
            profile.identity.schema.folded,
            skill.name.replace("-", "_"),
        )
        for skill in (skill for agent in agent_models for skill in agent.skills)
    }
    extensions.update(
        {
            skill.name.casefold(): QualifiedName.from_parts(
                profile.identity.database.folded,
                profile.identity.schema.folded,
                skill.name.replace("-", "_"),
            )
            for skill in (skill for agent in agent_models for skill in agent.skills)
        }
    )
    semantic_targets = {item.name.casefold(): item.rendered_artifact.target for item in semantic.compiled}
    agent_targets = {
        model.name.casefold(): QualifiedName.from_parts(
            _target_config_text(agent_defaults.get("+database"), profile, profile.identity.database.folded)
            or profile.identity.database.folded,
            _target_config_text(agent_defaults.get("+schema"), profile, profile.identity.schema.folded)
            or profile.identity.schema.folded,
            model.name,
        )
        for model in agent_models
        if model.enabled and agent_defaults.get("+enabled", True) is not False
    }
    agents = CompileAgents(
        tuple(model for model in agent_models if model.enabled and agent_defaults.get("+enabled", True) is not False),
        agent_diagnostics,
        AgentCompileContext(
            semantic_targets,
            tool_catalog,
            agent_targets,
            extensions,
            {"sha_version": _git_sha(project_dir)},
            _target_config_text(agent_defaults.get("+database"), profile, profile.identity.database.folded)
            or profile.identity.database.folded,
            _target_config_text(agent_defaults.get("+schema"), profile, profile.identity.schema.folded)
            or profile.identity.schema.folded,
            _target_config_text(agent_defaults.get("+warehouse"), profile, profile.identity.warehouse),
            _config_int(agent_defaults.get("+query_timeout")),
            _config_text(agent_defaults.get("+orchestration_model"), "auto") or "auto",
            _config_int(agent_defaults.get("+budget_seconds")),
            _config_int(agent_defaults.get("+budget_tokens")),
            _config_text(agent_defaults.get("+tool_not_accessible"), None),
            _config_bool(agent_defaults.get("+analytical_search")),
            _config_text(agent_defaults.get("+alias"), None),
            allowed_models,
            _config_text(skills_config.get("+version_prefix"), "") or "",
        ),
    ).run_result()
    resolved_agent_tools = {
        item.resolved.model.name.casefold(): tuple(sorted(item.resolved.agent_facing_tool_names))
        for item in agents.compiled
        if isinstance(item, CompiledAgent)
    }
    evals = CompileEvals(
        source.load_evals(
            tuple(
                model for model in agent_models if model.enabled and agent_defaults.get("+enabled", True) is not False
            ),
            agent_diagnostics,
            resolved_agent_tools,
        ),
        agent_targets=agent_targets,
    ).run_result()
    result = CompileArtifacts(
        (_StaticCompiler(semantic), _StaticCompiler(tools), _StaticCompiler(agents), _StaticCompiler(evals)),
        {name: value.ddl_position for name, value in SEMANTIC_REGISTRY.artifacts.items()},
    ).run_result()
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


class _StaticCompiler:
    def __init__(self, result: CompileResult) -> None:
        self._result = result

    def run_result(self) -> CompileResult:
        return self._result


def _config(project_dir: Path) -> dict[str, object]:
    import yaml

    value = yaml.safe_load((project_dir / "sst_config.yml").read_text(encoding="utf-8")) or {}
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _project_dir_value(config: dict[str, object], key: str, default: str) -> str:
    project = config.get("project")
    return str(project.get(key) or default) if isinstance(project, dict) else default


def _config_map(value: object) -> dict[str, object]:
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _config_text(value: object, default: str | None) -> str | None:
    if value is None:
        return default
    if not isinstance(value, str):
        return str(value)
    replacements = {
        "{{ target.database }}": default or "",
        "{{ target.schema }}": default or "",
        "{{ target.warehouse }}": default or "",
    }
    for raw, resolved in replacements.items():
        value = value.replace(raw, resolved)
    return value or default


def _target_config_text(
    value: object,
    profile: ProfileTarget,
    default: str | None,
) -> str | None:
    if not isinstance(value, str):
        return default if value is None else str(value)
    return (
        value.replace("{{ target.database }}", profile.identity.database.folded)
        .replace("{{ target.schema }}", profile.identity.schema.folded)
        .replace("{{ target.warehouse }}", profile.identity.warehouse or "")
        or default
    )


def _config_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _config_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


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
            prefix, name = value.split(":", 1)
            if prefix == "type":
                if name not in SEMANTIC_REGISTRY.artifacts:
                    raise SstUsageError(f"unknown artifact type {name!r}")
                types.add(name)
                continue
            if prefix not in SEMANTIC_REGISTRY.artifacts or not name:
                raise SstUsageError(f"unsupported selector {value!r}")
            keys.add(f"{prefix}:{name.casefold()}")
            continue
        keys.add(f"semantic_view:{value.casefold()}")
    return (frozenset(types) if types else None), (frozenset(keys) if keys else None)


def _file_checksums(project_dir: Path) -> dict[str, str]:
    config_path = project_dir / "sst_config.yml"
    semantic_models_dir = "semantic_models"
    if config_path.is_file():
        import yaml

        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if isinstance(config, dict) and isinstance(config.get("project"), dict):
            semantic_models_dir = str(config["project"].get("semantic_models_dir") or semantic_models_dir)
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir), _parse_yaml_bytes)
    checksums = {document.path: document.checksum for document in documents.documents}
    config = _config(project_dir) if config_path.is_file() else {}
    for directory in (
        _project_dir_value(config, "tools_dir", "tools"),
        _project_dir_value(config, "agents_dir", "agents"),
        _project_dir_value(config, "eval_metrics_dir", "eval_metrics"),
    ):
        root = project_dir / directory
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file():
                checksums[path.relative_to(project_dir).as_posix()] = sha256(path.read_bytes()).hexdigest()
    return checksums


def _build_manifest(project_dir: Path, result: CompileResult, manifest_path: Path | None) -> Manifest:
    dbt_project_name = ""
    dbt_project = project_dir / "dbt_project.yml"
    if dbt_project.is_file():
        import yaml

        value = yaml.safe_load(dbt_project.read_text(encoding="utf-8")) or {}
        if isinstance(value, dict):
            dbt_project_name = str(value.get("name") or "")
    dbt_path = manifest_path or project_dir / "target" / "manifest.json"
    catalog = load_manifest_catalog(dbt_path)
    dbt_projection = [
        {
            "name": model.name,
            "relation": model.relation_name,
            "primary_key": list(model.primary_key),
            "unique_keys": [list(key) for key in model.unique_keys],
            "columns": [
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "column_type": column.column_type,
                }
                for column in model.columns
            ],
        }
        for model in sorted(catalog.models, key=lambda item: item.name.casefold())
    ]
    try:
        recorded_dbt_path = dbt_path.resolve().relative_to(project_dir.resolve()).as_posix()
    except ValueError:
        recorded_dbt_path = str(dbt_path)
    config_path = project_dir / "sst_config.yml"
    config_checksum = ""
    semantic_path = "semantic_models"
    if config_path.is_file():
        import yaml

        config_value = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        config_checksum = sha256(canonical_json(config_value)).hexdigest()
        if isinstance(config_value, dict) and isinstance(config_value.get("project"), dict):
            semantic_path = str(config_value["project"].get("semantic_models_dir") or semantic_path)
    return build_manifest(
        result,
        project_root=".",
        semantic_path=semantic_path,
        dbt_project_name=dbt_project_name,
        config_checksum=config_checksum,
        dbt_manifest_path=recorded_dbt_path,
        dbt_schema_version=catalog.schema_version,
        dbt_digest=sha256(canonical_json(dbt_projection)).hexdigest(),
        model_count=len(catalog.models),
        file_checksums=_file_checksums(project_dir),
    )


def _validation_settings(project_dir: Path) -> tuple[bool, bool]:
    config_path = project_dir / "sst_config.yml"
    if not config_path.is_file():
        return False, True
    import yaml

    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    validation = config.get("validation") if isinstance(config, dict) else None
    if not isinstance(validation, dict):
        return False, True
    return bool(validation.get("strict", False)), bool(validation.get("snowflake_syntax_check", True))


def _effective_validation_settings(
    project_dir: Path,
    *,
    strict: bool | None,
    connected: bool | None,
) -> tuple[bool, bool]:
    configured_strict, configured_connected = _validation_settings(project_dir)
    return (
        configured_strict if strict is None else strict,
        configured_connected if connected is None else connected,
    )


def _diagnostic_json(value: Diagnostic) -> dict[str, object]:
    origin = getattr(value, "origin", None)
    spec = ERROR_REGISTRY[value.code]
    subject_parts = value.subject.split(":", 1) if value.subject and ":" in value.subject else None
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
            if subject_parts and subject_parts[0] in ("semantic_view", "agent", "tool", "skill", "eval")
            else None
        ),
        "member": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] not in ("semantic_view", "agent", "tool", "skill", "eval")
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


def _render_diagnostics(diagnostics: DiagnosticBag) -> None:
    for diagnostic in diagnostics:
        click.echo(
            f"{diagnostic.severity.name.lower()}[{diagnostic.code}]: {diagnostic.message}",
            err=True,
        )


def _guarded(action: Callable[[], None], *, command: str, output: str) -> None:
    try:
        action()
    except click.exceptions.Exit:
        raise
    except click.UsageError:
        raise
    except SnowflakePortError as exc:
        if output == "json":
            _emit_json(
                _json_envelope(
                    command,
                    DiagnosticBag(),
                    exit_code=CONNECTION,
                    status="error",
                    data={"error": str(exc)},
                ),
                CONNECTION,
            )
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
    }


def _print_plan(changeset: ChangeSet) -> None:
    counts = {action: sum(change.action is action for change in changeset.changes) for action in Action}
    click.echo(
        "Plan: "
        f"{counts[Action.CREATE]} to create, {counts[Action.UPDATE]} to update, "
        f"{counts[Action.PRUNE]} to prune, {counts[Action.NOOP]} unchanged, "
        f"{counts[Action.BLOCKED]} blocked."
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
        click.echo(
            f"{markers[change.action]} {change.key} {change.action.value.upper()} {target} {change.reason.value}"
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
        path = output / f"{change.artifact_type}__{change.key.split(':', 1)[1].replace('/', '_')}{suffix}"
        content = (
            change.rendered.content
            if suffix in (".json", ".yaml")
            else ";\n\n".join(change.rendered.statements) + ";\n"
        )
        path.write_text(content, encoding="utf-8")
    return output


def _plan_runtime(
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    include_prune: bool,
    strict: bool | None,
    connected: bool | None,
) -> tuple[
    CompileResult,
    Manifest | None,
    ProfileTarget | None,
    SnowflakeConnector | None,
    tuple[StateFileStore, State, ChangeSet, dict[str, EvalLifecycleHandler]] | None,
]:
    prune_types, prune_keys = _selection(selected)
    excluded_types, excluded_keys = _selection(excluded)
    if excluded_types is not None:
        prune_types = (
            frozenset(SEMANTIC_REGISTRY.artifacts) - excluded_types
            if prune_types is None
            else frozenset(prune_types - excluded_types)
        )
    if include_prune and excluded_keys is not None:
        if prune_keys is None:
            raise SstUsageError("--prune with --exclude requires --select so the prune scope is explicit")
        prune_keys = frozenset(prune_keys - excluded_keys)
    full_result = _compile_result(project_dir, target_name, manifest_path)
    compiled_manifest_store = ManifestFileStore(_target_dir(project_dir) / "manifest.json")
    compiled_manifest = compiled_manifest_store.read()
    if compiled_manifest is None:
        raise ProjectError("no SST manifest; run sst compile before plan or apply")
    compiled = tuple(
        item
        for item in full_result.compiled
        if (
            (prune_types is None and prune_keys is None)
            or (prune_types is not None and item.artifact_type in prune_types)
            or (prune_keys is not None and item.artifact_key in prune_keys)
        )
        and (excluded_types is None or item.artifact_type not in excluded_types)
        and (excluded_keys is None or item.artifact_key not in excluded_keys)
    )
    if selected and not compiled and not include_prune:
        raise ProjectError(f"selectors {selected!r} matched no artifact in {project_dir}")
    result = dataclasses.replace(full_result, compiled=compiled)
    if not result.success:
        return result, None, None, None, None
    effective_strict, effective_connected = _effective_validation_settings(
        project_dir,
        strict=strict,
        connected=connected,
    )
    manifest = _build_manifest(project_dir, full_result, manifest_path)
    if compiled_manifest.manifest_id != manifest.manifest_id:
        raise ProjectError("compiled SST manifest is stale; run sst compile before plan or apply")
    profile, port = _connect(project_dir, target_name)
    validation = ValidateArtifacts(port if effective_connected else None).run(
        result,
        strict=effective_strict,
        connected=effective_connected,
    )
    if not validation.success:
        port.close()
        failed_result = dataclasses.replace(result, diagnostics=validation.diagnostics)
        return failed_result, None, None, None, None
    state_store = StateFileStore(_target_dir(project_dir) / f"state.{profile.target_name}.json")
    state, state_diagnostics = read_state(
        state_store,
        port,
        state_table=profile.state_table,
        target=profile.identity,
    )
    config = _config(project_dir)
    apply_config = _config_map(config.get("apply"))
    stage_config = _config_map(apply_config.get("agent_spec_stage"))
    stage = QualifiedName.from_parts(
        _config_text(stage_config.get("database"), profile.identity.database.folded)
        or profile.identity.database.folded,
        _config_text(stage_config.get("schema"), profile.identity.schema.folded) or profile.identity.schema.folded,
        str(stage_config.get("stage") or "AGENT_SPECS"),
    )
    publication_compiled = tuple(
        (
            for_publication(
                item,
                stage=stage,
                git_sha=_git_sha(project_dir),
            )
            if isinstance(item, CompiledAgent)
            else item
        )
        for item in result.compiled
    )
    publication_result = dataclasses.replace(result, compiled=publication_compiled)
    publish = {artifact.key: artifact for artifact in publication_result.rendered_for_publish(manifest.manifest_id)}
    eval_stage_config = _config_map(apply_config.get("eval_config_stage"))
    lifecycle_handlers = {
        "eval": EvalLifecycleHandler(
            port,
            EvalLifecycleConfig(str(eval_stage_config.get("stage") or "EVAL_CONFIGS")),
        )
    }
    observation_targets = tuple(
        dict.fromkeys(
            (
                *(artifact.target for artifact in full_result.rendered),
                *(
                    QualifiedName.parse(entry.qualified_name)
                    for entry in state.applied.values()
                    if entry.qualified_name
                ),
            )
        )
    )
    changeset = PlanArtifacts(port, lifecycle_handlers=lifecycle_handlers).run(
        publish,
        manifest,
        state,
        profile.identity,
        fetched_at=SystemClock().now_iso(),
        include_prune=include_prune,
        prune_types=prune_types,
        prune_keys=prune_keys,
        observation_targets=observation_targets,
    )
    if state_diagnostics:
        changeset = dataclasses.replace(
            changeset,
            diagnostics=DiagnosticBag((*state_diagnostics, *changeset.diagnostics)),
        )
    return result, manifest, profile, port, (state_store, state, changeset, lifecycle_handlers)


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
        }
        if test_connection:
            port = SnowflakeConnector(profile.connection_params)
            try:
                data["current_role"] = port.current_role()
                data["current_account"] = port.current_account_locator()
            finally:
                port.close()
        if output == "json":
            _emit_json(_json_envelope("debug", DiagnosticBag(), data=data), OK)
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
    target_name: str | None,
    manifest_path: Path | None,
    output: str,
) -> None:
    """Compile semantic views and write the canonical manifest."""

    def action() -> None:
        full_result = _compile_result(project_dir, target_name, manifest_path)
        if not full_result.success:
            if output == "json":
                _emit_json(
                    _json_envelope(
                        "compile",
                        full_result.diagnostics,
                        artifact_count=len(full_result.compiled),
                    ),
                    ERROR,
                )
            _render_diagnostics(full_result.diagnostics)
            raise click.exceptions.Exit(ERROR)
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
            _emit_json(
                _json_envelope(
                    "compile",
                    result.diagnostics,
                    artifact_count=len(result.compiled),
                    data={
                        "manifest_path": str(destination),
                        "manifest_id": manifest.manifest_id,
                        "artifacts": artifacts,
                    },
                ),
                OK,
            )
        if print_ddl:
            click.echo("\n\n".join(item.rendered_artifact.content for item in result.compiled))
            return
        output_dir = emit_ddl_dir or ddl_output_dir
        if output_dir is not None:
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
            return
        click.echo(f"compiled {len(result.compiled)} artifact(s); manifest {destination}")

    _guarded(action, command="compile", output=output)


def _artifact_suffix(render_dialect: str) -> str:
    if render_dialect == "json":
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

    def action() -> None:
        result, manifest, _, port, runtime = _plan_runtime(
            project_dir,
            target_name,
            manifest_path,
            selected,
            excluded,
            prune,
            strict,
            snowflake_syntax_check,
        )
        if runtime is None or manifest is None or port is None:
            if output == "json":
                _emit_json(_json_envelope("plan", result.diagnostics, exit_code=ERROR), ERROR)
            _render_diagnostics(result.diagnostics)
            raise click.exceptions.Exit(ERROR)
        _, _, changeset, _ = runtime
        try:
            saved = SavedPlan.from_changeset(
                changeset,
                selected=selected,
                excluded=excluded,
                include_prune=prune,
            )
            destination = plan_out or _target_dir(project_dir) / "plan.json"
            if not no_plan_out:
                PlanFileStore(destination).write(saved)
            sql_path = _write_plan_sql(project_dir, changeset, sql_out)
        finally:
            port.close()
        if changeset.blocked or changeset.diagnostics.has_errors:
            exit_code = ERROR
        elif changeset.writes:
            exit_code = OK if no_detailed_exitcode else CHANGES
        else:
            exit_code = OK
        if output == "json":
            _emit_json(
                _json_envelope(
                    "plan",
                    changeset.diagnostics,
                    exit_code=exit_code,
                    artifact_count=len(changeset.changes),
                    data={
                        "manifest_id": manifest.manifest_id,
                        "plan_id": saved.plan_id,
                        "plan_path": None if no_plan_out else str(destination),
                        "sql_path": str(sql_path),
                        "changes": [_change_json(change) for change in changeset.changes],
                    },
                ),
                exit_code,
            )
        _render_diagnostics(changeset.diagnostics)
        _print_plan(changeset)
        if exit_code:
            raise click.exceptions.Exit(exit_code)

    _guarded(action, command="plan", output=output)


def _saved_plan_guard(saved: SavedPlan, current: SavedPlan) -> None:
    if saved.observation_fingerprint != current.observation_fingerprint:
        raise ProjectError("saved plan is stale; the live Snowflake observation changed")
    saved_changes = tuple(
        (
            item.key,
            item.artifact_type,
            item.action,
            item.reason,
            item.target,
            item.fingerprint,
            item.previous_marker,
            item.statement_hashes,
            item.depends_on,
            item.order,
            item.component_fingerprints,
            item.physical_resources,
            item.prune_executable,
        )
        for item in saved.changes
    )
    current_changes = tuple(
        (
            item.key,
            item.artifact_type,
            item.action,
            item.reason,
            item.target,
            item.fingerprint,
            item.previous_marker,
            item.statement_hashes,
            item.depends_on,
            item.order,
            item.component_fingerprints,
            item.physical_resources,
            item.prune_executable,
        )
        for item in current.changes
    )
    if saved_changes != current_changes:
        raise ProjectError("saved plan is stale; the ordered changes or statement hashes changed")


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

    def action() -> None:
        saved = None
        effective_selected = selected
        effective_excluded = excluded
        effective_prune = prune
        if plan_path is not None:
            saved = PlanFileStore(plan_path).read()
            if saved is None:
                raise ProjectError(f"no saved plan at {plan_path}")
            if selected and selected != saved.selected:
                raise SstUsageError("--select conflicts with the saved plan selection")
            if excluded and excluded != saved.excluded:
                raise SstUsageError("--exclude conflicts with the saved plan selection")
            if prune and not saved.include_prune:
                raise SstUsageError("--prune conflicts with a non-pruning saved plan")
            effective_selected = saved.selected
            effective_excluded = saved.excluded
            effective_prune = saved.include_prune
        result, manifest, profile, port, runtime = _plan_runtime(
            project_dir,
            target_name,
            manifest_path,
            effective_selected,
            effective_excluded,
            effective_prune,
            strict,
            snowflake_syntax_check,
        )
        if runtime is None or manifest is None or profile is None or port is None:
            if output == "json":
                _emit_json(_json_envelope("apply", result.diagnostics, exit_code=ERROR), ERROR)
            _render_diagnostics(result.diagnostics)
            raise click.exceptions.Exit(ERROR)
        state_store, previous, changeset, lifecycle_handlers = runtime
        current_saved = SavedPlan.from_changeset(
            changeset,
            selected=effective_selected,
            excluded=effective_excluded,
            include_prune=effective_prune,
        )
        if saved is not None:
            if not saved.matches(manifest.manifest_id, profile.identity):
                raise ProjectError("saved plan target or manifest does not match this apply")
            _saved_plan_guard(saved, current_saved)
        _write_plan_sql(project_dir, changeset, sql_out)
        if changeset.writes and not confirmed:
            _print_plan(changeset)
            click.confirm("Apply this plan?", abort=True)
        options = ApplyOptions(
            on_failure=(FailurePolicy.STOP_ALL if fail_fast else FailurePolicy.STOP_DEPENDENTS),
            allow_prune=effective_prune,
            break_stale_lock=break_stale_lock,
        )
        try:
            apply_result = ApplyArtifacts(
                port,
                state_store,
                SystemClock(),
                state_table=profile.state_table,
                git_sha=_git_sha(project_dir),
                actor=profile.identity.role or "",
                lifecycle_handlers=lifecycle_handlers,
            ).run(changeset, previous, options)
        finally:
            port.close()
        exit_code = OK if apply_result.success else ERROR
        if output == "json":
            _emit_json(
                _json_envelope(
                    "apply",
                    apply_result.diagnostics,
                    exit_code=exit_code,
                    artifact_count=len(apply_result.outcomes),
                    data={
                        "run_id": apply_result.run_id,
                        "state_written": apply_result.state_written,
                        "outcomes": [
                            {
                                "artifact_key": outcome.key,
                                "action": outcome.action.value,
                                "status": outcome.status.value,
                                "attempts": outcome.attempts,
                                "duration_ms": outcome.duration_ms,
                                "grant_check": outcome.grants.value,
                                "error": (outcome.error.message if outcome.error else None),
                            }
                            for outcome in apply_result.outcomes
                        ],
                    },
                ),
                exit_code,
            )
        _render_diagnostics(apply_result.diagnostics)
        for outcome in apply_result.outcomes:
            click.echo(f"{outcome.status.value}: {outcome.key} ({outcome.action.value})")
        if exit_code:
            raise click.exceptions.Exit(exit_code)

    _guarded(action, command="apply", output=output)


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
        manifest = ManifestFileStore(_target_dir(project_dir) / "manifest.json").read()
        if manifest is None:
            raise ProjectError("no SST manifest; run sst compile")
        states = tuple(sorted(_target_dir(project_dir).glob("state.*.json")))
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


def _golden_ddl(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if line.strip() and not line.lstrip().startswith("--"):
            return "\n".join(lines[index:]).rstrip("\n")
    raise ProjectError(f"golden {path} contains no DDL")


def _normalized_payload(value: str, *, git_sha: str) -> str:
    if git_sha and git_sha != "WORKTREE":
        return value.replace(f"GIT_{git_sha}", "GIT_0000000")
    return value


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
            if output == "json":
                _emit_json(_json_envelope("test", result.diagnostics, exit_code=ERROR), ERROR)
            _render_diagnostics(result.diagnostics)
            raise click.exceptions.Exit(ERROR)
        if suite == "golden":
            resolved = golden_dir if golden_dir.is_absolute() else project_dir / golden_dir
            failures: list[str] = []
            for item in result.compiled:
                payloads: tuple[tuple[Path, str, str, bool], ...]
                if item.artifact_type == "semantic_view":
                    payloads = (
                        (
                            resolved / f"{item.name.casefold()}.sql",
                            item.rendered_artifact.content,
                            f"compiled/{item.name.casefold()}.sql",
                            True,
                        ),
                    )
                elif item.artifact_type == "tool":
                    payloads = (
                        (
                            resolved.parent / "tool" / f"{item.name.casefold()}.sql",
                            item.rendered_artifact.content,
                            f"compiled/{item.name.casefold()}.sql",
                            True,
                        ),
                    )
                elif isinstance(item, CompiledEval):
                    payloads = (
                        (
                            resolved.parent / "eval" / f"{item.name.casefold()}_repeat.yaml",
                            item.rendered.config_yaml,
                            f"compiled/{item.name.casefold()}_repeat.yaml",
                            False,
                        ),
                        (
                            resolved.parent / "eval" / f"{item.name.casefold().removesuffix('_agent')}_source.sql",
                            item.rendered.source_table_sql,
                            f"compiled/{item.name.casefold()}_source.sql",
                            False,
                        ),
                    )
                else:
                    payloads = (
                        (
                            resolved.parent / "agent" / f"{item.name.casefold()}.json",
                            item.rendered_artifact.content,
                            f"compiled/{item.name.casefold()}.json",
                            False,
                        ),
                    )
                for path, content, compiled_path, is_ddl in payloads:
                    if not path.is_file():
                        failures.append(f"missing golden {path}")
                        continue
                    expected = (
                        _golden_ddl(path).rstrip() + "\n"
                        if is_ddl
                        else path.read_text(encoding="utf-8").rstrip() + "\n"
                    )
                    actual = (
                        _normalized_payload(
                            content,
                            git_sha=_git_sha(project_dir),
                        ).rstrip()
                        + "\n"
                    )
                    if expected != actual:
                        failures.append(
                            "\n".join(
                                difflib.unified_diff(
                                    expected.splitlines(),
                                    actual.splitlines(),
                                    fromfile=str(path),
                                    tofile=compiled_path,
                                    lineterm="",
                                )
                            )
                        )
            exit_code = ERROR if failures else OK
            if output == "json":
                _emit_json(
                    _json_envelope(
                        "test",
                        DiagnosticBag(),
                        exit_code=exit_code,
                        artifact_count=len(result.compiled),
                        data={"suite": "golden", "failures": failures},
                    ),
                    exit_code,
                )
            if failures:
                click.echo("golden suite failed:\n" + "\n\n".join(failures), err=True)
                raise click.exceptions.Exit(ERROR)
            click.echo(f"golden suite passed for {len(result.compiled)} artifact(s)")
            return
        if suite == "evals":
            if capture_baseline_requested and not reason:
                raise SstUsageError("--capture-baseline requires --reason")
            if reason and not capture_baseline_requested:
                raise SstUsageError("--reason requires --capture-baseline")
            evals = tuple(item for item in result.compiled if isinstance(item, CompiledEval))
            if not evals:
                raise ProjectError("no eval artifacts matched the project")
            compiled_manifest = ManifestFileStore(_target_dir(project_dir) / "manifest.json").read()
            if compiled_manifest is None:
                raise ProjectError("no SST manifest; run sst compile before evals")
            current_manifest = _build_manifest(project_dir, result, manifest_path)
            if compiled_manifest.manifest_id != current_manifest.manifest_id:
                raise ProjectError("compiled SST manifest is stale; run sst compile before evals")
            profile, port = _connect(project_dir, target_name)
            config = _config(project_dir)
            eval_defaults = _source(project_dir, target_name, manifest_path).load_evals().defaults
            eval_stage = _config_map(_config_map(config.get("apply")).get("eval_config_stage"))
            lifecycle_config = EvalLifecycleConfig(str(eval_stage.get("stage") or "EVAL_CONFIGS"))
            handler = EvalLifecycleHandler(port, lifecycle_config)
            state_store = StateFileStore(_target_dir(project_dir) / f"state.{profile.target_name}.json")
            eval_lock_id = f"eval-{SystemClock().new_run_id()}"
            locked, holder, _ = state_store.acquire_lock(eval_lock_id, break_stale=False)
            if not locked:
                port.close()
                raise ProjectError(f"cannot run evals while {holder or 'another operation'} holds the target lock")
            try:
                state, state_diagnostics = read_state(
                    state_store,
                    port,
                    state_table=profile.state_table,
                    target=profile.identity,
                )
                publication_diagnostics = validate_eval_publication(evals, current_manifest, state, handler)
                preflight = DiagnosticBag((*state_diagnostics, *publication_diagnostics))
                if preflight.has_errors:
                    eval_result = None
                    data = {"suite": "evals", **empty_eval_suite_json()}
                    exit_code = ERROR
                else:
                    config_digests = {
                        item.artifact_key: digest
                        for item in evals
                        if (entry := state.applied.get(item.artifact_key)) is not None
                        if (digest := dict(entry.component_fingerprints).get("config_stage_md5")) is not None
                    }
                    eval_result = RunEvalSuite(port, SystemClock(), lifecycle_config).run(
                        evals,
                        defaults=eval_defaults,
                        options=EvalRunOptions(_git_sha(project_dir)),
                        fail_fast=fail_fast,
                        config_digests=config_digests,
                        baseline_capture=capture_baseline_requested,
                    )
                    preflight = DiagnosticBag((*preflight, *eval_result.diagnostics))
                    eval_state_table = QualifiedName(
                        profile.state_table.database,
                        profile.state_table.schema,
                        Identifier.parse(f"{profile.state_table.name.folded}_EVALS"),
                    )
                    eval_store = SnowflakeEvalStateStore(port, eval_state_table)
                    gate_verdicts = []
                    captured_records = []
                    for item, item_result in zip(evals, eval_result.evals):
                        if capture_baseline_requested:
                            captured_baseline = capture_baseline(
                                item,
                                item_result,
                                reason=reason or "",
                                captured_at=SystemClock().now_iso(),
                                default_tier=eval_defaults.eval_tier,
                                required_attempts=(
                                    item.resolved.config.run.baseline_runs
                                    if item.resolved.config.run is not None
                                    and item.resolved.config.run.baseline_runs is not None
                                    else eval_defaults.baseline_runs
                                ),
                            )
                            captured_records.append(captured_baseline)
                            continue
                        stored_baseline = eval_store.read_baseline(profile.target_name, item.artifact_key)
                        verdict, gate_diagnostics = evaluate_gate(
                            item,
                            item_result,
                            stored_baseline,
                            now=SystemClock().now_iso(),
                            default_tier=eval_defaults.eval_tier,
                        )
                        preflight = DiagnosticBag((*preflight, *gate_diagnostics))
                        persist_gate(
                            eval_store,
                            profile.target_name,
                            item,
                            item_result,
                            verdict,
                            evaluated_at=SystemClock().now_iso(),
                        )
                        gate_verdicts.append(verdict)
                    if capture_baseline_requested:
                        eval_store.write_baselines(profile.target_name, tuple(captured_records))
                    captured = [record.eval_key for record in captured_records]
                    if capture_baseline_requested:
                        exit_code = OK if eval_result.success and not preflight.has_errors else ERROR
                    else:
                        score_regression_only = (
                            preflight.count(Severity.ERROR) == 0
                            and bool(gate_verdicts)
                            and all(
                                verdict.reason is None and verdict.regression_count > 0 for verdict in gate_verdicts
                            )
                        )
                        exit_code = (
                            OK if eval_result.success and (not preflight.has_errors or score_regression_only) else ERROR
                        )
                    gate_status = (
                        "captured"
                        if capture_baseline_requested
                        else (
                            "no_signal"
                            if any(verdict.reason is not None for verdict in gate_verdicts)
                            else "regressed" if any(verdict.regression_count for verdict in gate_verdicts) else "passed"
                        )
                    )
                    data = {
                        "suite": "evals",
                        **eval_suite_json(eval_result),
                        "captured_baselines": captured,
                        "regression_count": sum(verdict.regression_count for verdict in gate_verdicts),
                        "gate_verdict": gate_status,
                        "gate_reasons": [verdict.reason for verdict in gate_verdicts if verdict.reason is not None],
                        "regressions": [
                            {"question_key": regression.question_key, "metric_name": regression.metric_name}
                            for verdict in gate_verdicts
                            for regression in verdict.regressions
                        ],
                    }
            finally:
                state_store.release_lock(eval_lock_id)
                port.close()
            if output == "json":
                _emit_json(
                    _json_envelope(
                        "test",
                        preflight,
                        exit_code=exit_code,
                        artifact_count=len(evals),
                        data=data,
                    ),
                    exit_code,
                )
            _render_diagnostics(preflight)
            if eval_result is not None:
                _print_eval_results(eval_result)
            if exit_code:
                raise click.exceptions.Exit(exit_code)
            return
        compiled_manifest = ManifestFileStore(_target_dir(project_dir) / "manifest.json").read()
        if compiled_manifest is None:
            raise ProjectError("no SST manifest; run sst compile before smoke")
        current_manifest = _build_manifest(project_dir, result, manifest_path)
        if compiled_manifest.manifest_id != current_manifest.manifest_id:
            raise ProjectError("compiled SST manifest is stale; run sst compile before smoke")
        profile, port = _connect(project_dir, target_name)
        try:
            state_store = StateFileStore(_target_dir(project_dir) / f"state.{profile.target_name}.json")
            state, state_diagnostics = read_state(
                state_store,
                port,
                state_table=profile.state_table,
                target=profile.identity,
            )
            ownership_errors = list(state_diagnostics)
            if state.manifest_id != current_manifest.manifest_id:
                ownership_errors.append(
                    D(
                        "SST-APL012",
                        artifact="manifest",
                        value="authoritative state does not match the compiled manifest",
                    )
                )
            published = {
                artifact.key: artifact for artifact in result.rendered_for_publish(current_manifest.manifest_id)
            }
            for artifact in result.rendered:
                entry = state.applied.get(artifact.key)
                expected_marker = OwnershipMarker(
                    current_manifest.manifest_id,
                    artifact.fingerprint,
                )
                if (
                    entry is None
                    or entry.fingerprint != artifact.fingerprint
                    or entry.manifest_id != current_manifest.manifest_id
                    or QualifiedName.parse(entry.qualified_name).folded != artifact.target.folded
                    or port.describe_marker(artifact.target) != expected_marker
                ):
                    ownership_errors.append(
                        D(
                            "SST-APL012",
                            artifact=artifact.key,
                            value=artifact.target.sql,
                        )
                    )
            smoke = (
                dataclasses.replace(
                    RunSmokeSuite(port).run(()),
                    diagnostics=DiagnosticBag(tuple(ownership_errors)),
                )
                if ownership_errors
                else RunSmokeSuite(port).run(
                    tuple(published.values()),
                    fail_fast=fail_fast,
                )
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

    _guarded(action, command="test", output=output)


def main() -> None:
    try:
        cli(standalone_mode=True)
    except KeyboardInterrupt:
        raise SystemExit(INTERRUPTED)


if __name__ == "__main__":
    main()

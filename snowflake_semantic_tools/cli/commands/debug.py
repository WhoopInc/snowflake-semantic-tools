"""`sst debug`: what `sst` resolved -- configuration, profile, target, registry -- and the connection.

The connection is tested by default; `--no-connect` reports everything else offline. Every
configuration path discovery checked is reported, so which file configured a run is never a
guess. `--snowflake-signatures` reports instead how often Snowflake refused something SST did not
recognise, from the run log every command appends to.
"""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.agent_kit import read_lock
from snowflake_semantic_tools.adapters.dbt.invoke import DbtRunner, installation_type, subprocess_runner
from snowflake_semantic_tools.adapters.dbt.manifest import SUPPORTED_SCHEMA_VERSIONS, schema_version_number
from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget, load_profile_target
from snowflake_semantic_tools.adapters.dbt.project import target_path
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import read_json_file
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_mapping
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.options import target_option
from snowflake_semantic_tools.cli.output import register_secrets
from snowflake_semantic_tools.cli.run_log import signature_report
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.cli.wiring.project import open_connector, target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import DbtSettings, config_block, dbt_settings
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# How `dbt --version` is run: looked up on each call, so a test replaces it here.
dbt_runner: DbtRunner = subprocess_runner


@click.command(cls=SstCommand)
@target_option()
@click.option("--no-connect", is_flag=True)
@click.option("--snowflake-signatures", is_flag=True)
@command_body("debug", config=ConfigNeed.OPTIONAL)
def debug(
    paths: ProjectPaths,
    target_name: str | None,
    no_connect: bool,
    snowflake_signatures: bool,
    manifest_path: Path | None,
) -> CommandResult:
    """Show the resolved configuration, profile, target, and registry, and test the connection.

    Reports the SST version and, when SST runs dbt for the manifest, the `dbt` on `PATH` it
    runs and that dbt's version; every configuration path discovery checked; the registered
    artifact types; where the dbt manifest comes from (`--manifest`, the file on disk under
    `dbt.invoke: false`, or the `dbt.command` SST runs), its schema, and the schema versions
    `dbt.manifest_schema_versions` accepts; which SST installed the project's agent kit, and
    whether it is this one; and the resolved target: profile, role, warehouse,
    database, schema, state table, and how it authenticates, never a credential. The
    connection is then opened, and its role and account reported; with --no-connect nothing
    connects. --snowflake-signatures reports instead how often Snowflake refusals went
    unrecognised, from `target/sst/run_log.jsonl`, offline.

    Exit 0 when everything resolved, 1 when a resolved value is invalid, 4 when the configuration
    or profile cannot be, and 5 when the connection fails.

    Diagnostics:
        SST-CFG001: there is no configuration file.
        SST-DBT017: the dbt manifest on disk declares a schema the run does not accept.
        SST-PRT001: the connection could not be opened.
        Each code the configuration file's own checks report, at exit 1 when one is an error.
    """
    if snowflake_signatures:
        report = signature_report(target_dir(paths.project_dir))
        data: dict[str, object] = {"signatures": report}
        return CommandResult(data=data, human=lambda: _print_fields(report))
    settings = _dbt_settings(paths, target_name)
    manifest = _manifest(paths, manifest_path, settings)
    data = {
        "versions": _versions(paths, runs_dbt=manifest["runs_dbt"] is True),
        "config": _config(paths),
        "registry": sorted(SEMANTIC_REGISTRY.artifacts),
        "manifest": manifest,
        "agent_kit": _agent_kit(paths.project_dir),
    }
    if paths.config_file is None:
        missing = D("SST-CFG001", subject="config:discovery", path=str(paths.project_dir))
        return CommandResult(CONFIG, DiagnosticBag((missing,)), data, human=lambda: _print_fields(data))
    try:
        checked = resolved_config(paths, target_name).diagnostics
        state = config_block(resolved_config(paths, target_name).tree.get("state"))
        profile = load_profile_target(paths, target_name, state=state)
    except ProjectError as exc:
        return CommandResult(CONFIG, DiagnosticBag(exc.diagnostics), data, human=lambda: _print_fields(data))
    # As every connecting command does, so no output of this run can echo a credential.
    register_secrets(profile.secrets)
    data["target"] = _target(profile, paths)
    invalid = (*checked, *_unsupported_manifest(manifest, allowed=paths.allow_unsupported_manifest_schema))
    diagnostics = DiagnosticBag((*invalid, *profile.diagnostics, *profile.connection_warnings))
    exit_code = ERROR if diagnostics.has_errors else OK
    if no_connect:
        data["connection"] = {"tested": False}
    else:
        data["connection"], failure = _connection(profile)
        if failure is not None:
            exit_code = CONNECTION
            diagnostics = DiagnosticBag((*diagnostics, failure))
    return CommandResult(exit_code, diagnostics, data, human=lambda: _print_fields(data))


def _dbt_settings(paths: ProjectPaths, target_name: str | None) -> DbtSettings:
    """Return the `dbt:` block the run resolves; its defaults when no configuration resolves."""
    if paths.config_file is None:
        return DbtSettings()
    try:
        return dbt_settings(resolved_config(paths, target_name).tree)
    except ProjectError:
        # Reported, with its exit code, once the configuration is resolved below.
        return DbtSettings()


def _versions(paths: ProjectPaths, *, runs_dbt: bool) -> dict[str, object]:
    """Report SST's version and, when SST runs dbt, which `dbt` on `PATH`, its version and installation.

    The installation is the kind `installation_type` names -- `core`, `cloud`, `v2` or `v2-oss` --
    and None when dbt names none SST knows.
    """
    if not runs_dbt:
        return {"sst": VERSION, "dbt": None, "dbt_installation": None, "dbt_path": None}
    version, installation = _dbt_version(paths.project_dir)
    return {"sst": VERSION, "dbt": version, "dbt_installation": installation, "dbt_path": shutil.which("dbt")}


def _dbt_version(project_dir: Path) -> tuple[str | None, str | None]:
    """Return the version `dbt --version` reports, as SST runs it, and the installation it names.

    Either is None when dbt cannot say.
    """
    try:
        completed = dbt_runner(("dbt", "--version"), project_dir)
    except OSError:
        return None, None
    output = f"{completed.stdout}\n{completed.stderr}"
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if completed.returncode != 0 or not lines:
        return None, None
    installation = installation_type(output)
    if installation in ("v2", "v2-oss"):
        # dbt v2 prints one line, `dbt 2.0.8`.
        return lines[0].split()[1], installation
    for line in lines:
        # dbt-core prints `- installed: 1.8.0`; the dbt Cloud CLI prints `dbt Cloud CLI - 0.38.6`.
        if "installed:" in line:
            return line.split("installed:", 1)[1].split()[0], installation
        if "cloud cli" in line.casefold() and " - " in line:
            return line.rsplit(" - ", 1)[1].split()[0], installation
    return lines[0], installation


def _agent_kit(project_dir: Path) -> dict[str, object]:
    """Report which SST installed the project's agent kit, and whether that is this one.

    A teammate on another SST sees the kit was written for it; `sst agent-kit status` compares
    the files themselves. An unreadable lock is reported here, never as the run's failure.
    """
    try:
        lock = read_lock(project_dir)
    except ProjectError as exc:
        return {"installed_by": None, "agents": [], "matches_sst": None, "error": str(exc)}
    agents = [agent.value for agent in lock.agents]
    return {
        "installed_by": lock.sst_version or None,
        "agents": agents,
        "matches_sst": lock.sst_version == VERSION if agents else None,
    }


def _config(paths: ProjectPaths) -> dict[str, object]:
    return {
        "file": str(paths.config_file) if paths.config_file is not None else None,
        "candidates": [{"path": str(path), "exists": path.is_file()} for path in paths.candidates],
        "shadowed": [str(path) for path in paths.shadowed],
        "profiles_candidates": [str(path) for path in paths.profiles_candidates()],
    }


def _manifest(paths: ProjectPaths, manifest_path: Path | None, settings: DbtSettings) -> dict[str, object]:
    """Report where the dbt manifest comes from, the schemas accepted, and the one on disk.

    The path is the one SST reads: `--manifest`, else the one dbt writes under
    `dbt_project.yml`'s `target-path`.
    """
    runs_dbt = manifest_path is None and settings.invoke
    if manifest_path is not None:
        source = "--manifest"
    else:
        source = f"dbt {settings.command}" if settings.invoke else "on disk (dbt.invoke: false)"
    accepted = settings.manifest_schema_versions or SUPPORTED_SCHEMA_VERSIONS
    path = manifest_path or _target_manifest(paths.project_dir)
    schema = None
    if path.is_file():
        try:
            document = read_json_file(path)
            metadata = document.get("metadata", {}) if isinstance(document, dict) else None
            schema = metadata.get("dbt_schema_version") if isinstance(metadata, dict) else None
        except (OSError, ValueError):
            schema = None
    return {
        "source": source,
        "runs_dbt": runs_dbt,
        "path": str(path),
        "schema_version": schema,
        "accepted_schema_versions": [f"v{number}" for number in sorted(accepted)],
        "supported": schema_version_number(schema) in accepted if schema is not None else None,
    }


def _target_manifest(project_dir: Path) -> Path:
    """Return the manifest dbt writes for the project; `target/manifest.json` when that cannot be read."""
    try:
        return target_path(project_dir, read_yaml_mapping)
    except ProjectError:
        return project_dir / "target" / "manifest.json"


def _unsupported_manifest(manifest: Mapping[str, object], *, allowed: bool) -> tuple[Diagnostic, ...]:
    """Report a dbt manifest on disk whose schema the run refuses to read, unless allowed."""
    if manifest["supported"] is not False or allowed:
        return ()
    accepted = manifest["accepted_schema_versions"]
    expected = ", ".join(str(item) for item in accepted) if isinstance(accepted, list) else ""
    return (D("SST-DBT017", found=str(manifest["schema_version"]), expected=expected),)


def _target(profile: ProfileTarget, paths: ProjectPaths) -> dict[str, object]:
    return {
        "profile": profile.profile_name,
        "name": profile.target_name,
        "profiles_file": str(paths.profiles_file()),
        "role": profile.identity.role,
        "warehouse": profile.identity.warehouse,
        "database": profile.identity.database.sql,
        "schema": profile.identity.schema.sql,
        "state_table": profile.state_table.sql,
        "authentication": profile.authentication,
    }


def _connection(profile: ProfileTarget) -> tuple[dict[str, object], Diagnostic | None]:
    """Open a session and report its role and account; on failure, report why instead.

    Diagnostics:
        SST-PRT001: the connection could not be opened.
    """
    try:
        port = open_connector(profile.connection_params)
    except SnowflakePortError as exc:
        failure = exc.diagnostic or D(
            "SST-PRT001", subject="profile:" + profile.target_name, value=profile.target_name, detail=str(exc)
        )
        return {"tested": True, "ok": False, "error": str(exc)}, failure
    try:
        return {
            "tested": True,
            "ok": True,
            "role": port.current_role(),
            "account": port.current_account_locator(),
        }, None
    finally:
        port.close()


def _print_fields(data: Mapping[str, object], indent: str = "") -> None:
    for key, value in data.items():
        if isinstance(value, Mapping):
            click.echo(f"{indent}{key}:")
            _print_fields(value, indent + "  ")
        elif isinstance(value, list):
            click.echo(f"{indent}{key}:")
            for item in value:
                click.echo(f"{indent}  - {item}")
        else:
            click.echo(f"{indent}{key}: {value}")

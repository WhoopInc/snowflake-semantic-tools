"""`sst debug`: what `sst` resolved -- configuration, profile, target, registry -- and the connection.

The connection is tested by default; `--no-connect` reports everything else offline. Every
configuration path discovery checked is reported, so which file configured a run is never a
guess. `--snowflake-signatures` reports instead how often Snowflake refused something SST did not
recognise, from the run log every command appends to.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import click

from snowflake_semantic_tools._version import __version__ as VERSION
from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget, load_profile_target
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.cli.exit_codes import CONFIG, CONNECTION, OK
from snowflake_semantic_tools.cli.options import target_option
from snowflake_semantic_tools.cli.run_log import signature_report
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.cli.wiring.project import open_connector, target_dir
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# The dbt manifest schema this release reads; any other is refused unless allowed for one run.
_SUPPORTED_MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"


@click.command()
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

    Exit 0 when everything resolved, 4 when the configuration or profile cannot be, and 5 when
    the connection fails.
    """
    if snowflake_signatures:
        report = signature_report(target_dir(paths.project_dir))
        data: dict[str, object] = {"signatures": report}
        return CommandResult(data=data, human=lambda: _print_fields(report))
    data = {
        "versions": _versions(),
        "config": _config(paths),
        "registry": sorted(SEMANTIC_REGISTRY.artifacts),
        "manifest": _manifest(paths, manifest_path),
    }
    if paths.config_file is None:
        missing = D("SST-CFG001", subject="config:discovery", path=str(paths.project_dir))
        return CommandResult(CONFIG, DiagnosticBag((missing,)), data, human=lambda: _print_fields(data))
    try:
        profile = load_profile_target(paths, target_name)
    except ProjectError as exc:
        return CommandResult(CONFIG, DiagnosticBag(exc.diagnostics), data, human=lambda: _print_fields(data))
    data["target"] = _target(profile, paths)
    diagnostics = DiagnosticBag((*profile.diagnostics, *profile.connection_warnings))
    exit_code = OK
    if no_connect:
        data["connection"] = {"tested": False}
    else:
        data["connection"], failure = _connection(profile)
        if failure is not None:
            exit_code = CONNECTION
            diagnostics = DiagnosticBag((*diagnostics, failure))
    return CommandResult(exit_code, diagnostics, data, human=lambda: _print_fields(data))


def _versions() -> dict[str, object]:
    try:
        dbt = version("dbt-core")
    except PackageNotFoundError:
        dbt = None
    return {"sst": VERSION, "dbt": dbt}


def _config(paths: ProjectPaths) -> dict[str, object]:
    return {
        "file": str(paths.config_file) if paths.config_file is not None else None,
        "candidates": [{"path": str(path), "exists": path.is_file()} for path in paths.candidates],
        "shadowed": [str(path) for path in paths.shadowed],
        "profiles_candidates": [str(path) for path in paths.profiles_candidates()],
    }


def _manifest(paths: ProjectPaths, manifest_path: Path | None) -> dict[str, object]:
    """Report where the dbt manifest comes from and, when one is on disk, its schema version."""
    path = manifest_path or paths.project_dir / "target" / "manifest.json"
    schema = None
    if path.is_file():
        try:
            metadata = json.loads(path.read_text(encoding="utf-8")).get("metadata", {})
            schema = metadata.get("dbt_schema_version") if isinstance(metadata, dict) else None
        except (OSError, ValueError, AttributeError):
            schema = None
    return {
        "source": "--manifest" if manifest_path is not None else "dbt parse",
        "path": str(path),
        "schema_version": schema,
        "supported": schema == _SUPPORTED_MANIFEST_SCHEMA if schema is not None else None,
    }


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

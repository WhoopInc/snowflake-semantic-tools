"""`sst import`: write live Snowflake objects as project files, one, a schema's, or a database's.

Import reads, maps, and reports; with `--write` it creates the files, never replacing one. It
writes nothing to Snowflake and records nothing in state: `sst state adopt` hands an imported
object to SST once its files compile to what is live. Run on a terminal with the type or the
name left out, it asks for each in turn and offers to write what it found.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import NoReturn

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app.importing import (
    SUPPORTED_TYPES,
    ImportObjects,
    ImportReport,
    ImportRequest,
    ImportScope,
    artifact_directories,
)
from snowflake_semantic_tools.cli import confirm as asking
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.globals import GlobalOptions, SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.import_wizard import Wizard
from snowflake_semantic_tools.cli.options import target_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring.importing import import_setup, write_imported, write_model_edits
from snowflake_semantic_tools.cli.wiring.project import closed_on_error, connect
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.importing.model import ImportedObject


def _refuse(
    artifact_type: str | None,
    scope: str | None,
    like: str | None,
    not_like: str | None,
    to_folder: str | None,
    agent_version: str | None,
    define: bool,
) -> None:
    """Refuse a command line import does not accept, before anything is resolved; exit 3.

    Diagnostics:
        SST-PRT100: an unknown type, a name that is not one to three parts, a pattern beside one
            object's name, a version for a type that has none, a `--to` outside the project, or
            a type or name left out with no terminal to ask for it on; raised.
    """
    if artifact_type is not None and artifact_type not in SUPPORTED_TYPES:
        _usage(f"sst import reads {', '.join(SUPPORTED_TYPES)}; '{artifact_type}' is not one of them")
    if scope is not None:
        _refuse_scope(artifact_type, scope, like, not_like)
    if agent_version is not None and artifact_type not in (None, "agent", "cowork_profile"):
        _usage("--agent-version picks the version of an agent or CoWork profile")
    if artifact_type == "eval" and to_folder is None:
        _usage("sst import eval needs --to, the agent folder its dataset goes in, such as agents/sales/my_agent")
    if define and artifact_type not in (None, "tool"):
        _usage("--define makes an imported tool member define its object; it takes no other type")
    if to_folder is not None:
        path = PurePosixPath(to_folder)
        if path.is_absolute() or ".." in path.parts:
            _usage(f"--to names a folder inside the project; '{to_folder}' is not one")
    if (artifact_type is None or scope is None) and not asking.can_ask():
        _usage("sst import needs <TYPE> and <DB>[.<SCHEMA>[.<NAME>]] when there is no terminal to ask for them on")


def _refuse_scope(artifact_type: str | None, scope: str, like: str | None, not_like: str | None) -> None:
    """Refuse a scope the type does not take: a stage path is a skill's, and an eval reads one table."""
    if scope.startswith("@"):
        if artifact_type not in (None, "skill"):
            _usage("a stage path holds a skill's folder; import it as `sst import skill @DB.SCHEMA.STAGE/path/`")
        return
    try:
        parsed = ImportScope.parse(scope)
    except ValueError:
        _usage(f"sst import takes <DB>, <DB>.<SCHEMA> or <DB>.<SCHEMA>.<NAME>; '{scope}' is none of them")
    if parsed.qualified is not None and (like or not_like):
        _usage("--like and --not-like narrow a schema or a database, not one named object")
    if artifact_type == "eval" and parsed.qualified is None:
        _usage("sst import eval reads one eval input table: <DB>.<SCHEMA>.<TABLE>")


@click.command(cls=SstCommand, name="import")
@click.argument("artifact_type", metavar="<TYPE>", required=False)
@click.argument("scope", metavar="<DB>[.<SCHEMA>[.<NAME>]]", required=False)
@click.option("--like")
@click.option("--not-like", "not_like")
@click.option("--to", "to_folder")
@click.option("--agent-version", "agent_version")
@click.option("--with-dependencies", is_flag=True)
@click.option("--define", is_flag=True)
@click.option("--write", "write_files", is_flag=True)
@target_option()
@click.option("--profile", "profile_name")
@command_body("import", refusals=_refuse)
def import_(
    paths: ProjectPaths,
    options: GlobalOptions,
    manifest_path: Path | None,
    artifact_type: str | None,
    scope: str | None,
    like: str | None,
    not_like: str | None,
    to_folder: str | None,
    agent_version: str | None,
    with_dependencies: bool,
    define: bool,
    write_files: bool,
    target_name: str | None,
    profile_name: str | None,
) -> CommandResult:
    """Write live Snowflake objects as project files: one object, a schema's, or a database's.

    <TYPE> is semantic_view, tool, skill, plugin, agent or cowork_profile. A three-part name
    imports one object; a two-part name every object of the type in that schema; one part, every
    one in the database.
    --like and --not-like narrow a schema or database by Snowflake LIKE patterns; a skill can
    also be read from a stage folder, `@DB.SCHEMA.STAGE/path/`, as a STAGE skill source names it.
    Each object
    goes in the folder whose routes publish it where it is now on --target, so its files
    compile back to the same name; --to names the folder below the type's directory
    instead. What the objects name is mapped to what the project publishes, and anything
    the project does not compile is reported; --with-dependencies imports it too. A tool member
    references its object, which SST then calls and never changes; --define makes it define the
    object instead, which SST then publishes. Without
    --write, import only reports; it never replaces a file, never writes to Snowflake, and
    records nothing in state. Leave out the type or the name on a terminal and import asks for
    each, then offers to write what it found. Grants are yours: a read the role is refused is
    reported, and the other objects still import.

    Exit 0 when every object was imported or there was nothing to import; 2 when files are
    pending and --write was not given; 1 when an object could not be read, mapped, or
    written; 3 when the command line is refused; 4 when the project or the target cannot be
    read; 5 when Snowflake cannot be reached.

    Diagnostics:
        SST-PRT100: the command line is refused; raised.
        SST-IMP001: the object or scope is not found, or the role cannot see it.
        SST-IMP002: one object named alone is of the other agent kind.
        SST-IMP003: the object carries SST's ownership marker; it is not imported.
        SST-IMP004: a file the import would write already exists.
        SST-IMP005: the role is refused a read of the object.
        SST-IMP006: the scope holds no object of the type.
        SST-IMP007: the project publishes the object already; it is not imported.
        SST-IMP012: an object names one the project does not compile.
        SST-IMP013: what the files publish differs from the live object.
        SST-IMP014: no folder routes to the object's location; its file names it.
    """
    profile, port = connect(paths, target_name, profile=profile_name)
    asked = artifact_type is None or scope is None
    with closed_on_error(port):
        only: tuple[str, ...] = ()
        if asked:
            answers = Wizard(port, profile.identity).ask(
                artifact_type, scope, with_dependencies=with_dependencies, to_folder=to_folder
            )
            artifact_type, scope, only = answers.artifact_type, answers.scope, answers.only
            with_dependencies, to_folder = answers.with_dependencies, answers.to_folder
        if artifact_type is None or scope is None:
            _usage("sst import needs <TYPE> and <DB>[.<SCHEMA>[.<NAME>]]")
        config = resolved_config(paths, profile.target_name).tree
        staged = scope.startswith("@")
        request = ImportRequest(
            artifact_type,
            ImportScope.parse(scope.removeprefix("@").split("/", 1)[0]) if staged else ImportScope.parse(scope),
            like=like,
            exclude=not_like,
            version=agent_version,
            with_dependencies=with_dependencies,
            to_folder=_folder(to_folder, artifact_directories(config)[artifact_type]),
            only=only,
            define=define,
            stage_path=scope if staged else None,
        )
        setup = import_setup(paths, profile, port, manifest_path)
        report = ImportObjects(port, setup).run(request)
    port.close()
    interactive = asked and options.output not in ("json", "agent")
    return _result(paths, profile.target_name, request, report, profile.connection_warnings, write_files, interactive)


def _result(
    paths: ProjectPaths,
    target_name: str,
    request: ImportRequest,
    report: ImportReport,
    warnings: Sequence[Diagnostic],
    write_files: bool,
    interactive: bool,
) -> CommandResult:
    """Write the files and model edits when asked to, on the command line or by the wizard, then report."""
    write = write_files
    offered = interactive and report.pending and not write_files
    if offered:
        _print(report, written=(), edited=())
        count = len(report.files) + len({edit.patch_file for edit in report.model_edits})
        answer = asking.ask(f"Write {count} file(s)? [y/N]", "n")
        write = answer.strip().casefold() in ("y", "yes")
    written = write_imported(paths.project_dir, report.files) if write else ()
    edited = write_model_edits(paths.project_dir, report.model_edits) if write else ()
    diagnostics = DiagnosticBag(
        (*warnings, *report.diagnostics, *(item for obj in report.objects for item in obj.diagnostics))
    )
    if report.has_errors:
        exit_code = ERROR
    elif report.pending and not write:
        exit_code = CHANGES
    else:
        exit_code = OK
    data = {
        "target": target_name,
        "type": request.artifact_type,
        "scope": request.scope.sql,
        "objects": [_object(item, write) for item in report.objects],
        "skipped": [name.sql for name in report.skipped],
        "written": list(written),
        "edited": list(edited),
    }
    return CommandResult(
        exit_code,
        diagnostics,
        data,
        human=(lambda: None) if offered and not write else (lambda: _print(report, written=written, edited=edited)),
        headline=_headline(report, written, edited),
    )


def _folder(to_folder: str | None, root: str) -> tuple[str, ...] | None:
    """Return `--to` as a folder below the type's directory: given inside it, or relative to it."""
    if to_folder is None:
        return None
    parts = PurePosixPath(to_folder).parts
    root_parts = PurePosixPath(root).parts
    if parts[: len(root_parts)] == root_parts:
        parts = parts[len(root_parts) :]
    return tuple(part for part in parts if part not in ("", "."))


def _status(item: ImportedObject, written: bool) -> str:
    if item.refused:
        return "refused"
    if not item.files:
        return "not imported"
    return "written" if written else "would write"


def _object(item: ImportedObject, written: bool) -> dict[str, object]:
    return {
        "type": item.artifact_type,
        "object": item.shown,
        "artifact": item.artifact_key,
        "status": _status(item, written),
        "files": [file.path for file in item.files],
        "model_edits": [
            {
                "model": edit.model,
                "file": edit.patch_file,
                "keys": [key for key, _ in edit.meta] + (["description"] if edit.description else []),
                "columns": [column.column for column in edit.columns],
            }
            for edit in item.model_edits
        ],
        "dependencies": [
            {"type": dependency.artifact_type, "name": dependency.name} for dependency in item.dependencies
        ],
    }


def _headline(report: ImportReport, written: Sequence[str], edited: Sequence[str]) -> str:
    imported = sum(1 for item in report.imported if item.files)
    if written or edited:
        return f"imported {imported} object(s); wrote {len(written)} file(s), edited {len(edited)} model file(s)"
    if report.pending:
        edits = len({edit.patch_file for edit in report.model_edits})
        pending = f"{len(report.files)} file(s) and {edits} model file(s)" if edits else f"{len(report.files)} file(s)"
        return f"{imported} object(s) to import; {pending} pending, written with --write"
    return "nothing to import"


def _print(report: ImportReport, *, written: Sequence[str], edited: Sequence[str]) -> None:
    done = set(written) | set(edited)
    for item in report.objects:
        click.echo(f"{_status(item, bool(done))}: {item.artifact_type} {item.shown}")
        if item.refused:
            continue
        for file in item.files:
            click.echo(f"  {'wrote' if file.path in done else 'would write'} {file.path}")
        for edit in item.model_edits:
            columns = f", {len(edit.columns)} column(s)" if edit.columns else ""
            verb = "edited" if edit.patch_file in done else "would edit"
            click.echo(f"  {verb} {edit.patch_file} (model {edit.model}{columns})")
    if report.skipped:
        click.echo(f"skipped {len(report.skipped)} object(s) of the other agent kind")


def _usage(detail: str) -> NoReturn:
    diagnostic: Diagnostic = D("SST-PRT100", subject="cli", detail=detail)
    raise SstUsageError(diagnostic.message, diagnostic=diagnostic)

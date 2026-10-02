"""`sst enrich`: fill dbt model column metadata from the warehouse, editing model YAML in place."""

from __future__ import annotations

import difflib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.app.enrich import EditedFile, EnrichReport, EnrichRequest, ModelReport
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import (
    Decorator,
    fail_fast_option,
    output_option,
    project_options,
    selection_options,
    stacked,
)
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring.enrich import (
    LazyEnrichPort,
    enrich_project,
    model_selectors,
    project_paths,
    relation_part,
)
from snowflake_semantic_tools.domain.enrich import (
    COMPONENT_NAMES,
    Component,
    EnrichOptions,
    parse_components,
    resolve_options,
)

# Each 0.3 flag `sst enrich` no longer takes, and what to pass instead.
_REMOVED_FLAGS: tuple[tuple[tuple[str, ...], bool, str], ...] = (
    (("--models", "-m"), True, "pass --select model:<name> once per model"),
    (("-t",), True, "use --target"),
    (("-d",), True, "use --database"),
    (("-s",), True, "use --schema"),
    (("--allow-non-prod",), False, "enrich reads the manifest of the target you pass; check --target"),
    (("--column-types", "-ct"), False, "use --include column-types"),
    (("--data-types", "-dt"), False, "use --include data-types"),
    (("--sample-values", "-sv"), False, "use --include sample-values"),
    (("--detect-enums", "-de"), False, "use --include enums"),
    (("--table-synonyms", "-ts"), False, "use --include table-synonyms"),
    (("--column-synonyms", "-cs"), False, "use --include column-synonyms"),
    (("--synonyms", "-syn"), False, "use --include synonyms"),
    (("--all",), False, "use --include all"),
    (("--force-synonyms",), False, "use --force synonyms"),
    (("--force-column-types",), False, "use --force column-types"),
    (("--force-data-types",), False, "use --force data-types"),
    (("--force-all",), False, "use --force all"),
    (("--include-sources",), False, "SST 1.0 enriches dbt models only"),
    (("--sources-only",), False, "SST 1.0 enriches dbt models only"),
    (("--source",), True, "SST 1.0 enriches dbt models only"),
    (("--verbose", "-v"), False, "every change is reported; --output json gives the full report"),
)


def _refuse(replacement: str) -> Callable[[click.Context, click.Parameter, Any], None]:
    def callback(ctx: click.Context, param: click.Parameter, value: Any) -> None:
        if value:
            raise SstUsageError(f"{'/'.join(param.opts)} was removed in SST 1.0; {replacement}")

    return callback


def _removed_flags() -> Decorator:
    """Hidden options for every removed 0.3 flag, each refused with what replaces it."""
    return stacked(
        *(
            click.option(
                *names,
                is_flag=not takes_value,
                expose_value=False,
                hidden=True,
                callback=_refuse(replacement),
            )
            for names, takes_value, replacement in _REMOVED_FLAGS
        )
    )


def _components(values: tuple[str, ...], flag: str) -> frozenset[Component]:
    components, unknown = parse_components(values)
    if unknown:
        raise SstUsageError(f"{flag}: unknown component {unknown[0]!r}; choose from {', '.join(COMPONENT_NAMES)}")
    return components


@click.command()
@click.argument("paths", nargs=-1, type=click.Path(path_type=Path))
@project_options()
@selection_options()
@click.option("--include", "included", multiple=True, metavar="COMPONENTS")
@click.option("--force", "forced", multiple=True, metavar="COMPONENTS")
@click.option("--database")
@click.option("--schema")
@click.option("--check", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--no-detailed-exitcode", is_flag=True)
@fail_fast_option()
@_removed_flags()
@output_option()
@command_body("enrich")
def enrich(
    paths: tuple[Path, ...],
    project_dir: Path,
    target_name: str | None,
    manifest_path: Path | None,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
    included: tuple[str, ...],
    forced: tuple[str, ...],
    database: str | None,
    schema: str | None,
    check: bool,
    dry_run: bool,
    no_detailed_exitcode: bool,
    fail_fast: bool,
    output: str,
) -> CommandResult:
    """Fill dbt model column metadata from the warehouse, editing the model YAML in place.

    Reads each selected model's relation for its columns and types, and fills what the
    model YAML leaves out: column types and data types by default, and sample values,
    enums, and synonyms with --include. Values already written are kept unless --force
    names their component. PATH selects the models whose SQL or YAML file is under it.

    Exit 0 when done, 1 when a model failed, and 2 under --check when files would change.
    """
    options = resolve_options(_components(included, "--include"), _components(forced, "--force"))
    request = EnrichRequest(
        paths=project_paths(project_dir, paths),
        selected=model_selectors(selected, "--select"),
        excluded=model_selectors(excluded, "--exclude"),
        options=options,
        database=relation_part(database, "--database"),
        schema=relation_part(schema, "--schema"),
        fail_fast=fail_fast,
    )
    port = LazyEnrichPort(project_dir, target_name)
    try:
        project = enrich_project(project_dir, target_name, manifest_path, port)
        report = project.run(request)
        if not report.diagnostics.has_errors and report.selected == 0 and not report.stopped:
            raise ProjectError(f"no dbt model of the project in {project_dir} matched the selection")
        written = () if check or dry_run else project.write(report)
    finally:
        port.close()
    exit_code = _exit_code(report, check=check, detailed=not no_detailed_exitcode)
    return CommandResult(
        exit_code,
        report.diagnostics,
        data=_report_data(report, options, written),
        human=lambda: _print_report(report, written, dry_run=dry_run),
    )


def _exit_code(report: EnrichReport, *, check: bool, detailed: bool) -> int:
    if report.diagnostics.has_errors:
        return ERROR
    return CHANGES if check and detailed and report.changed else OK


def _model_data(item: ModelReport) -> dict[str, object]:
    enrichment = item.enrichment
    filled = {component.value: enrichment.filled(component) for component in Component} if enrichment else {}
    return {
        "model": item.model,
        "path": item.path,
        "status": "failed" if enrichment is None else "enriched" if enrichment.updates else "unchanged",
        "added": list(enrichment.added) if enrichment else [],
        "filled": {name: count for name, count in filled.items() if count},
        "table_synonyms": [edit.view for edit in item.table_synonyms],
    }


def _report_data(report: EnrichReport, options: EnrichOptions, written: tuple[str, ...]) -> dict[str, object]:
    return {
        "components": [component.value for component in options.ordered],
        "forced": [component.value for component in Component if options.forces(component)],
        "stopped": report.stopped,
        "written": list(written),
        "models": [_model_data(item) for item in report.models],
        "files": [
            {
                "path": item.path,
                "changed": item.changed,
                "created": item.before is None,
                "reformatted": item.reformatted,
            }
            for item in report.files
        ],
    }


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _model_line(item: ModelReport) -> str:
    enrichment = item.enrichment
    if enrichment is None:
        return f"{item.model}: failed"
    parts = [f"{component.value} {count}" for component in Component if (count := enrichment.filled(component))]
    if enrichment.added:
        parts.insert(0, f"{_plural(len(enrichment.added), 'column')} added")
    if item.table_synonyms:
        parts.append(f"table synonyms in {_plural(len(item.table_synonyms), 'view')}")
    return f"{item.model}: {', '.join(parts) or 'unchanged'}"


def _diff(item: EditedFile) -> str:
    before = (item.before or "").splitlines(keepends=True)
    origin = f"a/{item.path}" if item.before is not None else "/dev/null"
    return "".join(difflib.unified_diff(before, item.after.splitlines(keepends=True), origin, f"b/{item.path}"))


def _print_report(report: EnrichReport, written: tuple[str, ...], *, dry_run: bool) -> None:
    """Print each model's line, then each changed file, or its diff under --dry-run, then a summary."""
    for item in report.models:
        click.echo(_model_line(item))
    for edited in report.changed:
        if dry_run:
            click.echo(_diff(edited), nl=False)
        else:
            click.echo(f"{'wrote' if edited.path in written else 'would write'} {edited.path}")
    if report.stopped:
        click.echo("stopped at the first failure; nothing was written")
    failed = sum(1 for item in report.models if item.failed)
    files = _plural(len(written) if written else len(report.changed), "file")
    click.echo(
        f"{files} {'written' if written else 'to change'}; {_plural(len(report.models), 'model')} read, {failed} failed"
    )

"""`sst migrate refs` and `sst migrate split`: rewrite an earlier project into the 1.0 dialect."""

from __future__ import annotations

import click

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.discover import YAML_SUFFIXES
from snowflake_semantic_tools.adapters.yaml.migrate import (
    agent_files,
    create_file,
    files_under,
    filter_sites,
    hook_files,
    remove_file,
    semantic_files,
    uses_crlf,
    write_file,
)
from snowflake_semantic_tools.adapters.yaml.outline import outline
from snowflake_semantic_tools.app.migrate_refs import MigrateRefs, MigrationReport, RefsFiles
from snowflake_semantic_tools.app.migrate_split import MigrateSplit, SplitFiles, SplitReport
from snowflake_semantic_tools.cli.exit_codes import CHANGES, ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.help_form import SstSubGroup
from snowflake_semantic_tools.cli.runner import CommandResult, WriteFailure, command_body
from snowflake_semantic_tools.cli.settings import project_config, semantic_models_dir
from snowflake_semantic_tools.domain.model.config_schema import configured_dir

_MARKDOWN = frozenset((".md",))


@click.group(cls=SstSubGroup)
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """Rewrite an earlier project into the 1.0 dialect: `split` its lists, then rewrite its `refs`.

    The two commands are independent and either may run first; each is a dry run until --write.
    """
    inherited = dict(ctx.default_map or {})
    ctx.default_map = {name: dict(inherited) for name in ("refs", "split")}


def _dir(paths: ProjectPaths, key: str, default: str) -> str:
    return configured_dir(project_config(paths), key, default)


@migrate.command(cls=SstCommand, name="refs")
@click.option("--write", "write_files", is_flag=True)
@command_body("migrate refs")
def migrate_refs_command(paths: ProjectPaths, write_files: bool) -> CommandResult:
    """Rewrite references as template calls, move old settings under config:, and label filters.

    Reads every YAML file under `project.semantic_models_dir` and `project.tools_dir`, each agent
    file and eval file under `project.agents_dir`, each hook manifest under `project.hooks_dir`,
    each command under `project.commands_dir`, and the configuration file's `tool()` calls.
    Dry-run by default: exit 2 when rewrites are pending, 0 when there are none, and 1 when
    something sits where no rewrite is safe, each such place named on stderr. With --write the
    files are rewritten, and the command exits 0, or 1 while such a place remains.
    """
    project_dir = paths.project_dir
    agents, evals = agent_files(project_dir, _dir(paths, "agents_dir", "agents"))
    config_file = paths.config_file
    inside = config_file is not None and config_file.is_relative_to(project_dir)
    files = RefsFiles(
        semantic=semantic_files(project_dir, semantic_models_dir(paths)),
        tools=files_under(project_dir, _dir(paths, "tools_dir", "tools"), YAML_SUFFIXES),
        agents=agents,
        evals=evals,
        hooks=hook_files(project_dir, _dir(paths, "hooks_dir", "hooks")),
        commands=files_under(project_dir, _dir(paths, "commands_dir", "commands"), _MARKDOWN),
        config={paths.config_name: config_file.read_text(encoding="utf-8")}
        if config_file is not None and inside
        else {},
    )
    report = MigrateRefs(files, filter_sites, outline).run()
    if write_files:
        for item in report.changed:
            try:
                write_file(project_dir, item.path, item.result.text)
            except OSError as exc:
                raise WriteFailure(project_dir / item.path, exc) from exc
    if report.untouched:
        exit_code = ERROR
    elif report.changed and not write_files:
        exit_code = CHANGES
    else:
        exit_code = OK
    return CommandResult(
        exit_code,
        data=_report_data(report, write_files),
        human=lambda: _print_report(report, write_files),
    )


@migrate.command(cls=SstCommand, name="split")
@click.option("--write", "write_files", is_flag=True)
@command_body("migrate split")
def migrate_split_command(paths: ProjectPaths, write_files: bool) -> CommandResult:
    """Write each listed member to a file of its own, tool group members to their group's folder.

    Every member, a view too, stays in the folder of the file that listed it.

    Reads every YAML file under `project.semantic_models_dir` and `project.tools_dir`, and the
    configuration file, whose `skills.catalog`, `skills.stage` and `skills.extensions` move to
    the `skills:` root and the `plugins:`, `coco_desktop_profiles:` and `extensions:` blocks. A file that
    would overwrite one already there is left whole and named on stderr. Dry-run by default:
    exit 2 when a split is pending, 0 when there is none, and 1 when a file is left whole or a
    configuration key needs a hand edit. With --write the files are written, then the lists
    they came from removed, and the command exits 0, or 1 while such a file remains.
    """
    project_dir = paths.project_dir
    tools_dir = _dir(paths, "tools_dir", "tools")
    plugins = files_under(project_dir, _dir(paths, "plugins_dir", "plugins"), YAML_SUFFIXES)
    config_file = paths.config_file
    semantic_dir = semantic_models_dir(paths)
    files = SplitFiles(
        semantic=semantic_files(project_dir, semantic_dir),
        tools=files_under(project_dir, tools_dir, YAML_SUFFIXES),
        tools_dir=tools_dir,
        config=(paths.config_name, config_file.read_text(encoding="utf-8")) if config_file is not None else None,
        has_plugins=any(path.rsplit("/", 1)[-1].startswith("plugin.") for path in plugins),
    )
    report = MigrateSplit(files, outline, lambda path: (project_dir / path).exists()).run()
    if write_files:
        _write_split(paths, report)
    unresolved = bool(report.refused) or (report.config is not None and bool(report.config[1].untouched))
    if unresolved:
        exit_code = ERROR
    elif report.changed and not write_files:
        exit_code = CHANGES
    else:
        exit_code = OK
    return CommandResult(
        exit_code, data=_split_data(report, write_files), human=lambda: _print_split(report, write_files)
    )


def _write_split(paths: ProjectPaths, report: SplitReport) -> None:
    """Write each split's files, then remove the list it came from, then the configuration.

    Raises:
        WriteFailure: A file cannot be written or removed.
    """
    project_dir = paths.project_dir
    for plan in report.splits:
        crlf = uses_crlf(project_dir, plan.path)
        try:
            # The list is removed only once every other file is written; a member named for the
            # list's own file is written last, in its place.
            for item in plan.files:
                if item.path != plan.path:
                    create_file(project_dir, item.path, item.text, crlf=crlf)
            remove_file(project_dir, plan.path)
            for item in plan.files:
                if item.path == plan.path:
                    create_file(project_dir, item.path, item.text, crlf=crlf)
        except OSError as exc:
            raise WriteFailure(project_dir / plan.path, exc) from exc
    if report.config is not None and report.config[1].changed and paths.config_file is not None:
        try:
            write_file(paths.config_file.parent, paths.config_file.name, report.config[1].text)
        except OSError as exc:
            raise WriteFailure(paths.config_file, exc) from exc


def _report_data(report: MigrationReport, write_files: bool) -> dict[str, object]:
    return {
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
    }


def _print_report(report: MigrationReport, write_files: bool) -> None:
    """Print each rewritten file's counts on stdout and each place left unchanged on stderr."""
    for item in report.files:
        if item.result.changed:
            counts = ", ".join(f"{value} {kind}" for kind, value in item.counts().items() if value)
            click.echo(f"{'rewrote' if write_files else 'would rewrite'} {item.path}: {counts}")
        for entry in item.result.untouched:
            click.echo(f"{item.path}:{entry.line}:{entry.col}: left {entry.text} unchanged: {entry.reason}", err=True)
    if not report.changed and not report.untouched:
        click.echo("no legacy references found")


def _split_data(report: SplitReport, write_files: bool) -> dict[str, object]:
    config = report.config
    return {
        "written": write_files and report.changed,
        "files": [
            {"path": plan.path, "into": [item.path for item in plan.files], "refused": plan.refused}
            for plan in report.plans
        ],
        "config": None
        if config is None or not (config[1].changed or config[1].untouched)
        else {
            "path": config[0],
            "rewrites": [
                {"line": item.line, "before": item.before, "after": item.after} for item in config[1].rewrites
            ],
            "untouched": [
                {"line": item.line, "text": item.text, "reason": item.reason} for item in config[1].untouched
            ],
        },
    }


def _print_split(report: SplitReport, write_files: bool) -> None:
    """Print each split on stdout, and each file left whole or key left alone on stderr."""
    verb = "split" if write_files else "would split"
    for plan in report.plans:
        if plan.refused is not None:
            click.echo(f"{plan.path}: left whole: {plan.refused}", err=True)
        else:
            click.echo(
                f"{verb} {plan.path} into {len(plan.files)} files: {', '.join(item.path for item in plan.files)}"
            )
    if report.config is not None:
        path, result = report.config
        rewrote = "rewrote" if write_files else "would rewrite"
        for rewrite in result.rewrites:
            change = f"{rewrite.before or '(new)'} -> {rewrite.after or '(removed)'}"
            click.echo(f"{rewrote} {path}:{rewrite.line}: {change}")
        for entry in result.untouched:
            click.echo(f"{path}:{entry.line}:{entry.col}: left {entry.text} unchanged: {entry.reason}", err=True)
    if not report.changed and not report.refused:
        click.echo("nothing to split")

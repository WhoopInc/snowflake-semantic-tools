"""`sst format`: rewrite semantic-layer and dbt YAML into SST's canonical form, offline.

With no `PATH`, the configured paths are formatted: every artifact type's directory from
`sst_config.yml`, and dbt's `model-paths`. A `PATH` is a file, a directory, or a glob, taken from
the project directory when relative. `--check` writes nothing and exits 2 when a file would
change; `--dry-run` writes nothing and prints each change as a diff; `--force` rewrites even a
file already canonical; `--sanitize` also repairs apostrophes in synonyms and sample values and
Jinja in descriptions. All four compose. A file that does not parse is reported and left alone,
and the run exits 1.
"""

from __future__ import annotations

import difflib
import glob
from collections.abc import Iterable
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.dbt.project import model_paths
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.discover import YAML_SUFFIXES, registry_roots
from snowflake_semantic_tools.adapters.yaml.format import canonical_yaml
from snowflake_semantic_tools.adapters.yaml.parse import read_yaml_mapping
from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, ERROR, OK
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import no_detailed_exitcode_option
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body, project_path, write_text
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag

# Directories a walk never enters: build output, installed packages, and anything hidden.
_SKIPPED_DIRS = frozenset(("target", "dbt_packages", "dbt_modules", "node_modules", "logs"))


@click.command(name="format")
@click.argument("targets", metavar="[PATH]...", nargs=-1)
@click.option("--check", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--force", is_flag=True)
@click.option("--sanitize", is_flag=True)
@no_detailed_exitcode_option()
@command_body("format", config=ConfigNeed.OPTIONAL)
def format_command(
    paths: ProjectPaths,
    targets: tuple[str, ...],
    check: bool,
    dry_run: bool,
    force: bool,
    sanitize: bool,
    no_detailed_exitcode: bool,
) -> CommandResult:
    """Rewrite YAML into canonical form: indentation, key order, and `|-` block scalars.

    Exit 0 when every file is formatted or already canonical, 1 when a file cannot be parsed
    or written, 2 with --check when a file would change, and 4 with no PATH and no
    configuration to find the paths in.

    Diagnostics:
        SST-CFG001: no PATH was given and there is no configuration file.
        SST-PRT100: a PATH names no YAML file; raised.
        SST-PRT009: a file cannot be read.
        SST-LOD001: a file is not YAML; it is left as it is.
        SST-INT003: a file's canonical form would change its value; it is left as it is.
    """
    if not targets and paths.config_file is None:
        missing = D("SST-CFG001", subject="config:discovery", path=str(paths.project_dir))
        return CommandResult(CONFIG, DiagnosticBag((missing,)))
    files, diagnostics = _files(paths, targets)
    report = _FormatReport()
    for path in files:
        diagnostics.extend(report.format_file(paths.project_dir, path, sanitize=sanitize))
    if not (check or dry_run):
        for path in report.would_change if not force else (*report.would_change, *report.unchanged_paths):
            write_text(path, report.canonical[path])
    data = report.data(paths.project_dir, written=not (check or dry_run), force=force)
    if diagnostics and any(item.blocks for item in diagnostics):
        exit_code = ERROR
    elif check and report.would_change and not no_detailed_exitcode:
        exit_code = CHANGES
    else:
        exit_code = OK
    return CommandResult(
        exit_code,
        DiagnosticBag(diagnostics),
        data,
        human=lambda: report.print(paths.project_dir, check=check, dry_run=dry_run),
    )


class _FormatReport:
    """Each file's canonical text, and which files it changes."""

    def __init__(self) -> None:
        self.original: dict[Path, str] = {}
        self.canonical: dict[Path, str] = {}

    @property
    def would_change(self) -> tuple[Path, ...]:
        return tuple(path for path in self.canonical if self.canonical[path] != self.original[path])

    @property
    def unchanged_paths(self) -> tuple[Path, ...]:
        return tuple(path for path in self.canonical if self.canonical[path] == self.original[path])

    def format_file(self, project_dir: Path, path: Path, *, sanitize: bool) -> tuple[Diagnostic, ...]:
        """Compute one file's canonical text; a file that cannot be read or parsed is reported."""
        name = _relative(project_dir, path)
        try:
            text = path.read_bytes().decode("utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            return (D("SST-PRT009", subject="cli", path=name, detail=str(exc)),)
        try:
            canonical = canonical_yaml(text, name, sanitize=sanitize)
        except ProjectError as exc:
            return tuple(exc.diagnostics)
        self.original[path] = text
        self.canonical[path] = canonical
        return ()

    def data(self, project_dir: Path, *, written: bool, force: bool) -> dict[str, object]:
        changed = [_relative(project_dir, path) for path in self.would_change]
        unchanged = [_relative(project_dir, path) for path in self.unchanged_paths]
        return {
            "formatted": (changed + (unchanged if force else [])) if written else [],
            "unchanged": [] if written and force else unchanged,
            "would_change": [] if written else changed,
        }

    def print(self, project_dir: Path, *, check: bool, dry_run: bool) -> None:
        for path in self.would_change:
            name = _relative(project_dir, path)
            if dry_run:
                diff = difflib.unified_diff(
                    self.original[path].splitlines(keepends=True),
                    self.canonical[path].splitlines(keepends=True),
                    fromfile=f"a/{name}",
                    tofile=f"b/{name}",
                )
                click.echo("".join(diff), nl=False)
            elif check:
                click.echo(f"would reformat {name}")
            else:
                click.echo(f"formatted {name}")
        pending = len(self.would_change)
        if check and pending:
            click.echo(f"{pending} file(s) would be reformatted; run `sst format`")
        elif not pending:
            click.echo(f"{len(self.canonical)} file(s) already canonical")


def _files(paths: ProjectPaths, targets: tuple[str, ...]) -> tuple[list[Path], list[Diagnostic]]:
    """Return the YAML files to format, in path order, and a diagnostic for each PATH naming none."""
    found: dict[Path, None] = {}
    diagnostics: list[Diagnostic] = []
    for target in targets or _configured(paths):
        matched = tuple(_yaml_files(_expanded(project_path(paths, Path(target)))))
        if not matched and targets:
            diagnostic = D("SST-PRT100", subject="cli", detail=f"PATH '{target}' names no YAML file")
            raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
        found.update(dict.fromkeys(matched))
    return sorted(found), diagnostics


def _configured(paths: ProjectPaths) -> tuple[str, ...]:
    """Return the configured directories: each artifact type's, then dbt's `model-paths`."""
    roots = registry_roots(resolved_config(paths).tree)
    directories = [root for root in roots.values() if "*" not in root]
    if (paths.project_dir / "dbt_project.yml").is_file():
        directories.extend(model_paths(paths.project_dir, read_yaml_mapping)[0])
    return tuple(dict.fromkeys(directories))


def _expanded(path: Path) -> tuple[Path, ...]:
    """Return the paths a glob matches, or the path itself when it is not one."""
    if glob.has_magic(str(path)):
        return tuple(Path(item) for item in sorted(glob.glob(str(path), recursive=True)))
    return (path,) if path.exists() else ()


def _yaml_files(roots: Iterable[Path]) -> Iterable[Path]:
    """Yield each YAML file a root is, or holds below it outside build, package and hidden dirs."""
    for root in roots:
        if root.is_file() and root.suffix in YAML_SUFFIXES:
            yield root
        elif root.is_dir():
            for path in sorted(root.rglob("*")):
                relative = path.relative_to(root).parts[:-1]
                skipped = any(part in _SKIPPED_DIRS or part.startswith(".") for part in relative)
                if path.is_file() and path.suffix in YAML_SUFFIXES and not skipped:
                    yield path


def _relative(project_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(project_dir).as_posix()
    except ValueError:
        return path.as_posix()

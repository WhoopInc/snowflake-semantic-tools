"""`sst docs`: write, or check, the reference pages generated from the engine's own registries.

`--only` names the references to generate -- `errors`, `coverage`, `artifacts`, `cli`, `config`
-- and every one is generated without it. `--check` exits 2 when a committed page differs, which
is a difference between declared and actual state, and keeps 1 for the generator itself
failing; `--no-detailed-exitcode` turns 2 into 0. The coverage page is the coverage gate: it
reads the package for where each code is raised and `tests/codes/` for its tests.
"""

from __future__ import annotations

from pathlib import Path

import click

from snowflake_semantic_tools.adapters.code_coverage import code_tests, prechecks
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import output_root
from snowflake_semantic_tools.cli.commands.explain import package_sites
from snowflake_semantic_tools.cli.exit_codes import CHANGES, EXIT_CODE_DOCS, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import cli
from snowflake_semantic_tools.cli.help_text import command_docs, option_docs
from snowflake_semantic_tools.cli.options import no_detailed_exitcode_option
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body, write_text
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.render.reference_docs import REFERENCES, CodeCoverage, reference_pages

# Where the coverage page reads each code's tests and pre-check, relative to the project.
CODE_TESTS = Path("tests") / "codes"


@click.command(cls=SstCommand)
@click.option("--check", is_flag=True)
@click.option("--only", type=click.Choice(tuple(REFERENCES)), multiple=True)
@click.option("--output-dir", type=click.Path(file_okay=False, path_type=Path))
@no_detailed_exitcode_option()
@command_body("docs", config=ConfigNeed.OPTIONAL)
def docs(
    paths: ProjectPaths, check: bool, only: tuple[str, ...], output_dir: Path | None, no_detailed_exitcode: bool
) -> CommandResult:
    """Write the generated reference pages under docs/reference/.

    The error-code, coverage, artifact, command-line, and configuration references are
    rendered from the engine's own registries, so they cannot drift from what the engine
    accepts. With --check, nothing is written and the command exits 2 when a committed page
    differs.
    """
    root = paths.project_dir
    chosen = only or tuple(REFERENCES)
    coverage = code_coverage(root) if "coverage" in chosen else {}
    pages = reference_pages(command_docs(cli), option_docs(cli), EXIT_CODE_DOCS, coverage, only=chosen)
    if output_dir is not None:
        pages = {str(Path(output_dir) / Path(path).name): text for path, text in pages.items()}
    drifted = [
        path
        for path, text in sorted(pages.items())
        if not (root / path).is_file() or (root / path).read_text(encoding="utf-8") != text
    ]
    if not check:
        for path in drifted:
            target = root / path
            write_text(output_root(root, target.parent), target, pages[path])
    exit_code = CHANGES if check and drifted and not no_detailed_exitcode else OK
    data = {
        "references": list(chosen),
        "pages": sorted(pages),
        "generated": [] if check else drifted,
        "stale": drifted if check else [],
        "output_dir": str(output_dir or "docs/reference"),
        "drifted": drifted,
        "written": [] if check else drifted,
    }
    return CommandResult(exit_code, data=data, human=lambda: _print_pages(drifted, len(pages), check=check))


def code_coverage(root: Path) -> dict[str, CodeCoverage]:
    """Return, for every registered code, where the package raises it and where `root` tests it."""
    codes = tuple(sorted(ERROR_REGISTRY))
    sites = package_sites()
    tests = code_tests(root / CODE_TESTS, codes)
    checks = prechecks(root / CODE_TESTS / "catalog.json")
    return {code: CodeCoverage(sites.get(code, ()), tests.get(code), checks.get(code, "")) for code in codes}


def _print_pages(drifted: list[str], page_count: int, *, check: bool) -> None:
    for path in drifted:
        click.echo(f"out of date: {path}" if check else f"wrote {path}")
    if check and drifted:
        click.echo("run `sst docs` to regenerate the reference pages")
    else:
        click.echo(f"{page_count} reference page(s) current")

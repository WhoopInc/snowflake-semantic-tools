"""`sst explain`: everything the registry knows about one code, offline and with no project.

The code may be a registered 1.0 code, an SST 0.3 code that survives as an alias, or a retired
1.0 number, in any case. Anything else is a usage error, exit 3. `--aliases` also lists every 0.3
code that resolves to the code, or, for a 0.3 code, every 1.0 code it was split into. The raise
sites are the package modules that name the code, found by reading the installed package.
"""

from __future__ import annotations

import dataclasses
import functools
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.code_coverage import raise_sites as package_raise_sites
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.runner import CommandResult, ConfigNeed, command_body
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.diagnostics.explain import Explanation, explain

PACKAGE = Path(__file__).resolve().parents[2]
REGISTRY_SPECS = PACKAGE / "domain" / "diagnostics" / "specs"


@click.command(cls=SstCommand, name="explain")
@click.argument("code", metavar="CODE")
@click.option("--aliases", "show_aliases", is_flag=True)
@command_body("explain", config=ConfigNeed.NONE)
def explain_command(code: str, show_aliases: bool) -> CommandResult:
    """Explain one diagnostic code from the registry: no project, configuration, or connection.

    Exit 0 when the code is explained, and 3 when it is not a registered code, a 0.3 alias, or
    a retired number.

    Diagnostics:
        SST-PRT100: the code is unknown; raised.
    """
    explanation = explain(code)
    if explanation is None:
        detail = f"'{code}' is not a registered code, an SST 0.3 alias, or a retired number"
        diagnostic = D("SST-PRT100", subject="cli", detail=detail)
        raise SstUsageError(diagnostic.message, diagnostic=diagnostic)
    sites = raise_sites(explanation.code) if explanation.kind == "code" else ()
    data = _data(explanation, sites)
    return CommandResult(data=data, human=lambda: _print(explanation, sites, show_aliases=show_aliases))


def _data(explanation: Explanation, sites: tuple[str, ...]) -> dict[str, object]:
    return {
        "code": explanation.code,
        "kind": explanation.kind,
        "title": explanation.title,
        "severity": explanation.severity,
        "non_demotable": explanation.non_demotable,
        "phase": explanation.phase,
        "condition": explanation.condition,
        "message_template": explanation.message_template,
        "suggestion_template": explanation.suggestion_template,
        "note": explanation.note,
        "help_url": explanation.help_url,
        "origin": list(explanation.origin),
        "aliases": [dataclasses.asdict(row) | {"targets": list(row.targets)} for row in explanation.aliases],
        "raise_sites": list(sites),
        "retired": explanation.retired,
        "retired_in": explanation.retired_in,
        "superseded_by": explanation.superseded_by,
        "deprecated_in": explanation.deprecated_in,
    }


def _print(explanation: Explanation, sites: tuple[str, ...], *, show_aliases: bool) -> None:
    """Print the code's heading, then each field the registry records, then the aliases asked for."""
    click.echo(f"{explanation.code}: {explanation.title}")
    if explanation.kind == "alias":
        [alias] = explanation.aliases
        click.echo(f"  SST 0.3 code, {alias.disposition.lower()}; never raised by 1.0")
        click.echo(f"  now: {', '.join(alias.targets) or 'nothing; the condition is gone'}")
    elif explanation.retired:
        click.echo(f"  retired in {explanation.retired_in}; the number is never reused")
        click.echo(f"  superseded by: {explanation.superseded_by or 'nothing; the condition is gone'}")
    else:
        flag = ", non-demotable" if explanation.non_demotable else ""
        click.echo(f"  severity: {explanation.severity}{flag}")
        click.echo(f"  phase: {explanation.phase}")
        if explanation.condition:
            click.echo(f"  raised when: {explanation.condition}")
        click.echo(f"  message: {explanation.message_template}")
        click.echo(f"  suggestion: {explanation.suggestion_template or '-'}")
        if explanation.note:
            click.echo(f"  note: {explanation.note}")
        if explanation.deprecated_in:
            click.echo(f"  deprecated in {explanation.deprecated_in}; use {explanation.superseded_by}")
        if sites:
            click.echo(f"  raised from: {', '.join(sites)}")
        if explanation.origin:
            click.echo(f"  from SST 0.3: {', '.join(explanation.origin)}")
    click.echo(f"  docs: {explanation.help_url}")
    if show_aliases and explanation.kind != "alias":
        for row in explanation.aliases:
            click.echo(f"  alias {row.code} ({row.title}): {row.disposition.lower()} -> {', '.join(row.targets)}")


def raise_sites(code: str) -> tuple[str, ...]:
    """Return the package modules that name `code` as a string constant, outside the registry."""
    return package_sites().get(code, ())


@functools.cache
def package_sites() -> dict[str, tuple[str, ...]]:
    """Index every exact code constant in the installed package, by code, as module paths."""
    return package_raise_sites(PACKAGE, exclude=REGISTRY_SPECS)

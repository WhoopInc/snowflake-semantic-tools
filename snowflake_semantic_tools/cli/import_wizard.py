"""Ask, on a terminal, for what `sst import` was not given: the type, the scope, and the objects.

Each question offers a default, which Enter takes, and goes to stderr through `cli.confirm`.
The answers build the same request the flags do, so a run the wizard set up imports exactly
what the equivalent command line would. Objects are listed from Snowflake for the user to
pick by number (`1,3-5`), or `all`.
"""

from __future__ import annotations

from dataclasses import dataclass

import click

from snowflake_semantic_tools.app.importing import SUPPORTED_TYPES, ImportScope
from snowflake_semantic_tools.cli import confirm as asking
from snowflake_semantic_tools.domain.importing.model import LISTED_AS
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.importing import ImportReadPort

_SCOPES = ("schema", "database", "object")


@dataclass(frozen=True, slots=True)
class Answers:
    """What the wizard settled: the arguments and flags of the run it builds.

    Attributes:
        only: The names picked from the listing; empty for every one.
    """

    artifact_type: str
    scope: str
    only: tuple[str, ...] = ()
    with_dependencies: bool = False
    to_folder: str | None = None


class Wizard:
    """Ask for each part of an import that is missing, listing what the target holds to pick from."""

    def __init__(self, port: ImportReadPort, target: TargetIdentity) -> None:
        self._port = port
        self._target = target

    def ask(
        self, artifact_type: str | None, scope: str | None, *, with_dependencies: bool, to_folder: str | None
    ) -> Answers:
        """Ask for the type, then the scope and objects, then where the files go and what else to import."""
        chosen_type = artifact_type or self._type()
        if scope is not None:
            return Answers(chosen_type, scope, (), with_dependencies, to_folder)
        kind = self._choice("Import one object, a schema, or a whole database?", _SCOPES, "schema")
        database = self._identifier("Database", self._target.database.folded)
        if kind == "database":
            parsed = ImportScope(database)
        else:
            parsed = ImportScope(database, self._identifier("Schema", self._target.schema.folded))
        only = self._objects(chosen_type, parsed, single=kind == "object")
        if to_folder is None:
            folder = asking.ask("Folder below the type's directory (Enter: each object's routed folder)", "")
            to_folder = folder.strip() or None
        if not with_dependencies:
            answer = asking.ask("Also import what they name that the project does not compile? [y/N]", "n")
            with_dependencies = answer.strip().casefold() in ("y", "yes")
        return Answers(chosen_type, parsed.sql, only, with_dependencies, to_folder)

    def _type(self) -> str:
        return self._choice("Type to import", SUPPORTED_TYPES, "agent")

    def _choice(self, question: str, choices: tuple[str, ...], default: str) -> str:
        for number, choice in enumerate(choices, start=1):
            click.echo(f"  {number}. {choice}", err=True)
        while True:
            answer = asking.ask(question, default).strip()
            if answer.isdigit() and 1 <= int(answer) <= len(choices):
                return choices[int(answer) - 1]
            if answer in choices:
                return answer
            click.echo(f"  choose 1-{len(choices)}, or one of: {', '.join(choices)}", err=True)

    def _identifier(self, question: str, default: str) -> Identifier:
        while True:
            answer = asking.ask(question, default)
            try:
                return Identifier.parse(answer)
            except ValueError:
                click.echo(f"  '{answer}' is not a Snowflake identifier", err=True)

    def _objects(self, artifact_type: str, scope: ImportScope, *, single: bool) -> tuple[str, ...]:
        """List the scope's objects of the type and ask which to import; empty for every one."""
        try:
            rows = self._port.show_objects_in(LISTED_AS[artifact_type], scope.database, scope.schema)
        except SnowflakePortError:
            # The run lists the scope again and reports it (SST-IMP001).
            return ()
        if not rows:
            return ()
        names = tuple(row.name for row in rows)
        for number, row in enumerate(rows, start=1):
            click.echo(f"  {number}. {row.qualified_name.sql}", err=True)
        default = "1" if single else "all"
        while True:
            answer = asking.ask("Objects to import (numbers such as 1,3-5, or all)", default).strip()
            picked = _picked(answer, len(names))
            if picked is not None:
                return () if picked == tuple(range(len(names))) and not single else tuple(names[i] for i in picked)
            click.echo(f"  choose numbers from 1 to {len(names)}, ranges such as 2-4, or all", err=True)


def _picked(answer: str, count: int) -> tuple[int, ...] | None:
    """Read `1,3-5` or `all` as the indexes picked; None when the answer is not one."""
    if answer.casefold() == "all":
        return tuple(range(count))
    indexes: list[int] = []
    for part in answer.split(","):
        part = part.strip()
        low, _, high = part.partition("-")
        if not low.isdigit() or (high and not high.isdigit()):
            return None
        start, end = int(low), int(high or low)
        if not 1 <= start <= end <= count:
            return None
        indexes.extend(range(start - 1, end))
    return tuple(dict.fromkeys(indexes)) or None

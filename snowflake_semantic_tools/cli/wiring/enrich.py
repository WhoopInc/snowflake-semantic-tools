"""Bind `sst enrich` to a project: its files, its PATH and selector arguments, and its target.

The Snowflake session is opened on the first warehouse read, so a run refused by its config,
or one whose selection matches nothing, never connects.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from snowflake_semantic_tools.adapters.enrich_files import ProjectFiles
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.app.enrich import EnrichProject
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.wiring.project import connect, project_inputs
from snowflake_semantic_tools.domain.model.enrich import WarehouseColumn
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.ports.enrich import EnrichPort


class LazyEnrichPort(EnrichPort):
    """The project target's Snowflake session, connected when enrich first reads through it."""

    def __init__(self, project_dir: Path, target_name: str | None) -> None:
        self._project_dir = project_dir
        self._target_name = target_name
        self._port: SnowflakeConnector | None = None

    def _connected(self) -> SnowflakeConnector:
        if self._port is None:
            _, self._port = connect(self._project_dir, self._target_name)
        return self._port

    def relation_columns(self, relation: QualifiedName) -> tuple[WarehouseColumn, ...] | None:
        return self._connected().relation_columns(relation)

    def distinct_values(
        self, relation: QualifiedName, columns: Sequence[str], limit: int
    ) -> Mapping[str, tuple[str, ...]]:
        return self._connected().distinct_values(relation, columns, limit)

    def complete_json(self, model: str, prompt: str, schema: Mapping[str, object]) -> object:
        return self._connected().complete_json(model, prompt, schema)

    def close(self) -> None:
        """Close the session, when one was opened."""
        if self._port is not None:
            self._port.close()


def enrich_project(
    project_dir: Path, target_name: str | None, manifest_path: Path | None, port: EnrichPort
) -> EnrichProject:
    """Bind the use case to the project's files and inputs, reading the warehouse through `port`."""
    return EnrichProject(project_inputs(project_dir, target_name, manifest_path), port, ProjectFiles(project_dir))


def project_paths(project_dir: Path, paths: Sequence[Path]) -> tuple[str, ...]:
    """Return PATH arguments relative to the project, in POSIX form; empty when one is the project.

    A relative PATH is read from the project directory, as dbt reads its selectors.

    Raises:
        SstUsageError: a PATH lies outside the project directory.
    """
    root = project_dir.resolve()
    converted = []
    for path in paths:
        absolute = (path if path.is_absolute() else root / path).resolve()
        try:
            relative = absolute.relative_to(root).as_posix()
        except ValueError:
            raise SstUsageError(f"{path} is not inside the project directory {project_dir}") from None
        if relative == ".":
            return ()
        converted.append(relative)
    return tuple(converted)


def model_selectors(values: Sequence[str], flag: str) -> tuple[str, ...]:
    """Return the model names or globs that `--select` or `--exclude` values name.

    `model:<name>` and a bare `<name>` both name a model.

    Raises:
        SstUsageError: a value holds a comma, names another artifact type, or names nothing.
    """
    names = []
    for value in values:
        if "," in value:
            raise SstUsageError(f"commas are not accepted in {flag}; repeat {flag} for each model")
        if "/" in value:
            raise SstUsageError(f"{flag} selects models by name; pass directories as PATH arguments instead")
        prefix, separator, name = value.partition(":")
        if separator and prefix.strip().casefold() != "model":
            raise SstUsageError(f"sst enrich selects dbt models only; write {flag} model:<name>")
        name = (name if separator else value).strip()
        if not name:
            raise SstUsageError(f"{flag} {value!r} names no model")
        names.append(name)
    return tuple(names)


def relation_part(value: str | None, flag: str) -> Identifier | None:
    """Return a `--database` or `--schema` value as one Snowflake identifier; None when not given.

    Raises:
        SstUsageError: the value is not one valid identifier.
    """
    if value is None:
        return None
    try:
        return Identifier.parse(value)
    except ValueError as exc:
        raise SstUsageError(f"{flag}: {exc}") from None

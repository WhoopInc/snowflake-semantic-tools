"""The project files `sst enrich` reads and edits: dbt model YAML and semantic view YAML.

Implements `domain.ports.enrich.EnrichFilesPort` by composing the dbt writer, the view writer,
and the contained atomic file write, which is why it sits beside the adapter subpackages rather
than in one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_model_updates
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.yaml.view_writer import write_table_synonyms
from snowflake_semantic_tools.domain.enrich import ColumnUpdate, TableSynonymEdit
from snowflake_semantic_tools.domain.ports.enrich import EnrichFilesPort, WrittenFile


class ProjectFiles(EnrichFilesPort):
    """Read and edit YAML files under one project directory."""

    def __init__(self, project_dir: Path) -> None:
        self._project_dir = project_dir

    def read(self, path: str) -> str | None:
        target = self._project_dir / path
        if not target.is_file():
            return None
        try:
            return target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ProjectError(f"{path}: cannot read it to enrich it: {exc}") from exc

    def edit_models(self, text: str | None, path: str, updates: Mapping[str, Sequence[ColumnUpdate]]) -> WrittenFile:
        return write_model_updates(text, path, updates)

    def edit_views(self, text: str, path: str, edits: Sequence[TableSynonymEdit]) -> WrittenFile:
        return write_table_synonyms(text, path, edits)

    def write(self, path: str, text: str) -> None:
        write_within(self._project_dir, self._project_dir / path, text)

"""The project files `sst enrich` reads and edits: dbt model YAML.

Implements `domain.ports.enrich.EnrichFilesPort` by composing the dbt writer and the contained
atomic file write, which is why it sits beside the adapter subpackages rather than in one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_model_updates
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.enrich import ColumnUpdate
from snowflake_semantic_tools.domain.ports.enrich import EnrichFilesPort, WrittenFile


class ProjectFiles(EnrichFilesPort):
    """Read and edit YAML files under one project directory."""

    def __init__(self, project_dir: Path) -> None:
        self._project_dir = project_dir

    def read(self, path: str) -> str | None:
        """Return the text of the file at `path` in the project; None when there is none.

        Raises:
            ProjectError: the file exists and cannot be read as UTF-8 (SST-PRT009).

        Diagnostics:
            SST-PRT009: the file cannot be read; raised.
        """
        target = self._project_dir / path
        if not target.is_file():
            return None
        try:
            return target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            diagnostic = D("SST-PRT009", subject="cli", path=path, detail=str(exc))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc

    def edit_models(
        self,
        text: str | None,
        path: str,
        updates: Mapping[str, Sequence[ColumnUpdate]],
        synonyms: Mapping[str, Sequence[str]],
    ) -> WrittenFile:
        return write_model_updates(text, path, updates, synonyms)

    def write(self, path: str, text: str) -> None:
        write_within(self._project_dir, self._project_dir / path, text)

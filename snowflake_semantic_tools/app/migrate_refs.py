"""Orchestrate `sst migrate refs` over a project's semantic model files."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..domain.model.migrate import FilterSite, MigrationResult, add_filter_labels, migrate_refs


@dataclass(frozen=True, slots=True)
class FileMigration:
    path: str
    result: MigrationResult

    def counts(self) -> dict[str, int]:
        kinds = ("ref", "bare", "column", "labels")
        return {kind: sum(item.kind == kind for item in self.result.rewrites) for kind in kinds}


@dataclass(frozen=True, slots=True)
class MigrationReport:
    files: tuple[FileMigration, ...]

    @property
    def changed(self) -> tuple[FileMigration, ...]:
        return tuple(item for item in self.files if item.result.changed)

    @property
    def untouched(self) -> int:
        return sum(len(item.result.untouched) for item in self.files)


class MigrateRefs:
    """Rewrite every file in memory; the caller decides whether to write."""

    def __init__(self, files: Mapping[str, str], locate: Callable[[str, str], tuple[FilterSite, ...]]) -> None:
        self._files = files
        self._locate = locate

    def run(self) -> MigrationReport:
        migrated: list[FileMigration] = []
        for path, text in sorted(self._files.items()):
            result = migrate_refs(text)
            result = add_filter_labels(result, self._locate(result.text, path))
            migrated.append(FileMigration(path, result))
        return MigrationReport(tuple(migrated))

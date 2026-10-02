"""Orchestrate `sst migrate refs` over a project's semantic model files."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.migrate.refs import FilterSite, MigrationResult, add_filter_labels, migrate_refs


@dataclass(frozen=True, slots=True)
class FileMigration:
    """One file after the migration: its text with every rewrite made, and each call left alone.

    Attributes:
        path: The file's key in the files the run was given; a project-relative path for the CLI.
        result: The reference rewrite, then the filter labels; `result.text` is the whole file,
            changed or not.
    """

    path: str
    result: MigrationResult

    def counts(self) -> dict[str, int]:
        """Count the file's rewrites by kind: `ref`, `bare`, `column`, and `labels`, in that order.

        Every kind is present, 0 when the file has none of it.
        """
        kinds = ("ref", "bare", "column", "labels")
        return {kind: sum(item.kind == kind for item in self.result.rewrites) for kind in kinds}


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """Every file one run migrated in memory, in path order, whether or not anything in it changed."""

    files: tuple[FileMigration, ...]

    @property
    def changed(self) -> tuple[FileMigration, ...]:
        """Return the files at least one rewrite changed, in path order; only these have anything to write."""
        return tuple(item for item in self.files if item.result.changed)

    @property
    def untouched(self) -> int:
        """Count the `table()` calls left alone across every file, where no rewrite of them is safe."""
        return sum(len(item.result.untouched) for item in self.files)


class MigrateRefs:
    """Rewrite every file in memory; the caller decides whether to write."""

    def __init__(self, files: Mapping[str, str], locate: Callable[[str, str], tuple[FilterSite, ...]]) -> None:
        self._files = files
        self._locate = locate

    def run(self) -> MigrationReport:
        """Migrate every file in memory, in path order, and report each one; nothing is written.

        A file's references are rewritten first; its filters are then located in the rewritten
        text, which is what `locate` is given, and each boolean one without labels gains them.
        """
        migrated: list[FileMigration] = []
        for path, text in sorted(self._files.items()):
            result = migrate_refs(text)
            result = add_filter_labels(result, self._locate(result.text, path))
            migrated.append(FileMigration(path, result))
        return MigrationReport(tuple(migrated))

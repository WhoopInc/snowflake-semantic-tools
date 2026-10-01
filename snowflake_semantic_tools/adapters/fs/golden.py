"""Committed goldens on disk: the DDL directory, and the per-type directories beside it."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.ports.golden import GoldenPath, GoldenStore


class GoldenFileStore(GoldenStore):
    """Goldens under one DDL directory and its siblings, read as UTF-8 and never written.

    Implements `domain.ports.golden.GoldenStore`, whose methods document the contract. A
    golden's name is its path built from the directory exactly as given, so a relative
    `--golden-dir` names relative paths.
    """

    def __init__(self, ddl_dir: Path) -> None:
        self._ddl_dir = ddl_dir

    def exists(self, path: GoldenPath) -> bool:
        return self._path(path).is_file()

    def read(self, path: GoldenPath) -> str | None:
        file = self._path(path)
        if not file.is_file():
            return None
        return file.read_text(encoding="utf-8")

    def name(self, path: GoldenPath) -> str:
        return str(self._path(path))

    def _path(self, path: GoldenPath) -> Path:
        base = self._ddl_dir if path.beside is None else self._ddl_dir.parent / path.beside
        return base.joinpath(*path.parts)

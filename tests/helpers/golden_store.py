"""An in-memory `GoldenWriter` for application tests: goldens held as text, by location."""

from __future__ import annotations

from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.ports.golden import GoldenPath


@dataclass
class InMemoryGoldenStore:
    """Holds committed goldens by `GoldenPath`, named `golden/<beside or ddl>/<parts>` in reports."""

    goldens: dict[GoldenPath, str] = field(default_factory=dict)

    def exists(self, path: GoldenPath) -> bool:
        return path in self.goldens

    def read(self, path: GoldenPath) -> str | None:
        return self.goldens.get(path)

    def name(self, path: GoldenPath) -> str:
        return "/".join(("golden", path.beside or "ddl", *path.parts))

    def write(self, path: GoldenPath, text: str) -> None:
        self.goldens[path] = text

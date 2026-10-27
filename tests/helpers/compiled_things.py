"""A minimal compiled artifact of any type, for tests of the compile loop and its merge."""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import StandaloneArtifact
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from tests.helpers.artifact_builders import rendered


@dataclass(frozen=True, slots=True)
class Thing(StandaloneArtifact):
    """A compiled artifact named `label`, of type `kind`, whose payload is its label upper-cased."""

    label: str
    kind: str = "semantic_view"

    @property
    def name(self) -> str:
        return self.label

    @property
    def artifact_key(self) -> str:
        return f"{self.kind}:{self.label}"

    @property
    def artifact_type(self) -> str:
        return self.kind

    @property
    def source_files(self) -> tuple[str, ...]:
        return (f"{self.label}.yml",)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return rendered(self.label.upper())

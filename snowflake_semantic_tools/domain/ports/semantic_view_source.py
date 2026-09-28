"""The port `app/` uses to obtain semantic views, without knowing where they live.

A `Protocol` rather than a base class, so an adapter satisfies it structurally and
owes this module no import -- `adapters/` depends on `domain/ports/`, never the
reverse.

This is the seam that makes `app/` testable without mocks: a test passes an
in-memory implementation that is real code, and nothing has to be patched. 0.3 has
roughly 570 inline mock usages because it has no seam like this one.
"""

from __future__ import annotations

from typing import Protocol

from ..model.project import SemanticViewProject
from ..model.semantic_view import SemanticView


class SemanticViewSource(Protocol):
    """Anything that can produce fully-resolved semantic views."""

    def load_semantic_views(self) -> tuple[SemanticView, ...]:
        """Return every enabled semantic view, with refs and target resolved."""
        ...

    def load_project(self) -> SemanticViewProject:
        """Return healthy views plus every collected compiler diagnostic."""
        ...

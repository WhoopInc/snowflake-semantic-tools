"""SST-REG011: the type dependency graph has a cycle.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg011_fires() -> None:
    view = artifact("semantic_view", dependency_types=("tool",))
    tool = artifact("tool", 200, dependency_types=("semantic_view",))
    refused(lambda: freeze((view, tool)), "SST-REG011", "type dependency cycle: semantic_view -> tool -> semantic_view")


def test_sst_reg011_silent() -> None:
    registry = freeze((artifact("semantic_view"), artifact("tool", 200, dependency_types=("semantic_view",))))
    assert set(registry.artifacts) == {"semantic_view", "tool"}

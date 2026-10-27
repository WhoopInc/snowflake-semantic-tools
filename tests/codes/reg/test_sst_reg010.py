"""SST-REG010: a type is ordered before a type it depends on.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg010_fires() -> None:
    refused(
        lambda: freeze((artifact("semantic_view"), artifact("tool", 50, dependency_types=("semantic_view",)))),
        "SST-REG010",
        "tool is ordered at 50 but depends on semantic_view, ordered later",
    )


def test_sst_reg010_silent() -> None:
    registry = freeze((artifact("semantic_view"), artifact("tool", 200, dependency_types=("semantic_view",))))
    assert registry.artifacts["tool"].ddl_position > registry.artifacts["semantic_view"].ddl_position

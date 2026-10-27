"""SST-REG007: a type names a member type, owner, or dependency that is not registered.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg007_fires() -> None:
    view = artifact("semantic_view", member_types=("metric",))
    refused(
        lambda: freeze((view,), (member("metric", "semantic_veiw"),)),
        "SST-REG007",
        "metric names 'semantic_veiw', which is not registered",
    )


def test_sst_reg007_fires_for_a_dependency() -> None:
    refused(
        lambda: freeze((artifact("semantic_view"), artifact("tool", 200, dependency_types=("view",)))),
        "SST-REG007",
        "tool names 'view', which is not registered",
    )


def test_sst_reg007_silent() -> None:
    registry = freeze((artifact("semantic_view"), artifact("tool", 200, dependency_types=("semantic_view",))))
    assert registry.artifacts["tool"].dependency_types == ("semantic_view",)

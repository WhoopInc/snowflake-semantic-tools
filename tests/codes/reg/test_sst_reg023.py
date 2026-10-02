"""SST-REG023: the semantic view member index does not cover the declared member types.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY, check_member_index
from snowflake_semantic_tools.domain.render.semantic_view import MEMBER_INDEX
from tests.helpers.registry_types import artifact, freeze, member, refused


def test_sst_reg023_fires() -> None:
    view = artifact("semantic_view", member_types=("dimension", "metric"))
    registry = freeze((view,), (member("dimension"), member("metric", position=20)))
    refused(
        lambda: check_member_index(("metric",), registry),
        "SST-REG023",
        "MemberIndex covers ['metric']; semantic_view declares ['dimension', 'metric']",
    )


def test_sst_reg023_silent() -> None:
    check_member_index(MEMBER_INDEX, ARTIFACT_REGISTRY)
    assert set(MEMBER_INDEX) == set(ARTIFACT_REGISTRY.artifacts["semantic_view"].member_types)

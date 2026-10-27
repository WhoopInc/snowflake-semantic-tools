"""SST-REG021: a type's grant handling contradicts how it is published.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.registry import ArtifactLifecycle, GrantPreservation
from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg021_fires() -> None:
    refused(
        lambda: freeze((artifact("semantic_view", grant_preservation=GrantPreservation.NONE),)),
        "SST-REG021",
        "type 'semantic_view' -- it replaces its object and declares no grant preservation",
    )


def test_sst_reg021_fires_for_a_composite_object_type() -> None:
    composite = artifact(
        "skill",
        lifecycle=ArtifactLifecycle.COMPOSITE,
        prunable=False,
        replaces_on_update=False,
        grant_preservation=GrantPreservation.NONE,
    )
    refused(
        lambda: freeze((composite,)),
        "SST-REG021",
        "type 'skill' -- a composite type's grants are its handler's, so it cannot declare an object type",
    )


def test_sst_reg021_silent() -> None:
    additive = artifact("agent", replaces_on_update=False, grant_preservation=GrantPreservation.NONE)
    assert freeze((additive,)).artifacts["agent"].grant_preservation is GrantPreservation.NONE

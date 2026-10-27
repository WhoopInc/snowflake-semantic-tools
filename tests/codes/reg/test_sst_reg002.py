"""SST-REG002: a name or a code is registered twice.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg002_fires() -> None:
    view = artifact("semantic_view")
    refused(lambda: freeze((view, view)), "SST-REG002", "semantic_view is already registered")


def test_sst_reg002_fires_for_an_error_code() -> None:
    from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, build_registry

    entry = ERROR_REGISTRY["SST-REF001"]
    refused(lambda: build_registry((entry, entry)), "SST-REG002", "SST-REF001 is already registered")


def test_sst_reg002_silent() -> None:
    assert len(freeze((artifact("semantic_view"), artifact("tool", 200))).artifacts) == 2

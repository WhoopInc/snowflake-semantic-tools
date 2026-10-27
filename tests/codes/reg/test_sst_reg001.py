"""SST-REG001: a registration leaves a field the generated references or discovery read empty.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg001_fires() -> None:
    context = refused(
        lambda: freeze((artifact("semantic_view", summary=""),)),
        "SST-REG001",
        "registration for semantic_view omits required field 'summary'",
    )
    assert context == {"type": "semantic_view", "field": "summary"}


def test_sst_reg001_fires_for_an_error_code_without_a_title() -> None:
    from snowflake_semantic_tools.domain.diagnostics import Severity, build_registry
    from snowflake_semantic_tools.domain.diagnostics.core import spec

    refused(
        lambda: build_registry((spec("SST-ABC001", Severity.ERROR, " ", "{value}", None),)),
        "SST-REG001",
        "registration for SST-ABC001 omits required field 'title'",
    )


def test_sst_reg001_silent() -> None:
    assert tuple(freeze((artifact("semantic_view"),)).artifacts) == ("semantic_view",)

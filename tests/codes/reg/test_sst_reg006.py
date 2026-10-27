"""SST-REG006: a type names a validation rule set that is not registered.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from tests.helpers.registry_types import artifact, freeze, refused


def test_sst_reg006_fires() -> None:
    refused(
        lambda: freeze((artifact("semantic_view", validation_rules=("shared", "sematic_view")),)),
        "SST-REG006",
        "semantic_view names rule 'sematic_view', which is not registered",
    )


def test_sst_reg006_silent() -> None:
    registry = freeze((artifact("semantic_view", validation_rules=("shared", "semantic_view")),))
    assert registry.artifacts["semantic_view"].validation_rules == ("shared", "semantic_view")

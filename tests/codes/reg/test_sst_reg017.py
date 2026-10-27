"""SST-REG017: a retired number is registered again.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg017_fires() -> None:
    entry = spec("SST-PRT007", Severity.ERROR, "Title", "{value}", None)
    refused(lambda: build_registry((entry,)), "SST-REG017", "'SST-PRT007' was retired in 1.0.0 and cannot be reused")


def test_sst_reg017_silent() -> None:
    assert "SST-PRT013" in build_registry((spec("SST-PRT013", Severity.ERROR, "Title", "{value}", None),))
    assert "SST-PRT007" not in ERROR_REGISTRY

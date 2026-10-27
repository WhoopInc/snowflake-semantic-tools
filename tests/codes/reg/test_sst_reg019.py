"""SST-REG019: a code outside INT and SNO declares internal detail.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg019_fires() -> None:
    entry = spec("SST-CFG999", Severity.ERROR, "Title", "{value}", None, internal_detail=True)
    refused(lambda: build_registry((entry,)), "SST-REG019", "'SST-CFG999' declares internal_detail but its area is CFG")


def test_sst_reg019_silent() -> None:
    entry = spec("SST-SNO999", Severity.ERROR, "Title", "{value}", None, internal_detail=True)
    assert build_registry((entry,))["SST-SNO999"].internal_detail
    assert ERROR_REGISTRY["SST-INT001"].internal_detail

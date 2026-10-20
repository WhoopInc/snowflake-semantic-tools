"""SST-REG025: a code that is not a warning declares strict_exempt.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg025_fires() -> None:
    entry = spec("SST-CFG999", Severity.ERROR, "Title", "{value}", None, strict_exempt=True)
    refused(
        lambda: build_registry((entry,)), "SST-REG025", "'SST-CFG999' declares strict_exempt but its severity is ERROR"
    )


def test_sst_reg025_silent() -> None:
    entry = spec("SST-CFG899", Severity.WARNING, "Title", "{value}", None, strict_exempt=True)
    assert build_registry((entry,))["SST-CFG899"].strict_exempt
    assert ERROR_REGISTRY["SST-PLN102"].strict_exempt
    assert not ERROR_REGISTRY["SST-PLN014"].strict_exempt

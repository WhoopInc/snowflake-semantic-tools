"""SST-REG013: a code's declared subsystem is not the area its letters name.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg013_fires() -> None:
    from dataclasses import replace

    entry = replace(spec("SST-ABC001", Severity.ERROR, "Title", "{value}", None), subsystem="ABD")
    refused(lambda: build_registry((entry,)), "SST-REG013", "'SST-ABC001' declares subsystem ABD")


def test_sst_reg013_silent() -> None:
    assert all(entry.subsystem == code[4:7] for code, entry in ERROR_REGISTRY.items())

"""SST-REG012: a code does not match the SST-<AREA><NNN> scheme.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg012_fires() -> None:
    entry = spec("SST-ABC01", Severity.ERROR, "Title", "{value}", None)
    context = refused(lambda: build_registry((entry,)), "SST-REG012", "'SST-ABC01' does not match SST-<AREA><NNN>")
    assert context == {"code": "SST-ABC01"}


def test_sst_reg012_silent() -> None:
    assert "SST-ABC001" in build_registry((spec("SST-ABC001", Severity.ERROR, "Title", "{value}", None),))
    assert all(code.startswith("SST-") and len(code) == 10 for code in ERROR_REGISTRY)

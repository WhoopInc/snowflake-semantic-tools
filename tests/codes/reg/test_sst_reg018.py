"""SST-REG018: a code is deprecated with no successor.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg018_fires() -> None:
    entry = spec("SST-ABC001", Severity.ERROR, "Title", "{value}", None, deprecated_in="1.1.0")
    refused(lambda: build_registry((entry,)), "SST-REG018", "'SST-ABC001' is deprecated_in 1.1.0 with no superseded_by")


def test_sst_reg018_silent() -> None:
    entry = spec(
        "SST-ABC001", Severity.ERROR, "Title", "{value}", None, deprecated_in="1.1.0", superseded_by="SST-ABC002"
    )
    assert build_registry((entry,))["SST-ABC001"].superseded_by == "SST-ABC002"

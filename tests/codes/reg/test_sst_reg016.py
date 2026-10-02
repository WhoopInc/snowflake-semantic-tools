"""SST-REG016: a message template names a placeholder outside the shared vocabulary.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg016_fires() -> None:
    entry = spec("SST-ABC001", Severity.ERROR, "Title", "{artifact} is {colour}", None)
    refused(
        lambda: build_registry((entry,)),
        "SST-REG016",
        "'SST-ABC001' template names colour, absent from declared params",
    )


def test_sst_reg016_silent() -> None:
    assert "SST-ABC001" in build_registry((spec("SST-ABC001", Severity.ERROR, "Title", "{artifact} is {value}", None),))

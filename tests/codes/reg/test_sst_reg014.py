"""SST-REG014: a 900-band code is not an error, or an INT or REG one is demotable.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity, build_registry
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.registry_types import refused


def test_sst_reg014_fires() -> None:
    entry = spec("SST-INT903", Severity.WARNING, "Title", "{value}", None, demotable=False)
    refused(lambda: build_registry((entry,)), "SST-REG014", "'SST-INT903' is in the 900 band with severity WARNING")


def test_sst_reg014_fires_for_a_demotable_internal_code() -> None:
    entry = spec("SST-REG903", Severity.ERROR, "Title", "{value}", None)
    refused(
        lambda: build_registry((entry,)),
        "SST-REG014",
        "'SST-REG903' is in the 900 band with severity ERROR (demotable)",
    )


def test_sst_reg014_silent() -> None:
    # Outside INT and REG a 900-band error may be demoted, as the catalog declares SST-RND900.
    assert "SST-ABC900" in build_registry((spec("SST-ABC900", Severity.ERROR, "Title", "{value}", None),))
    assert not ERROR_REGISTRY["SST-RND900"].always_error

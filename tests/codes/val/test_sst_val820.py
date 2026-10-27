"""SST-VAL820: a location SST uploads files under does not end in a separator.

A bundle uploads below its version prefix, and every PUT targets the directory of the file it
puts; a location without a trailing `/` would run the file name on from the last segment.
Compile checks each location before anything uploads.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.publication import put_target_diagnostics
from tests.helpers.publications import compiled_skill


def test_sst_val820_fires() -> None:
    [diagnostic] = put_target_diagnostics(
        "skill:month-close", "month-close", ("@DB.S.SKILL_BUNDLES/month-close/GIT_X",)
    )
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL820",
        Severity.ERROR,
        "skill:month-close",
    )
    assert diagnostic.message == (
        "skill 'month-close': PUT target '@DB.S.SKILL_BUNDLES/month-close/GIT_X' does not end in '/'"
    )


def test_sst_val820_silent() -> None:
    extension = compiled_skill()
    assert put_target_diagnostics(extension.artifact_key, extension.name, (extension.release.prefix,)) == ()

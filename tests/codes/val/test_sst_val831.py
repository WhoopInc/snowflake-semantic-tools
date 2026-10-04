"""SST-VAL831: CORTEX EXTENSION DDL surface used.

Each skill compiled for the catalog channel notes the statement surface it publishes through;
without a catalog channel nothing is published that way.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.skills import CompileSkills
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import CATALOG_CHANNEL, skill_catalog


def test_sst_val831_fires() -> None:
    diagnostic = only(CompileSkills(skill_catalog(), CATALOG_CHANNEL).run_result().diagnostics, "SST-VAL831")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "skill 'month-close': publishes as a skill Cortex Extension version, a statement surface the public SQL "
        "reference omits"
    )
    assert diagnostic.subject == "skill:month-close"


def test_sst_val831_silent() -> None:
    assert "SST-VAL831" not in codes(CompileSkills(skill_catalog(), None).run_result().diagnostics)

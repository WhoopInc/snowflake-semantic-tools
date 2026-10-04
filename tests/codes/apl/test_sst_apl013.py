"""SST-APL013: apply --temporary was pointed at a production-like target."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import production_like
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ChangeSet
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered, target
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake

TEMPORARY = ApplyOptions(temporary=True)


def temporary_plan(target_name: str) -> ChangeSet:
    plan = changeset(change(replace(rendered(), temporary=True)))
    return replace(plan, target=replace(target(), name=target_name))


def test_sst_apl013_fires() -> None:
    port = FakeSnowflake()
    result = apply_plan(temporary_plan("prod_us"), port, options=TEMPORARY)
    diagnostic = only(result.diagnostics, "SST-APL013")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v: --temporary against target 'prod_us' is not permitted"
    assert port.scripts == [] and not result.state_written


def test_sst_apl013_silent() -> None:
    assert "SST-APL013" not in codes(apply_plan(temporary_plan("dev"), options=TEMPORARY).diagnostics)
    assert [production_like(name) for name in ("prod", "eu-production", "PRD", "product", "dev_prodlike")] == [
        True,
        True,
        True,
        False,
        False,
    ]

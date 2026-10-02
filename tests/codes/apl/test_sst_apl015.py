"""SST-APL015: a temporary artifact's alias is ignored."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, RenderedArtifact
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered

TEMPORARY = ApplyOptions(temporary=True)


def aliased(*, temporary: bool) -> RenderedArtifact:
    return replace(rendered(), temporary=temporary, desired_alias="production")


def test_sst_apl015_fires() -> None:
    result = apply_plan(changeset(change(aliased(temporary=True))), options=TEMPORARY)
    diagnostic = only(result, "SST-APL015")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:v: alias is meaningless under --temporary and was ignored"
    assert result.success


def test_sst_apl015_silent() -> None:
    assert "SST-APL015" not in codes(apply_plan(changeset(change(aliased(temporary=False)))))

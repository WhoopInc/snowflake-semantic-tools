"""SST-PLN032: under --partial, an artifact with errors, or depending on one, is left out of the plan."""

from __future__ import annotations

from snowflake_semantic_tools.app.partial import partial_split
from snowflake_semantic_tools.domain.diagnostics import D, Severity
from tests.helpers.plan_codes import partial_result as result


def test_sst_pln032_fires() -> None:
    split = partial_split(result(D("SST-REF044", subject="semantic_view:menu", artifact="menu", found="x")))
    assert split is not None
    [menu, *_] = [item for item in split.notices if item.subject == "semantic_view:menu"]
    assert (menu.code, menu.severity) == ("SST-PLN032", Severity.INFO)
    assert menu.message == (
        "semantic_view:menu has errors, or depends on something that does, so this partial run leaves it unpublished"
    )
    assert [item.subject for item in split.notices] == ["agent:analyst", "eval:analyst", "semantic_view:menu"]


def test_sst_pln032_silent() -> None:
    split = partial_split(result(D("SST-VAL804", subject="skill:x", artifact="skill:x", value="SST_1")))
    assert split is not None and split.notices == ()

from __future__ import annotations

from snowflake_semantic_tools.domain.model.lifecycle.member import Definition, Member
from snowflake_semantic_tools.domain.plan.member_fingerprints import changed_members, member_fingerprints


def test_each_member_is_fingerprinted_by_its_properties_under_its_casefolded_key() -> None:
    before = Definition(
        (
            Member("metric", "ORDERS.Revenue", (("expression", "SUM(X)"), ("synonyms", ("sales", "income")))),
            Member("dimension", "orders.state", (("expression", "STATE"),)),
        )
    )
    after = Definition(
        (
            Member("metric", "orders.revenue", (("expression", "SUM(X)"), ("synonyms", ("sales", "income")))),
            Member("dimension", "orders.state", (("expression", "UPPER(STATE)"),)),
            Member("filter", "is_big", (("expression", "X > 1"),)),
        )
    )
    old, new = member_fingerprints(before), member_fingerprints(after)
    assert [key for key, _ in old] == ["dimension:orders.state", "metric:orders.revenue"]
    assert dict(old)["metric:orders.revenue"] == dict(new)["metric:orders.revenue"]
    assert changed_members(old, new) == ("dimension:orders.state", "filter:is_big")
    assert changed_members(new, old) == ("dimension:orders.state", "filter:is_big")
    assert changed_members(old, old) == ()

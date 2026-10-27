"""SST-MEM013: one membership function gave two answers for a member only table membership places.

The attachment code cannot produce this from a project, so the fires test hands the check
an answer that breaks the rule; the silent test runs the check on what attachment produced.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership import invariant_diagnostics
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership, request

MEMBER = member("metric", "m", ("products",))


def test_sst_mem013_fires() -> None:
    found = invariant_diagnostics(
        request(MEMBER), {MEMBER.key: ("semantic_view:menu",)}, {MEMBER.key: ()}, {MEMBER.key: ()}
    )
    [diagnostic] = coded(found, "SST-MEM013")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:m attached to semantic_view:menu then nothing"
    assert diagnostic.subject == "metric:m"


def test_sst_mem013_silent() -> None:
    assert coded(membership(MEMBER).diagnostics, "SST-MEM013") == []

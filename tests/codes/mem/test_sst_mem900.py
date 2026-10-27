"""SST-MEM900: running attachment a second time gave a different answer.

The attachment code cannot produce this from a project, so the fires test hands the check
an answer that breaks the rule; the silent test runs the check on what attachment produced.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership import invariant_diagnostics
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership, request

MEMBER = member("metric", "m", ("orders",))


def test_sst_mem900_fires() -> None:
    found = invariant_diagnostics(
        request(MEMBER), {MEMBER.key: ("semantic_view:menu",)}, {MEMBER.key: ("semantic_view:menu",)}, {MEMBER.key: ()}
    )
    [diagnostic] = coded(found, "SST-MEM900")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "attachment for metric:m changed on a second pass"
    assert diagnostic.subject == "metric:m"


def test_sst_mem900_silent() -> None:
    assert coded(membership(MEMBER).diagnostics, "SST-MEM900") == []

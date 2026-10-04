"""SST-MEM106: a verified query was placed other than where the one membership function put it.

The attachment code cannot produce this from a project, so the fires test hands the check
an answer that breaks the rule; the silent test runs the check on what attachment produced.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership import invariant_diagnostics
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership, request

MEMBER = member("verified_query", "q", ("orders",))


def test_sst_mem106_fires() -> None:
    found = invariant_diagnostics(
        request(MEMBER),
        {MEMBER.key: ("semantic_view:menu", "semantic_view:sales")},
        {MEMBER.key: ("semantic_view:menu",)},
        {MEMBER.key: ("semantic_view:menu",)},
    )
    [diagnostic] = coded(found, "SST-MEM106")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "verified_query:q used a verified-query-specific attachment path"
    assert diagnostic.subject == "verified_query:q"


def test_sst_mem106_silent() -> None:
    assert coded(membership(MEMBER).diagnostics, "SST-MEM106") == []

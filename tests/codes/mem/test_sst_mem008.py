"""SST-MEM008: an attachment places a member on a view that lacks one of its tables.

The attachment code cannot produce this from a project, so the fires test hands the check
an answer that breaks the rule; the silent test runs the check on what attachment produced.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.membership import invariant_diagnostics
from tests.helpers.resolve_builders import coded, member, membership, request

MEMBER = member("metric", "m", ("products",))


def test_sst_mem008_fires() -> None:
    found = invariant_diagnostics(
        request(MEMBER), {}, {MEMBER.key: ("semantic_view:sales",)}, {MEMBER.key: ("semantic_view:sales",)}
    )
    [diagnostic] = coded(found, "SST-MEM008")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:m attaches to semantic_view:sales, which lacks table 'products'"
    assert diagnostic.subject == "metric:m"


def test_sst_mem008_silent() -> None:
    assert coded(membership(MEMBER).diagnostics, "SST-MEM008") == []

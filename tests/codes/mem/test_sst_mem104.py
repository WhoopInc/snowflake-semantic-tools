"""SST-MEM104: a view whose tables are all dbt models holds no dimension, fact or metric."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership


def test_sst_mem104_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("customers",))).diagnostics, "SST-MEM104")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:menu resolves no members"
    assert diagnostic.subject == "semantic_view:menu"


def test_sst_mem104_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",))).diagnostics, "SST-MEM104") == []

"""SST-MEM002: a member declares no tables and its expression references none to infer."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, member, membership


def test_sst_mem002_fires() -> None:
    [diagnostic, *_] = coded(membership(member("filter", "f", ())).diagnostics, "SST-MEM002")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "filter:f declares no tables: and none can be inferred"
    assert diagnostic.subject == "filter:f"


def test_sst_mem002_silent() -> None:
    assert coded(membership(member("filter", "f", (), "{{ ref('orders', 'id') }} > 0")).diagnostics, "SST-MEM002") == []

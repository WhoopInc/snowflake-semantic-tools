"""SST-VAL617: a search service indexes a relation dbt rebuilds every run."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import compile_tools, search_member
from tests.helpers.diagnostic_filters import coded


def test_sst_val617_fires() -> None:
    [diagnostic] = coded(compile_tools((search_member(),), product_docs="table").diagnostics, "SST-VAL617")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "tool member 'docs_search' indexes DB.MARTS.PRODUCT_DOCS, materialized 'table' -- every dbt run rebuilds "
        "the relation, disabling change tracking and forcing a full re-embed"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val617_silent() -> None:
    assert coded(compile_tools((search_member(),), product_docs="incremental").diagnostics, "SST-VAL617") == []
    full = search_member(refresh_mode="FULL")
    assert coded(compile_tools((full,), product_docs="table").diagnostics, "SST-VAL617") == []

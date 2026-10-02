"""SST-VAL617: a search service indexes a relation dbt rebuilds every run."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import compile_tools, found, search_member


def test_sst_val617_fires() -> None:
    [diagnostic] = found(compile_tools((search_member(),), product_docs="table").diagnostics, "SST-VAL617")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "tool member 'docs_search' indexes DB.MARTS.PRODUCT_DOCS, materialized 'table' -- every dbt run rebuilds "
        "the relation, disabling change tracking and forcing a full re-embed"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val617_silent() -> None:
    assert found(compile_tools((search_member(),), product_docs="incremental").diagnostics, "SST-VAL617") == []
    full = search_member(refresh_mode="FULL")
    assert found(compile_tools((full,), product_docs="table").diagnostics, "SST-VAL617") == []

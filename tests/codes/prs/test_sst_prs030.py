"""SST-PRS030: a synonym holds a character it may not, or enrich refused a proposed one.

Every synonym is checked, whoever wrote it: a metric's in SST YAML, and a dbt model's or
column's in `meta.sst`. A quote is not refused, since the DDL escapes it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import rejected_synonyms
from tests.helpers.reference_project import edited_manifest, project_copy, reported, set_column_meta, set_model_meta


def test_sst_prs030_fires() -> None:
    [diagnostic] = rejected_synonyms(["gross\ntotal"], artifact="orders.amount", subject="dbt_model:orders", limit=4)
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRS030", Severity.WARNING)
    assert diagnostic.message == "orders.amount: synonym 'gross\\u000atotal' contains control characters"
    assert diagnostic.subject == "dbt_model:orders"


def test_sst_prs030_silent() -> None:
    assert rejected_synonyms(["gross total"], artifact="orders.amount", subject="dbt_model:orders", limit=4) == ()


def _dbt_synonyms(tmp_path: Path, synonym: str) -> list[str]:
    """The PRS030 messages a reference copy reports with `synonym` on a dbt model and on one of its columns."""

    def edit(document: dict[str, Any]) -> None:
        set_column_meta(document, "customers", "customer_type", synonyms=[synonym])
        set_model_meta(document, "customers", synonyms=[synonym])

    manifest = edited_manifest(tmp_path, edit)
    return [item.message for item in reported(project_copy(tmp_path), "SST-PRS030", manifest)]


def test_a_dbt_model_or_column_synonym_with_a_newline_is_reported(tmp_path: Path) -> None:
    assert sorted(_dbt_synonyms(tmp_path, "two\nlines")) == [
        "dbt_column:customers.customer_type: synonym 'two\\u000alines' contains control characters",
        "dbt_model:customers: synonym 'two\\u000alines' contains control characters",
    ]


def test_a_dbt_synonym_with_a_quote_is_not_reported(tmp_path: Path) -> None:
    assert _dbt_synonyms(tmp_path, "repeat buyer's status") == []

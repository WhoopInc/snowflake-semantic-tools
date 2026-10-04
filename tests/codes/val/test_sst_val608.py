"""SST-VAL608: a defined search service's on: names no dbt model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, search_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val608_fires() -> None:
    [diagnostic] = coded(checked_tools(group("platform", search_member(on_model="product_notes"))), "SST-VAL608")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'docs_search': on: 'product_notes' is not a model in the dbt manifest"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val608_silent() -> None:
    assert coded(checked_tools(group("platform", search_member())), "SST-VAL608") == []

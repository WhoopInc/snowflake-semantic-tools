"""SST-VAL609: a search column is not on the service's model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, search_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools


def test_sst_val609_fires() -> None:
    [diagnostic] = coded(checked_tools(group("platform", search_member(attribute_columns=("REGION",)))), "SST-VAL609")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'docs_search': 'REGION' is not on 'product_docs'"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val609_silent() -> None:
    assert coded(checked_tools(group("platform", search_member(attribute_columns=("DOC_ID",)))), "SST-VAL609") == []

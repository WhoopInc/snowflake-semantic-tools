"""SST-VAL322: two views describe one table differently."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.semantic_projects import found, with_view

VIEW = """  - name: {name}
    description: |-
      Use this view for questions about customers.
    tables:
      - "{{{{ ref('customers') }}}}"
    table_config:
      customers:
        description: {description}
"""


def test_sst_val322_fires(tmp_path: Path) -> None:
    views = VIEW.format(name="a", description="People who ordered.") + VIEW.format(name="b", description="Accounts.")
    [diagnostic] = found(with_view(tmp_path, views), "SST-VAL322")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:a and semantic_view:b share 'customers' with conflicting descriptions"
    assert diagnostic.subject == "semantic_view:b"


def test_sst_val322_silent(tmp_path: Path) -> None:
    views = VIEW.format(name="a", description="People who ordered.") + VIEW.format(
        name="b", description="People who ordered."
    )
    assert found(with_view(tmp_path, views), "SST-VAL322") == []

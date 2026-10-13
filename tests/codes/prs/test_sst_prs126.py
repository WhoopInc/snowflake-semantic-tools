"""SST-PRS126: a key is written that SST now works out for itself."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, metric_file

EXPR = "    expr: \"COUNT({{ ref('products', 'products_id') }})\"\n"


def test_sst_prs126_fires(tmp_path: Path) -> None:
    files = metric_file(EXPR + "    tables: [\"{{ ref('products') }}\"]\n")
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS126")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "metric:total: 'tables' is no longer written: a metric's tables are the models its expression names with ref()"
    )
    assert diagnostic.subject == "metric:total"


@pytest.mark.parametrize(
    ("root", "entry", "key"),
    [
        ("snowflake_metrics", "    expr: \"{{ metric('a') }} + 1\"\n    derived: true\n", "derived"),
        ("snowflake_filters", "    expr: \"{{ ref('products', 'products_id') }} > 0\"\n    tables: []\n", "tables"),
        (
            "snowflake_verified_queries",
            "    question: How many?\n    sql: \"SELECT 1 FROM {{ ref('products') }}\"\n    tables: []\n",
            "tables",
        ),
    ],
)
def test_sst_prs126_fires_for_every_inferred_key(tmp_path: Path, root: str, entry: str, key: str) -> None:
    files = {"semantic_models/m.yml": f"{root}:\n  - name: one\n    description: One.\n{entry}"}
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS126")
    assert f"'{key}' is no longer written" in diagnostic.message


def test_sst_prs126_silent(tmp_path: Path) -> None:
    files = metric_file(EXPR)
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS126") == []

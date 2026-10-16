"""SST-PRS006: a view lists one model as a table twice."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, view_file


def test_sst_prs006_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(
        SmallProject(tmp_path, files=view_file("      - \"{{ ref('products') }}\"\n")).load().diagnostics, "SST-PRS006"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "table 'products' is declared more than once"
    assert diagnostic.subject == "semantic_view:catalog"


def test_sst_prs006_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=view_file("")).load().diagnostics, "SST-PRS006") == []


VARIABLES = (
    "    variables:\n"
    "      - name: rate\n        data_type: NUMBER\n        default_value: 1\n"
    "      - name: {second}\n        data_type: NUMBER\n        default_value: 2\n"
)


@pytest.mark.parametrize("second", ["RATE", "rate"])
def test_sst_prs006_fires_for_a_repeated_variable_whatever_its_case(tmp_path: Path, second: str) -> None:
    files = view_file(VARIABLES.format(second=second))
    [diagnostic] = coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS006")
    assert diagnostic.message == f"variable '{second}' is declared more than once"
    assert diagnostic.subject == "semantic_view:catalog"
    assert diagnostic.origin is not None and diagnostic.origin.line == 10


def test_sst_prs006_silent_for_distinct_variables(tmp_path: Path) -> None:
    files = view_file(VARIABLES.format(second="rates"))
    assert coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS006") == []

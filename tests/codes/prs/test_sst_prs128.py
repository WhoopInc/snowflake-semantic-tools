"""SST-PRS128: a metric window's frame starts after it ends, which Snowflake refuses."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.parse.frame import frame_order_problem
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import ORDER_BY, SmallProject, metric_file


def _reported(tmp_path: Path, frame: str) -> list[Diagnostic]:
    files = metric_file("    expr: SUM(x)\n" + ORDER_BY + f"      frame: {frame}\n")
    return coded(SmallProject(tmp_path, files=files).load().diagnostics, "SST-PRS128")


def test_sst_prs128_fires(tmp_path: Path) -> None:
    [diagnostic] = _reported(tmp_path, "ROWS BETWEEN 5 FOLLOWING AND 2 PRECEDING")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "metric:total: window frame 'ROWS BETWEEN 5 FOLLOWING AND 2 PRECEDING' is refused: its start "
        "bound '5 FOLLOWING' comes after its end bound '2 PRECEDING'"
    )
    assert diagnostic.subject == "metric:total"


def test_sst_prs128_silent(tmp_path: Path) -> None:
    assert _reported(tmp_path, "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW") == []
    # A frame that is no frame clause is SST-PRS124's alone.
    assert _reported(tmp_path / "bad", "ROWS 2 PRECEDING") == []


@pytest.mark.parametrize(
    ("frame", "problem"),
    [
        ("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", None),
        ("ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING", None),
        (
            "ROWS BETWEEN 2 PRECEDING AND 5 PRECEDING",
            "its start bound '2 PRECEDING' comes after its end bound '5 PRECEDING'",
        ),
        ("ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING", None),
        (
            "ROWS BETWEEN 5 FOLLOWING AND 2 FOLLOWING",
            "its start bound '5 FOLLOWING' comes after its end bound '2 FOLLOWING'",
        ),
        ("ROWS BETWEEN CURRENT ROW AND CURRENT ROW", None),
        (
            "ROWS BETWEEN CURRENT ROW AND 1 PRECEDING",
            "its start bound 'CURRENT ROW' comes after its end bound '1 PRECEDING'",
        ),
        ("ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING", "a frame cannot start at UNBOUNDED FOLLOWING"),
        ("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING", "a frame cannot end at UNBOUNDED PRECEDING"),
        ("RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 day' PRECEDING", None),
        (
            "RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '7 days' PRECEDING",
            "its start bound 'INTERVAL '1 day' PRECEDING' comes after its end bound 'INTERVAL '7 days' PRECEDING'",
        ),
        # Units that differ on one side, or an interval this cannot read, are Snowflake's to check.
        ("RANGE BETWEEN INTERVAL '1 week' PRECEDING AND INTERVAL '3 days' PRECEDING", None),
        ("RANGE BETWEEN INTERVAL '1-2' PRECEDING AND CURRENT ROW", None),
        (
            "RANGE BETWEEN INTERVAL '1 day' FOLLOWING AND INTERVAL '3 hours' PRECEDING",
            "its start bound 'INTERVAL '1 day' FOLLOWING' comes after its end bound 'INTERVAL '3 hours' PRECEDING'",
        ),
        ("not a frame", None),
    ],
)
def test_each_bound_pair_is_ordered_as_snowflake_orders_it(frame: str, problem: str | None) -> None:
    assert frame_order_problem(frame) == problem

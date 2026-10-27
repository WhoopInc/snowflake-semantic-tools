"""SST-DBT028: `dbt parse` exits non-zero; dbt's own output is passed on unchanged."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt028_fires(tmp_path: Path) -> None:
    echoed: list[str] = []
    dbt = FakeDbt(parse=CompletedRun(2, "Compilation Error in model orders\n", ""))
    diagnostic = refused(tmp_path, dbt, echo=echoed.append)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT028", Severity.ERROR)
    assert diagnostic.message == "dbt exited 2: Compilation Error in model orders"
    assert echoed == ["Compilation Error in model orders\n"]
    # Without a line naming an error the last lines are quoted, and a long quote is cut short.
    for output, quoted in (("", "it printed nothing"), ("a\nb\nc\nd\n", "b | c | d")):
        silent = refused(tmp_path, FakeDbt(parse=CompletedRun(1, output, "")), echo=echoed.append)
        assert silent.message == f"dbt exited 1: {quoted}"
    long = refused(tmp_path, FakeDbt(parse=CompletedRun(1, "Error " + "x" * 500, "")), echo=echoed.append)
    assert len(long.message) == len("dbt exited 1: ") + 400 and long.message.endswith("...")


def test_sst_dbt028_silent(tmp_path: Path) -> None:
    echoed: list[str] = []
    parse_project(
        tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path), echo=echoed.append
    )
    assert echoed == []

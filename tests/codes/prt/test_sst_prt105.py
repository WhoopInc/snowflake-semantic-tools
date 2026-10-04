"""SST-PRT105: an `sst enrich` selector is not a dbt model selector."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def _refusal(args: list[str]) -> tuple[int, list[dict[str, object]]]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    envelope = json.loads(result.output)
    return result.exit_code, envelope["diagnostics"]


def test_sst_prt105_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = _refusal(["enrich", *common(project_copy(tmp_path)), "--select", "source:raw.orders"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT105", "error")
    assert diagnostic["message"] == "'source:raw.orders' is not a valid dbt model selector"


def test_sst_prt105_silent() -> None:
    from snowflake_semantic_tools.cli.wiring.enrich import model_selectors

    assert model_selectors(("model:orders", "fct_*"), "--select") == ("orders", "fct_*")

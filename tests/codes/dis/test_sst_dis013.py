"""SST-DIS013: `sst enrich` was given a PATH or `--select` value and no dbt model matched.

Enrich selects dbt models, not SST artifacts, so this is its own code rather than SST-DIS010,
whose message names `--select` and an artifact even when the miss was a PATH.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

_ORDER_ITEMS = "SST_REF_DEV.JAFFLE.ORDER_ITEMS"


class _Warehouse(ScriptedEnrich):
    """The scripted reads, plus what connecting asks of a session."""

    def current_role(self) -> str:
        return "ANALYST"

    def current_account_locator(self) -> str:
        return "ACCOUNT"

    def close(self) -> None:
        pass


def _enrich(monkeypatch: pytest.MonkeyPatch, project: Path, *args: str) -> Result:
    port = _Warehouse(columns={_ORDER_ITEMS: [("ORDER_ITEM_ID", "TEXT")]})
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    command = ["enrich", *args, "--check", "--allow-non-prod", "--project-dir", str(project)]
    return CliRunner().invoke(cli, [*command, "--manifest", str(DBT_MANIFEST), "-o", "json"])


def _dis013(result: Result) -> list[tuple[str, str]]:
    diagnostics = json.loads(result.output)["diagnostics"]
    return [(item["severity"], item["message"]) for item in diagnostics if item["code"] == "SST-DIS013"]


def test_sst_dis013_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    # A pattern: a bare model name that is not in the manifest is SST-DBT002 instead.
    result = _enrich(monkeypatch, project, "--select", "nope_*")
    assert result.exit_code == 4
    assert _dis013(result) == [("error", "nope_* matched no dbt model")]

    # A PATH that holds no model is named as the PATH it is, not as a --select value.
    (project / "models" / "empty").mkdir()
    result = _enrich(monkeypatch, project, "models/empty")
    assert result.exit_code == 4
    assert _dis013(result) == [("error", "models/empty matched no dbt model")]


def test_sst_dis013_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _enrich(monkeypatch, project_copy(tmp_path), "--select", "order_items")
    assert _dis013(result) == []
    assert "SST-DIS010" not in result.output

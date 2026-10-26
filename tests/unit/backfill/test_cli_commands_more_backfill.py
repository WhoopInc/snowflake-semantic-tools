"""More `sst` commands at their edges: diff's references and selectors, drop, docs, init, and enrich."""

from __future__ import annotations

import json
import stat
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands import drop as drop_command
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.artifact_builders import applied_entry
from tests.helpers.cli_projects import common, compile_project, invoke_with_port, on_a_terminal
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def _codes(output: str) -> list[str]:
    return [item["code"] for item in json.loads(output)["diagnostics"]]


def test_diff_against_a_saved_plan_path_that_is_a_folder_cannot_read_it(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    (project / "plans.json").mkdir()
    unreadable = CliRunner().invoke(cli, ["diff", *common(project), "--to", "plans.json", "-o", "json"])
    assert unreadable.exit_code == 1, unreadable.output
    assert _codes(unreadable.output) == ["SST-PRT009"]


def test_diff_of_two_targets_selects_with_no_compiled_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={"skill:bundle": applied_entry("DB.S.BUNDLE", "3" * 64)})
    result = invoke_with_port(
        monkeypatch, port, ["diff", *common(project), "--from", "dev", "--to", "prod", "--select", "type:skill"]
    )
    assert result.exit_code == 0, result.output
    assert "dev and prod agree" in result.output


def test_diff_walks_a_graph_operator_over_the_compiled_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = FakeSnowflake(state={})
    names = invoke_with_port(
        monkeypatch, port, ["diff", *common(project), "--select", "+agent:jaffle_minimal_agent", "--names-only"]
    )
    assert names.exit_code == 2, names.output
    assert "jaffle_minimal_agent" in names.stdout.splitlines()


VIEW = "SST_REF_DEV.JAFFLE.ORDERS"


def _drop(monkeypatch: pytest.MonkeyPatch, project: Path) -> str:
    args = ["drop", VIEW, "--type", "semantic_view", "--target", "dev", "--yes", "--project-dir", str(project)]
    result = invoke_with_port(monkeypatch, FakeSnowflake(existing=(VIEW,)), [*args, "-o", "json"])
    assert result.exit_code == 4, result.output
    return result.output


def test_drop_reports_a_dbt_project_file_it_cannot_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    unreadable = tmp_path / "dbt_project.yml"
    unreadable.write_text("name: p\nprofile: p\n", encoding="utf-8")
    unreadable.chmod(0)
    try:
        assert _codes(_drop(monkeypatch, tmp_path)) == ["SST-PRT009"]
    finally:
        unreadable.chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_drop_passes_on_a_dbt_project_refusal_other_than_a_missing_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: [\n", encoding="utf-8")
    codes = _codes(_drop(monkeypatch, tmp_path))
    assert codes and "SST-CFG056" not in codes


def test_the_lock_names_an_unknown_user_when_the_system_cannot_say(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_user() -> str:
        raise KeyError("getpwuid(): uid not found")

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.drop.getpass.getuser", no_user)
    assert drop_command._os_user() == "unknown"


def test_docs_writes_its_pages_into_the_output_folder_given(tmp_path: Path) -> None:
    out = tmp_path / "pages"
    written = CliRunner().invoke(
        cli, ["docs", "--project-dir", str(tmp_path), "--only", "config", "--output-dir", str(out), "-o", "json"]
    )
    assert written.exit_code == 0, written.output
    assert [path.parent for path in out.iterdir()] == [out]
    assert not (tmp_path / "docs").exists()


def test_init_asks_for_the_semantic_models_folder_on_a_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    on_a_terminal(monkeypatch)
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(tmp_path)], input="semantics\n")
    assert initialized.exit_code == 0, initialized.output
    assert (tmp_path / "semantics").is_dir()
    assert "semantics" in (tmp_path / "sst_config.yml").read_text(encoding="utf-8")


class _Warehouse(ScriptedEnrich):
    def current_role(self) -> str:
        return "ANALYST"

    def current_account_locator(self) -> str:
        return "ACCOUNT"

    def close(self) -> None:
        return None


@pytest.fixture
def read_only_marts(tmp_path: Path) -> Iterator[Path]:
    project = project_copy(tmp_path)
    marts = project / "models" / "marts"
    mode = marts.stat().st_mode
    marts.chmod(stat.S_IRUSR | stat.S_IXUSR)
    yield project
    marts.chmod(mode)


def test_enrich_reports_a_file_it_cannot_write(read_only_marts: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    columns = [("ORDER_ITEM_ID", "TEXT"), ("ORDER_ID", "TEXT"), ("PRODUCT_ID", "TEXT"), ("NEW_COLUMN", "NUMBER")]
    port = _Warehouse(columns={"SST_REF_DEV.JAFFLE.ORDER_ITEMS": columns})
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    args = ["--select", "order_items", "--allow-non-prod", "--project-dir", str(read_only_marts)]
    result = CliRunner().invoke(cli, ["enrich", *args, "--manifest", str(DBT_MANIFEST), "-o", "json"])
    assert result.exit_code == 1, result.output
    assert _codes(result.output) == ["SST-PRT008"]

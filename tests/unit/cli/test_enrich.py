"""`sst enrich` on the command line, over a copy of the reference project and a scripted warehouse."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

ORDER_ITEMS = "SST_REF_DEV.JAFFLE.ORDER_ITEMS"
ORDER_ITEMS_COLUMNS = [
    ("ORDER_ITEM_ID", "TEXT"),
    ("ORDER_ID", "TEXT"),
    ("PRODUCT_ID", "TEXT"),
    ("OCCURRED_AT", "TIMESTAMP_NTZ"),
    ("ITEM_PRICE", "NUMBER"),
    ("QUANTITY", "NUMBER"),
]
YAML = Path("models/marts/order_items.yml")


class _Warehouse(ScriptedEnrich):
    """The scripted reads, plus what connecting asks of a session."""

    closed = 0

    def current_role(self) -> str:
        return "ANALYST"

    def current_account_locator(self) -> str:
        return "ACCOUNT"

    def close(self) -> None:
        self.closed += 1


def _invoke(
    monkeypatch: pytest.MonkeyPatch, project: Path, port: object, *args: str, allow_non_prod: bool = True
) -> Result:
    opened: list[object] = []

    def connector(params: object) -> object:
        opened.append(params)
        if isinstance(port, Exception):
            raise port
        return port

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connector)
    # The reference project's default target is `dev`, which enrich refuses without the flag.
    flags = ["--allow-non-prod"] if allow_non_prod else []
    command = ["enrich", *args, *flags, "--project-dir", str(project), "--manifest", str(DBT_MANIFEST)]
    result = CliRunner().invoke(cli, command)
    result.opened = opened  # type: ignore[attr-defined]
    return result


@pytest.fixture
def project(tmp_path: Path) -> Path:
    return project_copy(tmp_path)


def _port() -> _Warehouse:
    return _Warehouse(columns={ORDER_ITEMS: ORDER_ITEMS_COLUMNS})


def test_enrich_adds_the_relations_new_column_and_reports_it(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    before = (project / YAML).read_text(encoding="utf-8")
    port = _port()
    result = _invoke(monkeypatch, project, port, "--select", "model:order_items")
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "order_items: 1 column added, column-types 1, data-types 1",
        "wrote models/marts/order_items.yml",
        "1 file written; 1 model read, 0 failed",
    ]
    after = (project / YAML).read_text(encoding="utf-8")
    assert after.startswith(before.rstrip("\n").rsplit("\n", 1)[0][:200])
    assert "      - name: quantity\n        config:\n          meta:\n            sst:\n" in after
    assert "              column_type: fact\n              data_type: NUMBER\n" in after
    assert port.closed == 1
    # The fixture manifest predates the first run, so the second run derives the same updates;
    # the writer finds them already written, and the file is left byte for byte as it is.
    again = _invoke(monkeypatch, project, _port(), "--select", "order_items", "--check")
    assert again.exit_code == 0, again.output
    assert "would write" not in again.output and (project / YAML).read_text(encoding="utf-8") == after


def test_check_and_dry_run_write_nothing(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    before = (project / YAML).read_text(encoding="utf-8")
    checked = _invoke(monkeypatch, project, _port(), "models/marts/order_items.yml", "--check")
    assert checked.exit_code == 2, checked.output
    assert "would write models/marts/order_items.yml" in checked.output
    quiet = _invoke(monkeypatch, project, _port(), "--select", "order_items", "--check", "--no-detailed-exitcode")
    assert quiet.exit_code == 0, quiet.output
    diff = _invoke(monkeypatch, project, _port(), "--select", "order_items", "--dry-run")
    assert diff.exit_code == 0, diff.output
    assert "--- a/models/marts/order_items.yml\n+++ b/models/marts/order_items.yml\n" in diff.output
    assert "+      - name: quantity\n" in diff.output
    assert (project / YAML).read_text(encoding="utf-8") == before


def test_the_json_report_lists_models_files_and_components(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    result = _invoke(
        monkeypatch,
        project,
        _port(),
        "--select",
        "order_items",
        "--include",
        "column-types",
        "--check",
        "--output",
        "json",
    )
    assert result.exit_code == 2, result.output
    envelope = json.loads(result.output)
    data = envelope["data"]
    assert data["components"] == ["column-types"] and data["forced"] == [] and data["written"] == []
    assert data["models"] == [
        {
            "model": "order_items",
            "path": "models/marts/order_items.yml",
            "status": "enriched",
            "added": ["quantity"],
            "filled": {"column-types": 1},
            "table_synonyms": [],
        }
    ]
    assert data["files"] == [
        {"path": "models/marts/order_items.yml", "changed": True, "created": False, "reformatted": False}
    ]


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["-m", "orders"], "-m is reserved: it meant --models in SST 0.3; use --select"),
        (["--models", "orders"], "--models was removed in SST 1.0; pass --select model:<name> once per model"),
        (["--all"], "--all was removed in SST 1.0; use --include all"),
        (["-syn"], "--synonyms/-syn was removed in SST 1.0; use --include synonyms"),
        (["--include", "colour"], "--include: unknown component 'colour'"),
        (["--select", "source:raw.orders"], "sst enrich selects dbt models only"),
        (["--select", "a,b"], "selector 'a,b' contains a comma"),
        (["--exclude", "models/staging"], "--exclude selects models by name"),
        (["--select", "model:"], "names no model"),
        (["--database", "bad name"], "--database: invalid unquoted identifier"),
        (["/elsewhere"], "is not inside the project directory"),
    ],
)
def test_usage_errors_exit_3_before_connecting(
    monkeypatch: pytest.MonkeyPatch, project: Path, args: list[str], message: str
) -> None:
    result = _invoke(monkeypatch, project, _port(), *args)
    assert result.exit_code == 3, result.output
    assert message in result.output
    assert result.opened == []  # type: ignore[attr-defined]


def test_a_target_that_is_not_production_like_is_refused_without_allow_non_prod(
    monkeypatch: pytest.MonkeyPatch, project: Path
) -> None:
    refused = _invoke(monkeypatch, project, _port(), "--select", "order_items", "-o", "json", allow_non_prod=False)
    assert refused.exit_code == 3, refused.output
    [diagnostic] = json.loads(refused.stdout)["diagnostics"]
    assert (diagnostic["code"], diagnostic["severity"]) == ("SST-PRT100", "error")
    assert "target 'dev', which is not production-like" in diagnostic["message"]
    assert refused.opened == []  # type: ignore[attr-defined]
    before = (project / YAML).read_text(encoding="utf-8")
    allowed = _invoke(monkeypatch, project, _port(), "--select", "order_items", "--check")
    assert allowed.exit_code == 2, allowed.output
    production = _invoke(
        monkeypatch, project, _port(), "--select", "order_items", "--target", "prod", "--check", allow_non_prod=False
    )
    assert production.exit_code == 2, production.output
    assert (project / YAML).read_text(encoding="utf-8") == before


def test_a_selection_naming_no_model_exits_1_without_connecting(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    result = _invoke(monkeypatch, project, _port(), "--select", "model:nope", ".")
    assert result.exit_code == 1, result.output
    assert "error[SST-DBT002]: model 'nope' is not in the dbt manifest" in result.output


def test_a_pattern_matching_no_model_exits_4_without_connecting(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    result = _invoke(monkeypatch, project, _port(), "--select", "model:nope_*", ".")
    assert result.exit_code == 4, result.output
    assert "no dbt model of the project" in result.output
    assert result.opened == []  # type: ignore[attr-defined]


def test_a_refused_collection_exits_1_without_connecting(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8") + "\nenrichment:\n  allow_sample_value_collection: false\n", encoding="utf-8"
    )
    result = _invoke(monkeypatch, project, _port(), "--include", "sample-values")
    assert result.exit_code == 1, result.output
    assert "SST-CFG038" in result.output
    assert result.opened == []  # type: ignore[attr-defined]


def test_a_missing_relation_fails_its_model_and_exits_1(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    result = _invoke(monkeypatch, project, _Warehouse(), "--select", "order_items", "--fail-fast")
    assert result.exit_code == 1, result.output
    assert "SST-SNO030" in result.output and "order_items: failed" in result.output
    assert "stopped at the first failure; nothing was written" in result.output


def test_a_connection_failure_exits_5(monkeypatch: pytest.MonkeyPatch, project: Path) -> None:
    failure = SnowflakePortError("refused", diagnostic=D("SST-PRT001", value="ACCOUNT", detail="refused"))
    result = _invoke(monkeypatch, project, failure, "--select", "order_items")
    assert result.exit_code == 5, result.output


def test_every_component_reads_values_and_asks_cortex_through_one_session(
    monkeypatch: pytest.MonkeyPatch, project: Path
) -> None:
    relation = "SST_REF_DEV.JAFFLE.LOCATIONS"
    columns = [
        ("LOCATION_ID", "TEXT"),
        ("LOCATION_NAME", "TEXT"),
        ("OPENED_AT", "TIMESTAMP_NTZ"),
        ("TAX_RATE", "NUMBER"),
    ]
    columns.append(("MANAGER", "TEXT"))

    def answer(prompt: str, schema: object) -> object:
        if "for the columns of one table" in prompt:
            return {"columns": [{"name": "manager", "synonyms": ["store manager"]}]}
        return {"synonyms": ["store", "shop"]}

    port = _Warehouse(columns={relation: columns}, values={relation: {"LOCATION_ID": ["L1", "L2"]}}, answer=answer)
    result = _invoke(monkeypatch, project, port, "--select", "locations", "--include", "all", "--dry-run")
    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0].startswith("locations: ")
    assert "table synonyms in 1 view" in result.output.splitlines()[0]
    assert [call[0] for call in port.calls] == ["columns", "values", "cortex", "cortex"]
    assert "+          - store\n" in result.output and "+                - store manager\n" in result.output
    assert port.closed == 1

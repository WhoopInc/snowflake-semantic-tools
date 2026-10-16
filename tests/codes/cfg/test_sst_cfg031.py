"""SST-CFG031: the configuration file does not say whether expressions are compiled against Snowflake."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import unstated_policy
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import CONFIG, edited


def test_sst_cfg031_fires() -> None:
    [diagnostic] = unstated_policy({"validation": {"strict": True}}, file="sst_config.yaml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG031", Severity.ERROR)
    assert diagnostic.message == "validation.snowflake_syntax_check is unset"
    assert diagnostic.subject == "config:validation.snowflake_syntax_check"
    assert diagnostic.origin is not None and diagnostic.origin.file == "sst_config.yaml"
    assert [item.subject for item in unstated_policy({})] == ["config:validation"]


def test_sst_cfg031_silent() -> None:
    assert unstated_policy({"validation": {"snowflake_syntax_check": False}}) == ()


def test_sst_cfg031_fires_for_the_key_written_with_no_value() -> None:
    # `key:` with nothing after it is unset, so the policy is not stated.
    [diagnostic] = unstated_policy({"validation": {"snowflake_syntax_check": None}})
    assert diagnostic.subject == "config:validation.snowflake_syntax_check"


@pytest.mark.parametrize("strictness", ["--strict", "--no-strict"])
def test_sst_cfg031_stops_validate_before_it_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, strictness: str
) -> None:
    project = edited(tmp_path, CONFIG, "snowflake_syntax_check: false", "snowflake_syntax_check:")
    opened: list[object] = []

    def connector(params: object) -> object:
        opened.append(params)
        raise AssertionError("validate connected")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connector)
    code, diagnostics = validate_json(project, strictness)
    assert (code, opened) == (1, [])
    assert "SST-CFG031" in [item["code"] for item in diagnostics]


def test_sst_cfg031_demoted_leaves_the_default_check_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = edited(tmp_path, CONFIG, "snowflake_syntax_check: false", "snowflake_syntax_check:")
    path = project / CONFIG
    demoted = "    SST-VAL531: warning\n    SST-CFG031: warning\n"
    path.write_text(path.read_text(encoding="utf-8").replace("    SST-VAL531: warning\n", demoted), encoding="utf-8")
    opened: list[object] = []

    def connector(params: object) -> object:
        opened.append(params)
        raise ConnectionError("offline")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connector)
    validate_json(project, "--no-strict")
    # Not blocking, so the documented default holds: the syntax check connects.
    assert len(opened) == 1

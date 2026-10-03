"""0.3 input and inert keys are errors in 1.0, so a lenient run cannot publish a project written for 0.3.

Each case is the smallest change to the reference fixture that writes one of them, run as the
reference project's offline gate runs it: `--no-strict`, so no warning is promoted.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, project_copy

_INSTRUCTION = (
    "snowflake_custom_instructions:\n"
    "  - name: legacy_rounding\n"
    "    description: |-\n"
    "      Uses the 0.3 spelling of the SQL-generation instruction.\n"
    "    sql_generation: Round every monetary amount to two decimal places.\n"
)


def _errors(project: Path) -> tuple[int, Counter[str]]:
    result = CliRunner().invoke(
        cli, ["validate", *common(project), "--no-strict", "--no-snowflake-syntax-check", "--output", "json"]
    )
    payload = json.loads(result.output)
    return result.exit_code, Counter(item["code"] for item in payload["diagnostics"] if item["severity"] == "error")


def _with_instruction(project: Path, text: str) -> Path:
    folder = project / "semantic_models" / "legacy"
    folder.mkdir()
    (folder / "legacy.yml").write_text(text, encoding="utf-8")
    return project


def test_an_instruction_writing_both_spellings_is_an_error(tmp_path: Path) -> None:
    both = _INSTRUCTION + "    ai_sql_generation: Round every monetary amount to two decimal places.\n"
    exit_code, errors = _errors(_with_instruction(project_copy(tmp_path), both))
    assert (exit_code, errors) == (1, Counter({"SST-PRS020": 1}))


def test_an_instruction_in_its_0_3_spelling_alone_is_an_error(tmp_path: Path) -> None:
    exit_code, errors = _errors(_with_instruction(project_copy(tmp_path), _INSTRUCTION))
    assert (exit_code, errors) == (1, Counter({"SST-VAL012": 1}))


def test_the_deploy_alias_is_an_error(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    text = config.read_text(encoding="utf-8")
    config.write_text(text.replace("\napply:\n", "\ndeploy:\n", 1), encoding="utf-8")
    exit_code, errors = _errors(project)
    assert (exit_code, errors) == (1, Counter({"SST-CFG200": 1}))

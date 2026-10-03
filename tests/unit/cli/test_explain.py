"""`sst explain`: offline, project-free, and exit 3 for a code the registry does not know."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli


def _explain(tmp_path: Path, *args: str) -> tuple[int, str]:
    # A broken configuration file in the directory must not matter: explain reads no project.
    (tmp_path / "sst_config.yml").write_text("project: [unclosed\n", encoding="utf-8")
    result = CliRunner().invoke(cli, ["explain", *args, "--project-dir", str(tmp_path)])
    return result.exit_code, result.output


def test_explain_prints_a_registered_code_and_where_it_is_raised(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "sst-val009")
    assert exit_code == 0, output
    assert "SST-VAL009: File is not canonically formatted" in output
    assert "suggestion: run sst format" in output
    assert "raised from: snowflake_semantic_tools.domain.validate.semantic.files" in output


def test_explain_json_carries_the_spec_payload(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "SST-CFG033", "--output", "json")
    data = json.loads(output)["data"]
    assert exit_code == 0
    assert set(data) >= {
        "code",
        "title",
        "severity",
        "phase",
        "condition",
        "message_template",
        "suggestion_template",
        "note",
        "help_url",
        "origin",
        "aliases",
        "raise_sites",
        "retired",
    }
    assert (data["code"], data["severity"], data["non_demotable"], data["retired"]) == (
        "SST-CFG033",
        "error",
        True,
        False,
    )
    assert data["condition"] == "an override breaks the demotion floor, or --strict and strict: disagree"


def test_explain_prints_when_a_code_is_raised_and_its_note(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "SST-VAL326")
    assert exit_code == 0, output
    assert "  raised when: an attached member's `expr:` carries a bare identifier the view cannot resolve\n" in output
    assert "  note: Snowflake would reject the whole CREATE with `invalid identifier`\n" in output
    exit_code, output = _explain(tmp_path, "SST-VAL326", "--output", "json")
    data = json.loads(output)["data"]
    assert data["note"] == "Snowflake would reject the whole CREATE with `invalid identifier`"
    exit_code, output = _explain(tmp_path, "SST-V002", "--output", "json")
    assert (json.loads(output)["data"]["condition"], json.loads(output)["data"]["note"]) == (None, None)


def test_explain_aliases_lists_the_0_3_codes_and_what_they_became(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "SST-REF001", "--aliases")
    assert exit_code == 0
    assert "from SST 0.3: SST-V002" in output
    assert "alias SST-V002 (Unknown table reference): split -> SST-DBT002, SST-REF001, SST-MEM003" in output
    exit_code, output = _explain(tmp_path, "SST-V002")
    assert "SST 0.3 code, split; never raised by 1.0" in output
    assert "now: SST-DBT002, SST-REF001, SST-MEM003" in output
    exit_code, output = _explain(tmp_path, "SST-V081")
    assert "now: nothing; the condition is gone" in output


def test_explain_answers_a_retired_number_from_its_tombstone(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "SST-PRT007")
    assert exit_code == 0
    assert "superseded by: SST-DBT017" in output
    exit_code, output = _explain(tmp_path, "SST-CFG027")
    assert "superseded by: nothing; the condition is gone" in output


def test_explain_reports_a_deprecated_code_and_its_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY

    spec = ERROR_REGISTRY["SST-VAL009"]
    deprecated = dataclasses.replace(spec, deprecated_in="1.1.0", superseded_by="SST-LOD012")
    registry = {**ERROR_REGISTRY, "SST-VAL009": deprecated}
    monkeypatch.setattr("snowflake_semantic_tools.domain.diagnostics.explain.ERROR_REGISTRY", registry)
    exit_code, output = _explain(tmp_path, "SST-VAL009")
    assert exit_code == 0
    assert "deprecated in 1.1.0; use SST-LOD012" in output


def test_explain_refuses_an_unknown_code_with_exit_3(tmp_path: Path) -> None:
    exit_code, output = _explain(tmp_path, "SST-XYZ999")
    assert exit_code == 3
    assert "error[SST-PRT100]: 'SST-XYZ999' is not a registered code" in output
    machine = CliRunner().invoke(cli, ["explain", "nope", "--output", "json"])
    assert machine.exit_code == 3
    assert json.loads(machine.output)["diagnostics"][0]["code"] == "SST-PRT100"

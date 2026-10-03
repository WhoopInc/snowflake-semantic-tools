"""`sst plan` flags that shape what is read and shown: grants, prior definitions, detail, names."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import Result

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, ShowRow
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_projects import common, invoke_with_port, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

VIEW = "jaffle_minimal"
RECORDED = "c" * 64
OTHER_MANIFEST = "d" * 64
DEFINITION = "create or replace semantic view JAFFLE_MINIMAL\n  tables (orders)"


def _target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    """A project, and the target the view it plans is published to."""
    project = project_copy(tmp_path)
    first = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), "-o", "json"])
    change = next(item for item in json.loads(first.stdout)["data"]["changes"] if item["name"] == VIEW)
    return project, str(change["target"])


def _published(target: str) -> FakeSnowflake:
    """Snowflake holding the view as an earlier manifest published it, with one explicit grant."""
    database, schema, name = target.split(".")
    row = ShowRow(name, database, schema, "OWNER", "now", f"[sst:{OTHER_MANIFEST}:{RECORDED}]")
    entry = AppliedEntry(RECORDED, target, "now", "run", "applied", RECORDED, OTHER_MANIFEST)
    return FakeSnowflake(
        objects={("SEMANTIC VIEW", f"{database}.{schema}"): (row,)},
        grants={target: (GrantRow("SELECT", "ROLE", "ANALYST"),)},
        definitions={target: DEFINITION},
        state={f"semantic_view:{VIEW}": entry},
    )


def _plan(project: Path, port: FakeSnowflake, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    return invoke_with_port(monkeypatch, port, ["plan", *common(project), "--select", VIEW, "--no-plan-out", *flags])


def _update(result: Result) -> dict[str, Any]:
    changes = json.loads(result.stdout)["data"]["changes"]
    return next(item for item in changes if item["name"] == VIEW)


def _codes(result: Result) -> set[str]:
    return {item["code"] for item in json.loads(result.stdout)["diagnostics"]}


def test_grants_are_read_by_default_and_no_grants_skips_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, target = _target(tmp_path, monkeypatch)
    read = _plan(project, _published(target), monkeypatch, "-o", "json")
    assert read.exit_code == 2, read.output
    assert _update(read)["action"] == "update"
    assert "SST-PLN013" in _codes(read)
    skipped = _plan(project, _published(target), monkeypatch, "--no-grants", "-o", "json")
    assert skipped.exit_code == 2, skipped.output
    assert "SST-PLN013" not in _codes(skipped)
    assert _codes(_plan(project, _published(target), monkeypatch, "--grants", "-o", "json")) >= {"SST-PLN013"}


def test_capture_prior_reads_the_definition_an_update_replaces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, target = _target(tmp_path, monkeypatch)
    assert _update(_plan(project, _published(target), monkeypatch, "-o", "json"))["prior_definition"] is None
    captured = _plan(project, _published(target), monkeypatch, "--capture-prior", "-o", "json")
    assert _update(captured)["prior_definition"] == DEFINITION
    refused = _published(target)
    refused.definitions.clear()
    unreadable = _plan(project, refused, monkeypatch, "--capture-prior", "-o", "json")
    assert _update(unreadable)["prior_definition"] is None
    assert "SST-PLN001" in _codes(unreadable)


def test_full_names_the_properties_an_update_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, target = _target(tmp_path, monkeypatch)
    update = _update(_plan(project, _published(target), monkeypatch, "-o", "json"))
    assert [item["property"] for item in update["properties"]] == ["fingerprint", "manifest_id"]
    assert update["properties"][0]["before"] == RECORDED
    plain = _plan(project, _published(target), monkeypatch)
    assert "fingerprint:" not in plain.output
    full = _plan(project, _published(target), monkeypatch, "--full", "--capture-prior")
    assert f"    fingerprint: {RECORDED} -> " in full.output
    assert f"    manifest_id: {OTHER_MANIFEST} -> " in full.output
    assert "    prior definition:\n      create or replace semantic view JAFFLE_MINIMAL\n" in full.output


def test_names_only_prints_each_changed_name_and_nothing_else(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, target = _target(tmp_path, monkeypatch)
    result = _plan(project, _published(target), monkeypatch, "--names-only", "--full")
    assert result.exit_code == 2
    assert result.stdout.splitlines() == [VIEW]
    every = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), "--names-only", "--no-plan-out"]
    )
    data = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), "--no-plan-out", "-o", "json"]
    )
    changes = json.loads(data.stdout)["data"]
    assert every.stdout.splitlines() == [item["name"] for item in changes["changes"]]
    assert changes["counts"] == {"create": 14, "update": 0, "noop": 0, "prune": 0, "blocked": 0, "report_only": 0}


def test_no_validate_skips_strict_promotion_and_refuses_a_syntax_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    original = __import__("snowflake_semantic_tools.cli.wiring.compile", fromlist=["compile_result"]).compile_result

    def with_warning(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        return dataclasses.replace(result, diagnostics=DiagnosticBag((D("SST-LOD003", file="warning.yml"),)))

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", with_warning)
    args = ["plan", *common(project), "--strict", "--no-plan-out"]
    assert invoke_with_port(monkeypatch, FakeSnowflake(state={}), args).exit_code == 1
    unvalidated = invoke_with_port(monkeypatch, FakeSnowflake(state={}), [*args, "--no-validate", "-o", "json"])
    assert unvalidated.exit_code == 2, unvalidated.output
    assert "SST-VAL020" not in _codes(unvalidated)
    refused = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), [*args, "--no-validate", "--snowflake-syntax-check"]
    )
    assert refused.exit_code == 3
    assert "SST-PRT104" in refused.output

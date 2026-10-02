"""`sst drop`: one owned object, under the run lock, forgotten in state, and every refusal at exit 3."""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.local import StateFileStore, state_file
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.cli_projects import FIXTURE, invoke_with_port
from tests.helpers.recorded_snowflake import RecordedSnowflake

VIEW = "SST_REF_DEV.JAFFLE.ORDERS"
STATE_TABLE = QualifiedName.parse("SST_REF_DEV.JAFFLE.SST_STATE")
MARKER = OwnershipMarker("a" * 64, "b" * 64)


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    project.mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: p\nprofile: sst_reference_impl\n", encoding="utf-8")
    (project / "profiles.yml").write_text((FIXTURE / "profiles.yml").read_text(encoding="utf-8"), encoding="utf-8")
    return project


def _codes(envelope: dict[str, list[dict[str, str]]]) -> list[str]:
    """The codes reported, less the fixture profile's own warning that it sets no role."""
    return [item["code"] for item in envelope["diagnostics"] if item["code"] != "SST-CFG014"]


def _entry(target: str) -> AppliedEntry:
    return AppliedEntry("b" * 64, target, "then", "run", APPLIED, "b" * 64, "a" * 64)


def _port(*, exists: bool = True, marked: bool = True) -> RecordedSnowflake:
    port = RecordedSnowflake(
        existing=(VIEW,) if exists else (),
        markers={VIEW: MARKER} if marked else {},
        state={"semantic_view:orders": _entry(VIEW), "semantic_view:other": _entry("SST_REF_DEV.JAFFLE.OTHER")},
    )
    port.state_manifest = "a" * 64
    return port


def _drop(monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, project: Path, *extra: str) -> Result:
    args = ["drop", VIEW, "--type", "semantic_view", "--target", "dev", "--yes", "--project-dir", str(project)]
    return invoke_with_port(monkeypatch, port, [*args, *extra])


def test_drop_removes_an_owned_object_and_forgets_it_remote_and_local(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _project(tmp_path)
    cache = StateFileStore(state_file(project / "target" / "sst", "dev"), config_path="sst_config.yml")
    target = QualifiedName.parse(VIEW)
    identity = TargetIdentity("dev", "acct", target.database, target.schema)
    cache.write_local(
        State(1, identity, "a" * 64, "sst_config.yml", None, MappingProxyType({"semantic_view:orders": _entry(VIEW)}))
    )
    port = _port()
    result = _drop(monkeypatch, port, project, "--output", "json", "--quiet")
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data == {
        "object": VIEW,
        "type": "semantic_view",
        "target": "dev",
        "profile": "sst_reference_impl",
        "role": "RECORDED_ROLE",
        "outcome": "dropped",
        "statement": f"DROP SEMANTIC VIEW {VIEW}",
        "state_table": STATE_TABLE.sql,
        "forgotten": ["semantic_view:orders"],
    }
    assert port.scripts == [(f"DROP SEMANTIC VIEW {VIEW}",)]
    assert set(port.state) == {"semantic_view:other"}
    cached = cache.read_local()
    assert cached is not None and "semantic_view:orders" not in cached.applied
    assert port.run_locks.holder("dev") is None
    first, second = result.stderr.splitlines()[:2]
    assert first.startswith(
        f"warn  drop: role=RECORDED_ROLE target=dev profile=sst_reference_impl type=semantic_view object={VIEW} caller="
    )
    assert second.startswith(f"warn  drop: outcome=dropped type=semantic_view object={VIEW} elapsed=")
    silenced = _drop(monkeypatch, _port(), _project(tmp_path / "s"), "--log-level", "error")
    assert (silenced.exit_code, "warn  drop:" in silenced.stderr) == (0, False)


def test_an_object_without_sst_marker_is_refused_and_left_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _port(marked=False)
    result = _drop(monkeypatch, port, _project(tmp_path), "--output", "json")
    envelope = json.loads(result.stdout)
    assert result.exit_code == 1
    assert envelope["data"]["outcome"] == "refused"
    assert _codes(envelope) == ["SST-PLN024"]
    assert port.scripts == []
    assert set(port.state) == {"semantic_view:orders", "semantic_view:other"}


def test_an_absent_object_is_exit_1_not_a_silent_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _drop(monkeypatch, _port(exists=False), _project(tmp_path), "--output", "json")
    envelope = json.loads(result.stdout)
    assert (result.exit_code, envelope["data"]["outcome"]) == (1, "absent")
    assert _codes(envelope) == ["SST-PRT005"]


def test_a_refused_statement_is_reported_and_state_kept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = _port()
    port.refused = ("DROP SEMANTIC VIEW",)
    result = _drop(monkeypatch, port, _project(tmp_path), "--output", "json")
    envelope = json.loads(result.stdout)
    assert (result.exit_code, envelope["data"]["outcome"]) == (1, "rejected")
    assert _codes(envelope) == ["SST-APL001", "SST-SNO001"]
    assert "semantic_view:orders" in port.state


def test_a_held_run_lock_refuses_the_drop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = _port()
    port.run_locks.acquire_run_lock(STATE_TABLE, "dev", LockClaim("other-run"), break_stale=False)
    result = _drop(monkeypatch, port, _project(tmp_path), "--output", "json")
    envelope = json.loads(result.stdout)
    assert (result.exit_code, envelope["data"]["outcome"]) == (1, "refused")
    assert _codes(envelope) == ["SST-APL011"]
    assert port.scripts == []


def test_an_object_state_does_not_record_is_dropped_with_nothing_forgotten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _port()
    port.state = {}  # type: ignore[assignment]
    result = _drop(monkeypatch, port, _project(tmp_path), "--output", "json")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["forgotten"] == []


def test_the_profile_comes_from_profile_without_a_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    outside = tmp_path / "anywhere"
    outside.mkdir()
    profiles = str(_project(tmp_path))
    no_profile = _drop(monkeypatch, _port(), outside, "--profiles-dir", profiles)
    assert no_profile.exit_code == 4
    assert "no --profile was given" in no_profile.output
    named = _drop(monkeypatch, _port(), outside, "--profiles-dir", profiles, "--profile", "sst_reference_impl")
    assert named.exit_code == 0, named.output
    unknown = _drop(monkeypatch, _port(), outside, "--profiles-dir", profiles, "--profile", "nope")
    assert (unknown.exit_code, "SST-CFG010" in unknown.output) == (4, True)
    undeclared = _drop(monkeypatch, _port(), _project(tmp_path / "t"), "--target", "staging")
    assert (undeclared.exit_code, "SST-CFG010" in undeclared.output) == (4, True)


def test_a_configured_state_table_is_honoured_and_a_broken_config_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _project(tmp_path)
    (project / "sst_config.yml").write_text("state:\n  +table: SST_STATE_ELSEWHERE\n", encoding="utf-8")
    moved = json.loads(_drop(monkeypatch, _port(), project, "--output", "json").stdout)["data"]
    assert moved["state_table"] == "SST_REF_DEV.JAFFLE.SST_STATE_ELSEWHERE"
    (project / "sst_config.yml").write_text("state: [unclosed\n", encoding="utf-8")
    broken = _drop(monkeypatch, _port(), project, "--output", "json")
    assert broken.exit_code == 0, broken.output
    assert json.loads(broken.stdout)["data"]["state_table"] == STATE_TABLE.sql


@pytest.mark.parametrize(
    ("args", "code", "message"),
    [
        ([], "SST-PRT100", "sst drop removes exactly one object; 0 given"),
        (["A.B.C", "D.E.F"], "SST-PRT100", "sst drop removes exactly one object; 2 given"),
        (["B.C"], "SST-PRT108", "sst drop requires a fully-qualified name; 'B.C' is not one"),
        (["A.B.ORDERS_*"], "SST-PRT110", "sst drop takes no selector; 'A.B.ORDERS_*' is not accepted"),
        (["A.B.C"], "SST-PRT100", "sst drop requires --type; no --type; one of agent, semantic_view"),
        (["A.B.C", "--type", "tool"], "SST-PRT100", "sst drop requires --type; --type 'tool' is not a droppable type"),
        (["A.B.C", "--type", "agent"], "SST-PRT100", "sst drop requires --target; it has no default"),
        (["A.B.C", "--type", "agent", "-t", "dev"], "SST-PRT109", "sst drop requires --yes"),
        (["A.B.C", "--select", "tag:core"], "SST-PRT110", "sst drop takes no selector; 'tag:core' is not accepted"),
    ],
)
def test_every_refusal_exits_3_with_nothing_executed(
    args: list[str], code: str, message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SST_TARGET", "dev")
    result = CliRunner().invoke(cli, ["drop", *args, "--output", "json"])
    [diagnostic] = json.loads(result.stdout)["diagnostics"]
    assert (result.exit_code, diagnostic["code"]) == (3, code)
    assert diagnostic["message"].startswith(message)


def test_a_quoted_name_may_hold_wildcard_characters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = CliRunner().invoke(cli, ["drop", 'DB.S."WEIRD*NAME"', "--type", "agent", "-t", "dev", "--output", "json"])
    assert json.loads(result.stdout)["diagnostics"][0]["code"] == "SST-PRT109"
    assert "--prune" in CliRunner().invoke(cli, ["drop", "A.B.C", "--prune"]).output

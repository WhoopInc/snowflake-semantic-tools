"""`sst state unlock` against a project directory and the in-memory Snowflake double."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import Result

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.cli_projects import invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

STATE_TABLE = QualifiedName.parse("SST_REF_DEV.JAFFLE.SST_STATE")


def unlock_project(tmp_path: Path) -> Path:
    """A copy of the reference project, whose dev target's state table is `STATE_TABLE`."""
    return project_copy(tmp_path)


def locked_port(run_id: str = "crashed") -> FakeSnowflake:
    """The double, with `run_id`'s live lock on the dev target."""
    port = FakeSnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "dev", LockClaim(run_id, "ROLE", "ci-box"), break_stale=False)
    return port


def unlock(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, project: Path, *extra: str) -> Result:
    args = ["state", "unlock", "--target", "dev", "--project-dir", str(project), "--output", "json", *extra]
    return invoke_with_port(monkeypatch, port, args)


def codes(result: Result) -> list[str]:
    """The codes reported, less the fixture profile's own warning that it sets no role."""
    return [item["code"] for item in json.loads(result.stdout)["diagnostics"] if item["code"] != "SST-CFG014"]

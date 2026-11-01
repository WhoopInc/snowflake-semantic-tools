"""`deployed` reports each artifact as the target's state table records it, by `sst plan`'s rule.

The reads run against the shared Snowflake fake, whose `read_state` returns the entries given.
"""

from __future__ import annotations

import dataclasses
import importlib
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.api import ConnectionParams, Deployed, ProjectSession, deployed
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

VIEW = "semantic_view:jaffle_menu"
# The module, which the package's `deployed` function shadows as an attribute.
deployed_module = importlib.import_module("snowflake_semantic_tools.api.deployed")


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path_factory.mktemp("deployed")), manifest_path=DBT_MANIFEST)


def _entry(fingerprint: str) -> AppliedEntry:
    return AppliedEntry(
        fingerprint=fingerprint,
        qualified_name="SST_REF_DEV.JAFFLE.JAFFLE_MENU",
        applied_at="2026-10-30T12:00:00+00:00",
        run_id="run-1",
        outcome="applied",
        ddl_sha256=fingerprint,
        manifest_id="manifest-1",
        git_sha="abc123",
    )


def _fingerprint(session: ProjectSession, key: str) -> str:
    return next(item.fingerprint for item in session.catalog() if item.key == key)


def test_nothing_recorded_is_not_deployed(session: ProjectSession) -> None:
    rows = deployed(session, FakeSnowflake(state={}))

    assert [row.key for row in rows] == [item.key for item in session.catalog()]
    assert {row.status for row in rows} == {"not_deployed"}
    assert all(row.deployed_fingerprint is None and row.applied_at is None for row in rows)


def test_a_recorded_fingerprint_equal_to_the_compiled_one_is_deployed(session: ProjectSession) -> None:
    fingerprint = _fingerprint(session, VIEW)

    row = next(row for row in deployed(session, FakeSnowflake(state={VIEW: _entry(fingerprint)})) if row.key == VIEW)

    assert row == Deployed(
        key=VIEW,
        status="deployed",
        fingerprint=fingerprint,
        deployed_fingerprint=fingerprint,
        qualified_name="SST_REF_DEV.JAFFLE.JAFFLE_MENU",
        applied_at="2026-10-30T12:00:00+00:00",
        git_sha="abc123",
        outcome="applied",
    )


def test_another_recorded_fingerprint_is_changed(session: ProjectSession) -> None:
    row = next(row for row in deployed(session, FakeSnowflake(state={VIEW: _entry("older")})) if row.key == VIEW)

    assert (row.status, row.deployed_fingerprint) == ("changed", "older")
    assert row.fingerprint == _fingerprint(session, VIEW)


def test_a_recorded_artifact_the_project_no_longer_compiles_is_removed_and_listed_last(
    session: ProjectSession,
) -> None:
    state = {"semantic_view:zz_gone": _entry("x"), "semantic_view:aa_gone": _entry("y")}

    rows = deployed(session, FakeSnowflake(state=state))

    assert [(row.key, row.status, row.fingerprint) for row in rows[-2:]] == [
        ("semantic_view:aa_gone", "removed", None),
        ("semantic_view:zz_gone", "removed", None),
    ]
    assert len(rows) == len(session.catalog()) + 2


def test_a_state_table_that_cannot_be_read_raises_rather_than_reading_as_empty(session: ProjectSession) -> None:
    port = FakeSnowflake()
    port.remote_state = None

    with pytest.raises(SnowflakePortError, match="cannot be read"):
        deployed(session, port)


def test_deployed_reads_the_state_table_and_target_plan_reads(session: ProjectSession) -> None:
    assert session.state_table() == ("SST_REF_DEV.JAFFLE.SST_STATE", "dev")


def test_state_table_refuses_a_target_profiles_yml_does_not_declare(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    (project / "profiles.yml").unlink()

    with pytest.raises(ProjectError):
        ProjectSession.open(project, manifest_path=DBT_MANIFEST).state_table()


class _Closing(FakeSnowflake):
    """The fake, recording each `close`, as `deployed` must call it on a connector it opened."""

    def __init__(self, state: dict[str, AppliedEntry] | None) -> None:
        super().__init__(state={})
        self.remote_state = state
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_connection_params_open_a_connector_and_close_it(
    session: ProjectSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[dict[str, object]] = []
    port = _Closing({})

    def connector(params: dict[str, object]) -> FakeSnowflake:
        opened.append(params)
        return port

    monkeypatch.setattr(deployed_module, "SnowflakeConnector", connector)
    connection = ConnectionParams(account="acct", user="me", role="R", token="secret")

    deployed(session, connection)

    assert opened == [{"account": "acct", "user": "me", "authenticator": "oauth", "token": "secret", "role": "R"}]
    assert port.closed == 1


def test_connection_params_close_the_connector_when_the_read_fails(
    session: ProjectSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _Closing(None)
    monkeypatch.setattr(deployed_module, "SnowflakeConnector", lambda params: port)

    with pytest.raises(SnowflakePortError):
        deployed(session, ConnectionParams(account="acct", user="me", token="t"))

    assert port.closed == 1


def test_connection_params_take_a_key_pair_and_a_host() -> None:
    params = ConnectionParams(account="a", user="u", warehouse="W", private_key_file="/k.pem", host="h.example")

    assert params.connector_params() == {
        "account": "a",
        "user": "u",
        "private_key_file": "/k.pem",
        "warehouse": "W",
        "host": "h.example",
    }


@pytest.mark.parametrize("credentials", [{}, {"token": "t", "private_key_file": "/k.pem"}])
def test_connection_params_take_exactly_one_credential(credentials: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        ConnectionParams(account="a", user="u", **credentials)


def test_connection_params_never_show_a_credential() -> None:
    shown = repr(ConnectionParams(account="a", user="u", token="secret-token"))
    keyed = repr(dataclasses.replace(ConnectionParams(account="a", user="u", private_key_file="/secret.pem")))

    assert "secret" not in shown and "secret" not in keyed

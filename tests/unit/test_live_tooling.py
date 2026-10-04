"""The live layers' own machinery, offline: credentials, scratch-schema names, the sweep, the project.

None of this connects. The sweep decides what gets dropped in a shared account, so its refusals are
pinned here rather than discovered there; the live project is compiled and planned against a
recorded session, so the inputs the live layers publish are known to be valid before any of them
runs against Snowflake.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.live_project import live_project, live_view, orders_table, orders_table_statements, project_args
from tests.helpers.live_snowflake import (
    ENV_PREFIX,
    SCRATCH_MARKER,
    LiveAccount,
    LiveConfigurationError,
    create_scratch,
    drop_scratch,
    profile_target,
    run_token,
    scratch_created_at,
    scratch_schema_name,
    scratch_scope,
    sweepable,
)
from tests.helpers.snowflake_fake import FakeSnowflake

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
ACCOUNT = LiveAccount("acct", "ci_user", "/keys/ci.p8", "CI_ROLE", "CI_WH", "SCRATCH_DB")
ENVIRONMENT = {
    ENV_PREFIX + name: value
    for name, value in zip(
        ("ACCOUNT", "USER", "PRIVATE_KEY_PATH", "ROLE", "WAREHOUSE", "DATABASE"),
        ("acct", "ci_user", "/keys/ci.p8", "CI_ROLE", "CI_WH", "SCRATCH_DB"),
        strict=True,
    )
}


def test_an_account_is_read_from_the_environment_or_absent_but_never_half_set() -> None:
    assert LiveAccount.from_environment({}) is None
    assert LiveAccount.from_environment(ENVIRONMENT) == ACCOUNT
    with pytest.raises(LiveConfigurationError, match=f"{ENV_PREFIX}ROLE"):
        LiveAccount.from_environment({**ENVIRONMENT, ENV_PREFIX + "ROLE": ""})
    assert ACCOUNT.connection_params()["authenticator"] == "SNOWFLAKE_JWT"
    assert ACCOUNT.connection_params()["private_key_file"] == "/keys/ci.p8"


def test_a_profile_target_names_the_credentials_without_holding_them() -> None:
    target = profile_target("SST_IT_X")
    assert target["schema"] == "SST_IT_X"
    assert target["private_key_path"] == "{{ env_var('SST_TEST_SNOWFLAKE_PRIVATE_KEY_PATH') }}"
    assert not any(value in str(target.values()) for value in ("acct", "ci_user", "/keys/ci.p8"))


def test_a_scratch_name_carries_its_creation_time_run_and_worker() -> None:
    run = run_token({"SST_TEST_RUN_ID": "12345-2"})
    assert run == "12345X2"
    name = scratch_schema_name(run, "gw3apply", NOW)
    assert name == "SST_IT_20260101120000_12345X2_GW3APPLY"
    assert scratch_created_at(name) == NOW
    assert run_token({}).startswith("L")
    for other in ("ANALYTICS", "SST_IT_", "SST_IT_2026_RUN_W", "SST_IT_20260101120000_RUN-1_W"):
        assert scratch_created_at(other) is None


def test_the_sweep_drops_only_old_marked_scratch_schemas_or_one_runs_own() -> None:
    old = scratch_schema_name("R1", "MAIN", NOW - timedelta(hours=7))
    young = scratch_schema_name("R2", "MAIN", NOW - timedelta(hours=1))
    unmarked = scratch_schema_name("R3", "MAIN", NOW - timedelta(days=3))
    listed = [
        (old, SCRATCH_MARKER),
        (young, SCRATCH_MARKER),
        (unmarked, "a developer's schema"),
        ("ANALYTICS", SCRATCH_MARKER),
        (scratch_schema_name("R1", "GW1", NOW), SCRATCH_MARKER),
    ]
    assert sweepable(listed, NOW, older_than=timedelta(hours=6)) == (old,)
    assert sweepable(listed, NOW, older_than=timedelta(hours=6), run="R1") == (
        old,
        scratch_schema_name("R1", "GW1", NOW),
    )
    assert sweepable(listed, NOW, older_than=timedelta(hours=6), run="R3") == ()


def test_scratch_statements_mark_the_schema_and_refuse_to_drop_any_other() -> None:
    port = FakeSnowflake()
    scope = scratch_scope(ACCOUNT, scratch_schema_name("R1", "MAIN", NOW))
    create_scratch(port, scope)
    assert drop_scratch(port, scope)
    [created], [dropped] = port.scripts
    assert created == f"CREATE SCHEMA SCRATCH_DB.{scope.schema.value} COMMENT = '{SCRATCH_MARKER}'"
    assert dropped == f"DROP SCHEMA IF EXISTS SCRATCH_DB.{scope.schema.value} CASCADE"
    with pytest.raises(ValueError, match="not an SST_IT_ scratch schema"):
        drop_scratch(port, scratch_scope(ACCOUNT, "ANALYTICS"))


def test_the_live_project_validates_compiles_and_plans_one_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    schema = scratch_scope(ACCOUNT, scratch_schema_name("R1", "MAIN", NOW))
    project, manifest = live_project(tmp_path, schema)
    statements: tuple[Sql, ...] = orders_table_statements(schema)
    assert [str(item).split(" (")[0] for item in statements] == [
        f"CREATE TABLE {orders_table(schema).sql}",
        f"INSERT INTO {orders_table(schema).sql} VALUES",
    ]
    args = project_args(project, manifest)
    validated = CliRunner().invoke(cli, ["validate", *args, "--strict", "--no-snowflake-syntax-check"])
    assert validated.exit_code == 0, validated.output
    assert CliRunner().invoke(cli, ["compile", *args]).exit_code == 0

    port = FakeSnowflake(existing=(orders_table(schema).sql,), role=ACCOUNT.role)
    port.close = lambda: None  # type: ignore[attr-defined]
    connected: list[dict[str, object]] = []

    def connect(params: dict[str, object]) -> FakeSnowflake:
        connected.append(dict(params))
        return port

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connect)
    planned = CliRunner().invoke(cli, ["--output", "json", "plan", *args, "--no-snowflake-syntax-check"])
    assert planned.exit_code == 2, planned.output
    [change] = json.loads(planned.output)["data"]["changes"]
    assert change["target"] == live_view(schema).sql
    assert connected[0]["schema"] == schema.schema.value
    assert connected[0]["authenticator"] == "snowflake_jwt"

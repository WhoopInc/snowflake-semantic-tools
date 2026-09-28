from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.profile import load_profile_target


def test_profile_target_resolves_env_and_fixed_state_location(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "test:\n  target: verify\n  outputs:\n    verify:\n      type: snowflake\n"
        "      account: \"{{ env_var('ACCOUNT') }}\"\n      user: user\n      database: SCRATCH\n"
        "      schema: SST_1_REFERENCE_IMPL\n      query_tag: SST_1_REFERENCE_IMPL\n",
        encoding="utf-8",
    )
    (tmp_path / "sst_config.yml").write_text(
        'state:\n  +table: SST_STATE\n  +database: "{{ target.database }}"\n  +schema: "{{ target.schema }}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("ACCOUNT", "acct")
    value = load_profile_target(tmp_path)
    assert value.identity.scope.sql == "SCRATCH.SST_1_REFERENCE_IMPL"
    assert value.state_table.sql == "SCRATCH.SST_1_REFERENCE_IMPL.SST_STATE"
    assert value.connection_params["session_parameters"] == {"QUERY_TAG": "SST_1_REFERENCE_IMPL"}


def test_profile_target_fails_closed_on_missing_env(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "test:\n  target: x\n  outputs:\n    x:\n      type: snowflake\n"
        "      account: \"{{ env_var('MISSING') }}\"\n      database: DB\n      schema: S\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="MISSING"):
        load_profile_target(tmp_path)

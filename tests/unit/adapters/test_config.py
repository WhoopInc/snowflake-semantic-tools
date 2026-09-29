"""The config adapter reads `sst_config.yml` once and reports its shape."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.config import config_tree, load_project_config
from snowflake_semantic_tools.adapters.profile import load_profile_target, resolve_profile_name
from snowflake_semantic_tools.adapters.project import ProjectError

PROFILES = """
sst:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: acct
      user: me
      database: DB
      schema: SCH
"""


def _write(root: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


def test_missing_config_is_empty_and_clean(tmp_path: Path) -> None:
    loaded = load_project_config(tmp_path)
    assert dict(loaded.tree) == {}
    assert loaded.diagnostics == ()
    assert loaded.has_dbt_project is False


def test_positions_and_deprecated_deploy_alias(tmp_path: Path) -> None:
    _write(tmp_path, {"dbt_project.yml": "profile: sst\n", "sst_config.yml": "deploy:\n  bogus: 1\n"})
    loaded = load_project_config(tmp_path)
    assert [item.code for item in loaded.diagnostics] == ["SST-CFG045", "SST-CFG003"]
    origin = loaded.diagnostics[1].origin
    assert origin is not None and (origin.file, origin.line) == ("sst_config.yml", 2)
    assert dict(loaded.tree) == {"apply": {"bogus": 1}}
    assert config_tree(tmp_path) == {"apply": {"bogus": 1}}


def test_deploy_beside_apply_is_refused_and_dropped(tmp_path: Path) -> None:
    _write(tmp_path, {"dbt_project.yml": "profile: sst\n", "sst_config.yml": "apply: {}\ndeploy: {}\n"})
    loaded = load_project_config(tmp_path)
    assert [item.code for item in loaded.diagnostics] == ["SST-CFG045", "SST-CFG043"]
    assert dict(loaded.tree) == {"apply": {}}


def test_dbt_only_configuration_is_refused_without_dbt_project(tmp_path: Path) -> None:
    _write(
        tmp_path,
        {
            "sst_config.yml": (
                "project:\n  target_profile: sst\n  agents_dir: agents\n"
                "semantic_views:\n  +schema: X\nskills:\n  stage:\n    +stage: S\n"
            )
        },
    )
    loaded = load_project_config(tmp_path)
    assert [(item.code, item.subject) for item in loaded.diagnostics] == [
        ("SST-CFG046", "config:semantic_views"),
        ("SST-CFG046", "config:project.agents_dir"),
    ]


def test_unreadable_config_raises(tmp_path: Path) -> None:
    config = tmp_path / "sst_config.yml"
    config.write_text("project: {}\n", encoding="utf-8")
    config.chmod(0)
    try:
        with pytest.raises(ProjectError, match="cannot read"):
            load_project_config(tmp_path)
    finally:
        config.chmod(0o644)


def test_profile_name_comes_from_dbt_or_target_profile(tmp_path: Path) -> None:
    dbt = _write(tmp_path / "dbt", {"dbt_project.yml": "profile: sst\n", "profiles.yml": PROFILES})
    assert resolve_profile_name(dbt) == "sst"
    _write(dbt, {"sst_config.yml": "project:\n  target_profile: sst\n"})
    assert resolve_profile_name(dbt) == "sst"
    _write(dbt, {"sst_config.yml": "project:\n  target_profile: other\n"})
    with pytest.raises(ValueError, match="disagrees"):
        resolve_profile_name(dbt)

    skills_only = _write(tmp_path / "skills", {"profiles.yml": PROFILES})
    with pytest.raises(ValueError, match="project.target_profile"):
        resolve_profile_name(skills_only)
    _write(skills_only, {"sst_config.yml": "project:\n  target_profile: sst\n"})
    target = load_profile_target(skills_only)
    assert (target.profile_name, target.target_name, target.identity.database.sql) == ("sst", "dev", "DB")

    unnamed = _write(tmp_path / "unnamed", {"dbt_project.yml": "name: x\n", "profiles.yml": PROFILES})
    with pytest.raises(ValueError, match="declares no profile"):
        resolve_profile_name(unnamed)


def test_semantic_targets_resolve_single_quoted_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.yaml.loader import resolve_target

    project = _write(
        tmp_path,
        {
            "dbt_project.yml": "name: p\nprofile: p\n",
            "profiles.yml": (
                "p:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n"
                "      database: '{{ env_var(''SST_TEST_DATABASE'', ''FALLBACK_DB'') }}'\n"
                "      schema: '{{ env_var(''SST_TEST_SCHEMA'') }}'\n"
            ),
        },
    )
    monkeypatch.setenv("SST_TEST_SCHEMA", "SEMANTIC")
    target = resolve_target(project)
    assert (target.database, target.schema) == ("FALLBACK_DB", "SEMANTIC")
    monkeypatch.delenv("SST_TEST_SCHEMA")
    with pytest.raises(ProjectError, match="SST_TEST_SCHEMA is required"):
        resolve_target(project)

"""The config adapter reads `sst_config.yml` once and reports its shape."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target, resolve_profile_name
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from tests.helpers.projects import project_paths

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
    loaded = load_project_config(project_paths(tmp_path))
    assert dict(loaded.tree) == {}
    assert loaded.diagnostics == ()
    assert loaded.has_dbt_project is False


def test_positions_and_the_deprecated_deploy_block(tmp_path: Path) -> None:
    config = "validation:\n  snowflake_syntax_check: false\napply: {}\ndeploy: {bogus: 1}\n"
    _write(tmp_path, {"dbt_project.yml": "profile: sst\n", "sst_config.yml": config})
    loaded = load_project_config(project_paths(tmp_path))
    # deploy: is the deprecated spelling of apply:, and is checked as apply: is.
    assert [item.code for item in loaded.diagnostics] == ["SST-CFG200", "SST-CFG003"]
    assert loaded.diagnostics[0].message == "config key 'deploy' is deprecated; use 'apply'"
    origin = loaded.diagnostics[0].origin
    assert origin is not None and (origin.file, origin.line) == ("sst_config.yml", 4)
    # With apply: present too, apply: is what is read.
    assert dict(loaded.tree)["apply"] == {}
    _write(tmp_path, {"sst_config.yml": "validation:\n  snowflake_syntax_check: false\ndeploy: {fail_fast: true}\n"})
    assert dict(load_project_config(project_paths(tmp_path)).tree)["apply"] == {"fail_fast": True}


def test_unsupported_and_removed_0_3_keys_are_errors(tmp_path: Path) -> None:
    config = (
        "dbt: {}\nvalidation:\n  exclude_dirs: []\n  snowflake_syntax_check: true\nenrichment: {}\n"
        "generation: {}\ndefer: {}\napply:\n  fail_fast: true\nsnowflake:\n  allow_unknown_keys: true\n"
    )
    _write(tmp_path, {"dbt_project.yml": "profile: sst\n", "sst_config.yml": config})
    loaded = load_project_config(project_paths(tmp_path))
    # enrichment: is read again; the 0.3 blocks around it are not.
    assert [(item.code, item.subject, item.severity.name) for item in loaded.diagnostics] == [
        ("SST-CFG044", "config:dbt", "ERROR"),
        ("SST-CFG043", "config:validation.exclude_dirs", "ERROR"),
        ("SST-CFG043", "config:generation", "ERROR"),
        ("SST-CFG043", "config:defer", "ERROR"),
        ("SST-CFG044", "config:snowflake.allow_unknown_keys", "ERROR"),
    ]
    # apply.fail_fast is read again: the --fail-fast flag pair overrides it for one run.
    assert dict(loaded.tree)["apply"] == {"fail_fast": True}


def test_dbt_only_configuration_is_refused_without_dbt_project(tmp_path: Path) -> None:
    _write(
        tmp_path,
        {
            "sst_config.yml": (
                "project:\n  target_profile: sst\n  agents_dir: agents\n"
                "semantic_views:\n  +schema: X\nenrichment: {}\nskills:\n  stage:\n    +stage: S\n"
            )
        },
    )
    loaded = load_project_config(project_paths(tmp_path))
    assert [(item.code, item.subject) for item in loaded.diagnostics] == [
        ("SST-CFG031", "config:validation"),
        ("SST-CFG046", "config:semantic_views"),
        ("SST-CFG046", "config:enrichment"),
        ("SST-CFG046", "config:project.agents_dir"),
    ]


def test_unreadable_config_raises(tmp_path: Path) -> None:
    config = tmp_path / "sst_config.yml"
    config.write_text("project: {}\n", encoding="utf-8")
    config.chmod(0)
    try:
        with pytest.raises(ProjectError, match="could not read sst_config.yml"):
            load_project_config(project_paths(tmp_path))
    finally:
        config.chmod(0o644)


def test_profile_name_comes_from_dbt_or_target_profile(tmp_path: Path) -> None:
    dbt = _write(tmp_path / "dbt", {"dbt_project.yml": "profile: sst\n", "profiles.yml": PROFILES})
    assert resolve_profile_name(project_paths(dbt)) == "sst"
    _write(dbt, {"sst_config.yml": "project:\n  target_profile: sst\n"})
    assert resolve_profile_name(project_paths(dbt)) == "sst"
    _write(dbt, {"sst_config.yml": "project:\n  target_profile: other\n"})
    with pytest.raises(ValueError, match="disagrees"):
        resolve_profile_name(project_paths(dbt))

    skills_only = _write(tmp_path / "skills", {"profiles.yml": PROFILES})
    with pytest.raises(ValueError, match="project.target_profile"):
        resolve_profile_name(project_paths(skills_only))
    _write(skills_only, {"sst_config.yml": "project:\n  target_profile: sst\n"})
    target = load_profile_target(project_paths(skills_only))
    assert (target.profile_name, target.target_name, target.identity.database.sql) == ("sst", "dev", "DB")

    unnamed = _write(tmp_path / "unnamed", {"dbt_project.yml": "name: x\n", "profiles.yml": PROFILES})
    with pytest.raises(ValueError, match="declares no profile"):
        resolve_profile_name(project_paths(unnamed))


def test_semantic_targets_resolve_single_quoted_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.adapters.dbt.project import resolve_target

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
    target = resolve_target(project_paths(project))
    assert (target.database, target.schema) == ("FALLBACK_DB", "SEMANTIC")
    monkeypatch.delenv("SST_TEST_SCHEMA")
    with pytest.raises(ProjectError, match=r"env_var\('SST_TEST_SCHEMA'\) is unset and has no default"):
        resolve_target(project_paths(project))

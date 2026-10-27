"""SST's build directory is `sst_target/`, beside dbt's `target/`: clean, first-run policy, and walks.

Before 1.0 moved it, SST wrote the same files to `target/sst/`; `sst clean` removes that too, and a
manifest left there still counts as a 1.0 run, so an upgrade does not reset the first-run policy.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.adapters.locations import LEGACY_SST_TARGET, SST_TARGET, sst_target_dir
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.policy import ran_under_1_0


def _project(tmp_path: Path) -> Path:
    (tmp_path / "sst_config.yml").write_text("validation:\n  snowflake_syntax_check: false\n", encoding="utf-8")
    return tmp_path


def _clean(project: Path, *args: str) -> dict[str, object]:
    result = CliRunner().invoke(cli, ["--output", "json", "clean", "--project-dir", str(project), *args])
    assert result.exit_code == 0, result.output
    return dict(json.loads(result.output)["data"])


def test_the_build_directory_is_sst_target_under_the_project(tmp_path: Path) -> None:
    assert sst_target_dir(tmp_path) == tmp_path / "sst_target" == tmp_path / SST_TARGET


def test_clean_removes_sst_target_and_a_legacy_target_sst_and_keeps_dbts_target(tmp_path: Path) -> None:
    project = _project(tmp_path)
    for folder in (project / SST_TARGET, project / LEGACY_SST_TARGET):
        folder.mkdir(parents=True)
        (folder / "manifest.json").write_text("{}", encoding="utf-8")
    (project / "target" / "manifest.json").write_text("{}", encoding="utf-8")
    both = [str(project / SST_TARGET), str(project / LEGACY_SST_TARGET)]
    assert _clean(project, "--dry-run") == {"removed": [], "would_remove": both}
    assert (project / SST_TARGET).is_dir() and (project / LEGACY_SST_TARGET).is_dir()
    assert _clean(project) == {"removed": both, "would_remove": []}
    assert not (project / SST_TARGET).exists() and not (project / LEGACY_SST_TARGET).exists()
    # dbt's own artifacts are dbt's to clean.
    assert (project / "target" / "manifest.json").is_file()
    assert _clean(project) == {"removed": [], "would_remove": []}


def test_a_manifest_in_either_build_directory_means_a_1_0_run_happened(tmp_path: Path) -> None:
    assert not ran_under_1_0(tmp_path)
    (tmp_path / LEGACY_SST_TARGET).mkdir(parents=True)
    (tmp_path / LEGACY_SST_TARGET / "manifest.json").write_text("{}", encoding="utf-8")
    assert ran_under_1_0(tmp_path)
    (tmp_path / LEGACY_SST_TARGET / "manifest.json").unlink()
    (tmp_path / SST_TARGET).mkdir()
    (tmp_path / SST_TARGET / "manifest.json").write_text("{}", encoding="utf-8")
    assert ran_under_1_0(tmp_path)


def test_format_never_walks_into_the_build_directory(tmp_path: Path) -> None:
    project = _project(tmp_path)
    (project / SST_TARGET).mkdir()
    (project / SST_TARGET / "built.yml").write_text("x:   1\n", encoding="utf-8")
    result = CliRunner().invoke(cli, ["--output", "json", "format", "--check", "--project-dir", str(project), "."])
    assert "sst_target" not in result.output
    assert (project / SST_TARGET / "built.yml").read_text(encoding="utf-8") == "x:   1\n"

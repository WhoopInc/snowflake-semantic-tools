"""What `dbt --version` answered is kept per dbt executable, and asked again once that executable changes."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun, executable_key, parse_project
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from tests.helpers.projects import project_paths
from tests.helpers.seam_projects import FakeDbt, SmallProject, manifest

CLOUD = CompletedRun(0, "dbt Cloud CLI - 0.38.6\n", "")


def _dbt_on_path(monkeypatch: pytest.MonkeyPatch, folder: Path, body: str = "#!/bin/sh\necho core\n") -> Path:
    """Put an executable `dbt` in `folder`, alone on `PATH`."""
    folder.mkdir(parents=True, exist_ok=True)
    executable = folder / "dbt"
    executable.write_text(body, encoding="utf-8")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", str(folder))
    return executable


def _parse(project: Path, dbt: FakeDbt, *, auto_compile: bool = False, cache: Path | None = None) -> tuple[str, ...]:
    """Parse with `dbt` and the cache under the project's target; return the codes it warned with."""
    target = project / "target" / "manifest.json"
    chosen = cache or project / "target" / "sst" / "dbt_installation.json"
    warnings = parse_project(project, None, target, runner=dbt, auto_compile=auto_compile, installation_cache=chosen)
    return tuple(item.code for item in warnings)


def _asked(dbt: FakeDbt) -> int:
    return sum(1 for run in dbt.runs if run[1] == "--version")


def _project(tmp_path: Path) -> tuple[Path, FakeDbt]:
    project = tmp_path / "project"
    project.mkdir()
    return project, FakeDbt(writes=(project / "target" / "manifest.json", manifest()))


def test_a_second_parse_with_the_same_dbt_does_not_ask_it_what_it_is(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    assert _parse(project, dbt) == ()
    assert _parse(project, dbt) == ()
    assert [run[1] for run in dbt.runs] == ["--version", "parse", "parse"]
    kept = json.loads((project / "target" / "sst" / "dbt_installation.json").read_text(encoding="utf-8"))
    assert kept == {"key": executable_key(), "kind": "core"}


@pytest.mark.parametrize(("banner", "kind"), [("dbt 2.0.8\n", "v2"), ("dbt-oss 2.0.8\n", "v2-oss")])
def test_a_dbt_v2_is_kept_like_core_and_still_writes_json_from_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, banner: str, kind: str
) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    dbt.version = CompletedRun(0, banner, "")
    assert _parse(project, dbt) == ()
    assert _parse(project, dbt) == ()
    assert _asked(dbt) == 1
    kept = json.loads((project / "target" / "sst" / "dbt_installation.json").read_text(encoding="utf-8"))
    assert kept["kind"] == kind
    assert all("--write-json" in run for run in dbt.runs if run[1] == "parse")


def test_a_rewritten_dbt_is_asked_again(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    executable = _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    _parse(project, dbt)
    # An upgrade rewrites the entry point: its size or its modification time changes.
    executable.write_text("#!/bin/sh\necho upgraded core\n", encoding="utf-8")
    _parse(project, dbt)
    assert _asked(dbt) == 2
    stamp = executable.stat().st_mtime_ns + 5_000_000_000
    os.utime(executable, ns=(stamp, stamp))
    _parse(project, dbt)
    assert _asked(dbt) == 3


def test_another_dbt_on_path_is_asked_again(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "one")
    project, dbt = _project(tmp_path)
    _parse(project, dbt)
    _dbt_on_path(monkeypatch, tmp_path / "two")
    _parse(project, dbt)
    assert _asked(dbt) == 2


def test_an_unidentified_or_failing_dbt_is_asked_every_run_and_warns_every_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    dbt.version = CompletedRun(0, "dbt-fusion 2.0\n", "")
    assert _parse(project, dbt) == ("SST-DBT020",)
    dbt.version = CompletedRun(2, "", "boom")
    assert _parse(project, dbt) == ("SST-DBT020",)
    assert _asked(dbt) == 2
    assert not (project / "target" / "sst" / "dbt_installation.json").exists()


def test_a_kept_cloud_cli_still_refuses_auto_compile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    dbt.version = CLOUD
    assert _parse(project, dbt) == ()
    with pytest.raises(ProjectError) as raised:
        _parse(project, dbt, auto_compile=True)
    assert [item.code for item in raised.value.diagnostics] == ["SST-DBT021"]
    assert _asked(dbt) == 1


def test_without_dbt_on_path_nothing_is_kept_and_dbt_is_asked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    project, dbt = _project(tmp_path)
    _parse(project, dbt)
    _parse(project, dbt)
    assert _asked(dbt) == 2
    assert executable_key() is None


def test_a_dbt_that_resolves_to_nothing_has_no_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    executable = _dbt_on_path(monkeypatch, tmp_path / "bin")
    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.dbt.invoke.os.path.realpath", lambda _: str(executable) + "x"
    )
    assert executable_key() is None


@pytest.mark.parametrize("held", ["not json", '["a list"]', '{"key": null, "kind": "core"}', "KEY_WITH_ODD_KIND"])
def test_a_cache_that_does_not_answer_for_this_dbt_is_asked_past_and_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: str
) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    cache = project / "target" / "sst" / "dbt_installation.json"
    cache.parent.mkdir(parents=True)
    if held == "KEY_WITH_ODD_KIND":
        held = json.dumps({"key": executable_key(), "kind": "fusion"})
    cache.write_text(held, encoding="utf-8")
    _parse(project, dbt)
    assert _asked(dbt) == 1
    assert json.loads(cache.read_text(encoding="utf-8"))["kind"] == "core"


def test_a_cache_outside_the_project_is_not_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    outside = tmp_path / "elsewhere" / "dbt_installation.json"
    _parse(project, dbt, cache=outside)
    _parse(project, dbt, cache=outside)
    assert _asked(dbt) == 2
    assert not outside.exists()


def test_without_a_cache_dbt_is_asked_every_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _dbt_on_path(monkeypatch, tmp_path / "bin")
    project, dbt = _project(tmp_path)
    for _ in range(2):
        parse_project(project, None, project / "target" / "manifest.json", runner=dbt)
    assert _asked(dbt) == 2


def test_only_the_dbt_on_path_is_cached_by_a_project_source(tmp_path: Path) -> None:
    SmallProject(tmp_path).write()
    paths = project_paths(tmp_path)
    assert YamlProjectSource(paths)._installation_cache() == tmp_path / "target" / "sst" / "dbt_installation.json"
    assert YamlProjectSource(paths, dbt_runner=FakeDbt())._installation_cache() is None

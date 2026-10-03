"""The writes a security review showed escaping the project are refused, end to end.

`sst format` rewrote the file a symbolic link pointed at; `sst compile --emit-ddl` wrote a
quoted artifact name's `..` path outside the emit folder, and followed a link planted in it.
Each now writes inside its root or refuses, and the file outside is left as it was.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.enrich_files import ProjectFiles
from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.adapters.paths import UnsafeWrite
from snowflake_semantic_tools.adapters.yaml.migrate import write_file
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.plan_output import write_plan_sql
from snowflake_semantic_tools.domain.model.lifecycle import ChangeSet
from tests.helpers.cli_projects import common, project_copy

_EVIL_VIEW = """
  - name: '"x/../../../escaped"'
    description: |-
      Use this view for questions about order volume and order value.
    tables:
      - "{{ ref('orders') }}"
    variables:
      - name: large_order_cents
        data_type: NUMBER
        description: |-
          Threshold above which an order counts as large.
        default_value: 1000
      - name: tax_inclusive
        data_type: BOOLEAN
        description: |-
          Whether revenue includes tax.
        default_value: false
"""


def _invoke(*arguments: str) -> tuple[int, dict[str, object]]:
    result = CliRunner().invoke(cli, [*arguments, "--output", "json"])
    return result.exit_code, json.loads(result.output)


def _codes(envelope: dict[str, object]) -> list[str]:
    return [str(item["code"]) for item in cast(list[dict[str, object]], envelope["diagnostics"])]


def _messages(envelope: dict[str, object], code: str) -> list[str]:
    items = cast(list[dict[str, object]], envelope["diagnostics"])
    return [str(item["message"]) for item in items if item["code"] == code]


def test_format_refuses_a_symlinked_file_and_leaves_its_target_alone(tmp_path: Path) -> None:
    project, outside = tmp_path / "project", tmp_path / "outside"
    (project / "models").mkdir(parents=True)
    (outside / "subdir").mkdir(parents=True)
    victim = outside / "victim.yml"
    victim.write_text("b:   2\na:     1\n", encoding="utf-8")
    (outside / "subdir" / "victim2.yml").write_text("z:   2\ny:     1\n", encoding="utf-8")
    os.symlink(victim, project / "models" / "link.yml")
    os.symlink(outside / "subdir", project / "models" / "linkdir")
    (project / "models" / "own.yml").write_text("b:   2\n", encoding="utf-8")
    exit_code, envelope = _invoke("format", "models", "--project-dir", str(project))
    assert exit_code == 1
    assert _messages(envelope, "SST-PRT009") == [
        "could not read models/link.yml: it is a symbolic link, which SST does not follow"
    ]
    assert victim.read_text(encoding="utf-8") == "b:   2\na:     1\n"
    assert (outside / "subdir" / "victim2.yml").read_text(encoding="utf-8") == "z:   2\ny:     1\n"
    assert (project / "models" / "link.yml").is_symlink()
    assert (project / "models" / "own.yml").read_text(encoding="utf-8") == "b: 2\n"


def test_format_refuses_a_file_reached_through_a_symlinked_folder_named_directly(tmp_path: Path) -> None:
    project, outside = tmp_path / "project", tmp_path / "outside"
    project.mkdir()
    outside.mkdir()
    (outside / "victim.yml").write_text("b:   2\n", encoding="utf-8")
    os.symlink(outside, project / "models")
    exit_code, envelope = _invoke("format", "models/victim.yml", "--project-dir", str(project))
    assert exit_code == 1
    assert _messages(envelope, "SST-PRT009") == [
        "could not read models/victim.yml: models is a symbolic link, which SST does not follow"
    ]
    assert (outside / "victim.yml").read_text(encoding="utf-8") == "b:   2\n"


def test_an_artifact_name_with_path_characters_is_emitted_inside_the_emit_folder(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(views.read_text(encoding="utf-8").rstrip("\n") + "\n" + _EVIL_VIEW, encoding="utf-8")
    exit_code, envelope = _invoke("compile", *common(project), "--emit-ddl", "emitted")
    assert exit_code == 0, _codes(envelope)
    files = cast(list[str], cast(dict[str, object], envelope["data"])["ddl_files"])
    # The view's name reads as `/escaped"`, which joined to the emit folder was an absolute path.
    assert "%2Fescaped%22.sql" in files
    assert (project / "emitted" / "%2Fescaped%22.sql").is_file()
    escaped = [path for path in tmp_path.rglob("*escaped*") if path.parent != project / "emitted"]
    assert escaped == []


@pytest.mark.parametrize(
    ("option", "planted"), [("--emit-ddl", "jaffle_sales.sql"), ("--emit-agent-spec", "jaffle_minimal_agent.json")]
)
def test_compile_refuses_a_link_planted_in_the_emit_folder(tmp_path: Path, option: str, planted: str) -> None:
    project = project_copy(tmp_path)
    (project / "emitted").mkdir()
    victim = tmp_path / "victim_file.txt"
    victim.write_text("ORIGINAL CONTENT\n", encoding="utf-8")
    os.symlink(victim, project / "emitted" / planted)
    exit_code, envelope = _invoke("compile", *common(project), option, "emitted")
    assert exit_code == 1
    assert _messages(envelope, "SST-PRT008") == [
        f"could not write {project / 'emitted' / planted}: it is a symbolic link, which SST does not write through"
    ]
    assert victim.read_text(encoding="utf-8") == "ORIGINAL CONTENT\n"
    assert (project / "emitted" / planted).is_symlink()


def test_compile_refuses_an_emit_folder_that_is_a_link_inside_the_project(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    target = tmp_path / "dirtarget"
    target.mkdir()
    os.symlink(target, project / "linked_dir")
    exit_code, envelope = _invoke("compile", *common(project), "--emit-ddl", "linked_dir")
    assert exit_code == 1
    [message] = _messages(envelope, "SST-PRT008")
    assert message.endswith(": linked_dir is a symbolic link, which SST does not write through")
    assert list(target.iterdir()) == []


def test_compile_writes_into_an_emit_folder_the_user_names_outside_the_project(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    chosen = tmp_path / "chosen" / "ddl"
    exit_code, envelope = _invoke("compile", *common(project), "--emit-ddl", str(chosen))
    assert exit_code == 0, _codes(envelope)
    assert (chosen / "jaffle_sales.sql").is_file()


def test_the_manifest_is_never_written_through_a_symlinked_build_folder(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, project / "target")
    exit_code, envelope = _invoke("compile", *common(project))
    assert exit_code == 1
    assert _messages(envelope, "SST-MAN007") == [
        f"could not write {project / 'target' / 'sst' / 'manifest.json'}: target is a symbolic link, "
        "which SST does not write through"
    ]
    assert list(outside.iterdir()) == []


def test_clean_refuses_a_build_folder_reached_through_a_link(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    outside = tmp_path / "outside"
    (outside / "sst").mkdir(parents=True)
    (outside / "sst" / "keep.txt").write_text("keep", encoding="utf-8")
    os.symlink(outside, project / "target")
    exit_code, envelope = _invoke("clean", "--project-dir", str(project))
    assert exit_code == 1 and _codes(envelope) == ["SST-PRT010"]
    assert (outside / "sst" / "keep.txt").exists()


def test_enrich_and_migrate_refuse_to_write_a_symlinked_yaml_file(tmp_path: Path) -> None:
    project, outside = tmp_path / "project", tmp_path / "outside"
    (project / "models").mkdir(parents=True)
    outside.mkdir()
    victim = outside / "victim.yml"
    victim.write_text("original\n", encoding="utf-8")
    os.symlink(victim, project / "models" / "orders.yml")
    with pytest.raises(UnsafeWrite):
        ProjectFiles(project).write("models/orders.yml", "pwned\n")
    with pytest.raises(UnsafeWrite):
        write_file(project, "models/orders.yml", "pwned\n")
    assert victim.read_text(encoding="utf-8") == "original\n"


def test_the_state_cache_and_its_lock_are_never_written_through_a_link(tmp_path: Path) -> None:
    project, outside = tmp_path / "project", tmp_path / "outside"
    project.mkdir()
    outside.mkdir()
    os.symlink(outside, project / "target")
    store = StateFileStore(project / "target" / "sst" / "state.dev.json", root=project)
    with pytest.raises(UnsafeWrite):
        store.acquire_lock("run", break_stale=False)
    assert list(outside.iterdir()) == []


def test_plan_statements_are_never_written_into_a_symlinked_folder(tmp_path: Path) -> None:
    project, outside = tmp_path / "project", tmp_path / "outside"
    project.mkdir()
    outside.mkdir()
    os.symlink(outside, project / "sql")
    changeset = cast(ChangeSet, SimpleNamespace(changes=()))
    with pytest.raises(UnsafeWrite):
        write_plan_sql(project, changeset, project / "sql")
    assert write_plan_sql(project, changeset, tmp_path / "chosen") == tmp_path / "chosen"
    assert (tmp_path / "chosen").is_dir() and list(outside.iterdir()) == []

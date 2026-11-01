"""A session with unsaved edits answers as a session opened on the edits saved, and saves nothing.

`with_overlay` reads a private copy of the project with the edits written over it, so each test
compares it with a fresh `open` of a copy where the same edits were saved, and checks the
project itself is byte for byte as it was.
"""

from __future__ import annotations

import functools
import json
import shutil
from collections.abc import Sequence
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.paths import UnsafeWrite
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.api import ProjectSession
from tests.helpers.reference_project import DBT_MANIFEST, MENU_VIEW, project_copy
from tests.helpers.seam_projects import FakeDbt

METRIC = "semantic_models/metrics/total_revenue.yml"


def _answers(session: ProjectSession) -> tuple[object, ...]:
    validation = session.validate()
    catalog = session.catalog()
    return (
        [dict(item) for item in validation.diagnostics],
        dict(validation.summary),
        validation.passed,
        catalog,
        [session.render(item.key) for item in catalog],
    )


def _snapshot(project: Path) -> dict[str, bytes]:
    return {path.relative_to(project).as_posix(): path.read_bytes() for path in project.rglob("*") if path.is_file()}


def _saved(tmp_path: Path, project: Path, edits: dict[str, bytes | None]) -> Path:
    """A copy of `project` with `edits` saved in it, for a fresh session to read."""
    saved = tmp_path / "saved" / project.name
    if saved.parent.exists():
        shutil.rmtree(saved.parent)
    shutil.copytree(project, saved)
    for relative, data in edits.items():
        if data is None:
            (saved / relative).unlink()
        else:
            (saved / relative).parent.mkdir(parents=True, exist_ok=True)
            (saved / relative).write_bytes(data)
    return saved


def _edited(project: Path, relative: str, old: str, new: str) -> bytes:
    text = (project / relative).read_text(encoding="utf-8")
    assert old in text, f"{old!r} is not in {relative}"
    return text.replace(old, new, 1).encode("utf-8")


@pytest.mark.parametrize(
    "edit",
    [
        lambda p: {METRIC: _edited(p, METRIC, "in cents.", "in cents, tax included.")},
        lambda p: {MENU_VIEW: _edited(p, MENU_VIEW, "ref('products')", "ref('no_such_model')")},
        lambda p: {"semantic_models/metrics/product_count.yml": None},
        lambda p: {
            "semantic_models/metrics/order_total_sum.yml": b"metric:\n  name: order_total_sum\n"
            b"  description: Sum of order totals.\n  expr: \"SUM({{ ref('orders', 'order_total') }})\"\n"
        },
        lambda p: {
            "sst_config.yml": _edited(p, "sst_config.yml", "completed_state: completed", "completed_state: done")
        },
        lambda p: {".sst/baseline.json": b"not a baseline"},
    ],
    ids=["metric", "broken-view", "deleted-member", "new-member", "config", "unreadable-baseline"],
)
def test_an_overlay_answers_as_the_edits_saved_would_and_saves_nothing(tmp_path: Path, edit) -> None:  # type: ignore[no-untyped-def]
    project = project_copy(tmp_path)
    before = _snapshot(project)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    edits = edit(project)

    overlaid = session.with_overlay(edits)
    fresh = ProjectSession.open(_saved(tmp_path, project, edits), manifest_path=DBT_MANIFEST)

    if ".sst/baseline.json" in edits:
        with pytest.raises(Exception, match="baseline"):
            overlaid.validate()
        with pytest.raises(Exception, match="baseline"):
            fresh.validate()
    else:
        assert _answers(overlaid) == _answers(fresh)
        assert _answers(overlaid) != _answers(session)
    assert _snapshot(project) == before
    assert overlaid.project_dir == session.project_dir
    assert dict(overlaid.overlay) == edits


def test_overlays_are_snapshots_of_the_project_and_of_each_other(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    base = _answers(session)
    first = session.with_overlay({METRIC: _edited(project, METRIC, "in cents.", "in cents (first).")})
    first_answers = _answers(first)

    second = session.with_overlay({METRIC: _edited(project, METRIC, "in cents.", "in cents (second).")})

    assert _answers(first) == first_answers
    assert _answers(second) != first_answers
    assert _answers(session) == base
    assert _answers(first.reload(dbt=False)) == first_answers


def test_an_overlay_reads_the_project_as_it_is_on_disk_when_asked_for(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    session.with_overlay({METRIC: _edited(project, METRIC, "in cents.", "in cents (overlay).")})
    menu = _edited(project, MENU_VIEW, "ref('products')", "ref('no_such_model')")
    (project / MENU_VIEW).write_bytes(menu)
    (project / "semantic_models/metrics/product_count.yml").unlink()

    overlaid = session.with_overlay({})

    assert _answers(overlaid) == _answers(ProjectSession.open(project, manifest_path=DBT_MANIFEST))


def test_an_overlay_adds_to_the_edits_a_session_already_reads(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    metric = _edited(project, METRIC, "in cents.", "in cents (kept).")
    deleted: dict[str, bytes | None] = {"semantic_models/metrics/product_count.yml": None}

    both = ProjectSession.open(project, manifest_path=DBT_MANIFEST).with_overlay({METRIC: metric}).with_overlay(deleted)

    assert dict(both.overlay) == {METRIC: metric, **deleted}
    saved = ProjectSession.open(_saved(tmp_path, project, {METRIC: metric, **deleted}), manifest_path=DBT_MANIFEST)
    assert _answers(both) == _answers(saved)


@pytest.mark.parametrize("relative", ["../outside.yml", "/etc/outside.yml", "semantic_models/../../outside.yml"])
def test_an_overlay_path_outside_the_project_is_refused(tmp_path: Path, relative: str) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)

    with pytest.raises(UnsafeWrite):
        session.with_overlay({relative: b"x"})
    assert not (tmp_path / "outside.yml").exists()


class _ParsingDbt(FakeDbt):
    """A dbt whose every parse writes the reference manifest under the folder it runs in."""

    def __call__(self, argv: Sequence[str], cwd: Path) -> CompletedRun:
        self.writes = (cwd / "target" / "manifest.json", json.loads(DBT_MANIFEST.read_text(encoding="utf-8")))
        return super().__call__(argv, cwd)


def test_dbt_parses_again_only_for_an_overlay_of_something_dbt_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    # Packages dbt has not installed are refused before dbt starts (SST-DBT026); this one needs none.
    (project / "packages.yml").unlink()
    dbt = _ParsingDbt()
    monkeypatch.setattr(
        "snowflake_semantic_tools.api.session.YamlProjectInputs", functools.partial(YamlProjectInputs, dbt_runner=dbt)
    )

    def parses() -> int:
        return sum(run[1] == "parse" for run in dbt.runs)

    session = ProjectSession.open(project)
    assert parses() == 1
    semantic = session.with_overlay({METRIC: _edited(project, METRIC, "in cents.", "in cents (semantic).")})
    assert parses() == 1
    model_yaml = "models/marts/orders.yml"
    session.with_overlay({model_yaml: _edited(project, model_yaml, "description:", "description: Edited.")})
    assert parses() == 2
    # Read from what the project's own session parsed, it answers as a session that parses does.
    assert _answers(semantic) == _answers(ProjectSession.open(_saved(tmp_path, project, dict(semantic.overlay))))

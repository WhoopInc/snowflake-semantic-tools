"""A session reloaded after an edit answers as a session opened fresh on the edited files does.

A reload reuses each view no input of which changed, so these edit one kind of file at a time,
in one session, and compare every answer to a new session's: the diagnostics and summary, the
catalog, and every artifact rendered.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.api import ProjectSession
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from tests.helpers.reference_project import DBT_MANIFEST, MENU_VIEW, VIEWS, project_copy

Edit = Callable[[Path], None]


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


def _replace(relative: str, old: str, new: str) -> Edit:
    def edit(project: Path) -> None:
        path = project / relative
        text = path.read_text(encoding="utf-8")
        assert old in text, f"{old!r} is not in {relative}"
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    return edit


def _delete(relative: str) -> Edit:
    def edit(project: Path) -> None:
        (project / relative).unlink()

    return edit


def _write(relative: str, text: str) -> Edit:
    def edit(project: Path) -> None:
        (project / relative).write_text(text, encoding="utf-8")

    return edit


# Each edit in turn, to one session: a metric every view may hold, a view, a member a view
# lists, a member deleted and one added, an edit that breaks a view and the one that fixes
# it, an agent, and the configuration every view reads.
_EDITS: list[tuple[str, Edit]] = [
    ("metric", _replace("semantic_models/metrics/total_revenue.yml", "in cents.", "in cents, tax included.")),
    ("view", _replace(MENU_VIEW, "ref('products')", "ref('supplies')")),
    ("view-restored", _replace(MENU_VIEW, "ref('supplies')", "ref('products')")),
    ("view-broken", _replace(MENU_VIEW, "ref('products')", "ref('no_such_model')")),
    ("view-fixed", _replace(MENU_VIEW, "ref('no_such_model')", "ref('products')")),
    ("member-deleted", _delete("semantic_models/metrics/product_count.yml")),
    (
        "member-added",
        _write(
            "semantic_models/metrics/order_total_sum.yml",
            "metric:\n  name: order_total_sum\n  description: Sum of order totals.\n"
            "  expr: \"SUM({{ ref('orders', 'order_total') }})\"\n",
        ),
    ),
    (
        "agent",
        _replace(
            "agents/jaffle_analytics/agent.yml",
            "description:",
            "description: Edited.",
        ),
    ),
    ("views-file", _replace(VIEWS, "description:", "description: Edited.")),
    ("config", _replace("sst_config.yml", "completed_state: completed", "completed_state: done")),
    # The configuration outside what a view's build reads, then the folder route a view resolves.
    ("config-agents", _replace("sst_config.yml", "  +query_timeout: 60", "  +query_timeout: 90")),
    ("config-route", _replace("sst_config.yml", "    core:\n      +schema: CORE", "    core:\n      +schema: CORE2")),
    (
        "config-tags",
        _replace(
            "sst_config.yml",
            '  default_prefix: "{{ target.database }}.{{ target.schema }}"',
            '  default_prefix: "{{ target.database }}.TAGS"',
        ),
    ),
]


def test_a_reload_after_each_edit_answers_as_a_fresh_session_does(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    for name, edit in _EDITS:
        before = _answers(session)
        edit(project)

        session = session.reload(dbt=False)

        fresh = _answers(ProjectSession.open(project, manifest_path=DBT_MANIFEST))
        assert _answers(session) == fresh, f"after the {name} edit, the reload differs from a fresh session"
        assert fresh != before, f"the {name} edit changed nothing, so it tests nothing"


def test_a_reload_with_nothing_changed_answers_as_the_session_it_came_from(tmp_path: Path) -> None:
    session = ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST)

    assert _answers(session.reload(dbt=False)) == _answers(session)


def _views(session: ProjectSession) -> dict[str, object]:
    return {item.artifact_key: item for item in session.compiled.compiled if item.artifact_type == "semantic_view"}


def test_a_view_no_edit_reached_is_reused_and_the_edited_one_is_built_again(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    before = _views(session)
    assert len(before) > 1

    _replace("agents/jaffle_analytics/agent.yml", "description:", "description: Edited.")(project)
    untouched = _views(session.reload(dbt=False))
    _replace(MENU_VIEW, "    Order line items,", "    Every order line item,")(project)
    edited = _views(session.reload(dbt=False))

    assert all(untouched[key] is view for key, view in before.items())
    rebuilt = {key for key, view in edited.items() if view is not before[key]}
    assert rebuilt == {"semantic_view:jaffle_menu"}


def test_an_edit_to_configuration_no_view_reads_reuses_every_view(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    before = _views(session)

    _replace("sst_config.yml", "  +query_timeout: 60", "  +query_timeout: 90")(project)
    after = _views(session.reload(dbt=False))
    _replace("sst_config.yml", "  completed_state: completed", "  completed_state: done")(project)
    revalued = _views(session.reload(dbt=False))

    assert all(after[key] is view for key, view in before.items())
    # `vars:` is read by every view's expressions, so each is built again.
    assert all(revalued[key] is not view for key, view in before.items())


def _skills(session: ProjectSession) -> dict[str, object]:
    return {item.artifact_key: item for item in session.compiled.compiled if item.artifact_type in ("skill", "plugin")}


def test_the_skill_catalog_is_read_again_only_when_a_skill_or_plugin_file_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    reads: list[Path] = []
    loaded = load_skill_catalog

    def loading(project_dir: Path, **options: Any) -> SkillCatalog:
        reads.append(project_dir)
        return loaded(project_dir, **options)

    monkeypatch.setattr("snowflake_semantic_tools.adapters.project_source.load_skill_catalog", loading)
    session.reload(dbt=False)
    _replace("agents/jaffle_analytics/agent.yml", "description:", "description: Edited.")(project)
    session.reload(dbt=False)
    assert reads == []

    skill = project / "skills/jaffle-catalogue/SKILL.md"
    skill.write_text(skill.read_text(encoding="utf-8") + "\nOne more line.\n", encoding="utf-8")
    edited = session.reload(dbt=False)
    (project / "skills/jaffle-catalogue/notes.md").write_text("Notes.\n", encoding="utf-8")
    added = session.reload(dbt=False)
    _replace("sst_config.yml", "  jaffle-operations:\n    +schema: JAFFLE", "  jaffle-operations:\n    +schema: OPS")(
        project
    )
    rerouted = session.reload(dbt=False)

    # A changed file, an added one, and a changed route each read the catalog again.
    assert len(reads) == 3
    monkeypatch.undo()
    assert _skills(edited) != _skills(session)
    assert _answers(rerouted) == _answers(ProjectSession.open(project, manifest_path=DBT_MANIFEST))
    assert {key for key, item in _skills(added).items() if item is not _skills(edited)[key]} >= {
        "skill:jaffle-catalogue"
    }

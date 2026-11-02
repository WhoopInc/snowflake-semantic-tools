"""New artifacts and members start as files where discovery finds them; a removal takes exactly one out.

Nothing here writes to the project: every answer is a set of edits, checked by handing it to
`with_overlay` and reading the session that returns.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from snowflake_semantic_tools.api import ProjectSession
from snowflake_semantic_tools.domain.authoring import NEW_KINDS
from snowflake_semantic_tools.domain.model.authored_schema.locations import schema_kind
from tests.helpers.reference_project import DBT_MANIFEST, VIEWS, project_copy

# The schema kind each new item's first file is checked as.
_CHECKED_AS = {
    "semantic_view": "semantic_file",
    "tool": "tool",
    "skill": "skill",
    "plugin": "plugin",
    "coco_profile": "coco_profile",
    "agent": "agent",
    "cowork_profile": "cowork_profile",
    "relationship": "semantic_file",
    "metric": "semantic_file",
    "filter": "semantic_file",
    "verified_query": "semantic_file",
    "custom_instruction": "semantic_file",
}


@pytest.fixture
def session(tmp_path: Path) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST)


def _snapshot(project: Path) -> dict[str, bytes]:
    return {path.relative_to(project).as_posix(): path.read_bytes() for path in project.rglob("*") if path.is_file()}


def _front_matter(text: str) -> dict[str, object]:
    return dict(yaml.safe_load(text.split("---\n")[1]))


def test_every_new_kind_starts_where_discovery_reads_it_and_writes_nothing(session: ProjectSession) -> None:
    assert set(NEW_KINDS) == set(_CHECKED_AS)
    before = _snapshot(session.project_dir)
    for kind in NEW_KINDS:
        files = session.new_files(kind, f"new_{kind}")
        first = next(iter(files))
        assert schema_kind(first, {}) == _CHECKED_AS[kind], kind
        text = files[first].decode("utf-8")
        document = _front_matter(text) if kind == "skill" else yaml.safe_load(text)
        assert f"new_{kind}" in str(document), kind
        session.with_overlay(files)
    assert _snapshot(session.project_dir) == before


def test_a_new_agent_in_a_routed_folder_publishes_where_the_folder_routes_it(session: ProjectSession) -> None:
    files = session.new_files("agent", "new_ops_agent", ("ops",))

    assert list(files) == ["agents/ops/new_ops_agent/agent.yml"]
    drafted = {item.key: item for item in session.with_overlay(files).catalog()}
    agent = drafted["agent:new_ops_agent"]
    assert agent.route_folder == ("ops", "new_ops_agent")
    assert agent.settings["schema"].source == "route:ops"
    assert agent.target.split(".")[1] == "OPS"


def test_a_new_skill_is_a_folder_named_for_it_with_its_front_matter(session: ProjectSession) -> None:
    files = session.new_files("skill", "fresh-skill", ("jaffle-operations",))

    assert list(files) == ["skills/jaffle-operations/fresh-skill/SKILL.md"]
    text = files["skills/jaffle-operations/fresh-skill/SKILL.md"].decode("utf-8")
    assert _front_matter(text) == {"name": "fresh-skill", "description": ""}
    assert text.endswith("---\n\n# fresh-skill\n")


@pytest.mark.parametrize(
    ("kind", "name", "folder"),
    [
        ("agent", "", ()),
        ("agent", "../escape", ()),
        ("agent", ".hidden", ()),
        ("agent", "fine", ("..",)),
        ("agent", "fine", ("a/b",)),
        ("coco_profile", "fine", ("nested",)),
        ("eval", "fine", ()),
        ("dimension", "fine", ()),
    ],
)
def test_a_new_item_refuses_a_kind_name_or_folder_it_cannot_place(
    session: ProjectSession, kind: str, name: str, folder: tuple[str, ...]
) -> None:
    with pytest.raises(ValueError):
        session.new_files(kind, name, folder)


def test_a_new_item_refuses_to_start_a_file_that_exists(session: ProjectSession) -> None:
    with pytest.raises(FileExistsError, match="agents/ops/jaffle_minimal/agent.yml"):
        session.new_files("agent", "jaffle_minimal", ("ops",))


def test_a_new_eval_starts_beside_its_agent_and_the_agent_names_it(session: ProjectSession) -> None:
    files = session.new_eval("agent:jaffle_minimal_agent")

    folder = "agents/ops/jaffle_minimal"
    assert set(files) == {f"{folder}/evals/dataset.yml", f"{folder}/evals/config.yml", f"{folder}/agent.yml"}
    manifest = yaml.safe_load(files[f"{folder}/agent.yml"])
    assert manifest["evals"] == {
        "dataset": "{{ eval_dataset('jaffle_minimal_agent_questions') }}",
        "config": "{{ eval_config('jaffle_minimal_agent_eval') }}",
    }
    assert yaml.safe_load(files[f"{folder}/evals/dataset.yml"])["name"] == "jaffle_minimal_agent_questions"
    with pytest.raises(ValueError, match="names an eval already"):
        session.new_eval("agent:jaffle_analytics_agent")
    with pytest.raises(ValueError, match="evaluates an agent"):
        session.new_eval("skill:jaffle-catalogue")


def test_every_artifact_removed_is_gone_from_the_project_it_leaves(session: ProjectSession) -> None:
    before = _snapshot(session.project_dir)
    for item in session.catalog():
        left = {other.key for other in session.with_overlay(session.removal(item.key)).catalog()}
        assert item.key not in left, item.key
    assert _snapshot(session.project_dir) == before


def test_removing_a_skill_keeps_a_skill_nested_in_its_folder(session: ProjectSession) -> None:
    edits = session.removal("skill:jaffle-operations")

    assert edits and all(value is None for value in edits.values())
    assert all(path.startswith("skills/jaffle-operations/") for path in edits)
    assert not any(path.startswith("skills/jaffle-operations/supply-backfill/") for path in edits)


def test_removing_one_view_of_a_file_of_several_keeps_the_others_and_the_comments(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    single = project / "semantic_models/semantic_views/jaffle_minimal.yml"
    entry = yaml.safe_dump([yaml.safe_load(single.read_text(encoding="utf-8"))["semantic_view"]], sort_keys=False)
    with (project / VIEWS).open("a", encoding="utf-8") as views:
        views.write("".join(f"  {line}\n" for line in entry.splitlines()))
    single.unlink()
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    assert {"semantic_view:jaffle_minimal", "semantic_view:jaffle_sales"} <= {item.key for item in session.catalog()}

    edits = session.removal("semantic_view:jaffle_sales")

    assert list(edits) == [VIEWS]
    rewritten = edits[VIEWS]
    assert rewritten is not None
    comment = next(line for line in (project / VIEWS).read_text(encoding="utf-8").splitlines() if line.startswith("#"))
    assert comment in rewritten.decode("utf-8")
    left = {item.key for item in session.with_overlay(edits).catalog()}
    assert "semantic_view:jaffle_minimal" in left and "semantic_view:jaffle_sales" not in left


def test_removing_the_only_view_of_its_file_deletes_the_file(session: ProjectSession) -> None:
    assert session.removal("semantic_view:jaffle_sales") == {VIEWS: None}


def test_removing_an_eval_deletes_its_files_and_its_agent_stops_naming_it(session: ProjectSession) -> None:
    edits = session.removal("eval:jaffle_analytics_agent")

    manifest = "agents/jaffle_analytics/agent.yml"
    rewritten = edits[manifest]
    assert rewritten is not None and "evals" not in yaml.safe_load(rewritten)
    assert [path for path, value in edits.items() if value is None]
    left = {item.key for item in session.with_overlay(edits).catalog()}
    assert "agent:jaffle_analytics_agent" in left and "eval:jaffle_analytics_agent" not in left


def test_removing_an_artifact_the_project_does_not_compile_is_refused(session: ProjectSession) -> None:
    with pytest.raises(KeyError):
        session.removal("agent:nobody")


def test_a_removal_reads_files_whose_templates_are_unquoted_as_sst_reads_them(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    member = project / "semantic_models/filters/is_large_order.yml"
    quoted = "expr: \"{{ ref('orders', 'order_total') }} > large_order_cents\""
    text = member.read_text(encoding="utf-8")
    assert quoted in text
    member.write_text(
        text.replace(quoted, "expr: {{ ref('orders', 'order_total') }} > large_order_cents"), encoding="utf-8"
    )
    session = ProjectSession.open(project, manifest_path=DBT_MANIFEST)
    assert "semantic_view:jaffle_sales" in {item.key for item in session.catalog()}

    assert session.removal("semantic_view:jaffle_sales") == {VIEWS: None}

"""Where a project publishes, per declared target, and which setting says so.

`locations` reads the configuration as a run for each target reads it. These tests hold it to
what a compile publishes: the placement it reports for a route folder is where every artifact in
that folder lands on the session's target, on the reference project as written and on one whose
routes use the shapes real projects use -- variables holding conditionals, tuples of targets,
nested routes, a route to a folder that is not there, and a target whose schema is an unset
`env_var()`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.api import BlockLocations, Locations, ObjectSummary, ProjectSession, RouteFolder
from tests.helpers.reference_project import CONFIG, DBT_MANIFEST, project_copy

_ROUTED = {
    "semantic_views": "semantic_view",
    "tools": "tool",
    "skills": "skill",
    "plugins": "plugin",
    "coco_profiles": "coco_profile",
    "agents": "agent",
    "cowork_profiles": "cowork_profile",
    "evals": "eval",
}

_SHAPES = """\
vars:
  completed_state: completed
  returned_state: returned
  production_targets: [prod]
  not_targets: [prod, nowhere]
  agents_database: "{{ 'PROD_AGENTS' if target.name in var('production_targets') else '' }}"
"""

_AGENTS = """\
  ops:
    +database: "{{ var('agents_database') }}"
    +schema: "{{ 'OPS' if target.name in ('prod', 'ci') else 'OPS_DEV' }}"
    jaffle_minimal:
      +budget_seconds: 30
  nowhere:
    +schema: NOWHERE
    deeper:
      +schema: DEEPER
"""


def _block(locations: Locations, name: str) -> BlockLocations:
    return next(block for block in locations.blocks if block.block == name)


def _folder(block: BlockLocations, *folder: str) -> RouteFolder:
    return next(item for item in block.folders if item.folder == folder)


def _placing(block: BlockLocations, item: ObjectSummary) -> RouteFolder:
    """The closest route folder `item`'s folder starts with: the one whose settings apply to it."""
    starts = [route for route in block.folders if item.route_folder[: len(route.folder)] == route.folder]
    return max(starts, key=lambda route: len(route.folder))


def _assert_placements_match_the_catalog(session: ProjectSession) -> int:
    """Each artifact a route places lands where `locations` says; returns how many were checked."""
    locations = session.locations()
    by_type = {block.artifact_type: block for block in locations.blocks}
    checked = 0
    for item in session.catalog():
        block = by_type.get(item.type)
        if block is None or not block.folders[0].placement:
            continue
        if any(setting.source == "config" for key, setting in item.settings.items() if key in ("database", "schema")):
            continue
        placed = _placing(block, item).placement[locations.session_target]
        assert ".".join(item.target.split(".")[:2]) == f"{placed.database}.{placed.schema}", item.key
        checked += 1
    return checked


def _shaped(tmp_path: Path) -> Path:
    project = project_copy(tmp_path)
    config = project / CONFIG
    text = config.read_text(encoding="utf-8")
    text = text.replace("vars:\n  completed_state: completed\n  returned_state: returned\n", _SHAPES, 1)
    text = text.replace("  ops:\n    +schema: OPS\n", _AGENTS, 1)
    config.write_text(text, encoding="utf-8")
    return project


@pytest.mark.parametrize("target", ["dev", "prod"])
def test_every_route_places_its_artifacts_where_the_compile_publishes_them(tmp_path: Path, target: str) -> None:
    session = ProjectSession.open(project_copy(tmp_path), target=target, manifest_path=DBT_MANIFEST)

    assert _assert_placements_match_the_catalog(session) == len(
        [item for item in session.catalog() if item.type != "eval"]
    )


def test_the_routes_of_a_real_project_place_its_artifacts_on_every_target(tmp_path: Path) -> None:
    project = _shaped(tmp_path)
    for target in ("dev", "prod", "ci"):
        session = ProjectSession.open(project, target=target, manifest_path=DBT_MANIFEST)
        assert _assert_placements_match_the_catalog(session) > 0, target


def test_locations_list_the_targets_and_the_variables_that_name_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SST_TEST_SNOWFLAKE_SCHEMA", raising=False)
    monkeypatch.setenv("SST_TEST_SNOWFLAKE_DATABASE", "VERIFY_DB")
    locations = ProjectSession.open(_shaped(tmp_path), manifest_path=DBT_MANIFEST).locations()

    assert locations.session_target == "dev"
    assert locations.default_target == "dev"
    assert [target.name for target in locations.targets] == ["dev", "prod", "ci", "verify"]
    verify = locations.targets[-1]
    assert (verify.database, verify.schema) == ("VERIFY_DB", None)
    assert verify.unreadable is not None and "SST_TEST_SNOWFLAKE_SCHEMA" in verify.unreadable
    assert all(target.unreadable is None for target in locations.targets[:-1])
    # `not_targets` names a target the profile lacks; the state vars are no lists.
    assert dict(locations.target_lists) == {"production_targets": ("prod",)}


def test_a_route_reports_each_targets_settings_with_where_they_came_from(tmp_path: Path) -> None:
    agents = _block(ProjectSession.open(_shaped(tmp_path), manifest_path=DBT_MANIFEST).locations(), "agents")

    assert (agents.artifact_type, agents.root, agents.routed) == ("agent", "agents", True)
    assert [item.folder for item in agents.folders] == [
        (),
        ("nowhere",),
        ("ops",),
        ("nowhere", "deeper"),
        ("ops", "jaffle_minimal"),
    ]
    ops = _folder(agents, "ops")
    # A variable holding a conditional resolves as the conditional, per target; '' is the target's own.
    assert ops.settings["prod"]["database"].value == "PROD_AGENTS"
    assert ops.settings["prod"]["database"].source == "route:ops"
    assert ops.settings["dev"]["database"].value == ""
    assert (ops.placement["prod"].database, ops.placement["prod"].schema) == ("PROD_AGENTS", "OPS")
    assert (ops.placement["ci"].database, ops.placement["ci"].schema) == ("SST_REF_CI", "OPS")
    assert (ops.placement["dev"].database, ops.placement["dev"].schema) == ("SST_REF_DEV", "OPS_DEV")
    # A nested route inherits what it does not set, and keeps its own source for what it does.
    minimal = _folder(agents, "ops", "jaffle_minimal")
    assert minimal.settings["dev"]["budget_seconds"].source == "route:ops/jaffle_minimal"
    assert minimal.settings["dev"]["schema"].source == "route:ops"
    assert minimal.settings["dev"]["orchestration_model"].source == "root"
    assert minimal.placement == ops.placement
    # The root's `{{ target.database }}` renders as each target's own database.
    root = _folder(agents)
    assert root.settings["dev"]["database"].value == "{{ target.database }}"
    assert root.placement["prod"].database == "SST_REF_PROD"


def test_a_route_to_a_missing_folder_is_reported_absent_with_its_children(tmp_path: Path) -> None:
    agents = _block(ProjectSession.open(_shaped(tmp_path), manifest_path=DBT_MANIFEST).locations(), "agents")

    assert [item.folder for item in agents.folders if not item.exists] == [("nowhere",), ("nowhere", "deeper")]
    assert all(item.exists for item in agents.folders if item.folder[:1] != ("nowhere",))


def test_each_block_names_its_keys_and_the_type_it_places(tmp_path: Path) -> None:
    locations = ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST).locations()

    assert {block.block: block.artifact_type for block in locations.blocks} == {**_ROUTED, "state": None}
    agents = _block(locations, "agents")
    keys = {key.key: key for key in agents.keys}
    assert keys["database"].location and keys["schema"].location and not keys["budget_seconds"].location
    assert keys["orchestration_model"].default == "auto"
    assert keys["schema"].summary
    # Evals publish into their agent's schema: no key of theirs places anything.
    evals = _block(locations, "evals")
    assert not any(key.location for key in evals.keys)
    assert all(not item.placement for item in evals.folders)
    state = _block(locations, "state")
    assert (state.routed, state.root, [item.folder for item in state.folders]) == (False, "", [()])
    assert state.folders[0].placement["prod"].database == "SST_REF_PROD"


def test_catalog_items_carry_their_route_folder_and_settings(tmp_path: Path) -> None:
    catalog = {
        item.key: item for item in ProjectSession.open(project_copy(tmp_path), manifest_path=DBT_MANIFEST).catalog()
    }

    view = catalog["semantic_view:jaffle_menu"]
    assert view.route_folder == ("semantic_views", "core")
    assert view.settings["schema"].source == "route:semantic_views/core"
    agent = catalog["agent:jaffle_minimal_agent"]
    assert agent.route_folder == ("ops", "jaffle_minimal")
    assert agent.settings["schema"].source == "route:ops"
    assert catalog["eval:jaffle_analytics_agent"].route_folder == ("jaffle_analytics",)
    assert catalog["coco_profile:jaffle_analyst"].route_folder == ("jaffle_analyst",)
    assert catalog["skill:supply-backfill"].route_folder == ("jaffle-operations", "supply-backfill")
    assert catalog["tool:order_status"].route_folder == ("platform",)

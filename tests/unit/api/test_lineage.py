"""`ProjectSession.lineage` traces what `sst lineage --no-manifest --output json` traces.

Each parity test runs the real command in-process on the same project and compares node by node
and edge by edge.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.api import Lineage, ProjectSession
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.reference_project import DBT_MANIFEST, project_copy


def _cli_lineage(project: Path, node: str, *flags: str) -> dict[str, Any]:
    args = ["lineage", node, "--project-dir", str(project), "--manifest", str(DBT_MANIFEST), "--no-manifest"]
    result = CliRunner().invoke(cli, [*args, *flags, "--output", "json"])
    assert result.exit_code == 0, result.output
    data: dict[str, Any] = json.loads(result.output)["data"]
    return data


def _as_json(lineage: Lineage) -> dict[str, Any]:
    nodes = [
        {
            "key": item.key,
            "kind": item.kind,
            "name": item.name,
            "direction": item.direction,
            "depth": item.depth,
            "relation": item.relation,
            "marker": item.marker,
        }
        for item in lineage.nodes
    ]
    edges = [{"from": item.source, "to": item.target, "kind": item.kind} for item in lineage.edges]
    return {"node": lineage.node, "start": list(lineage.start), "nodes": nodes, "edges": edges}


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path_factory.mktemp("lineage")), manifest_path=DBT_MANIFEST)


@pytest.mark.parametrize(
    ("node", "flags", "options"),
    [
        ("semantic_view:jaffle_menu", (), {}),
        ("semantic_view:jaffle_menu", ("--upstream",), {"downstream": False}),
        ("semantic_view:jaffle_menu", ("--downstream",), {"upstream": False}),
        ("semantic_view:jaffle_menu", ("--depth", "1"), {"depth": 1}),
        ("model:products", (), {}),
        ("jaffle_menu", (), {}),
    ],
)
def test_lineage_traces_what_the_command_traces(
    session: ProjectSession, node: str, flags: tuple[str, ...], options: dict[str, Any]
) -> None:
    expected = _cli_lineage(session.project_dir, node, *flags)
    actual = _as_json(session.lineage(node, **options))

    assert actual == {key: expected[key] for key in actual}
    assert len(actual["nodes"]) > 1


def test_lineage_refuses_a_node_the_graph_does_not_hold(session: ProjectSession) -> None:
    with pytest.raises(KeyError, match="semantic_view:no_such_view"):
        session.lineage("semantic_view:no_such_view")


def test_lineage_builds_the_graph_once_per_session(session: ProjectSession) -> None:
    first = session._lineage_graph()

    session.lineage("model:products")

    assert session._lineage_graph() is first

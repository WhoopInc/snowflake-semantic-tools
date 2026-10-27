"""Single-item files: a singular root read as a one-item list, wherever discovery finds it."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments, load_documents
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.model.authored import SourcePosition
from tests.helpers.diagnostic_filters import coded
from tests.helpers.file_trees import write_tree
from tests.helpers.seam_projects import KEY, VIEWS, SmallProject

METRIC = f'---\n# One metric.\nmetric:\n  name: total\n  description: Total.\n  expr: "COUNT({KEY})"\n'
LISTED = f'snowflake_metrics:\n  - name: total\n    description: Total.\n    expr: "COUNT({KEY})"\n'
VIEW = (
    "semantic_view:\n  name: catalog\n  description: Use this view for questions about the product catalog.\n"
    "  tables:\n    - \"{{ ref('products') }}\"\n"
)


def _loaded(tmp_path: Path, files: dict[str, str]) -> RawDocuments:
    write_tree(tmp_path, {f"semantic_models/{path}": text for path, text in files.items()})
    return load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)


def test_a_singular_root_reads_as_a_one_item_list_with_its_positions_moved(tmp_path: Path) -> None:
    [document] = _loaded(tmp_path, {"metrics/total.yml": "other: 1\n" + METRIC.removeprefix("---\n")}).documents
    assert document.root_keys == ("other", "snowflake_metrics")
    assert document.single_item_roots == frozenset({"snowflake_metrics"})
    assert document.tree["snowflake_metrics"] == [{"name": "total", "description": "Total.", "expr": f"COUNT({KEY})"}]
    assert document.position(("snowflake_metrics",)) == SourcePosition(4, 3)
    assert document.position(("snowflake_metrics", 0)) == SourcePosition(4, 3)
    assert document.position(("snowflake_metrics", 0, "expr")) == SourcePosition(6, 9)
    assert document.position(("metric", "expr")) is None
    assert document.position(("other",)) == SourcePosition(1, 8)


def test_a_list_root_is_read_as_written(tmp_path: Path) -> None:
    [document] = _loaded(tmp_path, {"metrics/m.yml": LISTED}).documents
    assert document.single_item_roots == frozenset()
    assert document.position(("snowflake_metrics", 0, "expr")) == SourcePosition(4, 11)


def test_both_forms_of_one_type_in_a_file_read_the_list_alone(tmp_path: Path) -> None:
    loaded = _loaded(tmp_path, {"metrics/m.yml": LISTED + METRIC.removeprefix("---\n# One metric.\n")})
    [diagnostic] = coded(loaded.diagnostics, "SST-PRS003")
    assert diagnostic.message == (
        "semantic_models/metrics/m.yml: 'root' expects one of metric: and snowflake_metrics:, found both"
    )
    assert diagnostic.origin is not None and diagnostic.origin.line == 6
    [document] = loaded.documents
    assert (document.root_keys, document.single_item_roots) == (("snowflake_metrics",), frozenset())
    assert [node["name"] for node in document.tree["snowflake_metrics"]] == ["total"]
    assert not any(path and path[0] == "metric" for path in document.line_index)


def test_a_singular_root_that_holds_no_mapping_is_refused_and_dropped(tmp_path: Path) -> None:
    loaded = _loaded(tmp_path, {"metrics/m.yml": "metric:\n  - name: total\n", "filters/f.yml": "filter:\n"})
    assert [item.message for item in coded(loaded.diagnostics, "SST-PRS003")] == [
        "semantic_models/filters/f.yml: 'filter' expects a mapping, found NoneType",
        "semantic_models/metrics/m.yml: 'metric' expects a mapping, found list",
    ]
    assert [(item.root_keys, dict(item.line_index)) for item in loaded.documents] == [
        ((), {(): SourcePosition(1, 1)})
    ] * 2


def test_single_item_files_are_discovered_anywhere_and_build_as_their_list_would(tmp_path: Path) -> None:
    # SmallProject's own view is the list form of VIEW, at VIEWS.
    single = SmallProject(
        tmp_path / "single", files={"semantic_models/anywhere/deep/total.yml": METRIC, VIEWS: VIEW}
    ).load()
    listed = SmallProject(tmp_path / "listed", files={"semantic_models/metrics/m.yml": LISTED}).load()
    assert {item.code for item in single.diagnostics} == {item.code for item in listed.diagnostics}
    [view], [other] = single.views, listed.views
    assert view.fqn == other.fqn and view.fqn.endswith("CATALOG")
    assert [metric.name for metric in view.metrics] == [metric.name for metric in other.metrics] == ["TOTAL"]
    origin = view.member_origins["metric:total"]
    assert (origin.file, origin.line, origin.col) == ("semantic_models/anywhere/deep/total.yml", 4, 3)

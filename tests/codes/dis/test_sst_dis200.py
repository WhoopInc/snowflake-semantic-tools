"""SST-DIS200: discovery gave a semantic-model file to the types that own it."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import load_documents
from snowflake_semantic_tools.adapters.yaml.ownership import assign_owners, ownership_report
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dis_codes import project
from tests.helpers.projects import project_paths


def test_sst_dis200_fires(tmp_path: Path) -> None:
    root = project(tmp_path, "snowflake_metrics: []\nsnowflake_filters: []\n")
    documents = load_documents(discover_yaml(root, "semantic_models"), parse_yaml_bytes)
    _, [diagnostic] = assign_owners(documents)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS200", Severity.INFO)
    assert diagnostic.message == "semantic_models/a.yml assigned to metric, filter"
    assert [item.code for item in ownership_report(project_paths(root), {})] == ["SST-DIS200"]


def test_sst_dis200_silent(tmp_path: Path) -> None:
    root = project(tmp_path, "dashboards: []\n")
    assert ownership_report(project_paths(root), {}) == ()
    (tmp_path / "semantic_models" / "a.yml").unlink()
    (tmp_path / "semantic_models").rmdir()
    assert ownership_report(project_paths(root), {}) == ()

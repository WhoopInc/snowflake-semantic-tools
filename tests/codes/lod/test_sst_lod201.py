"""SST-LOD201: a file read again in the same run, with unchanged bytes, is served from the load cache."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.documents import LoadCache, discover_yaml, load_documents
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import SmallProject

VIEWS = "semantic_models/semantic_views/views.yml"


def test_sst_lod201_fires(tmp_path: Path) -> None:
    SmallProject(tmp_path).write()
    cache = LoadCache()
    files = discover_yaml(tmp_path, "semantic_models")
    first = load_documents(files, parse_yaml_bytes, cache)
    [diagnostic] = load_documents(files, parse_yaml_bytes, cache).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD201", Severity.INFO)
    assert diagnostic.message == f"{VIEWS} served from the load cache"
    assert diagnostic.origin == Origin(VIEWS)
    assert first.diagnostics == ()


def test_sst_lod201_silent(tmp_path: Path) -> None:
    SmallProject(tmp_path).write()
    cache = LoadCache()
    load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes, cache)
    # An edited file is parsed again rather than served stale.
    (tmp_path / VIEWS).write_text((tmp_path / VIEWS).read_text(encoding="utf-8") + "# edited\n", encoding="utf-8")
    again = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes, cache)
    assert "SST-LOD201" not in [item.code for item in again.diagnostics]

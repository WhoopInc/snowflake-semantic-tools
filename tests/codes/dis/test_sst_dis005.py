"""SST-DIS005: a directory link loops back into the tree, or the tree is too deep."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.discover import MAX_DEPTH, discover_yaml
from snowflake_semantic_tools.domain.diagnostics import Severity


def models(project: Path, *files: str) -> Path:
    root = project / "semantic_models"
    root.mkdir()
    for name in files:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("snowflake_metrics: []\n", encoding="utf-8")
    return root


def test_sst_dis005_fires(tmp_path: Path) -> None:
    root = models(tmp_path, "sub/metrics.yml")
    (root / "sub" / "loop").symlink_to("..")
    [diagnostic] = discover_yaml(tmp_path, "semantic_models").diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS005", Severity.ERROR)
    assert diagnostic.message == "semantic_models/sub/loop exceeds the traversal depth limit"


def test_sst_dis005_fires_past_the_depth_limit(tmp_path: Path) -> None:
    models(tmp_path, "/".join(["d"] * (MAX_DEPTH + 1)) + "/metrics.yml")
    [diagnostic] = discover_yaml(tmp_path, "semantic_models").diagnostics[:1]
    assert diagnostic.code == "SST-DIS005"


def test_sst_dis005_silent(tmp_path: Path) -> None:
    # A link to a directory outside the tree is not a loop; it is not walked, as before.
    root = models(tmp_path, "metrics.yml")
    (tmp_path / "elsewhere").mkdir()
    (root / "shared").symlink_to(tmp_path / "elsewhere")
    assert discover_yaml(tmp_path, "semantic_models").diagnostics == ()

"""What the per-code DIS tests share: semantic-model folders and one-file projects on disk."""

from __future__ import annotations

from pathlib import Path


def models(project: Path, *files: str) -> Path:
    """Create `semantic_models` under `project` holding an empty metrics file at each of `files`."""
    root = project / "semantic_models"
    root.mkdir()
    for name in files:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text("snowflake_metrics: []\n", encoding="utf-8")
    return root


def project(tmp_path: Path, text: str) -> Path:
    """A project under `tmp_path` whose one semantic-model file holds `text`."""
    (tmp_path / "sst_config.yml").write_text("project: {}\n", encoding="utf-8")
    (tmp_path / "semantic_models").mkdir()
    (tmp_path / "semantic_models" / "a.yml").write_text(text, encoding="utf-8")
    return tmp_path

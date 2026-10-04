"""Write a tree of files below a folder, the one way tests lay out a project on disk."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


def write_tree(root: Path, files: Mapping[str, str | bytes]) -> Path:
    """Write each file below `root`, creating its folders, and return `root`."""
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
    return root

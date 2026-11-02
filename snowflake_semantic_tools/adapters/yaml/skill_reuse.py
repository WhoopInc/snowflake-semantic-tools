"""Read a project's skills and plugins again only when what they are read from has changed.

The skill catalog is every `SKILL.md` folder and plugin manifest, read whole; on a project with
many skills, reading them is a large part of every load. `SkillCatalogs` keeps the catalog the
last load read with what it was read from -- each file's bytes and link status under both
roots, and the configuration blocks whose routes are checked against them -- and a load whose
inputs are identical takes that catalog instead of reading it again.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.model.skill import SkillCatalog

# The configuration blocks a skill catalog reads: its folder routes, checked against its roots.
_READ_BLOCKS = ("skills", "plugins")


class SkillCatalogs:
    """The skill catalog one session's last load read, and the inputs it was read from."""

    def __init__(self) -> None:
        self._held: tuple[tuple[object, ...], SkillCatalog] | None = None

    def get(
        self,
        project_dir: Path,
        skills_dir: str,
        plugins_dir: str,
        config: Mapping[str, Any] | None,
        load: Callable[[], SkillCatalog],
    ) -> SkillCatalog:
        """Return the catalog read last when its inputs are identical; else call `load` and keep its result."""
        key = catalog_inputs(project_dir, skills_dir, plugins_dir, config)
        if self._held is not None and self._held[0] == key:
            return self._held[1]
        catalog = load()
        self._held = (key, catalog)
        return catalog


def catalog_inputs(
    project_dir: Path, skills_dir: str, plugins_dir: str, config: Mapping[str, Any] | None
) -> tuple[object, ...]:
    """Return what a skill catalog is read from: the roots, their entries' bytes and links, its routes.

    Every entry under each root is listed with whether it is a link or a folder, and a file
    with the SHA-256 of its bytes, so an edit, a new file, a deleted one or a new link each
    changes the inputs, whatever their modification times.
    """
    blocks = tuple((name, (config or {}).get(name)) for name in _READ_BLOCKS) if config is not None else None
    roots = tuple((root, _entries(project_dir / root)) for root in (skills_dir, plugins_dir))
    return (project_dir, blocks, roots)


def _entries(root: Path) -> tuple[object, ...]:
    if not root.exists() and not root.is_symlink():
        return ()
    entries: list[object] = [("", root.is_symlink(), root.is_dir(), None)]
    for path in sorted(root.rglob("*")):
        link = path.is_symlink()
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() and not link else None
        entries.append((path.relative_to(root).as_posix(), link, path.is_dir(), digest))
    return tuple(entries)

"""Orchestrate `sst migrate split`: plan each member list's split, and the configuration's rewrite."""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace

from snowflake_semantic_tools.domain.migrate.changes import merged
from snowflake_semantic_tools.domain.migrate.config_file import migrate_config
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.refs import MigrationResult
from snowflake_semantic_tools.domain.migrate.split import SplitPlan, split_semantic, split_tools


@dataclass(frozen=True, slots=True)
class SplitFiles:
    """What one split reads, files keyed by project-relative path.

    Attributes:
        tools_dir: The tools directory, project-relative, that tool routes are relative to.
        config: The configuration file's path and text; None when the run reads none.
        has_plugins: The project declares at least one plugin.
    """

    semantic: Mapping[str, str]
    tools: Mapping[str, str] = field(default_factory=dict)
    tools_dir: str = "tools"
    config: tuple[str, str] | None = None
    has_plugins: bool = False


@dataclass(frozen=True, slots=True)
class SplitReport:
    """Every list file the run would split, or refuses to, and the configuration's rewrite.

    Attributes:
        plans: One per file that lists members, in path order; a refused one names its reason.
        config: The configuration file's path and rewrite; None when it reads none.
    """

    plans: tuple[SplitPlan, ...]
    config: tuple[str, MigrationResult] | None = None

    @property
    def splits(self) -> tuple[SplitPlan, ...]:
        """Return the plans that split a file."""
        return tuple(plan for plan in self.plans if plan.refused is None)

    @property
    def refused(self) -> tuple[SplitPlan, ...]:
        """Return the plans that leave a file whole, each with its reason."""
        return tuple(plan for plan in self.plans if plan.refused is not None)

    @property
    def changed(self) -> bool:
        """Whether there is anything to write."""
        return bool(self.splits) or (self.config is not None and self.config[1].changed)


class MigrateSplit:
    """Plan the split in memory; the caller writes it."""

    def __init__(self, files: SplitFiles, outline: Callable[[str], Node | None], exists: Callable[[str], bool]) -> None:
        self._files = files
        self._outline = outline
        self._exists = exists

    def run(self) -> SplitReport:
        """Plan every file's split, refuse each that would overwrite a file, then the configuration.

        A plan is refused when a file it writes already exists, unless that file is the one it
        splits, or when an earlier plan writes it too. Only the routes of a plan that stands
        reach the configuration.
        """
        files = self._files
        folders = {posixpath.dirname(path) for path in files.tools}
        plans: list[SplitPlan] = []
        for path, text in sorted(files.semantic.items()):
            root = self._outline(text)
            plan = (
                split_semantic(text.splitlines(keepends=True), root, path) if root is not None and root.is_map else None
            )
            plans.extend(() if plan is None else (plan,))
        for path, text in sorted(files.tools.items()):
            root = self._outline(text)
            lines = text.splitlines(keepends=True)
            plan = (
                split_tools(lines, root, path, files.tools_dir, folders.__contains__)
                if root is not None and root.is_map
                else None
            )
            plans.extend(() if plan is None else (plan,))
        plans = self._checked(plans)
        return SplitReport(
            tuple(plans), self._config([route for plan in plans if plan.refused is None for route in plan.routes])
        )

    def _checked(self, plans: list[SplitPlan]) -> list[SplitPlan]:
        """Refuse each plan that would write over an existing file, or over another plan's."""
        claimed: set[str] = set()
        checked: list[SplitPlan] = []
        for plan in plans:
            targets = [item.path for item in plan.files]
            taken = next((t for t in targets if t in claimed), None)
            existing = next((t for t in targets if self._exists(t) and t != plan.path), None)
            if taken is not None:
                plan = replace(plan, files=(), routes=(), refused=f"another list is split into {taken} too")
            elif existing is not None:
                plan = replace(plan, files=(), routes=(), refused=f"{existing} already exists")
            claimed.update(targets if plan.refused is None else ())
            checked.append(plan)
        return checked

    def _config(self, routes: list[tuple[str, ...]]) -> tuple[str, MigrationResult] | None:
        """Rewrite the configuration file's moved blocks, unset keys and new routes."""
        if self._files.config is None:
            return None
        path, text = self._files.config
        root = self._outline(text)
        if root is None or not root.is_map:
            return path, MigrationResult(text)
        changes = migrate_config(text.splitlines(keepends=True), root, routes, has_plugins=self._files.has_plugins)
        return path, merged(MigrationResult(text), changes)

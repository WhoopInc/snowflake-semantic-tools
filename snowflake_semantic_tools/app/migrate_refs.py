"""Orchestrate `sst migrate refs` over a project's files.

It reads semantic model, tool, agent, eval, hook and command files, and the configuration file.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.migrate.agents import (
    agent_references,
    agent_structure,
    command_skill,
    eval_structure,
    hook_references,
)
from snowflake_semantic_tools.domain.migrate.calls import tool_arity
from snowflake_semantic_tools.domain.migrate.changes import Changes, merged
from snowflake_semantic_tools.domain.migrate.inferred_tables import inferred_tables
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.refs import (
    FilterSite,
    MigrationResult,
    add_filter_labels,
    migrate_refs,
    rewrite_member_keys,
)
from snowflake_semantic_tools.domain.migrate.semantic import semantic_references, semantic_structure
from snowflake_semantic_tools.domain.migrate.tools import ToolInfo, tool_index, tool_references, tool_structure

# Every kind of rewrite a file's counts report, in the order they are reported.
KINDS = (
    "ref",
    "endpoint",
    "column",
    "inferred",
    "file",
    "labels",
    "reference",
    "tool",
    "tool_column",
    "non_additive",
    "setting",
    "removed",
)


@dataclass(frozen=True, slots=True)
class FileMigration:
    """One file after the migration: its text with every rewrite made, and each call left alone.

    Attributes:
        path: The file's key in the files the run was given; a project-relative path for the CLI.
        result: Every pass's rewrites in turn; `result.text` is the whole file, changed or not.
    """

    path: str
    result: MigrationResult

    def counts(self) -> dict[str, int]:
        """Count the file's rewrites by kind, every kind of `KINDS` present, in that order."""
        return {kind: sum(item.kind == kind for item in self.result.rewrites) for kind in KINDS}


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """Every file one run migrated in memory, in path order, whether or not anything in it changed."""

    files: tuple[FileMigration, ...]

    @property
    def changed(self) -> tuple[FileMigration, ...]:
        """Return the files at least one rewrite changed, in path order; only these have anything to write."""
        return tuple(item for item in self.files if item.result.changed)

    @property
    def untouched(self) -> int:
        """Count what was left alone across every file, where no rewrite of it is safe."""
        return sum(len(item.result.untouched) for item in self.files)


@dataclass(frozen=True, slots=True)
class RefsFiles:
    """The files one run reads, each keyed by project-relative path, by what they hold.

    Attributes:
        config: The configuration file, whose `tool()` calls are rewritten; empty for none.
    """

    semantic: Mapping[str, str]
    tools: Mapping[str, str] = field(default_factory=dict)
    agents: Mapping[str, str] = field(default_factory=dict)
    evals: Mapping[str, str] = field(default_factory=dict)
    hooks: Mapping[str, str] = field(default_factory=dict)
    commands: Mapping[str, str] = field(default_factory=dict)
    config: Mapping[str, str] = field(default_factory=dict)


Outline = Callable[[str], Node | None]


class MigrateRefs:
    """Rewrite every file in memory; the caller decides whether to write."""

    def __init__(
        self, files: RefsFiles, locate: Callable[[str, str], tuple[FilterSite, ...]], outline: Outline
    ) -> None:
        self._files = files
        self._locate = locate
        self._outline = outline

    def run(self) -> MigrationReport:
        """Migrate every file in memory, in path order, and report each one; nothing is written.

        Every file first loses the group of its two-argument `tool()` calls. A semantic file's
        legacy calls and member keys are rewritten next, then each file's references, then its
        structure, each pass reading the result of the one before; a semantic file's boolean
        filters gain their labels last. The tool index is read from the tool files after their
        `tool()` calls are rewritten.
        """
        files = self._files
        tools = {path: tool_arity(text) for path, text in files.tools.items()}
        index = tool_index({path: root for path, item in tools.items() if (root := self._outline(item.text))})
        migrated: list[FileMigration] = []
        for path, text in sorted(files.semantic.items()):
            arity = tool_arity(text)
            result = _chained(arity, self._member_keys(migrate_refs(arity.text)))
            result = self._passes(result, "semantic", path, index)
            migrated.append(FileMigration(path, add_filter_labels(result, self._locate(result.text, path))))
        for path, result in tools.items():
            migrated.append(FileMigration(path, self._passes(result, "tools", path, index)))
        for kind, group in (("agents", files.agents), ("evals", files.evals), ("hooks", files.hooks)):
            migrated.extend(
                FileMigration(path, self._passes(tool_arity(text), kind, path, index)) for path, text in group.items()
            )
        for path, text in files.commands.items():
            arity = tool_arity(text)
            migrated.append(FileMigration(path, _chained(arity, command_skill(arity.text))))
        migrated.extend(FileMigration(path, tool_arity(text)) for path, text in files.config.items())
        return MigrationReport(tuple(sorted(migrated, key=lambda item: item.path)))

    def _member_keys(self, result: MigrationResult) -> MigrationResult:
        """Name each member's listed tables where it does not, then drop the keys SST infers."""
        root = self._outline(result.text)
        if root is None or not root.is_map:
            return rewrite_member_keys(result)
        changes, keep = inferred_tables(result.text.splitlines(keepends=True), root)
        return rewrite_member_keys(merged(result, changes), keep)

    def _passes(self, result: MigrationResult, kind: str, path: str, index: Mapping[str, ToolInfo]) -> MigrationResult:
        """Run a file's reference pass, then its structure pass, each on a fresh outline of the text."""
        root = self._outline(result.text)
        if root is not None and root.is_map:
            result = merged(result, _references(kind, result.text.splitlines(keepends=True), root, path, index))
        root = self._outline(result.text)
        if root is not None and root.is_map:
            result = merged(result, _structure(kind, result.text.splitlines(keepends=True), root))
        return result


def _references(kind: str, lines: list[str], root: Node, path: str, index: Mapping[str, ToolInfo]) -> Changes:
    """Return the reference pass of a file of `kind`; nothing for a kind that has none."""
    if kind == "semantic":
        return semantic_references(root)
    if kind == "tools":
        return tool_references(lines, root, path, index)
    if kind == "agents":
        return agent_references(root, lines, index)
    return hook_references(root) if kind == "hooks" else Changes()


def _structure(kind: str, lines: list[str], root: Node) -> Changes:
    """Return the structure pass of a file of `kind`; nothing for a kind that has none."""
    passes: dict[str, Callable[[list[str], Node], Changes]] = {
        "semantic": semantic_structure,
        "tools": tool_structure,
        "agents": agent_structure,
        "evals": eval_structure,
    }
    chosen = passes.get(kind)
    return chosen(lines, root) if chosen is not None else Changes()


def _chained(first: MigrationResult, then: MigrationResult) -> MigrationResult:
    """Return `then`, which was computed from `first.text`, with `first`'s reports kept too."""
    return MigrationResult(
        then.text,
        tuple(sorted((*first.rewrites, *then.rewrites), key=lambda item: (item.line, item.col))),
        (*first.untouched, *then.untouched),
    )

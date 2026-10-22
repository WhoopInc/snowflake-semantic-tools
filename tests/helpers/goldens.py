"""The engine's golden files: where each compiled payload's golden lives, and comparing and rewriting them.

A golden pins the exact bytes SST renders for one artifact of the reference project. Each
artifact type has one route: a semantic view's DDL sits in the DDL directory itself
(`tests/golden/expected/ddl`), and every other type's payload in a directory beside it named
for the type. Some goldens are optional -- a flattened skill, a plugin manifest, a profile's
prompt and MCP files -- and are compared only when committed.

`CompareGoldens` reads goldens through a `GoldenStore`, so it touches no file;
`UpdateGoldens` writes through a `GoldenWriter`. `GoldenFileStore` is the store on disk and
`tests.helpers.golden_store.InMemoryGoldenStore` the one in memory. This is test tooling:
`tests/unit/test_goldens.py` runs the comparison, and `python -m tests.helpers.update_goldens`
rewrites the goldens.
"""

from __future__ import annotations

import difflib
import itertools
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from snowflake_semantic_tools.adapters.dbt.invoke import INVOCATION_CODES
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.adapters.paths import write_within
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompiledCocoDesktopProfile
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.ports.project import UNKNOWN_GIT_SHA

REPO_ROOT = Path(__file__).resolve().parents[2]
GOLDEN_DDL_DIR = REPO_ROOT / "tests" / "golden" / "expected" / "ddl"
REFERENCE_PROJECT = REPO_ROOT / "tests" / "fixtures" / "reference_project"
DBT_MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"

# Optional goldens the reference project may commit for an extension: where each lives, and
# the bundle member it pins.
_OPTIONAL_EXTENSION_GOLDENS = {
    "skill": ("{name}-flattened.md", "skills/{name}/SKILL.md"),
    "plugin": ("{name}.plugin.json", ".cortex-plugin/plugin.json"),
}


@dataclass(frozen=True, slots=True)
class GoldenPath:
    """Where one golden lives, relative to the golden directories.

    Attributes:
        beside: The directory beside the DDL directory that holds the golden, such as
            `agent` or `eval`; None for the DDL directory itself.
        parts: The path below that directory, one segment per part, joined in order.
    """

    beside: str | None
    parts: tuple[str, ...]


class GoldenStore(Protocol):
    """Committed goldens, read by location."""

    def exists(self, path: GoldenPath) -> bool:
        """Report whether a golden file is at `path`; a missing directory is a missing golden."""
        ...

    def read(self, path: GoldenPath) -> str | None:
        """Return the golden at `path` as UTF-8 text; None when no golden file is there."""
        ...

    def name(self, path: GoldenPath) -> str:
        """Return how a report names the golden at `path`, whether or not a file is there."""
        ...


class GoldenWriter(GoldenStore, Protocol):
    """Committed goldens that `update_goldens` may also rewrite."""

    def write(self, path: GoldenPath, text: str) -> None:
        """Write `text` as the golden at `path`, in UTF-8, creating its directory when missing."""
        ...


class GoldenFileStore:
    """Goldens under one DDL directory and its siblings, read and written as UTF-8.

    A golden's name is its path, built from the DDL directory as given. Only `write` changes a
    file, and only inside the DDL directory's parent (`adapters.paths.write_within`).
    """

    def __init__(self, ddl_dir: Path = GOLDEN_DDL_DIR) -> None:
        self._ddl_dir = ddl_dir

    def exists(self, path: GoldenPath) -> bool:
        return self._path(path).is_file()

    def read(self, path: GoldenPath) -> str | None:
        file = self._path(path)
        if not file.is_file():
            return None
        return file.read_text(encoding="utf-8")

    def name(self, path: GoldenPath) -> str:
        return str(self._path(path))

    def write(self, path: GoldenPath, text: str) -> None:
        write_within(self._ddl_dir.parent, self._path(path), text)

    def _path(self, path: GoldenPath) -> Path:
        base = self._ddl_dir if path.beside is None else self._ddl_dir.parent / path.beside
        return base.joinpath(*path.parts)


@dataclass(frozen=True, slots=True)
class GoldenPayload:
    """One compiled payload and the golden it must equal.

    Attributes:
        compiled_path: How a diff names the compiled side.
        ddl: Whether the golden is DDL, whose leading blank and `--` comment lines are not compared.
    """

    golden: GoldenPath
    content: str
    compiled_path: str
    ddl: bool


@dataclass(frozen=True, slots=True)
class GoldenReport:
    """What a comparison found: one failure per golden that is missing or differs, in compile order.

    Attributes:
        failures: `missing golden <name>`, or a unified diff from the golden to the payload.
        missing: The name of each golden that does not exist, also listed in `failures`.
    """

    failures: tuple[str, ...]
    missing: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        """Report whether every golden was present and equal."""
        return not self.failures


class CompareGoldens:
    """Compare every compiled payload with its committed golden, byte for byte after normalizing.

    Trailing whitespace is not compared, and neither are a DDL golden's leading comments. A
    payload's `GIT_<commit>` becomes `GIT_0000000`, so goldens do not change with each commit.
    """

    def __init__(self, store: GoldenStore, git_sha: Callable[[], str]) -> None:
        self._store = store
        self._git_sha = git_sha

    def run(self, result: CompileResult) -> GoldenReport:
        """Compare each artifact's payloads with their goldens, in compile order.

        The commit is asked for once per golden that exists, when it is compared.

        Raises:
            ValueError: an artifact's type has no golden route, a DDL golden holds no DDL, or a
                golden is not valid UTF-8.
        """
        failures: list[str] = []
        missing: list[str] = []
        for item in result.compiled:
            for payload in golden_payloads(item, self._store):
                failure = self.compare(payload)
                if failure is not None:
                    failures.append(failure)
                if not self._store.exists(payload.golden):
                    missing.append(self._store.name(payload.golden))
        return GoldenReport(tuple(failures), tuple(missing))

    def compare(self, payload: GoldenPayload) -> str | None:
        """Return how a payload fails its golden: missing, or a diff; None when they are equal."""
        text = self._store.read(payload.golden)
        name = self._store.name(payload.golden)
        if text is None:
            return f"missing golden {name}"
        expected = (_ddl_statements(text, name) if payload.ddl else text).rstrip() + "\n"
        actual = _normalized(payload.content, self._git_sha()).rstrip() + "\n"
        if expected == actual:
            return None
        diff = difflib.unified_diff(
            expected.splitlines(),
            actual.splitlines(),
            fromfile=name,
            tofile=payload.compiled_path,
            lineterm="",
        )
        return "\n".join(diff)


class UpdateGoldens:
    """Rewrite every golden a compiled payload no longer equals.

    A golden is rewritten only when `CompareGoldens` would fail it, and as it would expect it:
    trailing whitespace dropped, one final newline, and `GIT_<commit>` as `GIT_0000000`. A DDL
    golden keeps the leading blank and `--` comment lines it already has.
    """

    def __init__(self, store: GoldenWriter, git_sha: Callable[[], str]) -> None:
        self._store = store
        self._git_sha = git_sha
        self._goldens = CompareGoldens(store, git_sha)

    def run(self, result: CompileResult) -> tuple[str, ...]:
        """Write each missing or differing golden, in compile order; return the names written.

        Raises:
            ValueError: as `CompareGoldens.run` raises.
        """
        written: list[str] = []
        for item in result.compiled:
            for payload in golden_payloads(item, self._store):
                if self._goldens.compare(payload) is None:
                    continue
                existing = self._store.read(payload.golden)
                header = _header(existing) if payload.ddl else ""
                text = header + _normalized(payload.content, self._git_sha()).rstrip() + "\n"
                self._store.write(payload.golden, text)
                written.append(self._store.name(payload.golden))
        return tuple(written)


@dataclass(frozen=True, slots=True)
class Compiled:
    """A project compiled twice, the way the goldens are taken from it.

    Attributes:
        result: The first compile, whose payloads the goldens pin.
        again: A second compile of the same files, from the dbt manifest the first one read.
        git_sha: The commit the project's payloads name, asked for when a golden is compared.
    """

    result: CompileResult
    again: CompileResult
    git_sha: Callable[[], str]

    def unstable_codes(self) -> list[str]:
        """Name each code whose fingerprints differ between the two compiles.

        What starting dbt says is left out: only the first compile can start it.
        """
        first = (item for item in self.result.diagnostics if item.code not in INVOCATION_CODES)
        return unstable_codes(first, self.again.diagnostics)


def compile_twice(project_dir: Path, manifest: Path | None) -> Compiled:
    """Compile `project_dir` as `sst compile` would, then again from the dbt manifest it read.

    With no `manifest`, SST runs `dbt parse` itself, as on a user's project; the second
    compile reads the manifest that wrote, so dbt runs once.
    """
    paths = locate_project(project_dir, required=False)
    result = compile_result(paths, None, manifest)
    if manifest is None and (project_dir / "dbt_project.yml").is_file():
        manifest = YamlProjectSource(paths).manifest_file()
    again = compile_result(paths, None, manifest)
    return Compiled(result, again, project_inputs(paths, None, manifest).git_sha)


def unstable_codes(first: Iterable[Diagnostic], second: Iterable[Diagnostic]) -> list[str]:
    """Name each code whose fingerprints differ between two compiles of the same project, sorted.

    The compiles are compared as multisets of fingerprints per code, so order does not matter.
    """
    before: dict[str, Counter[str]] = {}
    after: dict[str, Counter[str]] = {}
    for prints, items in ((before, first), (after, second)):
        for item in items:
            prints.setdefault(item.code, Counter())[item.fingerprint] += 1
    return sorted(code for code in set(before) | set(after) if before.get(code) != after.get(code))


def _header(existing: str | None) -> str:
    """Return a golden's leading blank and `--` comment lines, which a rewrite keeps; empty without any."""
    if existing is None:
        return ""
    lines = existing.splitlines(keepends=True)
    return "".join(itertools.takewhile(lambda line: not line.strip() or line.lstrip().startswith("--"), lines))


def golden_payloads(item: CompiledArtifact, store: GoldenStore) -> tuple[GoldenPayload, ...]:
    """Route an artifact's payloads to their goldens: one explicit route per registered type.

    An optional golden is routed only when the store has it. A golden is named after its
    artifact by `file_name`, as `sst compile --emit-ddl` names the file it writes.

    Raises:
        ValueError: the artifact's type has no golden route.
    """
    name = file_name(item.name.casefold())
    artifact_type = item.artifact_type
    if isinstance(item, CompiledEval):
        return (
            GoldenPayload(
                GoldenPath("eval", (f"{name}_repeat.yaml",)),
                item.rendered.config_yaml,
                f"compiled/{name}_repeat.yaml",
                False,
            ),
            GoldenPayload(
                GoldenPath("eval", (f"{name.removesuffix('_agent')}_source.sql",)),
                item.rendered.source_table_sql,
                f"compiled/{name}_source.sql",
                False,
            ),
        )
    if isinstance(item, CompiledExtension):
        return _extension_payloads(item, name, artifact_type, store)
    rendered = item.rendered_artifact
    if isinstance(item, CompiledCocoDesktopProfile):
        return _profile_payloads(item, name, rendered.content, store)
    if artifact_type == "semantic_view":
        return (GoldenPayload(GoldenPath(None, (f"{name}.sql",)), rendered.content, f"compiled/{name}.sql", True),)
    if artifact_type == "tool":
        return (GoldenPayload(GoldenPath("tool", (f"{name}.sql",)), rendered.content, f"compiled/{name}.sql", True),)
    if artifact_type == "agent":
        return (
            GoldenPayload(GoldenPath("agent", (f"{name}.json",)), rendered.content, f"compiled/{name}.json", False),
        )
    # A CoWork profile (a Snowflake private preview) pins its specification, as an agent does.
    if artifact_type == "cowork_profile":
        return (
            GoldenPayload(
                GoldenPath("cowork_profile", (f"{name}.json",)),
                rendered.content,
                f"compiled/cowork_profile/{name}.json",
                False,
            ),
        )
    raise ValueError(f"no golden route for artifact type {artifact_type!r}")


def _extension_payloads(
    item: CompiledExtension, name: str, artifact_type: str, store: GoldenStore
) -> tuple[GoldenPayload, ...]:
    """Route an extension's bundle manifest, and the bundle member it pins when that golden is committed."""
    payloads = [
        GoldenPayload(
            GoldenPath(artifact_type, (f"{name}.bundle.json",)),
            item.rendered_artifact.content,
            f"compiled/{artifact_type}/{name}.bundle.json",
            False,
        )
    ]
    golden_name, member_path = _OPTIONAL_EXTENSION_GOLDENS[artifact_type]
    golden = GoldenPath(artifact_type, (golden_name.format(name=name),))
    member = member_path.format(name=name)
    entry = next((entry for entry in item.release.bundle.entries if entry.path == member), None)
    if store.exists(golden) and entry is not None:
        payloads.append(
            GoldenPayload(golden, entry.content.decode("utf-8"), f"compiled/{artifact_type}/{member}", False)
        )
    return tuple(payloads)


def _profile_payloads(
    item: CompiledCocoDesktopProfile, name: str, content: str, store: GoldenStore
) -> tuple[GoldenPayload, ...]:
    """Route a profile's registry document, and each prompt and MCP file whose golden is committed."""
    payloads = [
        GoldenPayload(
            GoldenPath("coco_desktop_profile", (f"{name}.coco_desktop_profile.json",)),
            content,
            f"compiled/coco_desktop_profile/{name}.coco_desktop_profile.json",
            False,
        )
    ]
    for tree in item.release.trees:
        for tree_entry in tree.entries:
            golden = GoldenPath("coco_desktop_profile", (name, tree_entry.path))
            if tree.kind in ("prompts", "mcp") and store.exists(golden):
                payloads.append(
                    GoldenPayload(
                        golden,
                        tree_entry.content.decode("utf-8"),
                        f"compiled/coco_desktop_profile/{name}/{tree_entry.path}",
                        False,
                    )
                )
    return tuple(payloads)


def _ddl_statements(text: str, name: str) -> str:
    """Return a DDL golden from its first statement line on, skipping leading blank and `--` lines.

    Raises:
        ValueError: the golden holds no statement line.
    """
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() and not line.lstrip().startswith("--"):
            return "\n".join(lines[index:]).rstrip("\n")
    raise ValueError(f"golden {name} contains no DDL")


def _normalized(value: str, git_sha: str) -> str:
    """Replace the commit in a payload's `GIT_<commit>` with zeros; unchanged outside a git work tree."""
    if git_sha and git_sha != UNKNOWN_GIT_SHA:
        return value.replace(f"GIT_{git_sha}", "GIT_0000000")
    return value

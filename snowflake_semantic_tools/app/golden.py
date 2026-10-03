"""Compare compiled payloads with committed goldens, offline.

Each artifact type has one route to its goldens: a semantic view's DDL sits in the golden
directory itself, and every other type's payload in a directory beside it named for the
type. Some goldens are optional -- a flattened skill, a plugin manifest, a profile's prompt
and MCP files -- and are compared only when committed. `CompareGoldens` reads them through a
`GoldenStore`, so it touches no file.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.profiles import CompiledProfile
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.ports.golden import GoldenPath, GoldenStore

# Optional goldens a reference project may commit for an extension: where each lives, and
# the bundle member it pins.
_OPTIONAL_EXTENSION_GOLDENS = {
    "skill": ("{name}-flattened.md", "skills/{name}/SKILL.md"),
    "plugin": ("{name}.plugin.json", ".cortex-plugin/plugin.json"),
}


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
    """What a golden run found: one failure per golden that is missing or differs, in compile order.

    Attributes:
        failures: `missing golden <name>`, or a unified diff from the golden to the payload.
    """

    failures: tuple[str, ...]

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
            OSError: a golden exists but cannot be read.
        """
        failures: list[str] = []
        for item in result.compiled:
            for payload in golden_payloads(item, self._store):
                failure = self._compare(payload)
                if failure is not None:
                    failures.append(failure)
        return GoldenReport(tuple(failures))

    def _compare(self, payload: GoldenPayload) -> str | None:
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
    if isinstance(item, CompiledProfile):
        return _profile_payloads(item, name, rendered.content, store)
    if artifact_type == "semantic_view":
        return (GoldenPayload(GoldenPath(None, (f"{name}.sql",)), rendered.content, f"compiled/{name}.sql", True),)
    if artifact_type == "tool":
        return (GoldenPayload(GoldenPath("tool", (f"{name}.sql",)), rendered.content, f"compiled/{name}.sql", True),)
    if artifact_type == "agent":
        return (
            GoldenPayload(GoldenPath("agent", (f"{name}.json",)), rendered.content, f"compiled/{name}.json", False),
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


def _profile_payloads(item: CompiledProfile, name: str, content: str, store: GoldenStore) -> tuple[GoldenPayload, ...]:
    """Route a profile's registry document, and each prompt and MCP file whose golden is committed."""
    payloads = [
        GoldenPayload(
            GoldenPath("profile", (f"{name}.profile.json",)),
            content,
            f"compiled/profile/{name}.profile.json",
            False,
        )
    ]
    for tree in item.release.trees:
        for tree_entry in tree.entries:
            golden = GoldenPath("profile", (name, tree_entry.path))
            if tree.kind in ("prompts", "mcp") and store.exists(golden):
                payloads.append(
                    GoldenPayload(
                        golden,
                        tree_entry.content.decode("utf-8"),
                        f"compiled/profile/{name}/{tree_entry.path}",
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
    if git_sha and git_sha != "WORKTREE":
        return value.replace(f"GIT_{git_sha}", "GIT_0000000")
    return value

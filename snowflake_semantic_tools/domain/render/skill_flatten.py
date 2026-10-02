"""Flatten a skill folder for the catalog channel, and check that every path it names survives.

A Cortex Agent reads supporting files beside `SKILL.md` and never descends into a
subdirectory, so the catalog channel publishes each file under the `flattened_name` of its
path. Flattening is three operations in a fixed order: detect collisions in authored terms,
join path components with a double underscore, then rewrite every Markdown reference to the
flattened name and check the rewritten text again. Scripts are run and never rewritten, so
they are checked instead for the paths flattening would break.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.skill.model import SKILL_FILE, Skill, SkillFile
from snowflake_semantic_tools.domain.parse.skill_references import PathReference, scan_references
from snowflake_semantic_tools.domain.validate.shared import Emitter

# Directories an author conventionally bundles. A bare path starting with one of
# them is a bundle reference even when the directory is absent, which is what
# catches a skill that names `scripts/x.sql` while shipping `x.sql` at its root.
CONVENTIONAL_DIRS = frozenset(("scripts", "reference", "references", "examples", "assets", "templates"))

_ABSOLUTE = re.compile(r"(?<![\w./:-])((?:/(?:Users|home|opt)/|~/|[A-Za-z]:\\)[^\s`'\")\]]*)")
_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_SECRET = re.compile(
    r"(?i)\b(?:password|passwd|secret|api[_-]?key|private[_-]?key|access[_-]?token|auth[_-]?token)\b\s*[:=]\s*['\"][^'\"\s]{4,}"
)


def flattened_name(path: str) -> str:
    """Join the components below the skill folder with a double underscore."""
    return "__".join(path.split("/"))


@dataclass(frozen=True, slots=True)
class _Tree:
    """A skill's authored paths, with the two indexes that tell a bundle reference from prose."""

    paths: frozenset[str]
    top_level: frozenset[str]
    basenames: frozenset[str]

    @classmethod
    def of(cls, skill: Skill) -> _Tree:
        paths = frozenset(item.path for item in skill.files)
        return cls(
            paths,
            frozenset(path.split("/", 1)[0] for path in paths),
            frozenset(posixpath.basename(path) for path in paths),
        )


@dataclass(frozen=True, slots=True)
class _Resolution:
    reference: PathReference
    target: str


@dataclass(frozen=True, slots=True)
class _Flattened:
    """One file as published, the authored files its references name, and what they broke."""

    file: SkillFile
    referenced: frozenset[str]
    diagnostics: tuple[Diagnostic, ...]


def flatten_skill(skill: Skill) -> tuple[tuple[SkillFile, ...], tuple[tuple[str, str], ...], DiagnosticBag]:
    """Check for collisions, join paths with `__`, and rewrite Markdown references, for one skill.

    Returns:
        The published files in `files` order, (authored, published) for each nested path, and
        the diagnostics. A collision stops flattening, leaving the files and renames empty.

    Diagnostics:
        SST-VAL809: two authored paths flatten to one name; nothing else is checked then.
        SST-VAL808: a Markdown reference names a bundle file that does not exist.
        SST-VAL810: a Markdown reference names a directory, or walks down from the repository root.
        SST-VAL810: a rewritten reference no longer resolves against the flattened files.
        SST-VAL815: a script holds an absolute path, or any text file a credential literal.
        SST-VAL833: a script names a nested path, which flattening renames.
        SST-VAL813: a bundled file no Markdown reference names.
    """
    collisions = _collisions(skill)
    if collisions:
        return (), (), DiagnosticBag(collisions)
    tree = _Tree.of(skill)
    results = tuple(_flatten_file(skill, item, tree) for item in skill.files)
    flattened = tuple(result.file for result in results)
    referenced = {path for result in results for path in result.referenced}
    diagnostics = [diagnostic for result in results for diagnostic in result.diagnostics]
    diagnostics.extend(_recheck(skill, flattened))
    diagnostics.extend(_script_diagnostics(skill))
    diagnostics.extend(_unreferenced(skill, referenced))
    renames = tuple((item.path, flattened_name(item.path)) for item in skill.files if "/" in item.path)
    return flattened, renames, DiagnosticBag(diagnostics)


def _collisions(skill: Skill) -> tuple[Diagnostic, ...]:
    """Report SST-VAL809 once per set of authored paths that flatten to one name, naming its first two."""
    by_name: dict[str, list[str]] = {}
    for item in skill.files:
        by_name.setdefault(flattened_name(item.path), []).append(item.path)
    return tuple(
        D(
            "SST-VAL809",
            origin=skill.origin_of(authored[0]),
            subject=skill.key,
            artifact=skill.name,
            a=authored[0],
            b=authored[1],
        )
        for authored in sorted(paths for paths in by_name.values() if len(paths) > 1)
    )


def _flatten_file(skill: Skill, item: SkillFile, tree: _Tree) -> _Flattened:
    """Publish one file under its flattened name, rewriting each reference a Markdown file makes.

    A script or binary file is copied unchanged. A reference that resolves to an authored file
    is rewritten to that file's published name; one that does not is classified by `_unresolved`.
    """
    text = item.text
    published = flattened_name(item.path)
    if not item.is_markdown or text is None:
        return _Flattened(SkillFile(published, item.content), frozenset(), ())
    emit = Emitter(subject=skill.key, artifact=skill.name)
    resolutions: list[_Resolution] = []
    referenced: set[str] = set()
    for reference in scan_references(text, item.path):
        if _skip(reference):
            continue
        target = _resolve(reference, tree.paths)
        if target is not None:
            referenced.add(target)
            resolutions.append(_Resolution(reference, target))
            continue
        code, named = _unresolved(reference, skill.name, tree)
        referenced.update(named)
        if code is not None:
            emit(code, origin=skill.origin_of(item.path, reference.line, reference.col), path=reference.text)
    rewritten = SkillFile(published, _rewrite(text, resolutions).encode("utf-8"))
    return _Flattened(rewritten, frozenset(referenced), emit.diagnostics)


def _unresolved(reference: PathReference, skill_name: str, tree: _Tree) -> tuple[str | None, tuple[str, ...]]:
    """Classify a reference to no authored file: the code it earns, and any bundle file it still names.

    A repository-anchored path names its target, just not portably, so that file counts as
    referenced. Only a path that looks like a bundle file, on a line without the ignore marker,
    is reported as absent.
    """
    if _is_repo_anchored(reference, skill_name):
        return "SST-VAL810", _anchored_target(reference, skill_name, tree.paths)
    if _names_directory(reference, tree.paths):
        return "SST-VAL810", ()
    if _looks_bundled(reference, tree) and not reference.suppressed:
        return "SST-VAL808", ()
    return None, ()


def _resolve(reference: PathReference, paths: frozenset[str]) -> str | None:
    base = posixpath.dirname(reference.file)
    for candidate in (posixpath.join(base, reference.text), reference.text):
        normalized = posixpath.normpath(candidate)
        if normalized.startswith("../") or normalized == "..":
            continue
        if normalized in paths:
            return normalized
    return None


def _is_repo_anchored(reference: PathReference, skill_name: str) -> bool:
    """Tell whether a path walks down a `skills/` tree to this skill from the repository root."""
    parts = posixpath.normpath(reference.text).split("/")
    if reference.text.startswith(("./", "../")) or "skills" not in parts:
        return False
    return skill_name in parts[parts.index("skills") + 1 : -1]


def _anchored_target(reference: PathReference, skill_name: str, paths: frozenset[str]) -> tuple[str, ...]:
    """Find the bundle file a repo-anchored reference names: named, just not portably."""
    parts = posixpath.normpath(reference.text).split("/")
    below = parts[parts.index("skills") + 1 :]
    remainder = "/".join(below[below.index(skill_name) + 1 :])
    return (remainder,) if remainder in paths else ()


def _names_directory(reference: PathReference, paths: frozenset[str]) -> bool:
    """Tell whether a reference names a directory, which flattening removes."""
    base = posixpath.dirname(reference.file)
    for candidate in (posixpath.join(base, reference.text), reference.text):
        prefix = posixpath.normpath(candidate) + "/"
        if any(path.startswith(prefix) for path in paths):
            return True
    return False


def _looks_bundled(reference: PathReference, tree: _Tree) -> bool:
    if reference.form in ("link", "image", "definition"):
        return True
    if reference.text.startswith(("./", "../")):
        return True
    first = reference.text.split("/", 1)[0]
    return first in tree.top_level or first in CONVENTIONAL_DIRS or posixpath.basename(reference.text) in tree.basenames


def _skip(reference: PathReference) -> bool:
    return not reference.text or bool(_URL.match(reference.text)) or reference.text.startswith(("/", "~"))


def _rewrite(text: str, resolutions: list[_Resolution]) -> str:
    rewritten = text
    # Right to left, so each splice leaves the offsets of the ones still to come intact.
    for resolution in sorted(resolutions, key=lambda item: item.reference.start, reverse=True):
        published = flattened_name(resolution.target)
        reference = resolution.reference
        # Every flattened file sits beside SKILL.md, so a reference that already
        # resolves from the root to the published name needs no rewrite.
        if posixpath.normpath(reference.text) == published:
            continue
        rewritten = rewritten[: reference.start] + published + rewritten[reference.end :]
    return rewritten


def _recheck(skill: Skill, flattened: tuple[SkillFile, ...]) -> tuple[Diagnostic, ...]:
    """SST-VAL810: the rewritten Markdown must resolve against the flattened tree."""
    paths = frozenset(item.path for item in flattened)
    authored_paths = frozenset(item.path for item in skill.files)
    authored = {flattened_name(item.path): item for item in skill.files}
    diagnostics: list[Diagnostic] = []
    for item in flattened:
        text = item.text
        source = authored[item.path]
        if not item.is_markdown or text is None:
            continue
        resolvable = {
            reference.text
            for reference in scan_references(source.text or "", source.path)
            if not _skip(reference) and _resolve(reference, authored_paths) is not None
        }
        for reference in scan_references(text, item.path):
            if reference.text in resolvable and _resolve(reference, paths) is None:
                diagnostics.append(
                    D(
                        "SST-VAL810",
                        origin=skill.origin_of(source.path, reference.line, reference.col),
                        subject=skill.key,
                        artifact=skill.name,
                        path=reference.text,
                    )
                )
    return tuple(diagnostics)


def _script_diagnostics(skill: Skill) -> tuple[Diagnostic, ...]:
    """SST-VAL815 and the flattening hazard, both for scripts, which are run and never rewritten.

    A literal credential is reported in any text file. An absolute or home-directory
    path is reported only in a script: in Markdown it is an instruction to the agent
    ("save the export to ~/Downloads"), which is how a skill is meant to be written.
    """
    nested = tuple(item.path for item in skill.files if "/" in item.path)
    emit = Emitter(subject=skill.key, artifact=skill.name)
    for item in skill.files:
        text = item.text
        if text is None:
            continue
        for start, detail in _hazards(text, script=not item.is_markdown):
            line = text.count("\n", 0, start) + 1
            emit("SST-VAL815", origin=skill.origin_of(item.path, line), path=item.path, detail=detail)
        if item.is_markdown:
            continue
        for target in _moved_siblings(item.path, text, nested):
            emit("SST-VAL833", origin=skill.origin_of(item.path), path=item.path, target=target)
    return emit.diagnostics


def _hazards(text: str, *, script: bool) -> list[tuple[int, str]]:
    """Locate absolute paths, in a script only, then credential literals, each with how it reads."""
    absolute: Iterable[re.Match[str]] = _ABSOLUTE.finditer(text) if script else ()
    found = [(match.start(), f"the absolute path '{match.group(1)}'") for match in absolute]
    found.extend((match.start(), "a credential literal") for match in _SECRET.finditer(text))
    return found


def _moved_siblings(path: str, text: str, nested: tuple[str, ...]) -> Iterator[str]:
    """Yield each nested path a script names, as written or relative to the script's own folder."""
    for target in nested:
        relative = posixpath.relpath(target, posixpath.dirname(path) or ".")
        if target in text or (relative != target and relative in text):
            yield target


def _unreferenced(skill: Skill, referenced: set[str]) -> tuple[Diagnostic, ...]:
    return tuple(
        D("SST-VAL813", origin=skill.origin_of(item.path), subject=skill.key, artifact=skill.name, path=item.path)
        for item in skill.files
        if item.path != SKILL_FILE and item.path not in referenced
    )

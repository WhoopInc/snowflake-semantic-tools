"""Check the files a skill folder publishes, which every channel ships exactly as authored.

A skill is its folder: `SKILL.md` and every file and folder beside it, a nested skill's folder
included. The catalog extension and a CoCo Desktop profile's stage tree both publish it under
`skills/<name>/` with every path unchanged, so a relative path that resolves in the repository
resolves where the agent reads the skill. Only a Markdown link is held to resolve: a path
mentioned in prose or a code block is an instruction or an example, which the agent resolves by
reading the folder, and which may name a file the skill tells it to create.

These checks run for every skill `sst validate` reads, whichever channel ships it or none; a
bundle reads them only to decide whether it builds.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterable
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.skill.model import SKILL_FILE, Skill, SkillFile
from snowflake_semantic_tools.domain.parse.skill_references import PathReference, scan_references
from snowflake_semantic_tools.domain.validate.shared import Emitter

# Soft budget: SKILL.md is read on every orchestration turn.
SKILL_MD_BUDGET_BYTES = 25 * 1024
# The forms a Markdown renderer follows; a path in any other form is prose.
_LINK_FORMS = frozenset(("link", "image", "definition"))
# The top-level folders of a local machine's file system, by platform; a path below one, a
# home-directory path, or a drive path names a file the agent's sandbox does not hold. A path
# below any other root, such as a URL route `/api/v2/...` a script calls, is not reported.
_ROOTS = "Users|home|opt|etc|tmp|var|usr|private|root|mnt|Volumes|srv"
_ABSOLUTE = re.compile(rf"(?<![\w./:-])((?:/(?:{_ROOTS})/|~/|[A-Za-z]:\\)[^\s`'\")\]]*)")
_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
# A credential key, alone or as one word of a longer name such as AWS_SECRET_ACCESS_KEY, set to
# a literal of six or more characters. A quoted value is a literal; an unquoted one only when it
# ends the statement, since in code an unquoted value is a name or a call. A value that starts
# `$`, `{` or `<` is a variable, a template or a placeholder, not a credential.
_KEY_WORD = r"(?:password|passwd|secret|api[_-]?key|private[_-]?key|access[_-]?token|auth[_-]?token)"
_SECRET = re.compile(
    rf"(?i)(?<![A-Za-z0-9])(?:[A-Za-z0-9]+_)*{_KEY_WORD}(?:_[A-Za-z0-9]+)*\s*[:=]\s*"
    r"(?:['\"](?![$<{])[^'\"\s]{6,}|(?![$<{'\"])[^\s'\"()\[\].,;]{6,}(?=[\s;]|$))"
)


@dataclass(frozen=True, slots=True)
class _Tree:
    """A skill's authored paths, indexed for resolving the paths its Markdown names."""

    paths: frozenset[str]
    by_basename: dict[str, tuple[str, ...]]

    @classmethod
    def of(cls, skill: Skill) -> _Tree:
        paths = frozenset(item.path for item in skill.files)
        by_basename: dict[str, list[str]] = {}
        for path in sorted(paths):
            by_basename.setdefault(posixpath.basename(path), []).append(path)
        return cls(paths, {name: tuple(found) for name, found in by_basename.items()})

    def below(self, folder: str) -> tuple[str, ...]:
        """List the files under `folder`, a path relative to the skill folder."""
        prefix = folder.rstrip("/") + "/"
        return tuple(path for path in sorted(self.paths) if path.startswith(prefix))


def check_skill_files(skill: Skill) -> DiagnosticBag:
    """Check one skill's files as published: the links its Markdown makes, and what its files hold.

    Diagnostics:
        SST-VAL808: a Markdown link, image, or reference definition names no file or folder of the skill.
        SST-VAL810: a path names a file of the skill by its path from the repository root (warning).
        SST-VAL815: a script holds an absolute path, or any text file a credential literal.
        SST-VAL813: a file of the skill, outside any nested skill's folder, that nothing names.
        SST-VAL812: SKILL.md is over its size budget.
    """
    tree = _Tree.of(skill)
    emit = Emitter(subject=skill.key, artifact=skill.name)
    referenced: set[str] = set()
    for item in skill.files:
        if item.is_markdown and item.text is not None:
            referenced.update(_check_references(skill, item, tree, emit))
    _check_hazards(skill, emit)
    nested = tuple(folder.rstrip("/") + "/" for folder in skill.nested)
    for item in skill.files:
        if item.path == SKILL_FILE or item.path in referenced or item.path.startswith(nested):
            continue
        emit("SST-VAL813", origin=skill.origin_of(item.path), path=item.path)
    skill_md = next((item for item in skill.files if item.path == SKILL_FILE), None)
    if skill_md is not None and skill_md.size > SKILL_MD_BUDGET_BYTES:
        emit("SST-VAL812", origin=skill.origin, size=skill_md.size, expected=SKILL_MD_BUDGET_BYTES)
    return DiagnosticBag(emit.diagnostics)


def _check_references(skill: Skill, item: SkillFile, tree: _Tree, emit: Emitter) -> set[str]:
    """Report the links in one Markdown file that resolve to nothing; return every file it names.

    A path resolves against the folder of the file that names it, then against the skill
    folder, to a file or to every file below a folder. A path that walks down a `skills/` tree
    to one of the skill's own files names that file, from the repository root. A path in prose
    or a code block also names a file by its file name, when exactly one file of the skill has
    that name, since the agent finds it by reading the folder.
    """
    referenced: set[str] = set()
    for reference in scan_references(item.text or "", item.path):
        if _skip(reference):
            continue
        named = _resolve(reference, tree)
        if named:
            referenced.update(named)
            continue
        anchored = _anchored_target(reference, skill.name, tree.paths)
        if anchored is not None:
            referenced.add(anchored)
            origin = skill.origin_of(item.path, reference.line, reference.col)
            emit("SST-VAL810", origin=origin, path=reference.text, target=anchored)
            continue
        linked = reference.form in _LINK_FORMS
        by_name = () if linked else tree.by_basename.get(posixpath.basename(reference.text), ())
        if len(by_name) == 1:
            referenced.update(by_name)
        elif linked and not reference.suppressed:
            emit("SST-VAL808", origin=skill.origin_of(item.path, reference.line, reference.col), path=reference.text)
    return referenced


def _resolve(reference: PathReference, tree: _Tree) -> tuple[str, ...]:
    """Find the files a path names, from its file's folder or the skill folder; none when it names none."""
    base = posixpath.dirname(reference.file)
    for candidate in dict.fromkeys((posixpath.join(base, reference.text), reference.text)):
        normalized = posixpath.normpath(candidate)
        if normalized == ".." or normalized.startswith("../"):
            continue
        if normalized in tree.paths:
            return (normalized,)
        below = tree.below(normalized)
        if below:
            return below
    return ()


def _anchored_target(reference: PathReference, skill_name: str, paths: frozenset[str]) -> str | None:
    """Find the file a path names from the repository root, down a `skills/` tree to this skill."""
    if reference.text.startswith(("./", "../")):
        return None
    parts = posixpath.normpath(reference.text).split("/")
    if "skills" not in parts:
        return None
    below = parts[parts.index("skills") + 1 :]
    if skill_name not in below[:-1]:
        return None
    remainder = "/".join(below[below.index(skill_name) + 1 :])
    return remainder if remainder in paths else None


def _skip(reference: PathReference) -> bool:
    return not reference.text or bool(_URL.match(reference.text)) or reference.text.startswith(("/", "~"))


def _check_hazards(skill: Skill, emit: Emitter) -> None:
    """SST-VAL815, for an absolute path in a script and a credential literal in any text file.

    An absolute or home-directory path is reported only in a script: in Markdown it is an
    instruction to the agent ("save the export to ~/Downloads"), which is how a skill is meant
    to be written.
    """
    for item in skill.files:
        text = item.text
        if text is None:
            continue
        for start, detail in _hazards(text, script=not item.is_markdown):
            line = text.count("\n", 0, start) + 1
            emit("SST-VAL815", origin=skill.origin_of(item.path, line), path=item.path, detail=detail)


def _hazards(text: str, *, script: bool) -> list[tuple[int, str]]:
    """Locate absolute paths, in a script only, then credential literals, each with how it reads."""
    absolute: Iterable[re.Match[str]] = _ABSOLUTE.finditer(text) if script else ()
    found = [(match.start(), f"the absolute path '{match.group(1)}'") for match in absolute]
    found.extend((match.start(), "a credential literal") for match in _SECRET.finditer(text))
    return found

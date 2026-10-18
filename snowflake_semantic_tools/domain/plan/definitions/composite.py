"""The definitions of an artifact made of files and rows: a skill, plugin, profile or eval.

A file is one member, named by its path, whose content compares as text, line by line; a file
that is not UTF-8 text compares by its size and digest. A profile's registry row is one member per
column Desktop reads, a pointer's content-addressed digest left out, so a changed tree shows as the
files it changes rather than as a moved pointer. An eval is its config YAML and one member per
question, named by the question, holding its ground truth as JSON with sorted keys.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping

from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.plan.member_diff import Definition, Member, unique

# A tree's content-addressed folder in a stage pointer: twelve hex digits between slashes.
_DIGEST = re.compile(r"/[0-9a-f]{12}/")
# Registry columns that change with every publish whatever changed: the row's version digest.
_UNCOMPARED_COLUMNS = frozenset(("VERSION",))


def file_text(content: bytes) -> str:
    """Return a file's content as it compares: its text, or its size and digest when not UTF-8 text."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text is None or "\x00" in text:
        return f"(binary, {len(content)} bytes, sha256 {hashlib.sha256(content).hexdigest()[:12]})"
    return text


def files_members(files: Mapping[str, bytes], *, under: str = "") -> tuple[Member, ...]:
    """Return one `file` member per file, by `under` and its path, in path order."""
    return tuple(
        Member("file", f"{under}{path}", (("content", file_text(content)),)) for path, content in sorted(files.items())
    )


def extension_definition(files: Mapping[str, bytes], comment: str | None) -> Definition:
    """Return a skill's or plugin's definition: its comment, without a marker, and each file it bundles."""
    return Definition((Member("extension", "comment", (("text", unmarked_comment(comment)),)), *files_members(files)))


def profile_definition(columns: Mapping[str, object], trees: Mapping[str, Mapping[str, bytes]]) -> Definition:
    """Return a profile's definition: each registry column but its version, and each file of each tree.

    Args:
        columns: The row's columns as Desktop reads them, each parsed.
        trees: Each tree's files by path, keyed `<kind>/<scope>/`.
    """
    members = [
        Member("column", name.lower(), (("value", _DIGEST.sub("/…/", _json(value))),))
        for name, value in sorted(columns.items())
        if name.upper() not in _UNCOMPARED_COLUMNS
    ]
    for tree, files in sorted(trees.items()):
        members.extend(files_members(files, under=tree))
    return Definition(tuple(members))


def tree_root(pointer: str) -> tuple[str, str] | None:
    """Return the tree a profile's stage pointer points into: its `<kind>/<scope>/`, and its root location.

    A pointer names a tree's root, a folder in it or a file in it; None when it names no tree.

    Example:
        `@DB.S.STAGE/hooks/team/0123456789ab/guard/guard.sh` is in `hooks/team/`, whose root is
        `@DB.S.STAGE/hooks/team/0123456789ab/`.
    """
    match = re.match(r"(@[^/]+/([^/]+)/([^/]+)/[0-9a-f]{12}/)", pointer if pointer.endswith("/") else f"{pointer}/")
    return (f"{match.group(2)}/{match.group(3)}/", match.group(1)) if match else None


def eval_definition(config_yaml: str, questions: Iterable[tuple[str, object]] | None) -> Definition:
    """Return an eval's definition: its config, then each question with its ground truth.

    With `questions` None, the questions were not read, and the definition holds the config alone.
    """
    members = [Member("config", "yaml", (("text", config_yaml),))]
    if questions is None:
        return Definition(tuple(members), frozenset(("config",)))
    members += [Member("question", question, (("ground_truth", _json(truth)),)) for question, truth in questions]
    return Definition(unique(members))


def payload_questions(dataset_payload: str) -> tuple[tuple[str, object], ...]:
    """Return each question and ground truth of a rendered dataset payload, in its order."""
    rows = json.loads(dataset_payload)
    return tuple((str(row["input_query"]), row["ground_truth"]) for row in rows)


def _json(value: object) -> str:
    """A value as it compares: plain text as it is, anything else as JSON with sorted keys.

    Text that holds JSON, as a VARIANT column returns it, is parsed first.
    """
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        return parsed if isinstance(parsed, str) else json.dumps(parsed, sort_keys=True, ensure_ascii=False)
    return json.dumps(value, sort_keys=True, ensure_ascii=False)

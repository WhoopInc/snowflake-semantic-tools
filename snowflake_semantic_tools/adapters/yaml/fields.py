"""Typed readers for the fields of parsed SST YAML, shared by the artifact parsers.

A plain reader (`optional_string`, `optional_int`, `strings`, `mapping`) reads a value of
the wrong type as its empty sentinel without a word. A checked reader (`checked_text`,
`checked_strings`, `checked_list`, `checked_mapping`) does the same and also reports the value
(SST-PRS003) into the caller's diagnostics list, and `report_unknown_keys` reports the keys a
parser does not read (SST-PRS004). No reader raises. `project_relative` is the path a
diagnostic names a file by, `refused` reports a file a folder walk must not read, and
`resolve_file` reads the one file a `{{ file('<path>') }}` call names, for every field that
takes one.
"""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.paths import walk_refusal
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.calls import required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions


def optional_string(value: object) -> str | None:
    """Return `value` stripped when it is a string with text in it, else None."""
    return value.strip() if isinstance(value, str) and value.strip() else None


def optional_int(value: object) -> int | None:
    """Return `value` when it is an integer, else None; a YAML `true` is not the integer 1 here."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def strings(value: object) -> tuple[str, ...]:
    """Return each item of a list as `str(item)`, or `()` when `value` is not a list.

    Items are not checked: a number reads as its digits and a YAML `true` as `'True'`.
    """
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def mapping(value: object) -> dict[str, Any]:
    """Return a copy of a mapping with every key as `str(key)`, or `{}` when `value` is not one."""
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def checked_text(
    value: object,
    sink: list[Diagnostic],
    *,
    field: str,
    artifact: str,
    origin: Origin,
    subject: str | None = None,
) -> str | None:
    """Return a string field stripped, or None when absent or blank; report any other type.

    Diagnostics:
        SST-PRS003: `value` is present but not a string; it reads as None.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        sink.append(_wrong_type(value, "a string", field=field, artifact=artifact, origin=origin, subject=subject))
        return None
    return value.strip() or None


def checked_strings(
    value: object,
    sink: list[Diagnostic],
    *,
    field: str,
    artifact: str,
    origin: Origin,
    subject: str | None = None,
    expected: str = "list of strings",
) -> tuple[str, ...]:
    """Return a list-of-strings field as a tuple, or `()` when absent; report any other value.

    Args:
        expected: How the diagnostic names what the field takes.

    Diagnostics:
        SST-PRS003: `value` is present but not a list of strings; it reads as `()`.
    """
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        sink.append(_wrong_type(value, expected, field=field, artifact=artifact, origin=origin, subject=subject))
        return ()
    return tuple(value)


def member_names(
    value: object,
    reference: str,
    sink: list[Diagnostic],
    *,
    field: str,
    artifact: str,
    origin: Origin,
    subject: str | None = None,
) -> tuple[str, ...]:
    """Read a member list whose every entry is one `{{ <function>('<name>') }}` call, as the names.

    Args:
        reference: The list's entry in `REFERENCE_FIELDS`, which names the function it takes.

    Returns:
        The names, in order and with any repeats; an entry that is not that call is left out.

    Diagnostics:
        SST-PRS003: the value is present but not a list of strings; it reads as `()`.
        Every code `required_call` lists, for an entry that is not one call of the function.
    """
    names: list[str] = []
    functions = reference_functions(reference)
    expected = f"a list of {{{{ {functions[0]}('<name>') }}}} calls"
    for index, entry in enumerate(
        checked_strings(value, sink, field=field, artifact=artifact, origin=origin, subject=subject, expected=expected)
    ):
        found = required_call(entry, functions, origin, field=f"{field}[{index}]", artifact=artifact, subject=subject)
        if isinstance(found, Diagnostic):
            sink.append(found)
        else:
            names.append(found.args[0])
    return tuple(names)


def checked_list(
    value: object,
    sink: list[Diagnostic],
    *,
    field: str,
    artifact: str,
    origin: Origin,
    subject: str | None = None,
) -> list[Any]:
    """Return a list field as written, or `[]` when it is absent or empty; report any other value.

    The items are not checked; each caller reads them as it needs.

    Diagnostics:
        SST-PRS003: `value` is present and not a list; it reads as `[]`.
    """
    if not value:
        return []
    if not isinstance(value, list):
        sink.append(_wrong_type(value, "a list", field=field, artifact=artifact, origin=origin, subject=subject))
        return []
    return value


def checked_mapping(
    value: object,
    sink: list[Diagnostic],
    *,
    field: str,
    artifact: str,
    origin: Origin,
    subject: str | None = None,
) -> dict[Any, Any]:
    """Return a copy of a mapping field, or `{}` when it is absent or empty; report any other value.

    Keys are kept as written, unlike `mapping`, which turns each into text.

    Diagnostics:
        SST-PRS003: `value` is present and not a mapping; it reads as `{}`.
    """
    if not value:
        return {}
    if not isinstance(value, dict):
        sink.append(_wrong_type(value, "a mapping", field=field, artifact=artifact, origin=origin, subject=subject))
        return {}
    return dict(value)


def _wrong_type(
    value: object, expected: str, *, field: str, artifact: str, origin: Origin, subject: str | None
) -> Diagnostic:
    return D(
        "SST-PRS003",
        origin=origin,
        subject=subject,
        artifact=artifact,
        field=field,
        expected=expected,
        found=type(value).__name__,
    )


def unknown_keys(keys: Iterable[Any], allowed: frozenset[str]) -> list[Any]:
    """Return the keys outside `allowed`, sorted by their text so keys of mixed types still sort."""
    return sorted(set(keys) - allowed, key=str)


def report_unknown_keys(
    keys: Iterable[Any],
    allowed: frozenset[str],
    sink: list[Diagnostic],
    *,
    artifact: str,
    origin: Origin | Callable[[Any], Origin],
    subject: str | None = None,
) -> None:
    """Report each key outside `allowed` as a field SST does not read, in `unknown_keys` order.

    Args:
        origin: Where every report points, or a function giving each key its own position.

    Diagnostics:
        SST-PRS004: a key outside `allowed`.
    """
    for key in unknown_keys(keys, allowed):
        sink.append(
            D(
                "SST-PRS004",
                origin=origin(key) if callable(origin) else origin,
                subject=subject,
                artifact=artifact,
                field=key,
            )
        )


def project_relative(project_dir: Path, path: Path) -> str:
    """Return `path` relative to `project_dir`, in POSIX form, as diagnostics name a file.

    Raises:
        ValueError: `path` is not under `project_dir`.
    """
    return path.relative_to(project_dir).as_posix()


def refused(project_dir: Path, path: Path, diagnostics: list[Diagnostic], *, subject: str | None = None) -> bool:
    """Report a file or folder a project walk must not read, and return True; False when it may be read.

    Diagnostics:
        SST-PRT009: when `path`, or a folder between the project and it, is a symbolic link, or
            it resolves outside the project.
    """
    reason = walk_refusal(project_dir, path)
    if reason is None:
        return False
    file = project_relative(project_dir, path)
    diagnostics.append(D("SST-PRT009", origin=Origin(file), subject=subject, path=file, detail=reason))
    return True


@dataclass(frozen=True, slots=True)
class Sidecar:
    """The one file a `{{ file('<path>') }}` call names, read once.

    Attributes:
        path: The file, relative to the project root, with its `..` taken away.
        content: Its bytes, never only whitespace; valid UTF-8 when it was read as text.
    """

    path: str
    content: bytes

    @property
    def text(self) -> str:
        """Return the content as text; only for a sidecar `resolve_file` read as text."""
        return self.content.decode("utf-8")


def file_call(
    value: str, origin: Origin, *, field: str, artifact: str, subject: str | None = None
) -> TemplateCall | Diagnostic:
    """Read a field that names a sidecar: the one `{{ file('<path>') }}` call it must be.

    Diagnostics:
        Every code `required_call` lists, for the `file()` function.
    """
    return required_call(value, ("file",), origin, field=field, artifact=artifact, subject=subject)


def resolve_file(
    project_dir: Path,
    declaring_file: str,
    call: TemplateCall,
    *,
    origin: Origin,
    subject: str | None = None,
    text: bool = True,
) -> Sidecar | Diagnostic:
    """Read the file one `file()` call names, by the rules every sidecar follows, exactly once.

    The path is relative to the folder of the file that declares the call, and its `..` are
    taken away as written. It must stay inside the project without passing through a link: a
    link is refused wherever it is, even one that points inside the project.

    Args:
        declaring_file: The file holding the call, relative to the project root.
        call: A `file()` call with one argument, as `file_call` returns it.
        text: Whether the file must be UTF-8 text; a hook script is read as bytes.

    Returns:
        The file read; or the one diagnostic that refuses it.

    Diagnostics:
        SST-REF027: the path is absolute, or climbs out of the project.
        SST-PRT009: the file, or a folder between the project and it, is a symbolic link.
        SST-REF014: the path names no file.
        SST-LOD018: the file is there and cannot be read.
        SST-LOD006: a text sidecar is not UTF-8.
        SST-LOD019: the file holds only whitespace.
    """
    requested = call.args[0]
    relative = posixpath.normpath(posixpath.join(posixpath.dirname(declaring_file), requested))
    if posixpath.isabs(requested) or relative == ".." or relative.startswith("../"):
        return D("SST-REF027", path=requested, origin=origin, subject=subject)
    path = project_dir / relative
    refusal = walk_refusal(project_dir, path)
    if refusal is not None:
        return D("SST-PRT009", path=relative, detail=refusal, origin=origin, subject=subject)
    if not path.is_file():
        return D("SST-REF014", path=requested, origin=origin, subject=subject)
    try:
        content = path.read_bytes()
    except OSError:
        return D("SST-LOD018", file=declaring_file, path=requested, origin=origin, subject=subject)
    if text:
        try:
            content.decode("utf-8")
        except UnicodeDecodeError as exc:
            return D("SST-LOD006", file=relative, offset=exc.start, origin=Origin(relative), subject=subject)
    if not content.strip():
        return D("SST-LOD019", path=requested, file=declaring_file, origin=origin, subject=subject)
    return Sidecar(relative, content)


def read_sidecar(
    project_dir: Path,
    declaring_file: str,
    value: str,
    *,
    origin: Origin,
    field: str,
    artifact: str,
    subject: str | None = None,
    text: bool = True,
) -> Sidecar | Diagnostic:
    """Read the sidecar a field names: `file_call` on its value, then `resolve_file` on the call.

    Diagnostics:
        Every code `file_call` and `resolve_file` list.
    """
    call = file_call(value, origin, field=field, artifact=artifact, subject=subject)
    if isinstance(call, Diagnostic):
        return call
    return resolve_file(project_dir, declaring_file, call, origin=origin, subject=subject, text=text)

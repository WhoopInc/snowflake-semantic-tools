"""Typed readers for the fields of parsed SST YAML, shared by the artifact parsers.

A plain reader (`optional_string`, `optional_int`, `strings`, `mapping`) reads a value of
the wrong type as its empty sentinel without a word. A checked reader (`checked_text`,
`checked_strings`, `checked_list`, `checked_mapping`) does the same and also reports the value
(SST-PRS003) into the caller's diagnostics list, and `report_unknown_keys` reports the keys a
parser does not read (SST-PRS004). No reader raises. `project_relative` is the path a
diagnostic names a file by.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, Origin


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

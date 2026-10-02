"""Field readers shared by the eval dataset, config, and custom metric parsers.

Each reader reads one field of a parsed eval file. A value of the wrong type is reported into
the caller's diagnostics list and read as the field's empty sentinel (None or `()`); no reader
raises. `origin_at` finds where a node was written, falling back to the file's first line.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.fields import checked_strings, optional_int, optional_string
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.eval import ThresholdRange


def parse_threshold(
    source_file: str,
    field: str,
    value: object,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> ThresholdRange | None:
    """Read a `{min, max}` threshold range whose bounds are each optional.

    Returns:
        The range, a bound None when absent or not a number; None when the field is absent
        or not a mapping.

    Diagnostics:
        SST-PRS003: the field is not a mapping, or a bound is not a number.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return None
    minimum = _number(value.get("min"))
    maximum = _number(value.get("max"))
    for key, parsed in (("min", minimum), ("max", maximum)):
        if key in value and parsed is None:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"{field}.{key}",
                    expected="number",
                    found=type(value[key]).__name__,
                    origin=origin,
                )
            )
    return ThresholdRange(minimum, maximum)


def required_string(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
) -> str | None:
    """Read a field that must be a non-empty string; an absent one is reported at the root.

    Diagnostics:
        SST-PRS003: the field is present but not a non-empty string.
        SST-PRS002: the field is absent.
    """
    result = optional_string_field(value, field, source_file, parsed, diagnostics)
    if field not in value:
        diagnostics.append(
            D("SST-PRS002", artifact=source_file, field=field, origin=origin_at(parsed, (), source_file))
        )
    return result


def optional_string_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    parsed: ParsedYaml,
    diagnostics: list[Diagnostic],
    path: tuple[str | int, ...] = (),
) -> str | None:
    """Read an optional field that must be a non-empty string, stripped.

    `path` is the node path of the mapping that holds the field, so a report points at the
    field itself.

    Diagnostics:
        SST-PRS003: the field is present but not a non-empty string; it reads as None.
    """
    if field not in value:
        return None
    result = optional_string(value[field])
    if result is None:
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="non-empty string",
                found=type(value[field]).__name__,
                origin=origin_at(parsed, (*path, field), source_file),
            )
        )
    return result


def optional_bool_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> bool | None:
    """Read an optional boolean field; any other value is reported and reads as None.

    Diagnostics:
        SST-PRS019: the field is present but not a boolean.
    """
    if field not in value:
        return None
    raw = value[field]
    if isinstance(raw, bool):
        return raw
    diagnostics.append(D("SST-PRS019", artifact=source_file, field=field, found=raw, origin=origin))
    return None


def optional_int_field(
    value: Mapping[str, object],
    field: str,
    source_file: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> int | None:
    """Read an optional integer field; any other value, a YAML `true` included, reads as None.

    Diagnostics:
        SST-PRS003: the field is present but not an integer.
    """
    if field not in value:
        return None
    result = optional_int(value[field])
    if result is None:
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field=field,
                expected="integer",
                found=type(value[field]).__name__,
                origin=origin,
            )
        )
    return result


def string_tuple(
    value: object,
    field: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> tuple[str, ...]:
    """Read a list-of-strings field, naming in any report the file `origin` points into.

    Diagnostics:
        SST-PRS003: the value is present but not a list of strings; it reads as `()`.
    """
    return checked_strings(value, diagnostics, field=field, artifact=origin.file, origin=origin)


def origin_at(parsed: ParsedYaml, path: tuple[str | int, ...], source_file: str) -> Origin:
    """Return where the node at `path` was written, or the file's first line when not recorded."""
    position = parsed.line_index.get(path)
    return Origin(source_file, position.line if position else 1, position.col if position else 1)


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None

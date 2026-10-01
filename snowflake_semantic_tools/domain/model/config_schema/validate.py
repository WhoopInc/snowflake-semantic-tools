"""Check a parsed `sst_config.yml` tree against the key table in `keys.py`.

The walk visits each block once, in document order. Every entry resolves to its
declared key, or to the `<name>` or `<route>` slot its block's child policy allows;
anything else is reported, never ignored.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.model.config_schema.keys import (
    CHILDREN,
    CONFIG_FILE,
    CONFIG_KEYS,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    ConfigKey,
    KeyKind,
    KeyStatus,
)
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin

_TYPES: Mapping[KeyKind, tuple[type, ...]] = MappingProxyType(
    {
        KeyKind.BLOCK: (dict,),
        KeyKind.MAP: (dict,),
        KeyKind.STRING: (str,),
        KeyKind.BOOLEAN: (bool,),
        KeyKind.INTEGER: (int,),
        KeyKind.LIST: (list,),
        KeyKind.ENUM: (str,),
    }
)

Positions = Mapping[tuple[str | int, ...], tuple[int, int]]


def validate_config(tree: Mapping[Any, object], *, positions: Positions | None = None) -> DiagnosticBag:
    """Check every key of a parsed `sst_config.yml` against the declared schema.

    Args:
        positions: The (line, column) of each key path; a path without one is reported
            against the file alone.

    Returns:
        One diagnostic per problem, in document order; what a block leaves out is
        reported after the block's own entries.

    Diagnostics:
        SST-CFG003: a key is not declared where it is written.
        SST-CFG004: a value has the wrong type.
        SST-CFG006: a required key is absent or empty.
        SST-CFG007: an unknown top-level key is within two edits of a declared one.
        SST-CFG008: a value is outside its choices or bounds, or is not its fixed value; or
            `enrichment.sample_values_display_limit` exceeds `enrichment.distinct_limit`.
        SST-CFG015: `evals` sets `+database` or `+schema`.
        SST-CFG040: `vars` declares `sha_version`, which SST supplies.
        SST-CFG042: `evals` or `skills` declares a folder route.
        SST-CFG043: a removed key is set; the message says what replaced it.
        SST-CFG044: a key reserved for a later release is set.
        SST-VAL817: `skills` configures neither the catalog nor the stage channel.
        SST-VAL818: a channel's `+flatten` is not the value that channel requires.
        SST-VAL819: `skills.stage.+auto_compress` is true.
        SST-VAL821: `skills.stage.+layout` is not `by_type`.
    """
    diagnostics: list[Diagnostic] = []
    _walk("", ChildPolicy.DECLARED, tree, (), positions or {}, diagnostics)
    return DiagnosticBag(diagnostics)


def _origin(path: tuple[str, ...], positions: Positions) -> Origin:
    position = positions.get(path)
    return Origin(CONFIG_FILE, position[0], position[1]) if position is not None else Origin(CONFIG_FILE)


def _diagnostic(code: str, path: tuple[str, ...], positions: Positions, **context: Any) -> Diagnostic:
    return D(code, origin=_origin(path, positions), subject=f"config:{'.'.join(path)}", **context)


def _walk(
    entry_path: str,
    policy: ChildPolicy,
    value: Mapping[Any, object],
    actual: tuple[str, ...],
    positions: Positions,
    diagnostics: list[Diagnostic],
) -> None:
    """Check each entry of one block, then what the block as a whole leaves out.

    `entry_path` is the declared path whose children apply, which for a folder route
    is the routed block's own path; `actual` is the path as written in the file.
    """
    declared = CHILDREN.get(entry_path, {})
    for raw_key, child in value.items():
        key = str(raw_key)
        path = (*actual, key)
        spec, route = _child_spec(declared, policy, key)
        if spec is None:
            diagnostics.append(_unknown(path, positions))
            continue
        _check(spec, child, path, positions, diagnostics, route_of=entry_path if route else None)
    diagnostics.extend(_block_problems(entry_path, declared, value, actual, positions))


def _child_spec(declared: Mapping[str, ConfigKey], policy: ChildPolicy, key: str) -> tuple[ConfigKey | None, bool]:
    """Return the declared key an entry matches, and whether it matched as a folder route."""
    spec = declared.get(key)
    if spec is None and policy is ChildPolicy.NAMES:
        spec = declared.get("<name>")
    if spec is None and policy is ChildPolicy.ROUTES and not key.startswith("+"):
        spec = declared.get("<route>")
        return spec, spec is not None
    return spec, False


def _block_problems(
    entry_path: str,
    declared: Mapping[str, ConfigKey],
    value: Mapping[Any, object],
    actual: tuple[str, ...],
    positions: Positions,
) -> list[Diagnostic]:
    """Diagnose the required children a block leaves out, then a missing `one_of` alternative."""
    present = {str(key) for key in value}
    problems = [
        _diagnostic("SST-CFG006", (*actual, spec.name), positions, key=spec.path)
        for spec in declared.values()
        if spec.required and spec.name not in present
    ]
    block = CONFIG_KEYS.get(entry_path)
    if block is not None and block.one_of and not any(name in present for name in block.one_of):
        problems.append(_diagnostic("SST-VAL817", actual, positions, key=entry_path))
    if entry_path == "enrichment":
        problems.extend(_enrichment_limit_problems(value, actual, positions))
    return problems


def _enrichment_limit_problems(
    value: Mapping[Any, object], actual: tuple[str, ...], positions: Positions
) -> list[Diagnostic]:
    """Diagnose a display limit above the distinct limit: no more values than that are sampled.

    An absent limit takes its default. A limit that is not an integer, or is outside its own
    bounds, is reported on its own and not compared.
    """
    limits = []
    for name in ("distinct_limit", "sample_values_display_limit"):
        spec = CHILDREN["enrichment"][name]
        written = value.get(name, int(spec.default or 0))
        if not isinstance(written, int) or isinstance(written, bool) or _out_of_bounds(spec, written):
            return []
        limits.append(written)
    distinct, display = limits
    if display <= distinct:
        return []
    path = (*actual, "sample_values_display_limit")
    return [
        _diagnostic(
            "SST-CFG008",
            path,
            positions,
            key=".".join(path),
            found=str(display),
            expected=f"1..{distinct} (enrichment.distinct_limit)",
        )
    ]


def _unknown(path: tuple[str, ...], positions: Positions) -> Diagnostic:
    key = ".".join(path)
    if len(path) == 1:
        suggestion = _near_miss(path[0])
        if suggestion is not None:
            return _diagnostic("SST-CFG007", path, positions, key=key, suggestion=suggestion)
    return _diagnostic("SST-CFG003", path, positions, key=key)


def _near_miss(key: str) -> str | None:
    candidates = sorted((distance, known) for known in TOP_LEVEL_KEYS if (distance := _edit_distance(key, known)) <= 2)
    return candidates[0][1] if candidates else None


def _edit_distance(left: str, right: str) -> int:
    previous = list(range(len(right) + 1))
    for row, left_character in enumerate(left, start=1):
        current = [row]
        for column, right_character in enumerate(right, start=1):
            current.append(
                min(
                    previous[column] + 1,
                    current[column - 1] + 1,
                    previous[column - 1] + (left_character != right_character),
                )
            )
        previous = current
    return previous[-1]


def _check(
    spec: ConfigKey,
    value: object,
    path: tuple[str, ...],
    positions: Positions,
    diagnostics: list[Diagnostic],
    *,
    route_of: str | None,
) -> None:
    """Check one entry against its declared key, then walk into it when it is a block.

    A removed or unsupported key is reported without looking at its value, and a value
    that fails a check is not descended into. `route_of` names the routed block when the
    entry matched a folder route: the route's children are that block's own keys.
    """
    if spec.status is not KeyStatus.CURRENT:
        diagnostics.append(_status_problem(spec, path, positions))
        return
    if value is None:
        # An empty YAML value means unset: a scalar keeps its default, and a block
        # is checked as an empty block so its required children are still named.
        if spec.kind not in (KeyKind.BLOCK, KeyKind.MAP):
            if spec.required:
                diagnostics.append(_diagnostic("SST-CFG006", path, positions, key=spec.path))
            return
        value = {}
    problem = _value_problem(spec, value, path, positions)
    if problem is not None:
        diagnostics.append(problem)
        return
    if isinstance(value, dict) and spec.kind in (KeyKind.BLOCK, KeyKind.MAP):
        if route_of is not None:
            _walk(route_of, ChildPolicy.ROUTES, value, path, positions, diagnostics)
        else:
            _walk(spec.path, spec.children, value, path, positions, diagnostics)


def _status_problem(spec: ConfigKey, path: tuple[str, ...], positions: Positions) -> Diagnostic:
    """Diagnose a key that is set although SST no longer reads it or does not read it yet."""
    if spec.status is KeyStatus.UNSUPPORTED:
        return _diagnostic("SST-CFG044", path, positions, key=".".join(path))
    if spec.code == "SST-CFG042":
        return _diagnostic(spec.code, path, positions, block=path[0], key=path[-1])
    if spec.code == "SST-CFG015":
        return _diagnostic(spec.code, path, positions, key=path[-1])
    if spec.code == "SST-CFG040":
        return _diagnostic(spec.code, path, positions)
    return _diagnostic("SST-CFG043", path, positions, key=".".join(path), reason=spec.replacement)


def _value_problem(spec: ConfigKey, value: object, path: tuple[str, ...], positions: Positions) -> Diagnostic | None:
    """Return the first rule a set value breaks: its type, choices, fixed value, then bounds."""
    key = ".".join(path)
    expected = _TYPES.get(spec.kind)
    mistyped = expected is not None and not isinstance(value, expected)
    if mistyped or spec.kind is KeyKind.INTEGER and isinstance(value, bool):
        return _diagnostic("SST-CFG004", path, positions, key=key, expected=spec.kind.value, found=type(value).__name__)
    if spec.choices and value not in spec.choices:
        code = spec.code or "SST-CFG008"
        return _diagnostic(code, path, positions, key=key, found=repr(value), expected=", ".join(spec.choices))
    if spec.fixed is not None and value is not spec.fixed:
        return _diagnostic(
            spec.code or "SST-CFG008",
            path,
            positions,
            key=key,
            found=str(value).lower(),
            expected=str(spec.fixed).lower(),
        )
    if isinstance(value, int) and not isinstance(value, bool) and _out_of_bounds(spec, value):
        return _diagnostic(
            "SST-CFG008",
            path,
            positions,
            key=key,
            found=str(value),
            expected=f"{spec.minimum}..{spec.maximum}",
        )
    return None


def _out_of_bounds(spec: ConfigKey, value: int) -> bool:
    return (spec.minimum is not None and value < spec.minimum) or (spec.maximum is not None and value > spec.maximum)

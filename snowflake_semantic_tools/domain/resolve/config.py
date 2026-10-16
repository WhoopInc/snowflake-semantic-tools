"""Resolve the two template forms the configuration file evaluates: a target conditional, and `var()`.

A value that is wholly `{{ '<a>' if target.name == '<t>' else '<b>' }}` -- `!=` too, chained
through further `else '<x>' if target.name == '<u>'` arms, the final `else` optional -- becomes the
literal its first true arm names. `{{ var('<name>') }}` anywhere in a value becomes the variable
`vars:` declares. Every other template, `{{ target.database }}` among them, is left as written for
the reader of that key. Nothing else is evaluated, so the grammar stays enumerable. A value whose
template does not resolve is removed: its reader sees the key unset, and the diagnostic blocks.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.config_schema.keys import CONFIG_KEYS, KeyKind
from snowflake_semantic_tools.domain.resolve.calls import variable_problem

_LITERAL = r"'([^']*)'"
_ARM = re.compile(rf"\s*{_LITERAL}\s+if\s+target\.name\s*(==|!=)\s*{_LITERAL}\s*(?:else\b|$)")
_FALLBACK = re.compile(rf"\s*{_LITERAL}\s*$")
_CONDITIONAL = re.compile(r"^\s*\{\{(?P<body>.*\bif\s+target\.name\b.*)\}\}\s*$", re.DOTALL)
_VAR = re.compile(r"\{\{\s*var\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
# What `render` returns for a value whose template failed, so its key or list item is dropped.
_UNRESOLVED = object()


def has_target_conditional(tree: Mapping[str, Any]) -> bool:
    """Report whether any value outside `vars:` is a target conditional, which needs the target's name."""
    return any(_CONDITIONAL.match(value) for _, value in _strings(tree))


def render_config(
    tree: Mapping[str, Any], *, target_name: str | None, file: str
) -> tuple[dict[str, Any], DiagnosticBag]:
    """Return the tree with every target conditional and `var()` resolved, and what did not resolve.

    `vars:` itself is left as written. A value that does not resolve is removed, key or list
    item, so its reader sees it unset and reports nothing further about it -- neither a
    mistyped value nor a name that does not parse -- while the diagnostic still blocks.

    Args:
        target_name: The target the run resolves against; None when there is no conditional.
        file: How each diagnostic's origin names the configuration file.

    Diagnostics:
        SST-CFG016: a target conditional has no arm for the current target and no final else.
        SST-CFG029: `var()` names a variable `vars:` does not declare.
        SST-REF009: `var()` names a variable whose value is empty or null.
    """
    variables = tree.get("vars")
    declared = variables if isinstance(variables, Mapping) else {}
    diagnostics: list[Diagnostic] = []

    def render(path: tuple[str, ...], value: Any) -> Any:
        """Render one value; `_UNRESOLVED` when its template reported, so its key or item is dropped."""
        if isinstance(value, Mapping):
            entries = ((key, render((*path, str(key)), item)) for key, item in value.items())
            return {key: item for key, item in entries if item is not _UNRESOLVED}
        if isinstance(value, list):
            items = (render((*path, str(index)), item) for index, item in enumerate(value))
            return [item for item in items if item is not _UNRESOLVED]
        if not isinstance(value, str):
            return value
        reported = len(diagnostics)
        rendered = _render_vars(value, declared, path, file, diagnostics)
        chosen = _render_conditional(rendered, target_name, path, file, diagnostics)
        if len(diagnostics) > reported:
            return _UNRESOLVED
        return _typed(path, chosen) if chosen is not rendered else chosen

    entries = ((key, item if key == "vars" else render((key,), item)) for key, item in tree.items())
    return {key: item for key, item in entries if item is not _UNRESOLVED}, DiagnosticBag(diagnostics)


def _strings(tree: Mapping[str, Any], path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    """Return every string value outside `vars:`, with its key path."""
    found: list[tuple[tuple[str, ...], str]] = []
    for key, value in tree.items():
        here = (*path, str(key))
        if here == ("vars",):
            continue
        if isinstance(value, Mapping):
            found.extend(_strings(value, here))
        elif isinstance(value, str):
            found.append((here, value))
    return found


def _render_vars(
    value: str, declared: Mapping[str, Any], path: tuple[str, ...], file: str, diagnostics: list[Diagnostic]
) -> str:
    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        # The one rule project YAML's `var()` follows too: undeclared is CFG029, empty REF009.
        problem = variable_problem(name, declared, Origin(file), subject=f"config:{'.'.join(path)}")
        if problem is None:
            return str(declared[name])
        diagnostics.append(problem)
        return match.group(0)

    return _VAR.sub(substitute, value)


def _typed(path: tuple[str, ...], literal: str) -> Any:
    """Read the literal a conditional chose as its key's type, so a boolean or integer key can take one.

    `true` or `false` is a boolean for a boolean key, and decimal digits an integer for an integer
    key; anything else stays text, which validation checks.
    """
    kind = _declared_kind(path)
    if kind is KeyKind.BOOLEAN and literal.casefold() in ("true", "false"):
        return literal.casefold() == "true"
    if kind is KeyKind.INTEGER and literal.isascii() and literal.isdigit():
        return int(literal)
    return literal


def _declared_kind(path: tuple[str, ...]) -> KeyKind | None:
    """Return the declared kind of the key at `path`, reading a folder route as the block it routes."""
    exact = CONFIG_KEYS.get(".".join(path))
    if exact is not None:
        return exact.kind
    for index in range(1, len(path)):
        if f"{'.'.join(path[:index])}.<route>" in CONFIG_KEYS:
            return _declared_kind((*path[:index], *path[index + 1 :]))
        named = CONFIG_KEYS.get(".".join((*path[:index], "<name>", *path[index + 1 :])))
        if named is not None:
            return named.kind
    return None


def _render_conditional(
    value: str, target_name: str | None, path: tuple[str, ...], file: str, diagnostics: list[Diagnostic]
) -> str:
    """Resolve a whole-value target conditional to its first true arm; anything else is returned unchanged."""
    match = _CONDITIONAL.match(value)
    if match is None or target_name is None:
        return value
    body = match.group("body")
    position = 0
    while (arm := _ARM.match(body, position)) is not None:
        literal, operator, name = arm.groups()
        if (target_name == name) is (operator == "=="):
            return literal
        position = arm.end()
    fallback = _FALLBACK.match(body, position)
    if fallback is not None and position:
        return fallback.group(1)
    key = path[-1].removeprefix("+")
    diagnostics.append(
        D("SST-CFG016", origin=Origin(file), subject=f"config:{'.'.join(path)}", key=key, target=target_name)
    )
    return value

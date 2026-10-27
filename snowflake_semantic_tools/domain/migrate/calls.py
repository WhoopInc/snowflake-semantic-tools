"""Template-call text the migrations write, and the one rewrite that applies in every file.

`call` and `quoted_call` spell a reference the way the 1.0 readers take it. `one_arg_tool` turns
each two-argument `{{ tool('<group>', '<name>') }}`, from when tools had groups, into
`{{ tool('<name>') }}`; it is a text rewrite, so it applies inside any value, a stage path or a
configuration value included, and leaves every other byte alone.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.migrate.edits import double_quoted
from snowflake_semantic_tools.domain.migrate.refs import MigrationResult, Rewrite
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls

_SPAN = re.compile(r"\{\{.*?\}\}")
# The opening parenthesis and the first argument with its comma, in a two-argument call.
_FIRST_ARG = re.compile(r"(\(\s*)(['\"])[^'\"]*\2\s*,\s*")


def call(function: str, *args: str) -> str:
    """Return `{{ function('a', 'b') }}`."""
    return "{{ " + function + "(" + ", ".join(f"'{arg}'" for arg in args) + ") }}"


def quoted_call(function: str, *args: str) -> str:
    """Return the call as a YAML double-quoted scalar, which a call standing alone must be."""
    return double_quoted(call(function, *args))


def bare(value: str | None) -> str | None:
    """Return a value that names something without a template call, stripped; None otherwise."""
    if value is None or "{{" in value or not value.strip():
        return None
    return value.strip()


def relative_to(path: str, folder: str) -> str:
    """Return the project-relative `path` as seen from the project-relative `folder`.

    Both are normalized POSIX paths inside the project; "" or "." is the project root.

    Example:
        relative_to("tools/sql/a.sql", "tools/sales") == "../sql/a.sql"
    """
    target = [part for part in path.split("/") if part not in ("", ".")]
    base = [part for part in folder.split("/") if part not in ("", ".")]
    common = 0
    while common < min(len(target), len(base)) and target[common] == base[common]:
        common += 1
    return "/".join([".."] * (len(base) - common) + target[common:]) or "."


def one_arg_tool(text: str) -> tuple[str, int]:
    """Drop the group from every two-argument `tool()` call in `text`.

    A span that does not parse, or a call of another function or arity, is left as written.

    Returns:
        The text, and how many calls changed.
    """
    count = 0

    def rewrite(match: re.Match[str]) -> str:
        nonlocal count
        span = match.group(0)
        try:
            calls = scan_template_calls(span)
        except TemplateSyntaxError:
            return span
        if calls[0].function != "tool" or len(calls[0].args) != 2:
            return span
        count += 1
        return _FIRST_ARG.sub(r"\1", span, count=1)

    if "tool" not in text:
        return text, 0
    return _SPAN.sub(rewrite, text), count


def tool_arity(text: str) -> MigrationResult:
    """Drop the group from every two-argument `tool()` call, line by line, reporting each line."""
    lines = text.splitlines(keepends=True)
    rewrites: list[Rewrite] = []
    for index, line in enumerate(lines):
        rewritten, count = one_arg_tool(line)
        if count:
            rewrites.extend(Rewrite(index + 1, 1, line.strip(), rewritten.strip(), "tool") for _ in range(count))
            lines[index] = rewritten
    return MigrationResult("".join(lines), tuple(rewrites)) if rewrites else MigrationResult(text)

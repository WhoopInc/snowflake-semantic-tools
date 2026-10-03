"""Read the strings of semantic-model nodes and the tables they attach to, as YAML wrote them."""

from __future__ import annotations

import re
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, single_template_call
from snowflake_semantic_tools.domain.resolve.calls import syntax_problem


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    """Normalise a YAML scalar-or-list into a tuple of strings.

    Numbers are stringified because `sample_values: [1100, 700]` is authored as
    integers but renders as `'1100', '700'` -- YAML typing is not SQL typing.
    """
    if value is None:
        return ()
    if isinstance(value, bool):
        return ("true" if value else "false",)
    if isinstance(value, (str, int, float)):
        return (str(value),)
    if isinstance(value, list):
        return tuple("true" if v is True else "false" if v is False else str(v) for v in value)
    raise ProjectError(f"expected a scalar or list, found {type(value).__name__}")


def _table_refs(value: object) -> tuple[str, ...]:
    """Read a `tables:` list as the models it names, casefolded and in order.

    Each entry is a `{{ ref('<model>') }}` call, a `{{ source('<source>', '<table>') }}` call,
    named `<source>.<table>`, a legacy `{{ table('<model>') }}` call, or a bare model name; a
    value that is not a list names none. Both callers swallow the error:
    `_safe_table_refs` reads it as no tables, and `_table_refs_poisoned` as poison.

    Raises:
        ProjectError: An entry is a `ref()` without exactly one argument, is neither a call nor
            a model name, or holds a malformed template; only the last carries a diagnostic.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: when an entry's template does not parse, positioned
            within the entry's own text.
    """
    refs: list[str] = []
    for raw in value if isinstance(value, list) else []:
        try:
            call = single_template_call(str(raw), "ref")
        except TemplateSyntaxError as exc:
            diagnostic = syntax_problem(exc, "<tables>")
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
        if call is not None:
            if len(call.args) != 1:
                raise ProjectError(f"table attachment ref() must have one argument, found {raw!r}")
            refs.append(call.args[0].casefold())
            continue
        source = single_template_call(str(raw), "source")
        if source is not None and len(source.args) == 2:
            refs.append(".".join(source.args).casefold())
            continue
        legacy = single_template_call(str(raw), "table")
        if legacy is not None and len(legacy.args) == 1:
            refs.append(legacy.args[0].casefold())
            continue
        literal = str(raw).strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", literal):
            raise ProjectError(f"table attachment must be a model name, found {raw!r}")
        refs.append(literal.casefold())
    return tuple(refs)


def _safe_table_refs(value: object) -> tuple[str, ...]:
    try:
        return _table_refs(value)
    except ProjectError:
        return ()


def _table_refs_poisoned(value: object) -> bool:
    try:
        _table_refs(value)
    except ProjectError:
        return True
    return False

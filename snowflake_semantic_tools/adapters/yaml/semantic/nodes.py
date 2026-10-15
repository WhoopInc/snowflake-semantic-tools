"""Read the strings of semantic-model nodes and the tables they attach to, as YAML wrote them."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.parse.template import (
    TemplateSyntaxError,
    scan_template_calls,
    single_template_call,
)
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
    """Read a view's `tables:` list as the models it names, casefolded and in order.

    Each entry is a `{{ ref('<model>') }}` call, or a `{{ source('<source>', '<table>') }}`
    call, named `<source>.<table>`, as the build reads them; a value that is not a list names
    none. Any other entry, a bare model name or a legacy `{{ table() }}` call among them, names
    no table here, as the build refuses it (SST-REF044), so membership never attaches to a
    table the view cannot build. `_safe_table_refs` reads an error as no tables.

    Raises:
        ProjectError: An entry holds a malformed template.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: when an entry's template does not parse, positioned
            within the entry's own text.
    """
    refs: list[str] = []
    for raw in value if isinstance(value, list) else []:
        try:
            call = single_template_call(str(raw), "ref")
            source = single_template_call(str(raw), "source")
        except TemplateSyntaxError as exc:
            diagnostic = syntax_problem(exc, "<tables>")
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
        if call is not None and len(call.args) == 1:
            refs.append(call.args[0].casefold())
        elif source is not None and len(source.args) == 2:
            refs.append(".".join(source.args).casefold())
    return tuple(refs)


def _safe_table_refs(value: object) -> tuple[str, ...]:
    try:
        return _table_refs(value)
    except ProjectError:
        return ()


def _referenced_tables(*texts: str) -> tuple[tuple[str, ...], bool]:
    """Read the models the `ref()` calls in `texts` name: a member's tables, inferred.

    Returns:
        The models, casefolded, each once in first-seen order; and whether a text holds a
        malformed template, which names none and poisons the member.
    """
    tables: dict[str, None] = {}
    for text in texts:
        try:
            calls = scan_template_calls(text)
        except TemplateSyntaxError:
            return (), True
        tables.update((call.args[0].casefold(), None) for call in calls if call.function == "ref" and call.args)
    return tuple(tables), False

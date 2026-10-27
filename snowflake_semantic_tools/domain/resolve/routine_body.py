"""Expand the reference calls in a procedure's or function's body, and nothing else.

A body is code in another language, so SST runs no template engine over it: `expand_body`
rewrites exactly the `{{ ref() }}`, `{{ source() }}` and `{{ var() }}` calls, refuses any other
call of the dialect, and leaves every other character as written, a `{{` that opens no call of
the dialect included. A call renders as it does in a view's `tables:`, so each target's
routine reads that target's relations, and the relations it names are the routine's inputs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_calls_of
from snowflake_semantic_tools.domain.resolve.calls import (
    KNOWN_FUNCTIONS,
    call_problem,
    syntax_problem,
    variable_problem,
)
from snowflake_semantic_tools.domain.resolve.template import ROUTINE_BODY
from snowflake_semantic_tools.domain.validate.dbt_seam import unreadable_model
from snowflake_semantic_tools.domain.validate.sql import qualified_name_problem


@dataclass(frozen=True, slots=True)
class ExpandedBody:
    """A routine body with its reference calls replaced, and the relations they named.

    Attributes:
        text: The body with each call that resolved replaced; one that did not stays as written.
        relations: Each relation a call named, by the key the manifest records it under: a
            model's name or a source's `<source>.<table>`, casefolded.
    """

    text: str
    relations: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))


def expand_body(
    text: str, file: str, catalog: DbtCatalog, variables: Mapping[str, object], *, subject: str
) -> tuple[ExpandedBody, DiagnosticBag]:
    """Replace each `ref()`, `source()` and `var()` call in a body with its value for the target.

    Calls resolve from the last to the first. A diagnostic leaves its call as written.

    Args:
        file: The body's file, relative to the project root; each diagnostic points into it.
        catalog: The dbt models and sources of the target being compiled, deferral applied.
        variables: The project variables `var()` may name, by exact name.
        subject: The tool member's key, which every diagnostic carries.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: a call does not parse; nothing else is checked.
        SST-REF034, SST-REF035, SST-REF041, SST-REF015: as `call_problem` refuses the call, and
            SST-REF015 for a `ref()` naming a column, which a body has no use for.
        SST-REF001: `ref()` names no dbt model.
        SST-DBT009: `ref()` names a model that produces no relation.
        SST-DBT011: `source()` names no dbt source with a relation.
        SST-REF019: the relation is not a three-part name.
        SST-CFG029, SST-REF009: as `variable_problem` refuses a `var()`.
    """
    try:
        calls = scan_calls_of(text, KNOWN_FUNCTIONS)
    except TemplateSyntaxError as exc:
        return ExpandedBody(text), DiagnosticBag((syntax_problem(exc, file, subject=subject),))
    diagnostics: list[Diagnostic] = []
    relations: dict[str, str] = {}
    rendered = text
    for call in reversed(calls):
        origin = Origin(file, call.line, call.col)
        value = _value(call, origin, catalog, variables, subject)
        if isinstance(value, Diagnostic):
            diagnostics.append(value)
            continue
        if isinstance(value, tuple):
            key, relation = value
            relations[key] = relation
            value = relation
        rendered = rendered[: call.start] + value + rendered[call.end :]
    return ExpandedBody(rendered, MappingProxyType(dict(sorted(relations.items())))), DiagnosticBag(diagnostics)


def _value(
    call: TemplateCall, origin: Origin, catalog: DbtCatalog, variables: Mapping[str, object], subject: str
) -> str | tuple[str, str] | Diagnostic:
    """Return a `var()`'s text, a relation call's key and relation, or why the call does not resolve."""
    problem = call_problem(call, ROUTINE_BODY.allowed, origin, field="body", artifact=subject, subject=subject)
    if problem is None and call.function == "ref" and len(call.args) != 1:
        problem = D(
            "SST-REF015", origin=origin, subject=subject, ref_function="ref", expected="1", found=len(call.args)
        )
    if problem is not None:
        return problem
    if call.function == "var":
        name = call.args[0]
        missing = variable_problem(name, variables, origin, subject=subject)
        return str(variables[name]) if missing is None else missing
    found = _relation(call, catalog, origin, subject)
    if isinstance(found, Diagnostic):
        return found
    unqualified = qualified_name_problem(found[1], artifact=subject, subject=subject, origin=origin)
    if unqualified is not None:
        return D("SST-REF019", origin=origin, subject=subject, value=found[1])
    return found


def _relation(call: TemplateCall, catalog: DbtCatalog, origin: Origin, subject: str) -> tuple[str, str] | Diagnostic:
    """Return the key and relation a `ref()` or `source()` names, written as a view's table writes it."""
    if call.function == "ref":
        model = catalog.model(call.args[0])
        if model is not None:
            return model.name.casefold(), model.relation_name
        unreadable = unreadable_model(catalog, call.args[0], subject=subject)
        if unreadable is not None:
            return replace(unreadable, origin=origin)
        return D("SST-REF001", origin=origin, subject=subject, model=call.args[0])
    pair = f"{call.args[0]}.{call.args[1]}".casefold()
    source = catalog.source(call.args[0], call.args[1])
    if source is None or source.relation_name is None:
        return D("SST-DBT011", origin=origin, subject=subject, value=f"{call.args[0]}.{call.args[1]}")
    return pair, source.relation_name.upper()

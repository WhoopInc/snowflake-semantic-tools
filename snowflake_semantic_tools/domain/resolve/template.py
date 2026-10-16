"""Resolve the template calls in one authored scalar.

`resolve_scalar` replaces each ``{{ function('arg') }}`` call `domain.parse.template` finds
with its value, checked against the field's `RefPolicy` by `calls.call_problem`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import call_problem, malformed_field, syntax_problem
from snowflake_semantic_tools.domain.sql.names import column_ref


@dataclass(frozen=True, slots=True)
class Resolved:
    """A scalar with its template calls replaced by their values.

    Attributes:
        text: The scalar with each call that resolved replaced; one that did not stays as written.
        poisoned: Resolving it reported a diagnostic, so `text` may still hold unresolved calls.
    """

    text: str
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class RefPolicy:
    """Which template calls one kind of field accepts, and how many.

    A `table()` or `column()` call is rejected as legacy whatever `allowed` says.

    Attributes:
        allowed: The template functions the field may call; empty for a field that takes none.
        required: The field must hold at least one call.
        multi: False lets the field hold at most one call.
    """

    allowed: frozenset[str]
    required: bool = False
    multi: bool = True


METRIC_EXPR = RefPolicy(frozenset(("ref", "metric", "var")))
FILTER_EXPR = RefPolicy(frozenset(("ref", "var")))
VQR_SQL = RefPolicy(frozenset(("ref", "metric", "var")))
CUSTOM_INSTRUCTION_ITEM = RefPolicy(frozenset(("custom_instructions",)), required=True, multi=False)
# A custom instruction's prose names the filters, relationships and verified queries it
# talks about by reference, so a renamed member breaks the build instead of the guidance.
INSTRUCTION_TEXT = RefPolicy(frozenset(("filter", "relationship", "verified_query")))
TAG_NAME = RefPolicy(frozenset(("tag",)), required=True, multi=False)
DESCRIPTION = RefPolicy(frozenset())
# A stage path names a stage SST declares by its tool member, `{{ tool('<stage>') }}/<path>`;
# a path with no call is an external stage, kept as written.
STAGE_PATH = RefPolicy(frozenset(("tool",)), multi=False)

# The value of a two-argument `ref()`, given the call and its column: a value or a diagnostic.
ColumnValue = Callable[[TemplateCall, str], "str | Diagnostic"]

# The member-reference functions, by the code a name none declares is reported under.
_MEMBER_CODES: Mapping[str, str] = MappingProxyType(
    {
        "custom_instructions": "SST-REF007",
        "relationship": "SST-REF029",
        "filter": "SST-REF030",
        "verified_query": "SST-REF031",
    }
)


@dataclass(frozen=True, slots=True)
class ResolveContext:
    """What `resolve_scalar` resolves template calls against.

    Attributes:
        catalog: The dbt models `ref()` may name.
        metric_names: The casefolded names `metric()` may name.
        metric_values: By casefolded name, the text a `metric()` call becomes; a declared metric
            missing here becomes its upper-cased name.
        instruction_names: The casefolded names `custom_instructions()` may name.
        variables: The project variables `var()` may name, by exact name; a call becomes `str()`
            of the value.
        tags: By exact name, the object name a tag call becomes.
        relationship_names, filter_names, verified_query_names: The casefolded names
            `relationship()`, `filter()` and `verified_query()` may name; each call becomes the
            name as written.
    """

    catalog: DbtCatalog
    metric_names: frozenset[str] = frozenset()
    metric_values: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    instruction_names: frozenset[str] = frozenset()
    variables: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tags: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    relationship_names: frozenset[str] = frozenset()
    filter_names: frozenset[str] = frozenset()
    verified_query_names: frozenset[str] = frozenset()

    def member_names(self, function: str) -> frozenset[str]:
        """Return the casefolded names a member-reference function may name."""
        return {
            "custom_instructions": self.instruction_names,
            "relationship": self.relationship_names,
            "filter": self.filter_names,
        }.get(function, self.verified_query_names)


@dataclass(frozen=True, slots=True)
class _Site:
    """Where the calls being resolved are written, as every diagnostic names it."""

    origin: Origin
    field: str
    artifact: str
    subject: str | None


def resolve_scalar(
    text: str,
    policy: RefPolicy,
    origin: Origin,
    context: ResolveContext,
    *,
    field: str,
    artifact: str | None = None,
    subject: str | None = None,
    ref_value: Callable[[TemplateCall], str] | None = None,
    column_value: ColumnValue | None = None,
) -> tuple[Resolved, DiagnosticBag]:
    """Replace each template call in `text` with its value, checked against the field's policy.

    Calls resolve from the last to the first, so a scalar's call diagnostics come in
    reverse source order, followed by any problem with how many calls it holds. A call
    that does not resolve stays in the text as written, and any diagnostic poisons the
    result.

    Args:
        field: How diagnostics name the field being resolved.
        artifact: How diagnostics name what holds the field; None names it by `subject`, or else
            by `field`.
        subject: The artifact key every diagnostic carries; None for none.
        ref_value: The value of a one-argument `ref()`; None substitutes the model's relation.
        column_value: The value of a two-argument `ref()` whose column exists, given the call and
            the column as the model names it, or the diagnostic that refuses it; None
            substitutes `MODEL.COLUMN`.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: a template span does not parse; nothing else is
            checked (see `calls.syntax_problem`).
        SST-REF034, SST-REF035, SST-REF004, SST-REF008, SST-REF041, SST-REF015: a call is not
            legal in the field (see `calls.call_problem`).
        SST-REF001: `ref()` names no dbt model.
        SST-REF002: `ref()` names a column its model does not have.
        SST-PRS005: `ref()` names a model or column that cannot be written as an identifier.
        SST-REF006: `metric()` names no metric.
        SST-REF007, SST-REF029, SST-REF030, SST-REF031: `custom_instructions()`,
            `relationship()`, `filter()` or `verified_query()` names no such member.
        SST-CFG029: `var()` names no project variable.
        SST-REF028: `tag()` names no declared tag.
        SST-REF009: a call resolved to an empty string.
        SST-REF003: a field that requires a call has none, or one that takes one call has more.
    """
    site = _Site(origin, field, artifact or subject or field, subject)
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError as exc:
        return Resolved(text, poisoned=True), DiagnosticBag((syntax_problem(exc, origin.file, subject=subject),))

    diagnostics: list[Diagnostic] = []
    rendered = text
    # Last call first: a substitution changes the text's length after it, never before.
    for call in reversed(calls):
        value = _resolve_call(call, policy, site, context, (ref_value, column_value))
        if isinstance(value, Diagnostic):
            diagnostics.append(value)
            continue
        rendered = rendered[: call.start] + value + rendered[call.end :]
    diagnostics.extend(_call_count_problems(text, calls, policy, site))
    return Resolved(rendered, bool(diagnostics)), DiagnosticBag(diagnostics)


def _resolve_call(
    call: TemplateCall,
    policy: RefPolicy,
    site: _Site,
    context: ResolveContext,
    ref_values: tuple[Callable[[TemplateCall], str] | None, ColumnValue | None],
) -> str | Diagnostic:
    """Return one call's value, or the single diagnostic that keeps it from resolving."""
    problem = call_problem(
        call, policy.allowed, site.origin, field=site.field, artifact=site.artifact, subject=site.subject
    )
    if problem is not None:
        return problem
    function = call.function
    if function == "ref":
        value = _resolve_ref(call, site, context, *ref_values)
    elif function == "metric":
        value = _resolve_metric(call, site, context)
    elif function in _MEMBER_CODES:
        value = _resolve_member(call, site, context)
    elif function == "var":
        value = _resolve_var(call, site, context)
    else:
        value = _resolve_tag(call, site, context)
    if value == "":
        return D(
            "SST-REF009",
            origin=site.origin,
            subject=site.subject,
            ref_function=function,
            name="','".join(call.args),
        )
    return value


def _resolve_ref(
    call: TemplateCall,
    site: _Site,
    context: ResolveContext,
    ref_value: Callable[[TemplateCall], str] | None,
    column_value: ColumnValue | None,
) -> str | Diagnostic:
    """Resolve `ref()` to the model's relation or MODEL.COLUMN, unless a callback gives the value.

    MODEL.COLUMN is a column reference: a column dbt quotes keeps its exact case, and a reserved
    word is quoted (see `column_ref`).

    Diagnostics:
        SST-REF001: the model is not declared.
        SST-REF002: the model has no such column.
        SST-PRS005: the model or the column cannot be written as an identifier.
    """
    model = context.catalog.model(call.args[0])
    if model is None:
        return D("SST-REF001", origin=site.origin, subject=site.subject, model=call.args[0])
    if len(call.args) == 1:
        return ref_value(call) if ref_value is not None else model.relation_name
    column = model.column(call.args[1])
    if column is None:
        return D("SST-REF002", origin=site.origin, subject=site.subject, model=call.args[0], column=call.args[1])
    if column_value is not None:
        return column_value(call, column.name)
    try:
        return column_ref(Identifier(model.name.upper()), column.identifier).text
    except ValueError:
        value = f"{model.name}.{column.name}"
        return D("SST-PRS005", origin=site.origin, subject=site.subject, artifact=site.artifact, value=value)


def _resolve_metric(call: TemplateCall, site: _Site, context: ResolveContext) -> str | Diagnostic:
    """Resolve a declared metric, case-insensitively, to its value or else its upper-cased name."""
    if call.args[0].casefold() not in context.metric_names:
        return D("SST-REF006", origin=site.origin, subject=site.subject, name=call.args[0])
    return context.metric_values.get(call.args[0].casefold(), call.args[0].upper())


def _resolve_member(call: TemplateCall, site: _Site, context: ResolveContext) -> str | Diagnostic:
    """Resolve a declared member, case-insensitively, to its name.

    A custom instruction's name is upper-cased, as the DDL names it; any other member's is
    returned as written.
    """
    name = call.args[0]
    if name.casefold() not in context.member_names(call.function):
        return D(_MEMBER_CODES[call.function], origin=site.origin, subject=site.subject, name=name)
    return name.upper() if call.function == "custom_instructions" else name


def _resolve_var(call: TemplateCall, site: _Site, context: ResolveContext) -> str | Diagnostic:
    """Resolve a project variable, by its exact name, to its value as text."""
    if call.args[0] not in context.variables:
        return D("SST-CFG029", origin=site.origin, subject=site.subject, var=call.args[0])
    return str(context.variables[call.args[0]])


def _resolve_tag(call: TemplateCall, site: _Site, context: ResolveContext) -> str | Diagnostic:
    """Resolve a declared tag, by its exact name, to its object name."""
    if call.args[0] not in context.tags:
        return D("SST-REF028", origin=site.origin, subject=site.subject, name=call.args[0])
    return context.tags[call.args[0]]


def _call_count_problems(
    text: str, calls: tuple[TemplateCall, ...], policy: RefPolicy, site: _Site
) -> list[Diagnostic]:
    """Diagnose a field that requires a call and has none, then one that allows one call and has more."""
    problems: list[Diagnostic] = []
    if policy.required and not calls:
        problems.append(malformed_field(text, site.origin, subject=site.subject))
    if not policy.multi and len(calls) > 1:
        problems.append(malformed_field(text, site.origin, subject=site.subject))
    return problems

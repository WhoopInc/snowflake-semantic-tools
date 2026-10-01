"""Unresolved compiler records shared by parse, resolve, attach, and validate."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from types import MappingProxyType
from typing import Callable, Mapping

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls


class RefKind(Enum):
    """What a resolved template call refers to.

    A one-argument `ref()` is a TABLE and a two-argument one a COLUMN; a call to any function
    other than `ref`, `metric`, `custom_instructions`, and `var` is a TAG.
    """

    TABLE = auto()
    COLUMN = auto()
    METRIC = auto()
    CUSTOM_INSTRUCTION = auto()
    VAR = auto()
    TAG = auto()


@dataclass(frozen=True, slots=True)
class RefOrigin:
    """One template call a scalar resolved, as `Resolved.origins` records it.

    Attributes:
        raw: The call exactly as written, braces included.
        model_name: The model a `ref()` names; None for any other function.
        column_name: The column a two-argument `ref()` names; None otherwise.
    """

    kind: RefKind
    raw: str
    model_name: str | None = None
    column_name: str | None = None
    deprecated: bool = False


@dataclass(frozen=True, slots=True)
class Resolved:
    """A scalar with its template calls replaced by their values.

    Attributes:
        text: The scalar with each call that resolved replaced; one that did not stays as written.
        origins: The calls that resolved, in source order.
        poisoned: Resolving it reported a diagnostic, so `text` may still hold unresolved calls.
    """

    text: str
    origins: tuple[RefOrigin, ...] = ()
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class RefPolicy:
    """Which template calls one kind of field accepts, and how many.

    A `table()` or `column()` call is rejected as legacy whatever `allowed` says.

    Attributes:
        allowed: The template functions the field may call.
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
TAG_NAME = RefPolicy(frozenset(("tag",)), required=True, multi=False)
DESCRIPTION = RefPolicy(frozenset())


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
    """

    catalog: DbtCatalog
    metric_names: frozenset[str] = frozenset()
    metric_values: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    instruction_names: frozenset[str] = frozenset()
    variables: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    tags: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))


def _kind(function: str, args: tuple[str, ...]) -> RefKind:
    if function in ("ref", "table"):
        return RefKind.TABLE if len(args) == 1 else RefKind.COLUMN
    if function == "metric":
        return RefKind.METRIC
    if function == "custom_instructions":
        return RefKind.CUSTOM_INSTRUCTION
    if function == "var":
        return RefKind.VAR
    return RefKind.TAG


def resolve_scalar(
    text: str,
    policy: RefPolicy,
    origin: Origin,
    context: ResolveContext,
    *,
    field: str,
    ref_value: Callable[[TemplateCall], str] | None = None,
) -> tuple[Resolved, DiagnosticBag]:
    """Replace each template call in `text` with its value, checked against the field's policy.

    Calls resolve from the last to the first, so a scalar's call diagnostics come in
    reverse source order, followed by any problem with how many calls it holds. A call
    that does not resolve stays in the text as written, and any diagnostic poisons the
    result.

    Args:
        field: How diagnostics name the field being resolved.
        ref_value: The value of a one-argument `ref()`; None substitutes the model's relation.

    Diagnostics:
        SST-LOD004: a template call is malformed; nothing else is checked.
        SST-REF034: a legacy `table()` call.
        SST-REF035: a legacy `column()` call.
        SST-REF041: a call to a function the field does not allow.
        SST-REF042: a call has the wrong number of arguments, or a one-reference field holds more.
        SST-REF001: `ref()` names no dbt model.
        SST-REF002: `ref()` names a column its model does not have.
        SST-REF006: `metric()` names no metric.
        SST-REF039: `custom_instructions()` names no custom instruction.
        SST-REF038: `var()` names no project variable.
        SST-REF040: a tag call names no declared tag, or a field that requires a call has none.
    """
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError as exc:
        return Resolved(text, poisoned=True), DiagnosticBag((_malformed(origin, exc),))

    diagnostics: list[Diagnostic] = []
    rendered = text
    origins: list[RefOrigin] = []
    # Last call first: a substitution changes the text's length after it, never before.
    for call in reversed(calls):
        value = _resolve_call(call, policy, origin, context, field, ref_value)
        if isinstance(value, Diagnostic):
            diagnostics.append(value)
            continue
        rendered = rendered[: call.start] + value + rendered[call.end :]
        origins.insert(0, _ref_origin(call))
    diagnostics.extend(_call_count_problems(calls, policy, origin, field))
    return Resolved(rendered, tuple(origins), bool(diagnostics)), DiagnosticBag(diagnostics)


def _malformed(origin: Origin, exc: TemplateSyntaxError) -> Diagnostic:
    return D(
        "SST-LOD004",
        origin=Origin(origin.file, exc.line, exc.col),
        file=origin.file,
        line=exc.line,
        col=exc.col,
        reason=exc.reason,
    )


def _resolve_call(
    call: TemplateCall,
    policy: RefPolicy,
    origin: Origin,
    context: ResolveContext,
    field: str,
    ref_value: Callable[[TemplateCall], str] | None,
) -> str | Diagnostic:
    """Return one call's value, or the single diagnostic that keeps it from resolving.

    A legacy call is rejected before the policy is consulted. Any allowed function other
    than `ref`, `metric`, `custom_instructions`, and `var` resolves as a tag.
    """
    function = call.function
    if function in ("table", "column"):
        return _legacy_call(call, origin)
    if function not in policy.allowed:
        return D("SST-REF041", origin=origin, artifact=field, function=function, field=field)
    if function == "ref":
        return _resolve_ref(call, origin, context, field, ref_value)
    if function == "metric":
        return _resolve_metric(call, origin, context, field)
    if function == "custom_instructions":
        return _resolve_instruction(call, origin, context, field)
    if function == "var":
        return _resolve_var(call, origin, context, field)
    return _resolve_tag(call, origin, context, field)


def _legacy_call(call: TemplateCall, origin: Origin) -> Diagnostic:
    """Reject a `table()` or `column()` call, located at the call rather than at the field."""
    located = Origin(origin.file, call.line, call.col)
    model = call.args[0] if call.args else ""
    if call.function == "table":
        return D("SST-REF034", origin=located, file=origin.file, line=call.line, col=call.col, model=model)
    column = call.args[1] if len(call.args) > 1 else ""
    return D("SST-REF035", origin=located, file=origin.file, line=call.line, col=call.col, model=model, column=column)


def _resolve_ref(
    call: TemplateCall,
    origin: Origin,
    context: ResolveContext,
    field: str,
    ref_value: Callable[[TemplateCall], str] | None,
) -> str | Diagnostic:
    """Resolve `ref('<model>')` through `ref_value` or to the relation, and a column to MODEL.COLUMN."""
    if len(call.args) not in (1, 2):
        return D(
            "SST-REF042",
            origin=origin,
            artifact=field,
            detail=f"ref() takes one or two arguments, found {len(call.args)} in {call.raw}",
        )
    model = context.catalog.model(call.args[0])
    if model is None:
        return D("SST-REF001", origin=origin, model=call.args[0])
    if len(call.args) == 1:
        return ref_value(call) if ref_value is not None else model.relation_name
    column = model.column(call.args[1])
    if column is None:
        return D("SST-REF002", origin=origin, model=call.args[0], column=call.args[1])
    return f"{model.name.upper()}.{column.name.upper()}"


def _resolve_metric(call: TemplateCall, origin: Origin, context: ResolveContext, field: str) -> str | Diagnostic:
    """Resolve a declared metric, case-insensitively, to its value or else its upper-cased name."""
    if len(call.args) != 1:
        return _one_argument(origin, field, call)
    if call.args[0].casefold() not in context.metric_names:
        return D("SST-REF006", origin=origin, name=call.args[0])
    return context.metric_values.get(call.args[0].casefold(), call.args[0].upper())


def _resolve_instruction(call: TemplateCall, origin: Origin, context: ResolveContext, field: str) -> str | Diagnostic:
    """Resolve a declared custom instruction, case-insensitively, to its upper-cased name."""
    if len(call.args) != 1:
        return _one_argument(origin, field, call)
    if call.args[0].casefold() not in context.instruction_names:
        return D("SST-REF039", origin=origin, artifact=field, name=call.args[0])
    return call.args[0].upper()


def _resolve_var(call: TemplateCall, origin: Origin, context: ResolveContext, field: str) -> str | Diagnostic:
    """Resolve a project variable, by its exact name, to its value as text."""
    if len(call.args) != 1:
        return _one_argument(origin, field, call)
    if call.args[0] not in context.variables:
        return D("SST-REF038", origin=origin, artifact=field, name=call.args[0])
    return str(context.variables[call.args[0]])


def _resolve_tag(call: TemplateCall, origin: Origin, context: ResolveContext, field: str) -> str | Diagnostic:
    """Resolve a declared tag, by its exact name, to its object name."""
    if len(call.args) != 1 or call.args[0] not in context.tags:
        return D("SST-REF040", origin=origin, artifact=field, detail=f"{call.raw} names no declared tag")
    return context.tags[call.args[0]]


def _ref_origin(call: TemplateCall) -> RefOrigin:
    is_ref = call.function == "ref"
    return RefOrigin(
        _kind(call.function, call.args),
        call.raw,
        call.args[0] if is_ref and call.args else None,
        call.args[1] if is_ref and len(call.args) == 2 else None,
    )


def _call_count_problems(
    calls: tuple[TemplateCall, ...], policy: RefPolicy, origin: Origin, field: str
) -> list[Diagnostic]:
    """Diagnose a field that requires a call and has none, then one that allows one call and has more."""
    problems: list[Diagnostic] = []
    if policy.required and not calls:
        problems.append(
            D(
                "SST-REF040",
                origin=origin,
                artifact=field,
                detail=f"{field} must be a single {{{{ tag('<name>') }}}} call",
            )
        )
    if not policy.multi and len(calls) > 1:
        problems.append(
            D("SST-REF042", origin=origin, artifact=field, detail=f"{field} accepts one reference, found {len(calls)}")
        )
    return problems


def _one_argument(origin: Origin, field: str, call: TemplateCall) -> Diagnostic:
    return D(
        "SST-REF042",
        origin=origin,
        artifact=field,
        detail=f"{call.function}() takes one name, found {len(call.args)} in {call.raw}",
    )

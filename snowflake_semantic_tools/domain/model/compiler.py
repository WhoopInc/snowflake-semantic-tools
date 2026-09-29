"""Unresolved compiler records shared by parse, resolve, attach, and validate."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from types import MappingProxyType
from typing import Callable, Mapping

from .dbt import DbtCatalog
from .diagnostic import D, Diagnostic, DiagnosticBag, Origin
from .reference import TemplateCall, TemplateSyntaxError, scan_template_calls


class RefKind(Enum):
    TABLE = auto()
    COLUMN = auto()
    METRIC = auto()
    CUSTOM_INSTRUCTION = auto()
    VAR = auto()
    TAG = auto()


@dataclass(frozen=True, slots=True)
class RefOrigin:
    kind: RefKind
    raw: str
    model_name: str | None = None
    column_name: str | None = None
    deprecated: bool = False


@dataclass(frozen=True, slots=True)
class Resolved:
    text: str
    origins: tuple[RefOrigin, ...] = ()
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class RefPolicy:
    allowed: frozenset[str]
    required: bool = False
    multi: bool = True


METRIC_EXPR = RefPolicy(frozenset(("ref", "metric", "var")))
FILTER_EXPR = RefPolicy(frozenset(("ref", "var")))
VQR_SQL = RefPolicy(frozenset(("ref", "metric", "var")))
TABLE_ITEM = RefPolicy(frozenset(("ref", "table")), required=True, multi=False)
CUSTOM_INSTRUCTION_ITEM = RefPolicy(frozenset(("custom_instructions",)), required=True, multi=False)
TAG_NAME = RefPolicy(frozenset(("tag",)), required=True, multi=False)
DESCRIPTION = RefPolicy(frozenset())


@dataclass(frozen=True, slots=True)
class ResolveContext:
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
    diagnostics: list[Diagnostic] = []
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError as exc:
        diagnostic = D(
            "SST-LOD004",
            origin=Origin(origin.file, exc.line, exc.col),
            file=origin.file,
            line=exc.line,
            col=exc.col,
            reason=exc.reason,
        )
        return Resolved(text, poisoned=True), DiagnosticBag((diagnostic,))

    rendered = text
    origins: list[RefOrigin] = []
    for call in reversed(calls):
        function = call.function
        if function in ("table", "column"):
            code = "SST-REF034" if function == "table" else "SST-REF035"
            if function == "table":
                diagnostics.append(
                    D(
                        code,
                        origin=Origin(origin.file, call.line, call.col),
                        file=origin.file,
                        line=call.line,
                        col=call.col,
                        model=call.args[0] if call.args else "",
                    )
                )
            else:
                diagnostics.append(
                    D(
                        code,
                        origin=Origin(origin.file, call.line, call.col),
                        file=origin.file,
                        line=call.line,
                        col=call.col,
                        model=call.args[0] if call.args else "",
                        column=call.args[1] if len(call.args) > 1 else "",
                    )
                )
            continue
        if function not in policy.allowed:
            diagnostics.append(D("SST-REF041", origin=origin, artifact=field, function=function, field=field))
            continue
        value: str | None = None
        if function == "ref":
            if len(call.args) not in (1, 2):
                diagnostics.append(
                    D(
                        "SST-REF042",
                        origin=origin,
                        artifact=field,
                        detail=f"ref() takes one or two arguments, found {len(call.args)} in {call.raw}",
                    )
                )
                continue
            model = context.catalog.model(call.args[0])
            if model is None:
                diagnostics.append(D("SST-REF001", origin=origin, model=call.args[0]))
                continue
            if len(call.args) == 2:
                column = model.column(call.args[1])
                if column is None:
                    diagnostics.append(D("SST-REF002", origin=origin, model=call.args[0], column=call.args[1]))
                    continue
                value = f"{model.name.upper()}.{column.name.upper()}"
            else:
                value = ref_value(call) if ref_value is not None else model.relation_name
        elif function == "metric":
            if len(call.args) != 1:
                diagnostics.append(_one_argument(origin, field, call))
                continue
            if call.args[0].casefold() not in context.metric_names:
                diagnostics.append(D("SST-REF006", origin=origin, name=call.args[0]))
                continue
            value = context.metric_values.get(call.args[0].casefold(), call.args[0].upper())
        elif function == "custom_instructions":
            if len(call.args) != 1:
                diagnostics.append(_one_argument(origin, field, call))
                continue
            if call.args[0].casefold() not in context.instruction_names:
                diagnostics.append(D("SST-REF039", origin=origin, artifact=field, name=call.args[0]))
                continue
            value = call.args[0].upper()
        elif function == "var":
            if len(call.args) != 1:
                diagnostics.append(_one_argument(origin, field, call))
                continue
            if call.args[0] not in context.variables:
                diagnostics.append(D("SST-REF038", origin=origin, artifact=field, name=call.args[0]))
                continue
            value = str(context.variables[call.args[0]])
        else:
            if len(call.args) != 1 or call.args[0] not in context.tags:
                diagnostics.append(
                    D("SST-REF040", origin=origin, artifact=field, detail=f"{call.raw} names no declared tag")
                )
                continue
            value = context.tags[call.args[0]]
        assert value is not None
        rendered = rendered[: call.start] + value + rendered[call.end :]
        origins.insert(
            0,
            RefOrigin(
                _kind(function, call.args),
                call.raw,
                call.args[0] if function == "ref" and call.args else None,
                call.args[1] if function == "ref" and len(call.args) == 2 else None,
            ),
        )

    if policy.required and not calls:
        diagnostics.append(
            D(
                "SST-REF040",
                origin=origin,
                artifact=field,
                detail=f"{field} must be a single {{{{ tag('<name>') }}}} call",
            )
        )
    if not policy.multi and len(calls) > 1:
        diagnostics.append(
            D("SST-REF042", origin=origin, artifact=field, detail=f"{field} accepts one reference, found {len(calls)}")
        )
    return Resolved(rendered, tuple(origins), bool(diagnostics)), DiagnosticBag(diagnostics)


def _one_argument(origin: Origin, field: str, call: TemplateCall) -> Diagnostic:
    return D(
        "SST-REF042",
        origin=origin,
        artifact=field,
        detail=f"{call.function}() takes one name, found {len(call.args)} in {call.raw}",
    )

"""The rules every template call obeys before any resolver looks up what it names.

One function decides, for every field that holds template calls, whether a call is legal at
all: `call_problem` rejects a legacy global, a function the dialect does not have, one the
field does not accept, and a wrong argument count, in that order. `syntax_problem` names the
code for a span that does not parse. The resolver and the load-time checks both call them, so
a call is refused the same way wherever it is written.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.parse.template import (
    TemplateCall,
    TemplateSyntaxError,
    TemplateSyntaxKind,
    single_template_call,
)

# Every template function of the dialect. `table` and `column` are the legacy spellings of
# `ref`: they are known so that writing one is refused as legacy, not as an unknown function.
KNOWN_FUNCTIONS: frozenset[str] = frozenset(
    (
        "agent",
        "column",
        "custom_instructions",
        "eval_metric",
        "extension",
        "file",
        "filter",
        "metric",
        "plugin",
        "ref",
        "relationship",
        "semantic_view",
        "skill",
        "table",
        "tag",
        "tool",
        "var",
        "verified_query",
    )
)

# The argument counts a function accepts; a function not listed takes one name.
_ARITY: Mapping[str, tuple[int, ...]] = MappingProxyType({"ref": (1, 2), "tool": (2,)})


def accepted_arity(function: str) -> tuple[int, ...]:
    """Return the argument counts `function` accepts, fewest first."""
    return _ARITY.get(function, (1,))


def call_problem(
    call: TemplateCall,
    allowed: frozenset[str],
    origin: Origin | None,
    *,
    field: str,
    artifact: str,
    subject: str | None = None,
) -> Diagnostic | None:
    """Return the diagnostic that makes one call illegal where it is written, or None.

    The checks run in a fixed order and the first that fails is the one reported. A legacy
    call is located at the call itself, since the migration that fixes it is a rename there.

    Args:
        allowed: The functions the field accepts; empty for a field that takes no calls.
        field: How the diagnostic names the field.
        artifact: How the diagnostic names what holds the field.

    Diagnostics:
        SST-REF034: a legacy `table()` call.
        SST-REF035: a legacy `column()` call.
        SST-REF004: a function the dialect does not have.
        SST-REF008: any call in a field that accepts none.
        SST-REF041: a function the field does not accept.
        SST-REF015: a call with an argument count its function does not take.
    """
    function = call.function
    if function in ("table", "column"):
        return _legacy(call, origin, subject)
    if function not in KNOWN_FUNCTIONS:
        return D("SST-REF004", origin=origin, subject=subject, ref_function=function)
    if not allowed:
        return D("SST-REF008", origin=origin, subject=subject, artifact=artifact, field=field)
    if function not in allowed:
        return D("SST-REF041", origin=origin, subject=subject, artifact=artifact, function=function, field=field)
    arity = accepted_arity(function)
    if len(call.args) not in arity:
        return D(
            "SST-REF015",
            origin=origin,
            subject=subject,
            ref_function=function,
            expected=" or ".join(str(count) for count in arity),
            found=len(call.args),
        )
    return None


def _legacy(call: TemplateCall, origin: Origin | None, subject: str | None) -> Diagnostic:
    """Reject a `table()` or `column()` call, located at the call rather than at the field."""
    file = origin.file if origin is not None else "<template>"
    located = Origin(file, call.line, call.col)
    model = call.args[0] if call.args else ""
    if call.function == "table":
        return D("SST-REF034", origin=located, subject=subject, file=file, line=call.line, col=call.col, model=model)
    column = call.args[1] if len(call.args) > 1 else ""
    return D(
        "SST-REF035",
        origin=located,
        subject=subject,
        file=file,
        line=call.line,
        col=call.col,
        model=model,
        column=column,
    )


def syntax_problem(exc: TemplateSyntaxError, file: str, *, subject: str | None = None) -> Diagnostic:
    """Report a template span that does not parse, under the code for how it breaks.

    `exc` positions the break within its own scalar, so the diagnostic does too.

    Diagnostics:
        SST-LOD004: a `{{` never closes, or another opens inside it.
        SST-REF033: the span is not a `fn(args)` call at all.
        SST-REF003: the span is a call whose arguments do not parse.
    """
    origin = Origin(file, exc.line, exc.col)
    if exc.kind is TemplateSyntaxKind.GRAMMAR:
        return D(
            "SST-REF033",
            origin=origin,
            subject=subject,
            file=file,
            line=exc.line,
            col=exc.col,
            text=exc.span,
            detail=exc.reason,
        )
    if exc.kind is TemplateSyntaxKind.ARGUMENTS:
        return D("SST-REF003", origin=origin, subject=subject, file=file, line=exc.line, col=exc.col, value=exc.span)
    return D("SST-LOD004", origin=origin, subject=subject, file=file, line=exc.line, col=exc.col, reason=exc.reason)


def malformed_field(text: str, origin: Origin | None, *, subject: str | None = None) -> Diagnostic:
    """Report a field that must be exactly one template call and is something else (SST-REF003)."""
    file = origin.file if origin is not None else "<template>"
    line = origin.line if origin is not None and origin.line is not None else 1
    col = origin.col if origin is not None and origin.col is not None else 1
    return D("SST-REF003", origin=origin, subject=subject, file=file, line=line, col=col, value=text)


def literal_problem(
    text: str, origin: Origin | None, *, field: str, artifact: str, subject: str | None = None
) -> Diagnostic | None:
    """Report a template expression in a field that is published as written (SST-REF008), or None.

    Any `{{` counts, even one that would not parse: the field takes no expressions at all, so
    there is nothing to correct the syntax towards.
    """
    if "{{" not in text:
        return None
    return D("SST-REF008", origin=origin, subject=subject, artifact=artifact, field=field)


def variable_problem(
    name: str, variables: Mapping[str, object], origin: Origin | None, *, subject: str | None = None
) -> Diagnostic | None:
    """Return why `var('<name>')` cannot substitute a value, or None when it can.

    Diagnostics:
        SST-CFG029: `vars:` declares no variable of that exact name.
        SST-REF009: the variable's value is empty, so the call would substitute nothing.
    """
    if name not in variables:
        return D("SST-CFG029", origin=origin, subject=subject, var=name)
    if str(variables[name]) == "":
        return D("SST-REF009", origin=origin, subject=subject, ref_function="var", name=name)
    return None


def member_reference(text: str, function: str) -> str | None:
    """Return the name `text` references when the whole of it is one one-argument `function()` call.

    None when `text` is anything else, including a call that does not parse: the caller then
    reads it as a literal name.
    """
    try:
        call = single_template_call(text, function)
    except TemplateSyntaxError:
        return None
    if call is None or len(call.args) != 1:
        return None
    return call.args[0]

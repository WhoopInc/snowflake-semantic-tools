"""Resolve the `custom_instructions()` entries of each view to the custom instructions it attaches."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.project import ParsedView
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls


def _view_instructions(
    views: tuple[ParsedView, ...], known: frozenset[str]
) -> tuple[dict[str, frozenset[str]], tuple[Diagnostic, ...]]:
    """Resolve each view's `custom_instructions` entries to the casefolded names they attach.

    A `custom_instructions` value that is not a list names nothing.

    Returns:
        The names by view key, with an entry for every view, and a diagnostic for each
        entry or call that attaches nothing.

    Diagnostics:
        SST-LOD004: an entry is not a well-formed template.
    """
    names_by_view: dict[str, frozenset[str]] = {}
    diagnostics: list[Diagnostic] = []
    for view in views:
        raw_instructions = view.source.get("custom_instructions")
        names: set[str] = set()
        for raw in raw_instructions if isinstance(raw_instructions, list) else []:
            try:
                calls = scan_template_calls(str(raw))
            except TemplateSyntaxError as exc:
                diagnostics.append(
                    D(
                        "SST-LOD004",
                        origin=view.origin,
                        file=view.origin.file,
                        line=exc.line,
                        col=exc.col,
                        reason=exc.reason,
                        subject=artifact_key("semantic_view", view.name),
                    )
                )
                continue
            for call in calls:
                resolved = _instruction_name(view, call, known)
                if isinstance(resolved, str):
                    names.add(resolved)
                else:
                    diagnostics.append(resolved)
        names_by_view[artifact_key("semantic_view", view.name)] = frozenset(names)
    return names_by_view, tuple(diagnostics)


def _instruction_name(view: ParsedView, call: TemplateCall, known: frozenset[str]) -> str | Diagnostic:
    """Return the casefolded name one call attaches, or the diagnostic for why it attaches none.

    Diagnostics:
        SST-REF041: the call is to a function other than custom_instructions().
        SST-REF042: the call does not pass exactly one name.
        SST-REF039: the name matches no custom instruction.
    """
    view_subject = artifact_key("semantic_view", view.name)
    if call.function != "custom_instructions":
        return D(
            "SST-REF041",
            origin=view.origin,
            subject=view_subject,
            artifact=view_subject,
            function=call.function,
            field="custom_instructions",
        )
    if len(call.args) != 1:
        return D(
            "SST-REF042",
            origin=view.origin,
            subject=view_subject,
            artifact=view_subject,
            detail=f"custom_instructions() takes one name, found {len(call.args)} in {call.raw}",
        )
    instruction_name = call.args[0].casefold()
    if instruction_name not in known:
        return D("SST-REF039", origin=view.origin, subject=view_subject, artifact=view_subject, name=call.args[0])
    return instruction_name

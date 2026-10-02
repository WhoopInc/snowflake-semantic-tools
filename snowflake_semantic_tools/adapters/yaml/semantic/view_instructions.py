"""Resolve the custom instructions: each view's `custom_instructions()` entries, and each block's text.

`_view_instructions` turns a view's entries into the instructions it attaches;
`_instruction_texts` resolves the member references a block's prose makes.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.adapters.yaml.semantic.defs import InstructionDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedProject, ParsedView
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import call_problem, syntax_problem
from snowflake_semantic_tools.domain.resolve.template import (
    CUSTOM_INSTRUCTION_ITEM,
    INSTRUCTION_TEXT,
    ResolveContext,
    resolve_scalar,
)


def _view_instructions(
    views: tuple[ParsedView, ...], known: frozenset[str]
) -> tuple[dict[str, frozenset[str]], tuple[Diagnostic, ...]]:
    """Resolve each view's `custom_instructions` entries to the casefolded names they attach.

    A `custom_instructions` value that is not a list names nothing; a string is the 0.3
    bare-string form, which 1.0 never renders, so it is reported.

    Returns:
        The names by view key, with an entry for every view, and a diagnostic for each
        entry or call that attaches nothing.

    Diagnostics:
        SST-VAL408: a view's `custom_instructions` is a bare string.
        SST-LOD004, SST-REF033, SST-REF003: an entry is not a template that parses.
    """
    names_by_view: dict[str, frozenset[str]] = {}
    diagnostics: list[Diagnostic] = []
    for view in views:
        raw_instructions = view.source.get("custom_instructions")
        if isinstance(raw_instructions, str):
            subject = artifact_key("semantic_view", view.name)
            diagnostics.append(D("SST-VAL408", member=view.name, origin=view.origin, subject=subject))
        names: set[str] = set()
        for raw in raw_instructions if isinstance(raw_instructions, list) else []:
            try:
                calls = scan_template_calls(str(raw))
            except TemplateSyntaxError as exc:
                diagnostics.append(
                    syntax_problem(exc, view.origin.file, subject=artifact_key("semantic_view", view.name))
                )
                continue
            for call in calls:
                resolved = _instruction_name(view, call, known)
                if isinstance(resolved, str):
                    names.add(resolved)
                elif resolved is not None:
                    diagnostics.append(resolved)
        names_by_view[artifact_key("semantic_view", view.name)] = frozenset(names)
    return names_by_view, tuple(diagnostics)


def _instruction_name(view: ParsedView, call: TemplateCall, known: frozenset[str]) -> str | Diagnostic | None:
    """Return the casefolded name one call attaches, or the diagnostic for why it attaches none.

    None for a legacy `table()` or `column()` call, which the legacy check already names at its
    position.

    Diagnostics:
        SST-REF004, SST-REF041, SST-REF015: the call is not one one-name
            `custom_instructions()` call.
        SST-REF007: the name matches no custom instruction.
    """
    if call.function in ("table", "column"):
        return None
    view_subject = artifact_key("semantic_view", view.name)
    problem = call_problem(
        call,
        CUSTOM_INSTRUCTION_ITEM.allowed,
        view.origin,
        field="custom_instructions",
        artifact=view_subject,
        subject=view_subject,
    )
    if problem is not None:
        return problem
    instruction_name = call.args[0].casefold()
    if instruction_name not in known:
        return D("SST-REF007", origin=view.origin, subject=view_subject, name=call.args[0])
    return instruction_name


def _instruction_texts(
    parsed: ParsedProject, catalog: DbtCatalog
) -> tuple[tuple[ParsedMember, ...], tuple[Diagnostic, ...], frozenset[str]]:
    """Resolve the `filter()`, `relationship()` and `verified_query()` calls in each instruction's text.

    Each call becomes the name of the member it references, so the published guidance names a
    member that exists. A block whose text does not resolve keeps its text as written and is
    poisoned, so no view attaches it.

    Returns:
        Every member, each instruction's text resolved; the diagnostics, block by block and
        channel by channel; and the casefolded keys of the blocks that did not resolve.

    Diagnostics:
        Those of `resolve_scalar` under `INSTRUCTION_TEXT`; notably SST-REF029, SST-REF030 and
        SST-REF031 for a call that names no relationship, filter or verified query.
    """
    by_type = parsed.members_by_type
    context = ResolveContext(
        catalog,
        relationship_names=_names(by_type.get("relationship", ())),
        filter_names=_names(by_type.get("filter", ())),
        verified_query_names=_names(by_type.get("verified_query", ())),
    )
    members: list[ParsedMember] = []
    diagnostics: list[Diagnostic] = []
    poisoned: set[str] = set()
    for member in parsed.members:
        source = member.source
        if not isinstance(source, InstructionDef):
            members.append(member)
            continue
        sql, sql_found = _resolved_text(source.ai_sql_generation, "ai_sql_generation", member, context)
        question, question_found = _resolved_text(
            source.ai_question_categorization, "ai_question_categorization", member, context
        )
        diagnostics.extend((*sql_found, *question_found))
        if sql_found or question_found:
            poisoned.add(member.key.casefold())
        resolved = replace(source, ai_sql_generation=sql, ai_question_categorization=question)
        members.append(replace(member, source=resolved))
    return tuple(members), tuple(diagnostics), frozenset(poisoned)


def _resolved_text(
    text: str | None, channel: str, member: ParsedMember, context: ResolveContext
) -> tuple[str | None, tuple[Diagnostic, ...]]:
    """Resolve one channel of an instruction's text; None stays None."""
    if text is None:
        return None, ()
    value, found = resolve_scalar(text, INSTRUCTION_TEXT, member.origin, context, field=channel, subject=member.key)
    return value.text, tuple(found)


def _names(members: tuple[ParsedMember, ...]) -> frozenset[str]:
    return frozenset(member.name.casefold() for member in members)

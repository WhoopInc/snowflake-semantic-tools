"""Check what members say: hardcoded object names, predicates said twice, and prose naming nothing.

A three-part name pins an object regardless of target, so an expression, a query or a view's
table names its objects through templates instead. A custom instruction that repeats a filter's
predicate says the same thing in the unenforceable place, and one that names a metric or filter
the project does not declare points a reader at nothing.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.adapters.yaml.semantic.defs import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.project import ParsedView

_NAME = r"(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_$]*)"
# A three-part name: two dots joining identifiers, not part of a longer dotted chain.
_THREE_PART = re.compile(rf"(?<![\w$.\"]){_NAME}\.{_NAME}\.{_NAME}(?![\w$.\"])")
_TEMPLATE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_STRING = re.compile(r"'(?:''|[^'])*'")
_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
# Prose that names a member by its kind: "the total_supply_cost metric", "the is_x filter".
_NAMED_MEMBER = re.compile(r"\b([a-z][a-z0-9]*(?:_[a-z0-9]+)+)\s+(metric|filter)\b")


def hardcoded_names(text: str) -> tuple[str, ...]:
    """The three-part names `text` writes out, each once in order.

    Template calls, string literals and comments are masked first: a name in any of them
    is not one the text resolves.
    """
    masked = _TEMPLATE.sub(" ", text)
    masked = _COMMENT.sub(" ", masked)
    masked = _STRING.sub("''", masked)
    return tuple(dict.fromkeys(match.group(0) for match in _THREE_PART.finditer(masked)))


def _hardcoded_name_diagnostics(
    views: tuple[ParsedView, ...],
    metrics: tuple[MetricDef, ...],
    filters: tuple[FilterDef, ...],
    queries: tuple[VerifiedQueryDef, ...],
) -> tuple[Diagnostic, ...]:
    """Report each three-part name a view's tables, a metric, a filter or a query writes out.

    Diagnostics:
        SST-VAL006: an authored table entry, expression or query hardcodes a three-part name.
    """
    fields: list[tuple[str, str, str, str, Origin | None]] = [
        ("semantic_view", view.name, f"tables[{index}]", str(entry), view.origin)
        for view in views
        for index, entry in enumerate(
            view.source.get("tables") or [] if isinstance(view.source.get("tables"), list) else []
        )
    ]
    fields.extend(("metric", metric.name, "expr", metric.expr, metric.origin) for metric in metrics)
    fields.extend(("filter", item.name, "expr", item.expr, item.origin) for item in filters)
    fields.extend(("verified_query", query.name, "sql", query.sql, query.origin) for query in queries)
    return tuple(
        D(
            "SST-VAL006",
            origin=origin,
            subject=artifact_key(type_name, name),
            type=type_name,
            name=name,
            field=field,
            value=value,
        )
        for type_name, name, field, text, origin in fields
        for value in hardcoded_names(text)
    )


def _instruction_texts(instruction: InstructionDef) -> Iterator[str]:
    for text in (instruction.ai_sql_generation, instruction.ai_question_categorization):
        if text:
            yield text


def _normal(text: str) -> str:
    return " ".join(text.split()).casefold()


def _predicate(filter_def: FilterDef) -> str:
    """A filter's predicate as prose would write it: each `ref()` reduced to its column name."""
    bare = re.sub(r"\{\{\s*ref\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", r"\1", filter_def.expr)
    return _normal(bare)


def _overlap_diagnostics(
    filters: tuple[FilterDef, ...], instructions: Mapping[str, InstructionDef]
) -> tuple[Diagnostic, ...]:
    """Report each custom instruction that writes out a filter's predicate.

    A predicate is compared without its templates, whitespace or case; one that still holds a
    template call, or no comparison at all, is not compared.

    Diagnostics:
        SST-VAL017: a custom instruction's text holds a filter's predicate.
    """
    predicates = [
        (filter_def, predicate)
        for filter_def in filters
        if "{{" not in (predicate := _predicate(filter_def)) and re.search(r"[=<>]|\b(?:in|like)\b", predicate)
    ]
    diagnostics: list[Diagnostic] = []
    for name in sorted(instructions):
        instruction = instructions[name]
        texts = [_normal(text) for text in _instruction_texts(instruction)]
        diagnostics.extend(
            D(
                "SST-VAL017",
                origin=instruction.origin,
                subject=artifact_key("custom_instruction", instruction.name),
                type="custom_instruction",
                name=instruction.name,
                other=f"filter '{filter_def.name}'",
            )
            for filter_def, predicate in predicates
            if any(predicate in text for text in texts)
        )
    return tuple(diagnostics)


def _unresolved_prose_diagnostics(
    instructions: Mapping[str, InstructionDef], metric_names: frozenset[str], filter_names: frozenset[str]
) -> tuple[Diagnostic, ...]:
    """Report each metric or filter an instruction names that the project does not declare.

    Prose names a member as `the <name> metric` or `the <name> filter`.

    Diagnostics:
        SST-VAL019: an instruction's prose names a metric or filter that does not resolve.
    """
    known = {"metric": metric_names, "filter": filter_names}
    diagnostics: list[Diagnostic] = []
    for name in sorted(instructions):
        instruction = instructions[name]
        named = dict.fromkeys(
            (match.group(1), match.group(2))
            for text in _instruction_texts(instruction)
            for match in _NAMED_MEMBER.finditer(text)
        )
        diagnostics.extend(
            D(
                "SST-VAL019",
                origin=instruction.origin,
                subject=artifact_key("custom_instruction", instruction.name),
                type="custom_instruction",
                name=instruction.name,
                value=f"{kind} {member}",
            )
            for member, kind in named
            if member.casefold() not in known[kind]
        )
    return tuple(diagnostics)

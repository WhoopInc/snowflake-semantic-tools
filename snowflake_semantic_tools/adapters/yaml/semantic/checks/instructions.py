"""Check each custom instruction's channels, then the instructions each view attaches together."""

from __future__ import annotations

from collections.abc import Mapping
from itertools import combinations

from snowflake_semantic_tools.adapters.yaml.semantic.defs import InstructionDef
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.validate.instruction import (
    CATEGORIZATION_CHANNEL,
    SQL_CHANNEL,
    contradicts,
    misplaced_rule,
    state_keywords,
)


def _instruction_diagnostics(instructions: tuple[InstructionDef, ...]) -> tuple[Diagnostic, ...]:
    """Check each instruction's channels, instruction by instruction.

    A block that only spells its channels the 0.3 way is left to SST-PRS020, which names the
    1.0 key; it is not also reported as empty.

    Diagnostics:
        SST-VAL407: when neither channel holds any text.
        SST-VAL409: when a channel holds a rule only the other channel acts on, once per channel.
        SST-VAL411: when the block uses Cortex Analyst state keywords.
    """
    diagnostics: list[Diagnostic] = []
    for instruction in instructions:
        subject = artifact_key("custom_instruction", instruction.name)
        channels = (
            (SQL_CHANNEL, instruction.ai_sql_generation),
            (CATEGORIZATION_CHANNEL, instruction.ai_question_categorization),
        )
        if not any(text for _, text in channels):
            if not instruction.renamed:
                diagnostics.append(D("SST-VAL407", member=instruction.name, subject=subject, origin=instruction.origin))
            continue
        for channel, text in channels:
            found = misplaced_rule(text, channel) if text else None
            if found is not None:
                diagnostics.append(
                    D(
                        "SST-VAL409",
                        member=instruction.name,
                        found=found,
                        expected=channel,
                        subject=subject,
                        origin=instruction.origin,
                    )
                )
        keywords = state_keywords("\n".join(text for _, text in channels if text))
        if keywords:
            diagnostics.append(
                D(
                    "SST-VAL411",
                    member=instruction.name,
                    found=", ".join(keywords),
                    subject=subject,
                    origin=instruction.origin,
                )
            )
    return tuple(diagnostics)


def _contradiction_diagnostics(
    instructions: Mapping[str, InstructionDef], view_instructions: Mapping[str, frozenset[str]]
) -> tuple[Diagnostic, ...]:
    """Report each pair of instructions one view attaches whose directives contradict, view by view.

    Args:
        instructions: Every custom instruction, by casefolded name.
        view_instructions: The casefolded names each view attaches, by view key.

    Diagnostics:
        SST-VAL410: when one block directs, in either channel, what another forbids.
    """
    diagnostics: list[Diagnostic] = []
    for view_key, names in view_instructions.items():
        attached = [instructions[name] for name in sorted(names) if name in instructions]
        for first, second in combinations(attached, 2):
            if contradicts(_text(first), _text(second)):
                diagnostics.append(
                    D(
                        "SST-VAL410",
                        artifact=view_key,
                        a=first.name,
                        b=second.name,
                        subject=view_key,
                        origin=second.origin,
                        related=tuple(item.origin for item in (first,) if item.origin is not None),
                    )
                )
    return tuple(diagnostics)


def _text(instruction: InstructionDef) -> str:
    return "\n".join(text for text in (instruction.ai_sql_generation, instruction.ai_question_categorization) if text)

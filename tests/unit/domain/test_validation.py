"""The validator building blocks: Emitter, duplicates, and the two name policies."""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.validation import (
    PROFILE_NAMES,
    SKILL_NAMES,
    Emitter,
    NamePolicy,
    duplicates,
)

ORIGIN = Origin("skills/a/SKILL.md", 1, 1)
OTHER = Origin("skills/a/SKILL.md", 9, 3)


def test_an_emitter_binds_subject_origin_and_defaults_once() -> None:
    emit = Emitter(subject="skill:a", origin=ORIGIN, artifact="skill:a")
    first = emit("SST-VAL801", detail="bad name")
    second = emit("SST-VAL801", origin=OTHER, artifact="skill:b", detail="bad again")
    assert emit.diagnostics == (first, second)
    assert (first.subject, first.origin, first.context["artifact"]) == ("skill:a", ORIGIN, "skill:a")
    # A per-call origin or context value wins for that call only.
    assert (second.origin, second.context["artifact"]) == (OTHER, "skill:b")
    assert emit("SST-VAL801", detail="third").origin == ORIGIN


def test_an_emitter_without_defaults_emits_plain_diagnostics() -> None:
    emit = Emitter()
    diagnostic = emit("SST-LOD003", file="empty.yml")
    assert (diagnostic.subject, diagnostic.origin) == (None, None)
    assert emit.diagnostics == (diagnostic,)


def test_duplicates_pair_each_repeat_with_the_first_in_input_order() -> None:
    names = ["a", "B", "c", "b", "A", "a"]
    assert duplicates(names, str.casefold) == [("B", "b"), ("a", "A"), ("a", "a")]
    assert duplicates([], str.casefold) == []


def test_name_policies_state_each_rule_once() -> None:
    assert SKILL_NAMES.problem("sales-toolkit") is None
    assert SKILL_NAMES.problem("sales_toolkit") == "lowercase kebab-case of at most 64 characters"
    assert SKILL_NAMES.problem("a" * 65) is not None
    assert SKILL_NAMES.problem("a" * 64) is None
    assert PROFILE_NAMES.problem("data_analyst") is None
    assert PROFILE_NAMES.problem("shared") == "lowercase letters, digits, '-' or '_', and not 'shared'"
    assert PROFILE_NAMES.problem("Analyst") is not None
    unlimited = NamePolicy(re.compile(r"[a-z]+"), None, "lowercase letters")
    assert unlimited.problem("a" * 500) is None

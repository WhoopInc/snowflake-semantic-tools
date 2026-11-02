"""A per-target value of `sst_config.yml` read as structure: text, one `var()`, or a conditional.

`domain.resolve.config` resolves a value for the one target a run is for; an editor needs the
value itself -- its arms, what each tests, what each target gets, and which targets get the same
-- so it can show "SEMANTIC_VIEWS on dev, prod; the target's own on scratch, studio" instead of the
template, and write a change back in the grammar the resolver reads. The patterns are this
module's, and `config` reads them from here, so the two can never read a value differently.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum

LITERAL = r"'([^']*)'"
# One or more quoted names in parentheses or brackets, a trailing comma allowed: `('a',)`, `['a', 'b']`.
NAMES = r"(\(\s*'[^']*'(?:\s*,\s*'[^']*')*\s*,?\s*\)|\[\s*'[^']*'(?:\s*,\s*'[^']*')*\s*,?\s*\])"
# A list variable as a membership arm's operand: `var('production_targets')`.
NAMES_VAR = r"var\(\s*'([^']+)'\s*\)"
ARM = re.compile(
    rf"\s*{LITERAL}\s+if\s+target\.name\s*(?:(==|!=)\s*{LITERAL}|(not\s+in|in)\s*(?:{NAMES}|{NAMES_VAR}))\s*(?:else\b|$)"
)
QUOTED = re.compile(LITERAL)
FALLBACK = re.compile(rf"\s*{LITERAL}\s*$")
# One template, so `{{ a }} and {{ b }}` is not read as one conditional.
CONDITIONAL = re.compile(r"^\s*\{\{(?P<body>(?:(?!\}\}).)*\bif\s+target\.name\b(?:(?!\}\}).)*)\}\}\s*$", re.DOTALL)
# A target conditional with other text beside it, which never resolves.
EMBEDDED = re.compile(r"\{\{(?:(?!\}\}).)*\bif\s+target\.name\b", re.DOTALL)
VAR = re.compile(r"\{\{\s*var\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")


class ValueForm(Enum):
    """How a value is written: the same for every target, one variable, a conditional, or unreadable.

    `UNREADABLE` is a conditional the grammar does not cover, or one beside other text: an editor
    shows it as written and never rewrites it.
    """

    PLAIN = "plain"
    VAR = "var"
    CONDITIONAL = "conditional"
    UNREADABLE = "unreadable"


@dataclass(frozen=True, slots=True)
class ArmTest:
    """What one arm tests the target's name against.

    Attributes:
        op: `==`, `!=`, `in` or `not in`.
        names: The names an `==`/`!=` arm names (one) or an `in` arm lists; empty for a variable.
        variable: The `vars:` list an `in` arm reads; None when it lists its names.
        bracket: How a listed `in` arm writes its names: `(` for a tuple, `[` for a list; empty otherwise.
    """

    op: str
    names: tuple[str, ...] = ()
    variable: str | None = None
    bracket: str = ""


@dataclass(frozen=True, slots=True)
class Arm:
    """One arm of a conditional.

    Attributes:
        value: What a target the test holds for gets.
        test: What the target's name is tested against.
    """

    value: str
    test: ArmTest


@dataclass(frozen=True, slots=True)
class TargetValue:
    """A value as the grammar reads it.

    Attributes:
        form: How it is written.
        text: The value as written.
        variable: The variable a `VAR` value is; None for any other form.
        arms: A conditional's arms, in order; empty for any other form.
        otherwise: A conditional's final `else`; None when it has none, or for any other form.
    """

    form: ValueForm
    text: str
    variable: str | None = None
    arms: tuple[Arm, ...] = ()
    otherwise: str | None = None


@dataclass(frozen=True, slots=True)
class Resolved:
    """What a value resolves to for one target, or why it does not.

    Attributes:
        value: The text the target gets; None when it does not resolve.
        chosen: Whether a conditional chose it, rather than every target getting it.
        problem: Why it does not resolve, as SST's diagnostic says it; None when it does.
    """

    value: str | None
    chosen: bool = False
    problem: str | None = None


@dataclass(frozen=True, slots=True)
class TargetGroup:
    """Targets a value resolves alike for, named as a person would read them.

    Attributes:
        label: The `vars:` list that lists exactly these targets, else their names joined by `, `.
        targets: The targets, in the order the profile declares them.
        resolved: What each of them gets.
    """

    label: str
    targets: tuple[str, ...]
    resolved: Resolved


def parse_target_value(text: str) -> TargetValue:
    """Read a value as `domain.resolve.config` reads it, without resolving it."""
    whole = VAR.fullmatch(text.strip())
    if whole is not None:
        return TargetValue(ValueForm.VAR, text, variable=whole.group(1))
    match = CONDITIONAL.match(text)
    if match is None:
        return TargetValue(ValueForm.UNREADABLE if EMBEDDED.search(text) else ValueForm.PLAIN, text)
    body = match.group("body")
    arms: list[Arm] = []
    position = 0
    while (arm := ARM.match(body, position)) is not None:
        literal, operator, name, membership, names, variable = arm.groups()
        if operator is not None:
            test = ArmTest(operator, names=(name,))
        elif names is not None:
            test = ArmTest(" ".join(membership.split()), names=tuple(QUOTED.findall(names)), bracket=names[0])
        else:
            test = ArmTest(" ".join(membership.split()), variable=variable)
        arms.append(Arm(literal, test))
        position = arm.end()
    fallback = FALLBACK.match(body, position)
    if not arms or (position < len(body.rstrip()) and fallback is None):
        return TargetValue(ValueForm.UNREADABLE, text)
    return TargetValue(
        ValueForm.CONDITIONAL, text, arms=tuple(arms), otherwise=None if fallback is None else fallback.group(1)
    )


def format_target_value(value: TargetValue) -> str:
    """Write a value as `sst_config.yml` spells it: a conditional's arms in order, then its final else.

    A plain or unreadable value is its text; formatting what `parse_target_value` read gives back a
    value that reads the same.
    """
    if value.form is ValueForm.VAR:
        return f"{{{{ var('{value.variable}') }}}}"
    if value.form is not ValueForm.CONDITIONAL:
        return value.text
    arms = " else ".join(f"'{arm.value}' if target.name {_test(arm.test)}" for arm in value.arms)
    otherwise = "" if value.otherwise is None else f" else '{value.otherwise}'"
    return f"{{{{ {arms}{otherwise} }}}}"


def _test(test: ArmTest) -> str:
    if test.variable is not None:
        return f"{test.op} var('{test.variable}')"
    if test.op in ("==", "!="):
        return f"{test.op} '{test.names[0]}'"
    names = ", ".join(f"'{name}'" for name in test.names)
    if test.bracket == "[":
        return f"{test.op} [{names}]"
    return f"{test.op} ({names}{',' if len(test.names) == 1 else ''})"


def target_list(name: str, variables: Mapping[str, object]) -> tuple[str, ...] | None:
    """Return the targets a `vars:` list names; None when it is not a non-empty list of names."""
    held = variables.get(name)
    if isinstance(held, list) and held and all(isinstance(item, str) and item for item in held):
        return tuple(held)
    return None


def resolve_for(text: str, target: str, variables: Mapping[str, object]) -> Resolved:
    """Return what `text` resolves to for `target`, as a run for that target resolves it.

    `var()` is substituted as text first, then a conditional chooses; any other template is left
    as written.
    """
    problem: list[str] = []

    def substitute(match: re.Match[str]) -> str:
        held = variables.get(match.group(1))
        if match.group(1) not in variables:
            problem.append(f"vars: declares no {match.group(1)}")
        elif held is None or held == "":
            problem.append(f"{match.group(1)} is empty")
        elif isinstance(held, (list, Mapping)):
            problem.append(f"{match.group(1)} holds {'a list' if isinstance(held, list) else 'a mapping'}, not text")
        else:
            return str(held)
        return match.group(0)

    rendered = VAR.sub(substitute, text)
    if problem:
        return Resolved(None, problem=problem[0])
    value = parse_target_value(rendered)
    if value.form is ValueForm.UNREADABLE:
        return Resolved(None, problem="the conditional is not one SST reads")
    if value.form is not ValueForm.CONDITIONAL:
        return Resolved(rendered)
    for arm in value.arms:
        names = arm.test.names if arm.test.variable is None else target_list(arm.test.variable, variables)
        if names is None:
            return Resolved(None, problem=f"{arm.test.variable} is not a list of target names")
        if (target in names) is (arm.test.op in ("==", "in")):
            return Resolved(arm.value, chosen=True)
    if value.otherwise is not None:
        return Resolved(value.otherwise, chosen=True)
    return Resolved(None, problem=f"no arm is for {target}, and there is no final else")


def target_groups(
    text: str, targets: Sequence[str], lists: Mapping[str, Sequence[str]], variables: Mapping[str, object]
) -> tuple[TargetGroup, ...]:
    """Return the targets grouped by what `text` resolves to, in the order each group first appears.

    A group whose targets are exactly those a `vars:` list in `lists` names takes that list's name,
    so a value reads as "production_targets" rather than the names it lists.
    """
    groups: dict[Resolved, list[str]] = {}
    for target in targets:
        groups.setdefault(resolve_for(text, target, variables), []).append(target)
    named = {frozenset(names): name for name, names in lists.items()}
    return tuple(
        TargetGroup(named.get(frozenset(members), ", ".join(members)), tuple(members), resolved)
        for resolved, members in groups.items()
    )

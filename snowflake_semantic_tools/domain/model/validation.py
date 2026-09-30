"""Small building blocks the domain validators share: an emitter, duplicate detection, name rules.

Validators report problems as `Diagnostic` values, most of them for one subject at one
origin. `Emitter` binds those once so each check names only its code and context.
`duplicates` finds repeated names in a single pass, keeping first-seen order so
diagnostics stay in authoring order. `NamePolicy` states a naming rule once, with the
exact wording its diagnostic uses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Hashable, Iterable, TypeVar

from .diagnostic import D, Diagnostic, Origin

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)


class Emitter:
    """Collect diagnostics that share a subject, origin and context defaults.

    Example:
        ``emit = Emitter(subject="skill:a", origin=origin, artifact="skill:a")`` then
        ``emit("SST-VAL801", detail="...")`` appends one diagnostic; ``emit.diagnostics``
        returns everything emitted, in order.
    """

    def __init__(self, *, subject: str | None = None, origin: Origin | None = None, **defaults: Any) -> None:
        self._subject = subject
        self._origin = origin
        self._defaults = defaults
        self._emitted: list[Diagnostic] = []

    def __call__(self, code: str, *, origin: Origin | None = None, **context: Any) -> Diagnostic:
        """Record one diagnostic; `origin` and context override the emitter's defaults for this call only."""
        diagnostic = D(
            code,
            origin=origin if origin is not None else self._origin,
            subject=self._subject,
            **{**self._defaults, **context},
        )
        self._emitted.append(diagnostic)
        return diagnostic

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Everything emitted so far, in emission order."""
        return tuple(self._emitted)


def duplicates(items: Iterable[T], key: Callable[[T], K]) -> list[tuple[T, T]]:
    """Return (first, repeat) for every item whose key an earlier item already had.

    One pass, in input order; the third copy of a key pairs with the first, not the second.
    """
    first: dict[K, T] = {}
    repeats: list[tuple[T, T]] = []
    for item in items:
        value = key(item)
        if value in first:
            repeats.append((first[value], item))
        else:
            first[value] = item
    return repeats


@dataclass(frozen=True, slots=True)
class NamePolicy:
    """One naming rule: the pattern a name must match, its length limit, and names it may not take.

    Attributes:
        pattern: Anchored regular expression the whole name must match.
        max_length: Longest allowed name, or None for no limit.
        reserved: Names the pattern accepts but that are not allowed.
        description: How the rule reads in a diagnostic, e.g. "lowercase kebab-case".
    """

    pattern: re.Pattern[str]
    max_length: int | None
    description: str
    reserved: frozenset[str] = frozenset()

    def problem(self, name: str) -> str | None:
        """Return why `name` breaks this rule, in the policy's own words, or None when it complies."""
        too_long = self.max_length is not None and len(name) > self.max_length
        if name in self.reserved or too_long or not self.pattern.fullmatch(name):
            return self.description
        return None


# Skills and plugins publish as extension folders: lowercase kebab-case, at most 64 characters.
SKILL_NAMES = NamePolicy(re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*"), 64, "lowercase kebab-case of at most 64 characters")
# Profiles become CoCo Desktop profile names, which also allow '_'; 'shared' names the common layer.
PROFILE_NAMES = NamePolicy(
    re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)*"),
    None,
    "lowercase letters, digits, '-' or '_', and not 'shared'",
    frozenset(("shared",)),
)

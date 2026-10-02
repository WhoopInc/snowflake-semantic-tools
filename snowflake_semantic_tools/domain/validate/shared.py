"""Small building blocks the domain validators share, and the rules every artifact type shares.

Validators report problems as `Diagnostic` values, most of them for one subject at one
origin. `Emitter` binds those once so each check names only its code and context.
`duplicates` finds repeated names in a single pass, keeping first-seen order so
diagnostics stay in authoring order. `NamePolicy` states a naming rule once, with the
exact wording its diagnostic uses. The rest are the shared rules: a description that never
says when to use its object, a reference cycle, a name taken in another namespace, keys
rendered without being modelled, and a comparison of values that were never normalised.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin

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


# Words a description uses to say when its object applies: "use when", "invoke for", "if".
_INVOCATION = re.compile(r"\b(?:use|uses|using|invoke|invoked|call|ask|asks|when|whenever|if|for questions)\b", re.I)


def lacks_invocation(description: str | None) -> bool:
    """Report whether a routed object's description says what it is and never when to use it.

    The description is the only text an agent matches a routed object by, so one with no
    invocation cue is never chosen; SST-VAL005 reports it. An absent description is not this
    rule's: SST-VAL003 reports that.
    """
    return bool(description and description.strip()) and _INVOCATION.search(description or "") is None


def reference_cycle(edges: Mapping[str, Iterable[str]]) -> tuple[str, ...]:
    """Return the first cycle among `edges`, its first node repeated at the end; empty when acyclic.

    Nodes are walked in sorted order and each node's targets in sorted order, so the same cycle
    is returned on every run. A target that is not itself a node closes no cycle.
    """
    graph = {node: tuple(sorted(targets)) for node, targets in edges.items()}
    done: set[str] = set()
    path: list[str] = []

    def walk(node: str) -> tuple[str, ...]:
        if node in path:
            return (*path[path.index(node) :], node)
        if node in done or node not in graph:
            return ()
        path.append(node)
        for target in graph[node]:
            found = walk(target)
            if found:
                return found
        path.pop()
        done.add(node)
        return ()

    for node in sorted(graph):
        found = walk(node)
        if found:
            return found
    return ()


def namespace_collisions(declared: Iterable[tuple[str, str, str]], others: Mapping[str, str]) -> tuple[Diagnostic, ...]:
    """Report each declared name another namespace already holds, in declaration order.

    Args:
        declared: Each declaration as its subject, type and name.
        others: The names the other namespace holds, casefolded, mapped to how a diagnostic
            names the holder.

    Diagnostics:
        SST-VAL002: a declared name is held in the other namespace.
    """
    return tuple(
        D("SST-VAL002", subject=subject, type=type_name, name=name, other=others[name.casefold()])
        for subject, type_name, name in declared
        if name.casefold() in others
    )


def unmodelled_key_diagnostics(
    type_name: str, name: str, keys: Iterable[str], *, allow: bool, subject: str | None = None
) -> tuple[Diagnostic, ...]:
    """Report the keys an artifact renders without SST modelling them.

    Args:
        allow: `snowflake.allow_unknown_keys`: True reports all of them in one warning; False
            makes each an error.

    Diagnostics:
        SST-VAL014: with `allow`, the artifact renders one or more unmodelled keys.
        SST-VAL013: without `allow`, once per unmodelled key.
    """
    found = sorted(dict.fromkeys(keys))
    if not found:
        return ()
    if allow:
        return (D("SST-VAL014", subject=subject, type=type_name, name=name, count=len(found)),)
    return tuple(D("SST-VAL013", subject=subject, type=type_name, name=name, key=key) for key in found)


_DIGEST = re.compile(r"[0-9a-f]{64}")


def is_digest(value: str) -> bool:
    """Report whether `value` is a SHA-256 digest in lowercase hex: the form a normalised definition takes."""
    return _DIGEST.fullmatch(value) is not None

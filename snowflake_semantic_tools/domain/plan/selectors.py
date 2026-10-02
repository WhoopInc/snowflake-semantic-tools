"""Resolve `--select` and `--exclude` selectors into the artifact types and keys they name.

The grammar is closed: a name (with `*` and `?` globs), `type:<type>`, `path:<glob>`,
`state:<state>`, or an artifact key `<type>:<name>`. Anything else is refused rather than read
as a literal name, because a selector that silently matches nothing turns a prune or a rollback
into a run that reports success having done nothing. A comma is refused outright: union is
spelled with spaces, and intersection is not supported. Names, types, and states compare
casefolded; a path glob compares against the project-relative POSIX paths an artifact was
compiled from.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key

# The selector kinds the grammar registers, in the order a refusal lists them.
SELECTOR_KINDS = ("type", "path", "tag", "state", "model", "source")
STATES = ("new", "modified", "unmodified", "orphaned")
_GLOB = frozenset("*?[")


@dataclass(frozen=True, slots=True)
class Selectable:
    """One artifact a selector can name, as the compiled manifest records it.

    Attributes:
        name: The artifact's name, casefolded.
        source_files: The project-relative POSIX paths the artifact was compiled from.
    """

    key: str
    type: str
    name: str
    fingerprint: str
    source_files: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Selection:
    """The types and keys a list of selectors names; None for each part they leave unnamed."""

    types: frozenset[str] | None
    keys: frozenset[str] | None


def resolve_selectors(
    values: Iterable[str],
    *,
    artifact_types: Iterable[str],
    universe: Iterable[Selectable] = (),
    previous: Mapping[str, str] | None = None,
) -> Selection | Diagnostic:
    """Resolve selectors against the artifacts there are, or return the refusal of the first that fails.

    A bare name without a glob names the semantic view of that name when no compiled artifact
    carries it, so an orphan whose source was deleted can still be selected for a prune.

    Args:
        artifact_types: Every registered artifact type, which `type:` and `<type>:<name>` name.
        universe: The compiled artifacts, which names, paths, and states are resolved against.
        previous: Each artifact key's fingerprint in the previous run's manifest, from
            `--state`; None when no state was given.

    Diagnostics:
        SST-PRT101: a selector contains a comma.
        SST-PRT102: a selector's prefix is not a registered kind, or globs a fixed vocabulary.
        SST-PRT103: a `state:` selector was given without `--state`.
        SST-PRT100: a registered kind names an unknown type or state, or is not supported here.
    """
    known_types = frozenset(artifact_types)
    artifacts = tuple(universe)
    types: set[str] = set()
    keys: set[str] = set()
    for value in values:
        resolved = _resolve_one(value, known_types, artifacts, previous)
        if isinstance(resolved, Diagnostic):
            return resolved
        kind, names = resolved
        (types if kind == "type" else keys).update(names)
    return Selection(frozenset(types) or None, frozenset(keys) or None)


def _resolve_one(
    value: str,
    known_types: frozenset[str],
    artifacts: tuple[Selectable, ...],
    previous: Mapping[str, str] | None,
) -> tuple[str, frozenset[str]] | Diagnostic:
    """Resolve one selector to `("type", types)` or `("key", keys)`, or return its refusal."""
    if "," in value:
        return D("SST-PRT101", subject="cli", value=value)
    if value.startswith("+") or value.endswith("+"):
        return _unsupported(value, "graph operators (+name, name+) are not supported in this release")
    prefix, separator, rest = value.partition(":")
    if not separator:
        return "key", _names(value.casefold(), artifacts)
    kind = prefix.casefold()
    if kind in known_types and _GLOB & set(rest):
        # Only a bare name takes a glob; `tool:*` is `type:tool`.
        return D("SST-PRT102", subject="cli", value=value)
    if kind in known_types:
        if not rest:
            return _unsupported(value, f"{prefix}: names no artifact")
        return "key", frozenset((artifact_key(kind, rest.casefold()),))
    if kind not in SELECTOR_KINDS or (kind in ("type", "state", "model", "source") and _GLOB & set(rest)):
        return D("SST-PRT102", subject="cli", value=value)
    if kind == "type":
        if rest.casefold() not in known_types:
            return _unsupported(value, f"unknown artifact type '{rest}'; one of {', '.join(sorted(known_types))}")
        return "type", frozenset((rest.casefold(),))
    if kind == "path":
        return "key", frozenset(
            item.key for item in artifacts if any(fnmatchcase(path, rest) for path in item.source_files)
        )
    if kind == "state":
        return _state(value, rest.casefold(), artifacts, previous)
    return _unsupported(value, f"{kind}: selectors are not supported by this command")


def _names(pattern: str, artifacts: tuple[Selectable, ...]) -> frozenset[str]:
    """Return the keys of every artifact whose name matches; a plain name falls back to a view's key."""
    matched = frozenset(item.key for item in artifacts if fnmatchcase(item.name, pattern))
    if matched or _GLOB & set(pattern):
        return matched
    return frozenset((artifact_key("semantic_view", pattern),))


def _state(
    value: str, state: str, artifacts: tuple[Selectable, ...], previous: Mapping[str, str] | None
) -> tuple[str, frozenset[str]] | Diagnostic:
    """Resolve `state:<state>` by comparing the compiled artifacts with the previous run's."""
    if previous is None:
        return D("SST-PRT103", subject="cli", value=value)
    if state not in STATES:
        return _unsupported(value, f"unknown state '{state}'; one of {', '.join(STATES)}")
    current = {item.key: item.fingerprint for item in artifacts}
    if state == "orphaned":
        return "key", frozenset(key for key in previous if key not in current)
    if state == "new":
        return "key", frozenset(key for key in current if key not in previous)
    unchanged = frozenset(key for key, fingerprint in current.items() if previous.get(key) == fingerprint)
    if state == "unmodified":
        return "key", unchanged
    return "key", frozenset(key for key in current if key in previous and key not in unchanged)


def _unsupported(value: str, detail: str) -> Diagnostic:
    return D("SST-PRT100", subject="cli", detail=f"selector '{value}': {detail}")

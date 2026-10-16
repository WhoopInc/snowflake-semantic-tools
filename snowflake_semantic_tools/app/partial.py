"""Split a compile result for `--partial`: artifacts that can publish, and those that cannot.

An artifact is healthy when no error names it or anything it contains, and
everything it depends on is healthy. An error on a member reference names the
artifact the member belongs to: a tool member's (`tool_ref:<name>`) is its tool's.
An error SST cannot pin to the artifacts it would change leaves nothing safe to
publish, so there is no split:

- configuration, `profiles.yml`, or a file that could not be attributed;
- a semantic view member, such as a metric: a failing member is dropped from
  every view it would join, so those views would publish without it.

What plan blocks against the target -- an object changed out of band, one SST
does not manage -- is left out of a partial plan the same way, by
`partial_changeset`, once the plan is decided.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.lifecycle import ChangeSet
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan.changeset import narrowed

# Inputs only a profile carries: an error in one blocks the profiles that list it.
PROFILE_INPUTS = frozenset(("command", "hook", "mcp"))
# A member reference's prefix, by the type of the artifact the member belongs to.
_MEMBER_OWNERS = {"tool_ref": "tool"}


@dataclass(frozen=True, slots=True)
class PartialSplit:
    """A compile result split for `--partial`: what can publish, and what is left out.

    Attributes:
        healthy: The healthy artifacts, in compile order, with every diagnostic of the whole
            result, the left-out artifacts' errors included, so it fails whenever the result did.
        excluded: Every subject an error names and every artifact left out, each once whatever
            its case, sorted casefolded.
    """

    healthy: CompileResult
    excluded: tuple[str, ...]

    @property
    def notices(self) -> DiagnosticBag:
        """Return one notice per excluded key, in `excluded` order.

        Diagnostics:
            SST-PLN032: the key is left out of this partial run.
        """
        return DiagnosticBag(tuple(D("SST-PLN032", subject=key, artifact=key) for key in self.excluded))


def _owner(subject: str | None) -> str | None:
    """Return the key an error's subject stands for: a member reference's artifact, else the subject."""
    if subject and ":" in subject:
        prefix, name = split_artifact_key(subject)
        owner = _MEMBER_OWNERS.get(prefix)
        if owner is not None:
            return f"{owner}:{name}"
    return subject


def _attributable(subject: str | None) -> bool:
    owner = _owner(subject)
    prefix = split_artifact_key(owner)[0] if owner and ":" in owner else ""
    return prefix in SEMANTIC_REGISTRY.artifacts or prefix in PROFILE_INPUTS


def partial_refusal(result: CompileResult) -> Diagnostic | None:
    """Why `--partial` cannot split this result, naming the first error it cannot place."""
    for item in result.diagnostics:
        if item.blocks and not _attributable(item.subject):
            return D("SST-PLN033", value=item.subject or "no artifact", found=item.code)
    return None


def partial_split(result: CompileResult) -> PartialSplit | None:
    """Split a compile result into the artifacts `--partial` may publish and those it leaves out.

    An artifact is healthy when no error names it or anything it contains, and when everything
    it depends on, and everything it contains that publishes on its own, is healthy too. Keys
    compare casefolded, and only errors count: a warning leaves out nothing.

    Returns:
        The split; None when an error cannot be traced to the artifacts it would change, which
        `partial_refusal` reports.
    """
    errors = [item for item in result.diagnostics if item.blocks]
    if any(not _attributable(item.subject) for item in errors):
        return None
    failed = {str(_owner(item.subject)).casefold() for item in errors}
    by_key = {item.artifact_key.casefold(): item for item in result.compiled}

    def contained(item: object) -> tuple[str, ...]:
        return tuple(key.casefold() for key in getattr(item, "contained_keys", ()))

    healthy = {key for key, item in by_key.items() if key not in failed and not failed.intersection(contained(item))}
    changed = True
    while changed:
        changed = False
        for key in sorted(healthy):
            item = by_key[key]
            # What an artifact depends on, or carries and also publishes on its own
            # (a plugin in a profile), must be healthy too.
            needed = (
                *(dependency.casefold() for dependency in item.rendered_artifact.depends_on),
                *(held for held in contained(item) if held in by_key),
            )
            if any(dependency not in healthy for dependency in needed):
                healthy.discard(key)
                changed = True
    names = {key: item.artifact_key for key, item in by_key.items()}
    excluded: dict[str, str] = {}
    for name in (*(str(_owner(item.subject)) for item in errors), *(names[key] for key in set(by_key) - healthy)):
        excluded.setdefault(name.casefold(), name)
    kept = tuple(item for item in result.compiled if item.artifact_key.casefold() in healthy)
    return PartialSplit(CompileResult(kept, result.diagnostics), tuple(sorted(excluded.values(), key=str.casefold)))


def partial_changeset(changeset: ChangeSet) -> tuple[ChangeSet, tuple[Diagnostic, ...]]:
    """Leave every blocked change out of a partial plan; return the plan, and what it left out.

    A change plan blocked against the target, such as an object changed out of band or one SST
    does not manage, is left out as an artifact with errors is; the changes that depend on it
    are blocked already, so they are left out too. Its diagnostics move from the plan to what
    the run left out, so apply goes ahead with the rest while the run still exits 1.

    Returns:
        The plan without its blocked changes or their diagnostics, its id hashed anew; then
        those diagnostics, and one SST-PLN032 per change left out, in plan order.

    Diagnostics:
        SST-PLN032: a blocked change is left out of the partial plan.
    """
    blocked = changeset.blocked
    if not blocked:
        return changeset, ()
    keys = {change.key for change in blocked}
    owned = [item for change in blocked for item in change.diagnostics]

    def left_out(item: Diagnostic) -> bool:
        return item.subject in keys or item in owned

    moved = [item for item in changeset.diagnostics if left_out(item)]
    moved.extend(item for item in owned if item not in moved)
    kept = DiagnosticBag(tuple(item for item in changeset.diagnostics if not left_out(item)))
    remaining = tuple(change for change in changeset.changes if change.key not in keys)
    notices = tuple(D("SST-PLN032", subject=change.key, artifact=change.key) for change in blocked)
    return narrowed(changeset, remaining, kept), (*moved, *notices)

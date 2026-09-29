"""Split a compile result for `--partial`: artifacts that can publish, and those that cannot.

An artifact is healthy when no error names it or anything it contains, and
everything it depends on is healthy. An error SST cannot pin to the artifacts it
would change leaves nothing safe to publish, so there is no split:

- configuration, `profiles.yml`, or a file that could not be attributed;
- a semantic view member, such as a metric: a failing member is dropped from
  every view it would join, so those views would publish without it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Severity
from ..domain.model.registry import SEMANTIC_REGISTRY
from .compile import CompileResult

# Inputs only a profile carries: an error in one blocks the profiles that list it.
PROFILE_INPUTS = frozenset(("command", "hook", "mcp"))


@dataclass(frozen=True, slots=True)
class PartialSplit:
    healthy: CompileResult
    excluded: tuple[str, ...]

    @property
    def notices(self) -> DiagnosticBag:
        return DiagnosticBag(tuple(D("SST-PLN032", subject=key, artifact=key) for key in self.excluded))


def _attributable(subject: str | None) -> bool:
    prefix = subject.split(":", 1)[0] if subject and ":" in subject else ""
    return prefix in SEMANTIC_REGISTRY.artifacts or prefix in PROFILE_INPUTS


def partial_refusal(result: CompileResult) -> Diagnostic | None:
    """Why `--partial` cannot split this result, naming the first error it cannot place."""
    for item in result.diagnostics:
        if item.severity is Severity.ERROR and not _attributable(item.subject):
            return D("SST-PLN033", value=item.subject or "no artifact", found=item.code)
    return None


def partial_split(result: CompileResult) -> PartialSplit | None:
    errors = [item for item in result.diagnostics if item.severity is Severity.ERROR]
    if any(not _attributable(item.subject) for item in errors):
        return None
    failed = {str(item.subject).casefold() for item in errors}
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
    for name in (*(str(item.subject) for item in errors), *(names[key] for key in set(by_key) - healthy)):
        excluded.setdefault(name.casefold(), name)
    kept = tuple(item for item in result.compiled if item.artifact_key.casefold() in healthy)
    return PartialSplit(CompileResult(kept, result.diagnostics), tuple(sorted(excluded.values(), key=str.casefold)))

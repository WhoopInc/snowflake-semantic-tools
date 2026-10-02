"""Match a run's diagnostics against a committed baseline, and report the baseline's expiry.

A baseline records the warnings a project knows about and has not fixed. A diagnostic whose
stable fingerprint is recorded is baselined: still counted and still in the JSON envelope, but
neither rendered by default nor blocking. Only a demotable code below ERROR can be baselined, so
a baseline suppresses and never demotes. Past its `expires_on` a baseline matches nothing and
every diagnostic it held blocks again. Dates are ISO `YYYY-MM-DD` text, compared as text, so this
module needs no clock: the caller passes today's date and the date 30 days from it.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import blake2b

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key


@dataclass(frozen=True, slots=True)
class BaselineEntry:
    """One recorded diagnostic.

    Attributes:
        fingerprint: The diagnostic's `stable_fingerprint`, which is all matching reads.
        artifact, file, note: Recorded for a reviewer; matching ignores them.
    """

    fingerprint: str
    code: str
    artifact: str = ""
    file: str = ""
    note: str = ""


@dataclass(frozen=True, slots=True)
class Baseline:
    """A baseline file as read.

    Attributes:
        path: How diagnostics about the file name it.
        expires_on: The ISO date after which the baseline matches nothing.
    """

    path: str
    expires_on: str
    entries: tuple[BaselineEntry, ...]


@dataclass(frozen=True, slots=True)
class BaselineMatch:
    """What a baseline did to one run's diagnostics.

    Attributes:
        baselined: The stable fingerprints of the diagnostics the baseline holds.
        notices: SST-CFG035, SST-CFG039 or SST-INT009 about the baseline itself; empty when none
            applies.
    """

    baselined: frozenset[str]
    notices: tuple[Diagnostic, ...]


def stable_fingerprint(diagnostic: Diagnostic) -> str:
    """Return the 16-hex identity baselining keys on, stable across a reformat that moves a line.

    It hashes the code, the artifact and member the subject names, the file, and the context,
    and never the line or column, so inserting a comment above a problem keeps its identity
    while renaming the artifact changes it.
    """
    subject = diagnostic.subject or ""
    kind, name = split_artifact_key(subject) if ":" in subject else ("", subject)
    params = "\x1e".join(f"{key}={diagnostic.context[key]}" for key in sorted(diagnostic.context))
    parts = (diagnostic.code, kind, name, diagnostic.origin.file if diagnostic.origin else "", params)
    return blake2b("\x00".join(parts).encode("utf-8"), digest_size=8).hexdigest()


def baselinable(code: str) -> bool:
    """Report whether a code may be baselined: registered below ERROR, and demotable."""
    spec = ERROR_REGISTRY.get(code)
    return spec is not None and spec.severity is not Severity.ERROR and spec.demotable


def match_baseline(
    diagnostics: Iterable[Diagnostic], baseline: Baseline | None, *, today: str, warn_from: str
) -> BaselineMatch:
    """Return which diagnostics `baseline` holds, and what to report about the baseline itself.

    Args:
        today: Today's ISO date; a baseline whose `expires_on` is before it has expired.
        warn_from: The ISO date 30 days from today; a baseline expiring by then is nearing expiry.

    Diagnostics:
        SST-CFG039: the baseline is past its expiry, so nothing is baselined.
        SST-CFG035: the baseline expires within 30 days.
        SST-INT009: an entry matches more than one diagnostic, so it baselines none of them;
            one per entry, in file order.
    """
    if baseline is None:
        return BaselineMatch(frozenset(), ())
    origin = Origin(baseline.path)
    count = len(baseline.entries)
    subject = f"config:{baseline.path}"
    if baseline.expires_on < today:
        expired = D("SST-CFG039", origin=origin, subject=subject, date=baseline.expires_on, count=count)
        return BaselineMatch(frozenset(), (expired,))
    recorded = tuple(dict.fromkeys(entry.fingerprint for entry in baseline.entries if baselinable(entry.code)))
    matched = Counter(stable_fingerprint(item) for item in diagnostics if baselinable(item.code))
    baselined = frozenset(fingerprint for fingerprint in recorded if matched[fingerprint] == 1)
    notices: tuple[Diagnostic, ...] = ()
    if baseline.expires_on <= warn_from:
        notices = (D("SST-CFG035", origin=origin, subject=subject, date=baseline.expires_on, count=count),)
    ambiguous = tuple(
        D("SST-INT009", subject=subject, value=fingerprint, count=matched[fingerprint])
        for fingerprint in recorded
        if matched[fingerprint] > 1
    )
    return BaselineMatch(baselined, (*notices, *ambiguous))

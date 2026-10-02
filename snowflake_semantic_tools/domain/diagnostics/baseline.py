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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
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
class Renewal:
    """One `sst baseline renew`: when it ran, why, and the expiry it set; the file's audit record."""

    renewed_on: str
    reason: str
    expires_on: str


@dataclass(frozen=True, slots=True)
class Baseline:
    """A baseline file as read or about to be written.

    Attributes:
        path: How diagnostics about the file name it.
        expires_on: The ISO date after which the baseline matches nothing.
        generated_at: When the file was first written, as recorded; empty when it does not say.
        generated_by: The SST version that first wrote it; empty when it does not say.
        renewals: Every renewal, oldest first.
    """

    path: str
    expires_on: str
    entries: tuple[BaselineEntry, ...]
    generated_at: str = ""
    generated_by: str = ""
    renewals: tuple[Renewal, ...] = ()


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


# How long a baseline lasts unless told otherwise, and the longest it may be told.
DEFAULT_EXPIRY_DAYS = 180
MAX_EXPIRY_DAYS = 365


def baseline_entry(diagnostic: Diagnostic, note: str) -> BaselineEntry:
    """Record one diagnostic: its stable fingerprint, and what a reviewer reads it by."""
    origin = diagnostic.origin.file if diagnostic.origin else ""
    return BaselineEntry(stable_fingerprint(diagnostic), diagnostic.code, diagnostic.subject or "", origin, note)


def with_entries(
    baseline: Baseline, diagnostics: Iterable[Diagnostic], note: Callable[[str], str]
) -> tuple[Baseline, tuple[BaselineEntry, ...]]:
    """Return `baseline` with an entry added for each baselinable diagnostic it does not record.

    Additive only: no entry is removed or changed. A fingerprint two diagnostics share is not
    added, since an entry matching more than one diagnostic baselines none of them.

    Args:
        note: The note for an entry of a code, given the code.

    Returns:
        The baseline, and the entries added, in the order the diagnostics came.
    """
    candidates = [item for item in diagnostics if baselinable(item.code)]
    counts = Counter(stable_fingerprint(item) for item in candidates)
    recorded = {entry.fingerprint for entry in baseline.entries}
    added: list[BaselineEntry] = []
    for item in candidates:
        fingerprint = stable_fingerprint(item)
        if counts[fingerprint] == 1 and fingerprint not in recorded:
            added.append(baseline_entry(item, note(item.code)))
            recorded.add(fingerprint)
    return replace(baseline, entries=(*baseline.entries, *added)), tuple(added)


def chosen_for_baseline(diagnostics: Iterable[Diagnostic], code: str | None) -> tuple[Diagnostic, ...]:
    """Return the diagnostics `sst baseline add` records: those of `code`, else every warning."""
    return tuple(
        item for item in diagnostics if (item.code == code if code is not None else item.severity is Severity.WARNING)
    )


def without_stale(
    baseline: Baseline, diagnostics: Iterable[Diagnostic], in_scope: Callable[[BaselineEntry], bool]
) -> tuple[Baseline, tuple[BaselineEntry, ...]]:
    """Return `baseline` without the entries in scope that no current diagnostic matches.

    Returns:
        The baseline, and the entries removed, in file order.
    """
    current = {stable_fingerprint(item) for item in diagnostics}
    pruned = tuple(entry for entry in baseline.entries if in_scope(entry) and entry.fingerprint not in current)
    kept = tuple(entry for entry in baseline.entries if entry not in pruned)
    return replace(baseline, entries=kept), pruned


def renewed(baseline: Baseline, *, today: str, expires_on: str, reason: str) -> Baseline:
    """Return `baseline` re-dated to `expires_on`, with the renewal and its reason on record."""
    renewal = Renewal(today, reason, expires_on)
    return replace(baseline, expires_on=expires_on, renewals=(*baseline.renewals, renewal))


def baseline_refusal(code: str) -> str | None:
    """Return why `code` cannot be baselined; None when it can.

    A baseline suppresses and never demotes, so an error and a non-demotable code are refused.
    """
    spec = ERROR_REGISTRY[code]
    if spec.severity is Severity.ERROR:
        return f"{code} is an error; a baseline suppresses and never demotes, so use severity_overrides"
    if not spec.demotable:
        return f"{code} is non-demotable, so no baseline may hold it"
    return None

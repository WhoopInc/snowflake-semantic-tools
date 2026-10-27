"""Match a run's diagnostics against a committed baseline, and report the baseline's expiry.

A baseline records the warnings a project knows about and has not fixed. A diagnostic whose
stable fingerprint is recorded is baselined: still counted and still in the JSON envelope, but
neither rendered by default nor blocking. Only a demotable code below ERROR can be baselined, so
a baseline suppresses and never demotes. Past its `expires_on` a baseline matches nothing and
every diagnostic it held blocks again. Dates are ISO `YYYY-MM-DD` text, compared as dates, so this
module needs no clock: the caller passes today's date and the date 30 days from it.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from hashlib import blake2b

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D, Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.core import Precheck
from snowflake_semantic_tools.domain.diagnostics.specs.aliases import ALIASES, TOMBSTONES
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key

# Context keys a message may print but a stable fingerprint never hashes: where a finding sits
# in its file, which an unrelated edit above it moves.
POSITION_KEYS = frozenset({"line", "col"})


@dataclass(frozen=True, slots=True)
class BaselineEntry:
    """One recorded diagnostic.

    Attributes:
        fingerprint: The diagnostic's `stable_fingerprint`, which is all matching reads.
        artifact, file, note: Recorded for a reviewer; matching ignores them.
        target: The `profiles.yml` target of the connected validate that found the diagnostic;
            empty for one an offline validate finds. Only a run against that target can
            tell whether a connected entry is stale.
    """

    fingerprint: str
    code: str
    artifact: str = ""
    file: str = ""
    note: str = ""
    target: str = ""

    @property
    def connected(self) -> bool:
        """Report whether a connected validate found the diagnostic, so an offline one cannot."""
        return bool(self.target)

    def judged_by(self, target: str | None) -> bool:
        """Report whether a validate against `target`, or offline when None, can find this entry."""
        return self.target == (target or "")


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
        hints: SST-INT011 for each blocking diagnostic an unmatched entry may once have held.
            They explain diagnostics the run already reports, so unlike a notice none decides
            whether it passes.
    """

    baselined: frozenset[str]
    notices: tuple[Diagnostic, ...]
    hints: tuple[Diagnostic, ...] = ()


def stable_fingerprint(diagnostic: Diagnostic) -> str:
    """Return the 16-hex identity baselining keys on, stable across a reformat that moves a line.

    It hashes the code, the artifact and member the subject names, the file, and the context
    but its `POSITION_KEYS`, and never the line or column, so inserting a comment above a
    problem keeps its identity while renaming the artifact changes it.
    """
    subject = diagnostic.subject or ""
    kind, name = split_artifact_key(subject) if ":" in subject else ("", subject)
    params = "\x1e".join(
        f"{key}={diagnostic.context[key]}" for key in sorted(diagnostic.context) if key not in POSITION_KEYS
    )
    parts = (diagnostic.code, kind, name, diagnostic.origin.file if diagnostic.origin else "", params)
    return blake2b("\x00".join(parts).encode("utf-8"), digest_size=8).hexdigest()


def baselinable(code: str) -> bool:
    """Report whether a code may be baselined: registered below ERROR, and demotable."""
    spec = ERROR_REGISTRY.get(code)
    return spec is not None and spec.severity is not Severity.ERROR and spec.demotable


def found_offline(code: str) -> bool:
    """Report whether an offline validate finds a registered code: its condition needs no connection."""
    return ERROR_REGISTRY[code].precheck is Precheck.LOCAL


def match_baseline(
    diagnostics: Iterable[Diagnostic], baseline: Baseline | None, *, today: str, warn_from: str
) -> BaselineMatch:
    """Return which diagnostics `baseline` holds, and what to report about the baseline itself.

    Args:
        today: Today's ISO date; a baseline whose `expires_on` is before it has expired.
        warn_from: The ISO date 30 days from today; a baseline expiring by then is nearing expiry.

    Raises:
        ValueError: a date is not `YYYY-MM-DD` numbers; the baseline file reader refuses such a file.

    Diagnostics:
        SST-CFG039: the baseline is past its expiry, so nothing is baselined.
        SST-CFG035: the baseline expires within 30 days.
        SST-INT009: an entry matches more than one diagnostic, so it baselines none of them;
            one per entry, in file order.
        SST-INT011: a blocking diagnostic the baseline does not hold has the code and artifact
            type of an entry that matches nothing, as after a rename; one per code and artifact.
        SST-CFG062: an entry records a code SST does not report; one per entry, in file order.
    """
    if baseline is None:
        return BaselineMatch(frozenset(), ())
    diagnostics = tuple(diagnostics)
    origin = Origin(baseline.path)
    count = len(baseline.entries)
    subject = f"config:{baseline.path}"
    expires = _day(baseline.expires_on)
    if expires < _day(today):
        expired = D("SST-CFG039", origin=origin, subject=subject, date=baseline.expires_on, count=count)
        return BaselineMatch(frozenset(), (expired,))
    recorded = tuple(dict.fromkeys(entry.fingerprint for entry in baseline.entries if baselinable(entry.code)))
    matched = Counter(stable_fingerprint(item) for item in diagnostics if baselinable(item.code))
    baselined = frozenset(fingerprint for fingerprint in recorded if matched[fingerprint] == 1)
    notices: tuple[Diagnostic, ...] = ()
    if expires <= _day(warn_from):
        notices = (D("SST-CFG035", origin=origin, subject=subject, date=baseline.expires_on, count=count),)
    ambiguous = tuple(
        D("SST-INT009", subject=subject, value=fingerprint, count=matched[fingerprint])
        for fingerprint in recorded
        if matched[fingerprint] > 1
    )
    hints = tuple(
        D("SST-INT011", origin=origin, subject=subject, code=code, artifact=artifact, other=", ".join(others))
        for (code, artifact), others in _moved(baseline.entries, diagnostics, matched, baselined).items()
    )
    return BaselineMatch(baselined, (*notices, *ambiguous, *unknown_codes(baseline)), hints)


def unknown_codes(baseline: Baseline) -> tuple[Diagnostic, ...]:
    """Warn of each entry whose code SST does not report, so it can never match a diagnostic.

    An upgrade that renamed or retired the code, or a hand edit, leaves one; the warning names
    the code that replaced it when one did.

    Diagnostics:
        SST-CFG062: one per such entry, numbered from 1 in file order.
    """
    found: list[Diagnostic] = []
    for index, entry in enumerate(baseline.entries, start=1):
        if entry.code in ERROR_REGISTRY:
            continue
        found.append(
            D(
                "SST-CFG062",
                origin=Origin(baseline.path),
                subject=f"config:{baseline.path}",
                index=index,
                code=entry.code or "(none)",
                detail=_successor(entry.code),
            )
        )
    return tuple(found)


def _successor(code: str) -> str:
    """Say which code took over `code`'s condition, or that none is known."""
    tombstone = TOMBSTONES.get(code)
    if tombstone is not None and tombstone.superseded_by:
        return f"it was replaced by {tombstone.superseded_by}"
    alias = ALIASES.get(code)
    if alias is not None and alias.targets:
        return f"it was replaced by {' or '.join(alias.targets)}"
    return "no code replaced it"


def _moved(
    entries: tuple[BaselineEntry, ...],
    diagnostics: tuple[Diagnostic, ...],
    matched: Counter[str],
    baselined: frozenset[str],
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Pair each blocking diagnostic not held with the artifacts of the unmatched entries it may be.

    A rename or a move changes an entry's fingerprint, so its warning comes back unheld under the
    new name. Only the code and the artifact type are compared, so every unmatched entry of the
    two is named. By `(code, subject)`, in the order the diagnostics come.
    """
    unmatched: dict[tuple[str, str], set[str]] = {}
    for entry in entries:
        if baselinable(entry.code) and not matched[entry.fingerprint] and ":" in entry.artifact:
            unmatched.setdefault((entry.code, split_artifact_key(entry.artifact)[0]), set()).add(entry.artifact)
    found: dict[tuple[str, str], tuple[str, ...]] = {}
    for item in diagnostics:
        artifact = item.subject or ""
        if not item.blocks or ":" not in artifact or stable_fingerprint(item) in baselined:
            continue
        others = unmatched.get((item.code, split_artifact_key(artifact)[0]))
        if others:
            found.setdefault((item.code, artifact), tuple(sorted(others)))
    return found


def _day(text: str) -> tuple[int, ...]:
    """Read an ISO `YYYY-MM-DD` date as (year, month, day), which orders as the dates do.

    Raises:
        ValueError: a part is not a number.
    """
    return tuple(int(part) for part in text.split("-"))


# How long a baseline lasts unless told otherwise, and the longest it may be told.
DEFAULT_EXPIRY_DAYS = 180
MAX_EXPIRY_DAYS = 365


def baseline_entry(diagnostic: Diagnostic, note: str, target: str = "") -> BaselineEntry:
    """Record one diagnostic: its stable fingerprint, what a reviewer reads it by, and its run's target."""
    origin = diagnostic.origin.file if diagnostic.origin else ""
    return BaselineEntry(
        stable_fingerprint(diagnostic), diagnostic.code, diagnostic.subject or "", origin, note, target
    )


def with_entries(
    baseline: Baseline, diagnostics: Iterable[Diagnostic], note: Callable[[str], str], target: str = ""
) -> tuple[Baseline, tuple[BaselineEntry, ...]]:
    """Return `baseline` with an entry added for each baselinable diagnostic it does not record.

    Additive only: no entry is removed or changed. A fingerprint two diagnostics share is not
    added, since an entry matching more than one diagnostic baselines none of them.

    Args:
        note: The note for an entry of a code, given the code.
        target: The target of the connected validate that reported `diagnostics`; empty for
            an offline one. Each entry added records it, but one of a code an offline validate
            finds (`found_offline`), which an offline prune judges whatever target found it.

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
            added.append(baseline_entry(item, note(item.code), "" if found_offline(item.code) else target))
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

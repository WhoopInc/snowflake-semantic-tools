"""Which version of a Cortex extension the catalog serves, and which one apply makes the default.

The catalog serves an extension's latest certified version once any version is certified, and
its default version otherwise. A default set by name stays where it is when a version is added
(docs/design/snowflake-behavior.md), so apply makes a release's version the default whenever
no certified version outranks it; one that does is served whatever the default, and plan warns
of it instead. Agents pin their version, so only catalog users are affected.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.ports.snowflake.catalog import ExtensionObservation, ExtensionVersion

CERTIFIED = "CERTIFIED"


def certified(extension: ExtensionObservation | None, version: ExtensionVersion) -> bool:
    """Report whether a version is certified, by its own status or as the extension's latest certified.

    Either signal counts: some versions tagged by the extensions pipeline report an empty
    per-version status while the extension names them its latest certified one. The
    extension may name that version by its system name or by its alias, so both identify it;
    matching the name alone would read an alias as uncertified and re-tag on every run.
    """
    if (version.certification_status or "").upper() == CERTIFIED:
        return True
    latest = extension.latest_certified_version if extension is not None else None
    return latest is not None and latest.upper() in _version_identity(version)


def certified_instead(
    extension: ExtensionObservation | None,
    versions: tuple[ExtensionVersion, ...],
    version: ExtensionVersion | None,
    *,
    certifies: bool,
) -> str | None:
    """Return the certified version the catalog serves instead of a release's; None when none does.

    Args:
        version: The release's version; None when it is not published yet.
        certifies: The release certifies its version, so once published it counts as certified.
    """
    found = {item.name.upper() for item in versions if certified(extension, item)}
    ours = version.name.upper() if version is not None else None
    if certifies:
        if ours is None:
            return None
        found.add(ours)
    if not found:
        return None
    latest = max(found, key=version_number)
    return None if latest == ours else latest


def default_elsewhere(
    extension: ExtensionObservation | None,
    versions: tuple[ExtensionVersion, ...],
    version: ExtensionVersion | None,
    *,
    certifies: bool,
) -> bool:
    """Report whether apply must make a release's published version the default.

    It must when the version exists and is not the default, and no certified version
    outranks it; one that does is served whatever the default is.
    """
    if version is None or version.is_default:
        return False
    return certified_instead(extension, versions, version, certifies=certifies) is None


# A plan-time check against what the catalog serves, observed through the Snowflake port,
# so it stays with the planner rather than in `domain.validate`.
def served_warning(
    key: str,
    target: str,
    alias: str,
    served: str | None,
) -> DiagnosticBag:
    """Warn when a certified version will be served instead of the release's.

    Args:
        served: What `certified_instead` found; None warns of nothing.

    Diagnostics:
        SST-VAL841: a certified version outranks the release's, whatever the default.
    """
    if served is None:
        return DiagnosticBag()
    return DiagnosticBag(
        (
            D(
                "SST-VAL841",
                subject=key,
                artifact=key,
                value=alias,
                target=target,
                found=served,
                detail="it is the latest certified version",
            ),
        )
    )


def version_number(name: str) -> int:
    """Return the number of a `VERSION$<n>` system name; -1 for any other name."""
    number = name.upper().removeprefix("VERSION$")
    return int(number) if number.isdigit() else -1


def _version_identity(version: ExtensionVersion) -> frozenset[str]:
    """Every name a version answers to, uppercased: its system name and, when it has one, its alias."""
    return frozenset(name.upper() for name in (version.name, version.alias) if name)

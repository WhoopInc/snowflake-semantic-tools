"""Read what a drift digest covers, once at apply after a write and again at every plan.

Both sides read the same properties the same way, so an object nobody touched digests the
same; `domain.model.lifecycle.drift` says which properties those are. Plan reads only for an
object a rendered artifact names and SST's marker claims, since only such an object is
compared: what SHOW already listed costs nothing more, a procedure takes one DESCRIBE, and a
semantic view one GET_DDL, both skipped by a basic drift check. An agent's digest comes from
the SHOW VERSIONS IN AGENT read plan makes anyway.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    DEFINED_TYPES,
    DESCRIBED_TYPES,
    DIGESTED_TYPES,
    DRIFT_CHECK_FULL,
    SHOWN_TYPES,
    RenderedArtifact,
    drift_properties,
    held_spec_fingerprint,
    live_digest,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# What a failed read is reported as, by the object type it reads beyond SHOW.
_READS = {"PROCEDURE": "DESCRIBE PROCEDURE", "SEMANTIC VIEW": "GET_DDL"}


def observed_digest(
    port: CatalogPort,
    object_type: str,
    name: QualifiedName,
    desired: RenderedArtifact,
    *,
    shown: Mapping[str, str],
    newest_spec: str | None,
    drift_check: str = DRIFT_CHECK_FULL,
) -> tuple[str | None, tuple[Diagnostic, ...]]:
    """Digest one listed object for plan, from its SHOW row and the reads its type needs.

    A read that fails leaves its properties out of the digest, so they are not compared, and
    is reported; the plan goes on.

    Args:
        shown: The object's SHOW row, columns lowercased.
        newest_spec: An agent's newest committed version's specification fingerprint.
        drift_check: `full` reads a procedure's DESCRIBE and a view's GET_DDL; `basic` does not.

    Returns:
        The digest, None when no property was read, and what failed.

    Diagnostics:
        SST-PLN103: a DESCRIBE or GET_DDL a drift digest needs failed.
    """
    kind = " ".join(object_type.upper().split())
    if kind not in DIGESTED_TYPES:
        return None, ()
    described, definition, failure = _reads(
        port, kind, name, desired.routine_signature, drift_check == DRIFT_CHECK_FULL
    )
    properties = drift_properties(kind, shown, described=described, definition=definition, newest_spec=newest_spec)
    problems = () if failure is None else (D("SST-PLN103", value=name.sql, field=failure[0], detail=failure[1]),)
    return live_digest(properties), problems


def written_digest(port: CatalogPort, rendered: RenderedArtifact) -> str | None:
    """Digest an object apply just wrote, reading back what plan will read; None when unreadable.

    Apply reads every property, whatever plan's drift check, so a later full check compares
    them all. A read that fails leaves the digest unknown, which plan never takes for drift.
    """
    kind = " ".join(rendered.object_type.upper().split())
    if kind not in DIGESTED_TYPES or rendered.temporary:
        return None
    try:
        shown = port.show_row(kind, rendered.target) if kind in SHOWN_TYPES else {}
        newest = port.observe_agent_versions(rendered.target).newest if kind == "AGENT" else None
    except SnowflakePortError:
        return None
    described, definition, failure = _reads(port, kind, rendered.target, rendered.routine_signature, True)
    if failure is not None or shown is None:
        return None
    newest_spec = held_spec_fingerprint(newest.comment, newest.specification) if newest is not None else None
    return live_digest(
        drift_properties(kind, shown, described=described, definition=definition, newest_spec=newest_spec)
    )


def _reads(
    port: CatalogPort, kind: str, name: QualifiedName, signature: tuple[str, ...], full: bool
) -> tuple[Mapping[str, str] | None, str | None, tuple[str, str] | None]:
    """Read a procedure's DESCRIBE or a view's GET_DDL when `full`; what failed, as (read, error)."""
    if not full:
        return None, None, None
    try:
        if kind in DESCRIBED_TYPES:
            return port.describe_routine(kind, name, signature), None, None
        if kind in DEFINED_TYPES:
            return None, port.get_ddl(kind, name), None
    except SnowflakePortError as exc:
        return None, None, (_READS.get(kind, "GET_DDL"), str(exc))
    return None, None, None

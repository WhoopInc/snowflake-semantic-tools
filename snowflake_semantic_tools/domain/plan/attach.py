"""Attach each CoWork profile a plan publishes to the account's CoWork object.

CoWork profiles are a Snowflake private preview (PrPr) feature; attaching follows the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

CoWork shows a profile only once it is attached to the account's one CoWork object, the
SNOWFLAKE INTELLIGENCE object. Plan reads that object and what is attached to it once
(`AttachmentReading`), then `attach_changes` adds the statement that attaches each profile not
attached yet to the change that publishes it. Attaching is never part of a profile's definition,
so it changes neither the fingerprint nor the drift digest: a profile detached by hand is attached
again, not blocked as changed out of band. Nothing here detaches a profile, as dropping one does,
and nothing touches another attachment.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.sql import Sql, ident, qname, sql

_Name = tuple[str, str, str]
# The actions whose profile is published, and so is attached once it is not.
_PUBLISHED = (Action.CREATE, Action.UPDATE, Action.NOOP)


@dataclass(frozen=True, slots=True)
class AttachmentReading:
    """What plan read of the account's CoWork object: its name, and the agents attached to it.

    Attributes:
        cowork_object: The CoWork object's name as SHOW SNOWFLAKE INTELLIGENCES lists it; None
            when the account has none.
        attached: The agents attached to it, each folded as `QualifiedName.folded`; empty when
            the account has no CoWork object.
    """

    cowork_object: str | None
    attached: frozenset[_Name] = frozenset()


def attach_statement(cowork_object: str, agent: QualifiedName) -> Sql:
    """Return the statement that attaches `agent` to the CoWork object `cowork_object` names."""
    return sql(
        "ALTER SNOWFLAKE INTELLIGENCE {cowork_object} ADD AGENT {agent}",
        cowork_object=ident(Identifier.shown(cowork_object)),
        agent=qname(agent),
    )


def attach_changes(
    changes: tuple[Change, ...], reading: AttachmentReading | None
) -> tuple[tuple[Change, ...], tuple[Diagnostic, ...]]:
    """Attach each published profile that wants attaching and is not attached, in change order.

    For a change whose rendered artifact wants attaching:

    - a create ends with `attach_statement`, since a new object is never attached;
    - an update ends with it when the profile is not attached;
    - an unchanged profile that is not attached becomes an update that runs only it, for
      `ChangeReason.DETACHED`, committing no version and staging nothing;
    - with no CoWork object in the account, nothing is attached.

    Each change that attaches names the CoWork object in `attaches_to`. Any other change, a
    blocked one included, and every change when `reading` is None, is returned as it was. The
    statement is added only to one not attached, since a second ADD AGENT fails (measured).

    Returns:
        The changes in their given order, and what attaching reported, in change order.

    Diagnostics:
        SST-PLN104: an unchanged profile is not attached to the CoWork object; apply attaches it.
        SST-PLN105: the account has no CoWork object, so the profile publishes unattached.
    """
    if reading is None:
        return changes, ()
    decided: list[Change] = []
    reported: list[Diagnostic] = []
    for change in changes:
        attached, found = _attached(change, reading)
        decided.append(attached)
        reported.extend(found)
    return tuple(decided), tuple(reported)


def _attached(change: Change, reading: AttachmentReading) -> tuple[Change, tuple[Diagnostic, ...]]:
    """Return one change as `attach_changes` decides it, and what deciding it reported."""
    rendered = change.rendered
    if rendered is None or not rendered.wants_attachment or change.action not in _PUBLISHED:
        return change, ()
    cowork_object = reading.cowork_object
    if cowork_object is None:
        return change, (D("SST-PLN105", subject=change.key, artifact=change.key),)
    if change.action is not Action.CREATE and rendered.target.folded in reading.attached:
        return change, ()
    statement = attach_statement(cowork_object, rendered.target)
    if change.action is not Action.NOOP:
        attaching = replace(rendered, statements=(*rendered.statements, statement), attaches_to=cowork_object)
        return replace(change, rendered=attaching), ()
    # Only the attachment changed: nothing is staged, and no version is committed.
    only = replace(
        rendered,
        statements=(statement,),
        attaches_to=cowork_object,
        adds_version=False,
        upload_path=None,
        upload_content=None,
        upload_stage=None,
    )
    detached = D("SST-PLN104", subject=change.key, artifact=change.key, value=cowork_object)
    return replace(change, action=Action.UPDATE, reason=ChangeReason.DETACHED, rendered=only), (detached,)

"""Which properties of a live object a planned update changes, as `plan --full` reports them.

An update is planned from what the object shows and what the artifact renders, so each
property compares the two: the object's name, its ownership marker's fingerprint and
manifest, an agent's version aliases, and the tags of an agent or a semantic view; and whether
a CoWork profile is attached to CoWork, which plan read apart from the object. A create
writes every property and a prune removes the object, so neither names one; nor does an
unchanged or blocked artifact.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.lifecycle import Action, Change


@dataclass(frozen=True, slots=True)
class PropertyChange:
    """One property an update changes: what the object shows now, and what it will show."""

    name: str
    before: str
    after: str


def changed_properties(change: Change, manifest_id: str) -> tuple[PropertyChange, ...]:
    """Return what an update changes on the object, in a fixed order; empty for any other change.

    `target` when the object's shown name differs from the declared one; `fingerprint` and
    `manifest_id` when its marker records others than the artifact renders and the plan's
    manifest, `manifest_id`, a missing marker reading as empty; `aliases` when an agent shows
    others than it declares, compared casefolded as an unquoted alias is stored upper-cased, an
    update unsetting every alias but the declared one; and `tags`
    when an agent or a semantic view shows others, which the update unsets or the replace drops.
    Tags that were not read are not compared, so an unread object never seems to gain them.
    `attached` goes from `no` to the CoWork object an update attaches a CoWork profile to
    (Snowflake private preview).
    """
    rendered, observed = change.rendered, change.observed
    if change.action is not Action.UPDATE or rendered is None or observed is None:
        return ()
    marker = observed.marker
    aliases = (rendered.desired_alias,) if rendered.desired_alias else ()
    pairs: list[tuple[str, str, str]] = [
        ("target", observed.qualified_name.sql, rendered.target.sql),
        ("fingerprint", marker.fingerprint if marker else "", rendered.fingerprint),
        ("manifest_id", marker.manifest_id if marker else "", manifest_id),
        ("aliases", ", ".join(sorted(observed.aliases, key=str.casefold)), ", ".join(aliases)),
    ]
    if observed.tags is not None:
        pairs.append(("tags", ", ".join(sorted(observed.tags)), ", ".join(sorted(rendered.desired_tags))))
    if rendered.attaches_to is not None:
        # Snowflake private preview (PrPr): CoWork profiles
        pairs.append(("attached", "no", rendered.attaches_to))
    return tuple(PropertyChange(name, before, after) for name, before, after in pairs if _differs(name, before, after))


def _differs(name: str, before: str, after: str) -> bool:
    """Whether a property's two values differ: an agent's aliases casefolded, anything else exactly."""
    return before.casefold() != after.casefold() if name == "aliases" else before != after

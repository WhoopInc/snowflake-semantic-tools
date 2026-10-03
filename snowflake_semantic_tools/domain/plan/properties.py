"""Which properties of a live object a planned update changes, as `plan --full` reports them.

An update is planned from what the object shows and what the artifact renders, so each
property compares the two: the object's name, its ownership marker's fingerprint and
manifest, and for an agent its version aliases and tags. A create writes every property and
a prune removes the object, so neither names one; nor does an unchanged or blocked artifact.
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
    manifest, `manifest_id`, a missing marker reading as empty; `aliases` and `tags` when an
    agent shows others than it declares, an update unsetting every alias but the declared one.
    """
    rendered, observed = change.rendered, change.observed
    if change.action is not Action.UPDATE or rendered is None or observed is None:
        return ()
    marker = observed.marker
    aliases = (rendered.desired_alias,) if rendered.desired_alias else ()
    pairs = (
        ("target", observed.qualified_name.sql, rendered.target.sql),
        ("fingerprint", marker.fingerprint if marker else "", rendered.fingerprint),
        ("manifest_id", marker.manifest_id if marker else "", manifest_id),
        ("aliases", ", ".join(sorted(observed.aliases)), ", ".join(aliases)),
        ("tags", ", ".join(sorted(observed.tags)), ", ".join(sorted(rendered.desired_tags))),
    )
    return tuple(PropertyChange(name, before, after) for name, before, after in pairs if before != after)

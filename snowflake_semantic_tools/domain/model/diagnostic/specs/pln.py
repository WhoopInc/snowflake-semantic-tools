"""Planning codes: live observation, ownership, drift, dependency cycles, pruning, and partial runs."""

from __future__ import annotations

from ..core import ErrorSpec, Severity, spec

TITLE: str = "Planning"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-PLN001",
        Severity.ERROR,
        "Observation query failed",
        "observation of {value} failed: {detail}",
        "check the connection and role, then re-run plan",
    ),
    spec(
        "SST-PLN002",
        Severity.ERROR,
        "Different object type exists",
        "{artifact}: {value} exists as a {found}",
        "rename the artifact or remove the conflicting object",
    ),
    spec(
        "SST-PLN003",
        Severity.WARNING,
        "Prune candidate is unmanaged",
        "{value} has no SST ownership marker; skipped",
        "adopt it explicitly or delete it by hand",
    ),
    spec(
        "SST-PLN004",
        Severity.WARNING,
        "Prune candidate is absent from state",
        "{value} carries an SST marker and is absent from state; skipped",
        "reconcile state or delete it by hand",
    ),
    spec(
        "SST-PLN005",
        Severity.ERROR,
        "Artifact dependency cycle",
        "artifact dependency cycle: {cycle}",
        "break the cycle",
    ),
    spec(
        "SST-PLN013",
        Severity.WARNING,
        "Live object carries explicit grants",
        "{artifact}: {count} explicit grants exist on {value}",
        "confirm COPY GRANTS is emitted before applying",
    ),
    spec(
        "SST-PLN014",
        Severity.ERROR,
        "Live definition differs from recorded state",
        "{artifact} was changed out of band",
        "review and explicitly reconcile the object before applying",
    ),
    spec(
        "SST-PLN021",
        Severity.INFO,
        "Prune is report-only",
        "{artifact} is no longer declared; SST never removes {value}: {detail}",
        "the plan lists it until the objects are removed by hand; it never counts as a change",
    ),
    spec(
        "SST-PLN023",
        Severity.WARNING,
        "Declared and live case differ",
        "{artifact}: declared '{value}', live object is '{found}'",
        "normalise the declaration to the rendered case",
    ),
    spec(
        "SST-PLN024",
        Severity.ERROR,
        "Existing object is not managed by SST",
        "{artifact}: {value} exists without trusted SST ownership",
        "adopt or remove the object explicitly before applying",
        demotable=False,
    ),
    spec(
        "SST-PLN025",
        Severity.ERROR,
        "Artifact target moved",
        "{artifact}: recorded target '{found}' differs from declared target '{expected}'",
        "move the object explicitly, then reconcile state and re-run plan",
        demotable=False,
    ),
    spec(
        "SST-PLN026",
        Severity.ERROR,
        "Stage is not an internal SSE stage",
        "{artifact}: stage {value} is {found}, expected INTERNAL NO CSE",
        "point the configuration at an internal stage with SNOWFLAKE_SSE encryption; SST never alters a stage",
        demotable=False,
    ),
    spec(
        "SST-PLN027",
        Severity.ERROR,
        "Version alias holds different content",
        "{artifact}: version alias {value} holds files that differ from the bundle ({detail})",
        "an earlier publish left a damaged version under this alias; remove the alias in Snowflake, then plan again",
        demotable=False,
    ),
    spec(
        "SST-PLN028",
        Severity.ERROR,
        "Registry row changed since SST last wrote it",
        "{artifact}: registry row '{value}' has VERSION {found}, and SST last wrote {expected}",
        "another writer changed the row; reconcile it, then plan again",
        demotable=False,
    ),
    spec(
        "SST-PLN029",
        Severity.ERROR,
        "Registry table has the wrong shape",
        "{artifact}: {value} {detail}",
        "SST never alters the registry; fix the table or point skills.stage at a compatible one",
        demotable=False,
    ),
    spec(
        "SST-PLN030",
        Severity.ERROR,
        "Pinned version is not part of this plan",
        "{artifact}: pins the published version of {value}; select {value} as well",
        "plan the pinned artifact in the same run; when its version is already published it plans as NOOP",
        demotable=False,
    ),
    spec(
        "SST-PLN032",
        Severity.INFO,
        "Excluded from a partial run",
        "{artifact} has errors, or depends on something that does, so this partial run leaves it unpublished",
        "fix the errors reported for it; what is live stays as it is, and state keeps its record",
    ),
    spec(
        "SST-PLN033",
        Severity.INFO,
        "Partial run cannot go ahead",
        "--partial publishes nothing: {found} on {value} cannot be traced to the artifacts it would change",
        "fix that error first; a configuration error, or an error in a semantic view member such as a metric, "
        "stops every run, because the views it belongs to would otherwise publish without it",
    ),
)

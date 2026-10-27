"""Check Cortex Code agent profiles: on their own, where they publish, and beside the project's agents.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature. Each check reads only
the values it is given; whether a skill a profile names exists is resolution's question. A
schema is compared as Snowflake resolves it.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.coco_profile import (
    DEFAULT_PROFILE,
    CocoProfileModel,
    SharedCocoProfile,
    listed_by_cortex_code,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


def coco_profile_diagnostics(
    model: CocoProfileModel, shared: SharedCocoProfile | None, allowed_models: frozenset[str]
) -> tuple[Diagnostic, ...]:
    """Check one profile's model, then each skill it repeats from the shared layer.

    Diagnostics:
        SST-VAL894: the profile's `model:` is not in `snowflake.orchestration_models`.
        SST-VAL845: the profile lists a skill the shared layer already carries.
    """
    found: list[Diagnostic] = []
    if model.orchestration_model is not None and model.orchestration_model not in allowed_models:
        found.append(
            D(
                "SST-VAL894",
                origin=model.origin,
                subject=model.key,
                artifact=model.name,
                found=model.orchestration_model,
            )
        )
    carried = set(shared.skills) if shared is not None else set()
    for skill in model.skills:
        if skill.ref == "skill" and skill.path in carried:
            found.append(D("SST-VAL845", origin=model.origin, subject=model.key, artifact=model.name, name=skill.path))
    return tuple(found)


def unlisted_profile(model: CocoProfileModel, target: QualifiedName) -> tuple[Diagnostic, ...]:
    """Report a profile that publishes where Cortex Code does not list profiles: a rehearsal of one.

    Diagnostics:
        SST-VAL891: the profile's database and schema are not CORTEX_CODE.CONFIG.
    """
    if listed_by_cortex_code(target):
        return ()
    schema = f"{target.database.sql}.{target.schema.sql}"
    return (D("SST-VAL891", origin=model.origin, subject=model.key, artifact=model.name, target=schema),)


def default_profile_problems(profiles: Sequence[tuple[CocoProfileModel, QualifiedName]]) -> tuple[Diagnostic, ...]:
    """Report that a role seeing several listed profiles gets none, when none of them is DEFAULT.

    Only the profiles published where Cortex Code lists them count; one profile is applied to
    every role that sees it, so it needs no DEFAULT.

    Diagnostics:
        SST-VAL893: more than one profile publishes to CORTEX_CODE.CONFIG and none is named DEFAULT.
    """
    listed = [(model, target) for model, target in profiles if listed_by_cortex_code(target)]
    if len(listed) < 2 or any(target.name.folded == DEFAULT_PROFILE.upper() for _, target in listed):
        return ()
    names = ", ".join(target.name.sql for _, target in listed)
    return (D("SST-VAL893", origin=Origin(listed[0][0].source_files[0]), subject=None, count=len(listed), names=names),)


def schema_problems(
    others: Sequence[tuple[str, QualifiedName]], profiles: Sequence[tuple[CocoProfileModel, QualifiedName]]
) -> tuple[Diagnostic, ...]:
    """Report each profile that publishes to a schema an agent or CoWork profile publishes to.

    Observation tells the AGENT objects of each kind apart by the schema each publishes to,
    so a profile may not share one; the first other object in its schema is named.

    Args:
        others: The name and target of each compiled agent and CoWork profile, in compile order.
        profiles: Each compiled profile, with where it publishes.

    Diagnostics:
        SST-VAL892: a profile publishes to a schema an agent or CoWork profile publishes to.
    """
    schemas: dict[tuple[str, str], str] = {}
    for name, target in others:
        schemas.setdefault(_schema(target), name)
    found: list[Diagnostic] = []
    for profile, target in profiles:
        other = schemas.get(_schema(target))
        if other is not None:
            found.append(
                D(
                    "SST-VAL892",
                    origin=profile.origin,
                    subject=profile.key,
                    artifact=profile.name,
                    target=f"{target.database.sql}.{target.schema.sql}",
                    other=other,
                )
            )
    return tuple(found)


def _schema(target: QualifiedName) -> tuple[str, str]:
    return target.database.folded, target.schema.folded

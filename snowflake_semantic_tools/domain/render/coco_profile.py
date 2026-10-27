"""Pure renderer for Cortex Code agent profile specifications, and the definition each fingerprints.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature. A profile's
specification accepts only `models.orchestration`, `instructions.response`, and `skills`, so
nothing else is ever rendered; each skill entry is pinned as an agent's is.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.coco_profile import (
    CocoProfileModel,
    ResolvedCocoProfile,
    SharedCocoProfile,
)
from snowflake_semantic_tools.domain.render.agent import render_spec_json
from snowflake_semantic_tools.domain.state import canonical_json


def assemble_instructions(shared: SharedCocoProfile | None, profile: CocoProfileModel) -> str:
    """Join the shared `AGENTS.md`, then the shared rules in name order, then the profile's `AGENTS.md`.

    Blank parts are dropped and the rest stripped, each separated by a blank line; empty when
    nothing is left.
    """
    parts = [
        *((shared.prompt or "",) if shared is not None else ()),
        *(text for _, text in (sorted(shared.rules) if shared is not None else ())),
        profile.prompt or "",
    ]
    kept = [part.strip() for part in parts if part.strip()]
    return "\n\n".join(kept) + "\n" if kept else ""


def render_coco_profile_spec(resolved: ResolvedCocoProfile) -> dict[str, object]:
    """Render a profile's complete specification as a JSON-ready document.

    `models.orchestration` appears only when the profile names a model, `instructions.response`
    only when it has instructions, and `skills` only when it pins any; a skill's `version`
    appears when it pins one.

    Example:
        A profile with instructions and one pinned skill renders:
            {"instructions": {"response": "Be brief.\\n"},
             "skills": [{"name": "sql-review", "source": {"type": "CORTEX_EXTENSION",
                         "path": "DB.S.SQL_REVIEW", "version": "SST_0123456789AB"}}]}
    """
    model = resolved.model
    document: dict[str, object] = {}
    if model.orchestration_model:
        document["models"] = {"orchestration": model.orchestration_model}
    if resolved.instructions:
        document["instructions"] = {"response": resolved.instructions}
    if model.skills:
        document["skills"] = [
            {
                "name": skill.name,
                "source": {
                    "type": skill.source_type,
                    "path": skill.path,
                    **({"version": skill.version} if skill.version else {}),
                },
            }
            for skill in model.skills
        ]
    return document


def render_coco_profile_json(resolved: ResolvedCocoProfile) -> str:
    """Render the specification as the profile's JSON payload, as `render_spec_json` serializes it."""
    return render_spec_json(render_coco_profile_spec(resolved))


def desired_coco_profile_definition(model: CocoProfileModel, spec: Mapping[str, object]) -> bytes:
    """Encode the profile's whole definition as the canonical JSON its fingerprint is a hash of.

    Besides the spec, the definition holds the comment, the published name, and `enabled`, so a
    change to any of them changes the fingerprint.
    """
    return bytes(
        canonical_json(
            {
                "comment": model.comment,
                "published_name": model.published_name,
                "enabled": model.enabled,
                "spec": dict(spec),
            }
        )
    )

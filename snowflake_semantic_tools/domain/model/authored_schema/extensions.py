"""The key tables of a plugin, a CoCo profile, and the shared layer every CoCo profile carries."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.coco_profile import REJECTED_COCO_PROFILE_KEYS

_STRING, _TEMPLATE, _LIST, _MAPPING = ValueKind.STRING, ValueKind.TEMPLATE, ValueKind.LIST, ValueKind.MAPPING

PLUGIN = FileKind(
    "plugin",
    "A plugin: project skills bundled and published as one extension.",
    "a `plugin.yml` or `plugin.yaml` file in a folder of the plugins directory",
    Node(
        (
            Field("name", _STRING, "The plugin's name; the folder names it, and this must match."),
            Field(
                "description",
                _STRING,
                "What the plugin bundles and when to use it; the extension's description.",
                required=True,
            ),
            Field("owner_team", _STRING, "The team that owns the plugin, published as the bundle's author."),
            Field(
                "skills",
                _LIST,
                "The project skills bundled, each a `skill()` call; at least one is required.",
                items=_TEMPLATE,
            ),
            Field("config", _MAPPING, "The plugin's own `plugins:` settings.", config_block="plugins"),
        )
    ),
)
_PINNED_SKILLS = Field("skills", _LIST, "Project skills the profile pins, each a `skill()` call.", items=_TEMPLATE)
COCO_PROFILE = FileKind(
    "coco_profile",
    "A Cortex Code agent profile: the model, skills and plugins a role's sessions start with.",
    "a `coco_profile.yml` or `coco_profile.yaml` file in a folder of the CoCo profiles directory",
    Node(
        (
            Field("name", _STRING, "The profile's name; the folder names it, and this must match."),
            Field("description", _STRING, "The published profile's comment."),
            Field("model", _STRING, "The orchestration model; leave it out to let the client choose."),
            _PINNED_SKILLS,
            Field(
                "plugins",
                _LIST,
                "Project plugins the profile pins, each a `plugin()` call, one entry per member.",
                items=_TEMPLATE,
            ),
            Field("config", _MAPPING, "The profile's own `coco_profiles:` settings.", config_block="coco_profiles"),
            *(
                Field(
                    key,
                    ValueKind.ANY,
                    f"Refused: {reason}.",
                    status=FieldStatus.REMOVED,
                    replacement=reason,
                    code="SST-VAL851",
                )
                for key, reason in REJECTED_COCO_PROFILE_KEYS.items()
            ),
        )
    ),
)
COCO_SHARED_PROFILE = FileKind(
    "coco_shared_profile",
    "The shared layer every CoCo profile carries: skills pinned before each profile's own.",
    "the optional `coco_profile.yml` or `coco_profile.yaml` in the CoCo profiles directory's `shared/` folder",
    Node(
        (
            Field(
                "skills",
                _LIST,
                "Skills pinned first in every profile, each a `skill()` call; a profile listing one again warns.",
                items=_TEMPLATE,
            ),
        )
    ),
)

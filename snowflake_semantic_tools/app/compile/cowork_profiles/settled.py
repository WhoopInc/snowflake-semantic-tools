"""Give a CoWork profile the values its resolved settings name, rendered for the target.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile's settings follow the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

Loading resolves each profile's `cowork_profiles:` settings through the one precedence chain, as
it resolves an agent's `agents:` ones; this module turns them into the profile's own fields as
`agents.settled` does for an agent, through the same tag and reporting rules.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.agents.settled import reported_settings, tag_pairs
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.config_schema import setting_value, target_text
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.resolve.tags import DeclaredTags

_RENDERED = ("database", "schema")


def settled_cowork_profile(
    model: CoworkProfileModel, target: TargetIdentity, tags: DeclaredTags | None = None
) -> tuple[CoworkProfileModel, tuple[Diagnostic, ...]]:
    """Return the profile with its orchestration model, alias, tags, and flags set from its settings.

    Text values have their `{{ target.* }}` templates rendered. An absent orchestration model is
    `auto`; an absent alias leaves none; `attach` is true unless a setting says false, and
    `allow_auto_model` false unless one says true. Each tag's `name:` is one `{{ tag('<name>') }}`
    call, resolved against `tags`; a tag whose name does not resolve is left out. The profile's
    `settings` become what it reports: its location rendered, and each built-in default it took.

    Args:
        tags: What the `tags:` block of the configuration declares; None declares none.

    Returns:
        The profile, and why each tag it leaves out does not resolve or cannot publish, in the
        order it lists them.

    Diagnostics:
        Every code `tag_pairs` lists, for the profile's `tags`.
    """
    settings = model.settings
    pairs, problems = tag_pairs(
        setting_value(settings, "tags"),
        tags or DeclaredTags(),
        origin=model.origin,
        key=model.key,
        reference="cowork_profile.config.tags.name",
    )
    defaults: dict[str, object] = {
        "database": target.database.folded,
        "schema": target.schema.folded,
        "orchestration_model": "auto",
        "enabled": True,
        # Snowflake private preview (PrPr): CoWork profiles
        "attach": True,
    }
    settled = replace(
        model,
        orchestration_model=target_text(setting_value(settings, "orchestration_model"), target, None) or "auto",
        allow_auto_model=setting_value(settings, "allow_auto_model") is True,
        alias=target_text(setting_value(settings, "alias"), target, None),
        tags=pairs,
        attach=setting_value(settings, "attach") is not False,
        settings=reported_settings(settings, target, _RENDERED, defaults),
    )
    return settled, problems

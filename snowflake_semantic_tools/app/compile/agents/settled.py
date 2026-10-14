"""Give an agent the values its resolved settings name, rendered for the target.

Loading resolves each agent's `agents:` settings through the one precedence chain
(`domain.model.config_schema.settings`); this module turns them into what compiling reads:
the agent's own fields, the execution environment its tools inherit, and where it publishes.
A built-in default applies only to a key no step of the chain set, so an explicit `0`, `[]`,
`false`, or `auto` is a value like any other.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.config_schema import (
    Setting,
    config_bool,
    config_int,
    setting_value,
    target_text,
    with_defaults,
)
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity

_RENDERED = ("database", "schema", "warehouse")


def settled_agent(model: AgentModel, target: TargetIdentity) -> AgentModel:
    """Return the agent with each field a setting gives set from its resolved settings.

    Text values have their `{{ target.* }}` templates rendered. An absent orchestration model
    is `auto` and an absent `secure` false; any other absent setting leaves its field unset.
    The agent's `settings` become what it reports: the location and warehouse rendered, and
    each built-in default it took recorded as such.
    """
    settings = model.settings

    def text(key: str) -> str | None:
        return target_text(setting_value(settings, key), target, None)

    return replace(
        model,
        orchestration_model=text("orchestration_model") or "auto",
        budget_seconds=config_int(setting_value(settings, "budget_seconds")),
        budget_tokens=config_int(setting_value(settings, "budget_tokens")),
        tool_not_accessible=text("tool_not_accessible"),
        analytical_search=config_bool(setting_value(settings, "analytical_search")),
        alias=text("alias"),
        secure=config_bool(setting_value(settings, "secure")) is True,
        tags=_tag_pairs(setting_value(settings, "tags")),
        settings=_reported(settings, target),
    )


def agent_location(model: AgentModel, target: TargetIdentity) -> tuple[str, str]:
    """Return the database and schema the agent publishes to: its settings', else the target's own."""
    database, schema = target.database.folded, target.schema.folded
    return (
        target_text(setting_value(model.settings, "database"), target, database) or database,
        target_text(setting_value(model.settings, "schema"), target, schema) or schema,
    )


def agent_environment(model: AgentModel, target: TargetIdentity) -> tuple[str | None, int | None]:
    """Return the warehouse and query timeout the agent's tools run with when they set none.

    The warehouse is the agent's `warehouse` setting, else the target's; a setting that renders
    empty is unset.
    """
    warehouse = target_text(setting_value(model.settings, "warehouse"), target, target.warehouse)
    return warehouse or target.warehouse, config_int(setting_value(model.settings, "query_timeout"))


def _reported(settings: Mapping[str, Setting], target: TargetIdentity) -> Mapping[str, Setting]:
    """The settings with the location and warehouse rendered, then each built-in default the agent took."""
    rendered = {
        key: Setting(key, target_text(settings[key].value, target, "") or "", settings[key].source)
        for key in _RENDERED
        if key in settings
    }
    defaults: dict[str, object] = {
        "database": target.database.folded,
        "schema": target.schema.folded,
        "warehouse": target.warehouse,
        "orchestration_model": "auto",
        "enabled": True,
        "secure": False,
    }
    return MappingProxyType({item.key: item for item in with_defaults({**settings, **rendered}, defaults)})


def _tag_pairs(value: object) -> tuple[tuple[str, str], ...]:
    """Read the `tags` setting: each `{name, value}` entry as a pair, in the order it lists them."""
    entries = value if isinstance(value, list) else []
    return tuple(
        (str(entry["name"]), str(entry["value"]))
        for entry in entries
        if isinstance(entry, Mapping) and entry.get("name") is not None and entry.get("value") is not None
    )

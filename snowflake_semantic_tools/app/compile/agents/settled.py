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

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
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
from snowflake_semantic_tools.domain.resolve.tags import DeclaredTags, tag_reference

_RENDERED = ("database", "schema", "warehouse")


def settled_agent(
    model: AgentModel, target: TargetIdentity, tags: DeclaredTags | None = None
) -> tuple[AgentModel, tuple[Diagnostic, ...]]:
    """Return the agent with each field a setting gives set from its resolved settings.

    Text values have their `{{ target.* }}` templates rendered. An absent orchestration model
    is `auto` and an absent `secure` false; any other absent setting leaves its field unset.
    Each tag's `name:` is one `{{ tag('<name>') }}` call, resolved against `tags`; a tag whose
    name does not resolve is left out. The agent's `settings` become what it reports: the
    location and warehouse rendered, and each built-in default it took recorded as such.

    Args:
        tags: What the `tags:` block of the configuration declares; None declares none.

    Returns:
        The agent, and why each tag it leaves out does not resolve or cannot publish, in the
        order it lists them.

    Diagnostics:
        Every code `tag_reference` lists, for a tag's `name:`.
        SST-PRS003: a tag's value is not text or a number, such as an unquoted `no`.
    """
    settings = model.settings

    def text(key: str) -> str | None:
        return target_text(setting_value(settings, key), target, None)

    pairs, problems = _tag_pairs(model, setting_value(settings, "tags"), tags or DeclaredTags())
    settled = replace(
        model,
        orchestration_model=text("orchestration_model") or "auto",
        budget_seconds=config_int(setting_value(settings, "budget_seconds")),
        budget_tokens=config_int(setting_value(settings, "budget_tokens")),
        tool_not_accessible=text("tool_not_accessible"),
        analytical_search=config_bool(setting_value(settings, "analytical_search")),
        alias=text("alias"),
        secure=config_bool(setting_value(settings, "secure")) is True,
        tags=pairs,
        settings=_reported(settings, target),
    )
    return settled, problems


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


def _tag_pairs(
    model: AgentModel, value: object, tags: DeclaredTags
) -> tuple[tuple[tuple[str, str], ...], tuple[Diagnostic, ...]]:
    """Read the `tags` setting: each `{name, value}` entry as an object name and value, in order.

    An entry without both is skipped; the settings checks report its shape.

    Diagnostics:
        Every code `tag_reference` lists, for a tag's `name:`.
        SST-PRS003: a tag's value is not text or a number, such as an unquoted `no`.
    """
    pairs: list[tuple[str, str]] = []
    problems: list[Diagnostic] = []
    for index, entry in enumerate(value if isinstance(value, list) else []):
        if not isinstance(entry, Mapping) or entry.get("name") is None or entry.get("value") is None:
            continue
        field = f"config.tags[{index}].name"
        found = tag_reference(
            str(entry["name"]),
            tags,
            model.origin,
            reference="agent.config.tags.name",
            field=field,
            artifact=model.key,
            subject=model.key,
        )
        written = entry["value"]
        if isinstance(found, Diagnostic):
            problems.append(found)
        elif not isinstance(written, (str, int, float)) or isinstance(written, bool):
            # An unquoted `no` is YAML 1.1's false; published as text it would read 'False'.
            problems.append(
                D(
                    "SST-PRS003",
                    artifact=model.key,
                    field=f"config.tags[{index}].value",
                    expected="a string",
                    found=type(written).__name__,
                    origin=model.origin,
                    subject=model.key,
                )
            )
        else:
            pairs.append((found, str(written)))
    return tuple(pairs), tuple(problems)

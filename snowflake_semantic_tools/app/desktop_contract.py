"""How CoCo Desktop reads a profile registry row, so a publish can prove what it will load.

This mirrors the installed Desktop runtime: it selects `* ... WHERE active = TRUE
ORDER BY config_name`, maps twelve columns, `JSON.parse`s every string value it
can, and treats an object as a pointer when it carries a `snowflake_stage` or
`source` string.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

DESKTOP_COLUMNS = (
    "CONFIG_NAME",
    "DESCRIPTION",
    "OWNER_TEAM",
    "VERSION",
    "SKILL_REPOS",
    "MCP_SERVERS",
    "COMMAND_REPOS",
    "SYSTEM_PROMPT_REPO",
    "HOOKS",
    "PLUGINS",
    "ENV_VARS",
    "SETTINGS_OVERRIDES",
)


def desktop_value(value: object) -> object:
    """Desktop parses a string as JSON when it can and keeps it as text otherwise."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def desktop_view(row: Mapping[str, object]) -> dict[str, object]:
    upper = {str(key).upper(): value for key, value in row.items()}
    return {column: desktop_value(upper.get(column)) for column in DESKTOP_COLUMNS}


def is_pointer(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    source = isinstance(value.get("source"), str)
    stage = isinstance(value.get("snowflake_stage"), str)
    ref = value.get("ref")
    return (source or stage) and (ref is None or isinstance(ref, str))


def stage_pointers(view: Mapping[str, object]) -> tuple[str, ...]:
    """Every stage location a Desktop client follows for one profile."""
    found: list[str] = []
    for column in ("SKILL_REPOS", "COMMAND_REPOS"):
        values = view.get(column)
        for item in values if isinstance(values, list) else ():
            if is_pointer(item) and isinstance(item.get("snowflake_stage"), str):
                found.append(str(item["snowflake_stage"]))
    for column in ("SYSTEM_PROMPT_REPO", "MCP_SERVERS"):
        value = view.get(column)
        if is_pointer(value) and isinstance(value, Mapping) and isinstance(value.get("snowflake_stage"), str):
            found.append(str(value["snowflake_stage"]))
    hooks = view.get("HOOKS")
    for groups in hooks.values() if isinstance(hooks, Mapping) else ():
        for group in groups if isinstance(groups, list) else ():
            entries = group.get("hooks") if isinstance(group, Mapping) else None
            for hook in entries if isinstance(entries, list) else ():
                source = hook.get("source") if isinstance(hook, Mapping) else None
                if isinstance(hook, Mapping) and hook.get("type") == "command" and isinstance(source, Mapping):
                    if isinstance(source.get("snowflake_stage"), str):
                        found.append(str(source["snowflake_stage"]))
    return tuple(found)

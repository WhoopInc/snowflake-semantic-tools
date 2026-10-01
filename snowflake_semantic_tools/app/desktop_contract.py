"""How CoCo Desktop reads a profile registry row, so a publish can prove what it will load.

This mirrors the installed Desktop runtime: it selects `* ... WHERE active = TRUE
ORDER BY config_name`, maps twelve columns, `JSON.parse`s every string value it
can, and treats an object as a pointer when it carries a `snowflake_stage` or
`source` string.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping

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
    """Return the twelve columns Desktop maps from a row, matched in any case and parsed as it parses them."""
    upper = {str(key).upper(): value for key, value in row.items()}
    return {column: desktop_value(upper.get(column)) for column in DESKTOP_COLUMNS}


def is_pointer(value: object) -> bool:
    """Report whether Desktop treats the value as a pointer it follows.

    A pointer is an object carrying a `source` or `snowflake_stage` string whose `ref`,
    when present, is a string too.
    """
    if not isinstance(value, Mapping):
        return False
    source = isinstance(value.get("source"), str)
    stage = isinstance(value.get("snowflake_stage"), str)
    ref = value.get("ref")
    return (source or stage) and (ref is None or isinstance(ref, str))


def stage_pointers(view: Mapping[str, object]) -> tuple[str, ...]:
    """Every stage location a Desktop client follows for one profile."""
    return (*_repo_stages(view), *_plugin_stages(view), *_single_stages(view), *_hook_stages(view))


def _repo_stages(view: Mapping[str, object]) -> Iterator[str]:
    for column in ("SKILL_REPOS", "COMMAND_REPOS"):
        for item in _listed(view.get(column)):
            stage = _pointer_stage(item)
            if stage is not None:
                yield stage


def _plugin_stages(view: Mapping[str, object]) -> Iterator[str]:
    # PLUGINS holds plain strings; Desktop fetches the ones naming a stage path.
    return (str(item) for item in _listed(view.get("PLUGINS")) if isinstance(item, str) and item.startswith("@"))


def _single_stages(view: Mapping[str, object]) -> Iterator[str]:
    for column in ("SYSTEM_PROMPT_REPO", "MCP_SERVERS"):
        stage = _pointer_stage(view.get(column))
        if stage is not None:
            yield stage


def _hook_stages(view: Mapping[str, object]) -> Iterator[str]:
    hooks = view.get("HOOKS")
    for groups in hooks.values() if isinstance(hooks, Mapping) else ():
        for group in _listed(groups):
            for hook in _listed(group.get("hooks") if isinstance(group, Mapping) else None):
                stage = _command_stage(hook)
                if stage is not None:
                    yield stage


def _pointer_stage(value: object) -> str | None:
    if is_pointer(value) and isinstance(value, Mapping) and isinstance(value.get("snowflake_stage"), str):
        return str(value["snowflake_stage"])
    return None


def _command_stage(hook: object) -> str | None:
    if not isinstance(hook, Mapping) or hook.get("type") != "command":
        return None
    source = hook.get("source")
    if isinstance(source, Mapping) and isinstance(source.get("snowflake_stage"), str):
        return str(source["snowflake_stage"])
    return None


def _listed(value: object) -> Iterable[object]:
    return value if isinstance(value, list) else ()

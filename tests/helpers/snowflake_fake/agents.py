"""The fake's versions of an agent the test models: what each ALTER AGENT of a comparison does.

Only an agent named in `modelled_agents` changes; every other ALTER AGENT is logged and has no
effect, as before. For a modeled agent, `agent_version_rows` is what SHOW VERSIONS IN AGENT
lists, the live version the row with no name, and each statement changes it as measured on a
live account (docs/design/snowflake-behavior.md):

- COMMIT COMMENT = '<c>' turns the live version into the next `VERSION$<n>`, carrying `<c>`;
  it fails with no live version.
- ADD VERSION FROM @<stage>/<dir>/ COMMENT = '<c>' commits the next `VERSION$<n>` from the
  staged `<dir>/agent_spec.yaml`; it fails beside a live version, or when nothing is staged.
- MODIFY VERSION <v> SET ALIAS = <a> sets the alias, which fails while another version holds
  it; MODIFY VERSION <v> UNSET ALIAS removes it. `agent_versions` resolves `alias:<a>`.
- ADD LIVE VERSION FROM LAST opens a live version holding the newest version's specification;
  it fails beside a live version.
- SET DEFAULT_VERSION = '<v>' pins the default to `<v>`, as DESCRIBE AGENT then reads it.
"""

from __future__ import annotations

import re
from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.world import SnowflakeWorld

_COMMIT = re.compile(r"COMMIT COMMENT = '((?:[^']|'')*)'$", re.IGNORECASE)
_ADD = re.compile(r"ADD VERSION FROM (@\S+?)/? COMMENT = '((?:[^']|'')*)'$", re.IGNORECASE)
_SET_ALIAS = re.compile(r"MODIFY VERSION (\S+) SET ALIAS = (\S+)$", re.IGNORECASE)
_UNSET_ALIAS = re.compile(r"MODIFY VERSION (\S+) UNSET ALIAS$", re.IGNORECASE)
_DEFAULT = re.compile(r"SET DEFAULT_VERSION = '([^']+)'$", re.IGNORECASE)
_NUMBER = re.compile(r"VERSION\$(\d+)", re.IGNORECASE)


def modeled_agent(world: SnowflakeWorld, normalized: str) -> str | None:
    """Return the agent an ALTER AGENT statement names, when the test models it; None otherwise."""
    if not normalized.upper().startswith("ALTER AGENT "):
        return None
    name = QualifiedName.parse(normalized.split()[2]).sql
    return name if name in world.modeled_agents else None


def apply_agent_statement(world: SnowflakeWorld, agent: str, normalized: str) -> None:
    """Change a modeled agent's versions as the statement does.

    Raises:
        SnowflakePortError: the statement fails on Snowflake, as the module docstring says.
    """
    clause = normalized.split(None, 3)[3]
    rows = list(world.agent_version_rows.get(agent, ()))
    live = next((row for row in rows if row.name is None), None)
    if (match := _COMMIT.match(clause)) is not None:
        if live is None:
            raise SnowflakePortError(f"Agent '{agent}' has no live version to commit.")
        rows[rows.index(live)] = replace(live, name=_next(rows), comment=_text(match.group(1)))
    elif (match := _ADD.match(clause)) is not None:
        if live is not None:
            raise SnowflakePortError(f"Cannot add a version to agent '{agent}' while it has a live version.")
        staged = world.staged_file_contents.get(f"{match.group(1)}/agent_spec.yaml")
        if staged is None:
            raise SnowflakePortError(f"No specification is staged at {match.group(1)}.")
        rows.append(AgentVersion(_next(rows), None, comment=_text(match.group(2)), specification=staged.decode()))
    elif (match := _SET_ALIAS.match(clause)) is not None:
        rows = _aliased(world, agent, rows, match.group(1), match.group(2))
    elif (match := _UNSET_ALIAS.match(clause)) is not None:
        for row in rows:
            if _same(row.name, match.group(1)) and row.alias:
                world.agent_versions.pop((agent, f"alias:{row.alias}".casefold()), None)
        rows = [replace(row, alias=None) if _same(row.name, match.group(1)) else row for row in rows]
    elif clause.upper() == "ADD LIVE VERSION FROM LAST":
        if live is not None:
            raise SnowflakePortError(f"Agent '{agent}' already has a live version.")
        newest = max((row for row in rows if row.name), key=lambda row: _number(row.name))
        rows.insert(0, AgentVersion(None, None, specification=newest.specification))
    elif (match := _DEFAULT.match(clause)) is not None:
        world.agent_default_settings[agent] = match.group(1)
        rows = [replace(row, is_default=_same(row.name, match.group(1))) for row in rows]
    world.agent_version_rows[agent] = tuple(rows)


def _aliased(
    world: SnowflakeWorld, agent: str, rows: list[AgentVersion], version: str, alias: str
) -> list[AgentVersion]:
    """Set `alias` on `version`, refusing an alias another version holds, as Snowflake does."""
    holder = next((row for row in rows if row.alias and row.alias.casefold() == alias.casefold()), None)
    if holder is not None and not _same(holder.name, version):
        raise SnowflakePortError(f"Alias '{alias}' is already set on version '{holder.name}'.")
    world.agent_versions[(agent, f"alias:{alias}".casefold())] = version.upper()
    return [replace(row, alias=alias.upper()) if _same(row.name, version) else row for row in rows]


def _next(rows: list[AgentVersion]) -> str:
    return f"VERSION${max((_number(row.name) for row in rows if row.name), default=0) + 1}"


def _number(name: str | None) -> int:
    match = _NUMBER.fullmatch(name or "")
    return int(match.group(1)) if match else 0


def _same(name: str | None, other: str) -> bool:
    return name is not None and name.casefold() == other.casefold()


def _text(quoted: str) -> str:
    return quoted.replace("''", "'")

"""Read what a planned change replaces, for `plan --capture-prior`, in a form it can be restored from.

Each kind is read the way it is published:

- A semantic view, Cortex search service, procedure, or function is replaced, so its prior is
  what GET_DDL prints: the CREATE OR REPLACE that re-creates it. A routine is addressed by
  its argument types.
- An agent is never replaced: an update adds a version and keeps every earlier one. GET_DDL
  would print a CREATE OR REPLACE AGENT of one version, which would drop the others and the
  agent's grants, so the prior is instead the version the agent serves now, read from SHOW
  VERSIONS IN AGENT: its specification, and the statements that point callers back at it.
- A stage tool is only ever created when absent, and GET_DDL cannot read a stage, so it has
  no prior to capture.
"""

from __future__ import annotations

import json
import re

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import ident, literal, qname, sql

# The object types apply never alters once they exist, so there is no prior to read.
_NEVER_ALTERED = frozenset(("STAGE",))
_VERSION_NUMBER = re.compile(r"VERSION\$(\d+)", re.IGNORECASE)
_UNLISTED = AgentVersions()


def prior_definition(
    port: CatalogPort,
    object_type: str,
    row: ShowRow,
    desired_artifact: RenderedArtifact | None,
    agent_versions: AgentVersions = _UNLISTED,
) -> tuple[str | None, tuple[Diagnostic, ...]]:
    """Read the definition of an object this plan may change; None when it is not one or cannot be read.

    An agent's prior comes from `agent_versions`, the SHOW VERSIONS IN AGENT read the
    observation already made for its update, so the agent is listed once.

    A read that fails is reported and leaves the definition None; the plan goes on.

    Diagnostics:
        SST-PLN037: the definition could not be read, or the agent lists no version.
    """
    if desired_artifact is None or object_type in _NEVER_ALTERED:
        return None, ()
    if object_type == "AGENT":
        prior = agent_prior(row.qualified_name, agent_versions.versions, desired_artifact.desired_alias)
        if prior is None:
            return None, (D("SST-PLN037", value=row.qualified_name.sql, detail="the agent lists no version"),)
        return prior, ()
    try:
        # Snowflake's docs do not say GET_DDL rewrites a semantic view; the server's output can fail
        # to re-execute as returned (docs/design/snowflake-behaviour.md).
        return port.get_ddl(object_type, row.qualified_name, desired_artifact.routine_signature), ()
    except SnowflakePortError as exc:
        return None, (D("SST-PLN037", value=row.qualified_name.sql, detail=str(exc)),)


def agent_prior(agent: QualifiedName, versions: tuple[AgentVersion, ...], alias: str | None) -> str | None:
    """Describe the version an agent serves now, and how to point callers back at it after apply.

    The served version is the one carrying `alias`, the alias apply moves, else the default
    version, else the last committed one, else the live version. The text names it, gives the
    ALTER AGENT statements that restore the alias and the default version as they are now,
    then the version's specification as indented JSON, as SST renders one. None when the
    agent lists no version.
    """
    named = tuple(version for version in versions if version.name is not None)
    aliased = next(
        (version for version in named if alias and version.alias and version.alias.casefold() == alias.casefold()),
        None,
    )
    default = next((version for version in named if version.is_default), None)
    last = max(named, key=_version_number, default=None)
    served = aliased or default or last or next(iter(versions), None)
    if served is None:
        return None
    lines = [f"-- agent {qname(agent)} serves {_described(served, last)}."]
    restore = []
    if aliased is not None and alias is not None:
        restore.append(_restore_alias(agent, aliased, alias))
    if default is not None:
        restore.append(_restore_default(agent, default))
    elif served.name is not None and aliased is None:
        restore.append(_restore_default(agent, served))
    if restore:
        lines.append("-- An update adds a version and keeps this one; to roll back after apply, run:")
        lines.extend(restore)
    else:
        lines.append("-- An update commits the live version before it adds one, so this one is kept.")
    if served.specification is None:
        lines.append(f"-- The role may not read the specification of {_label(served)}.")
    else:
        lines.append(f"-- The specification of {_label(served)}:")
        lines.append(_specification(served.specification))
    return "\n".join(lines) + "\n"


def _described(version: AgentVersion, last: AgentVersion | None) -> str:
    """Name a version with what makes it served: its alias, being the default, or being last."""
    roles = []
    if version.alias:
        roles.append(f"alias {version.alias}")
    if version.is_default:
        roles.append("the default")
    elif version is last:
        roles.append("the last committed")
    return _label(version) + (f" ({', '.join(roles)})" if roles else "")


def _label(version: AgentVersion) -> str:
    return version.name or "the live version"


def _restore_alias(agent: QualifiedName, version: AgentVersion, alias: str) -> str:
    statement = sql(
        "ALTER AGENT {agent} MODIFY VERSION {version} SET ALIAS = {alias};",
        agent=qname(agent),
        version=ident(Identifier.parse(str(version.name))),
        alias=ident(Identifier.parse(alias)),
    )
    return str(statement)


def _restore_default(agent: QualifiedName, version: AgentVersion) -> str:
    statement = sql(
        "ALTER AGENT {agent} SET DEFAULT_VERSION = {version};",
        agent=qname(agent),
        version=literal(str(version.name)),
    )
    return str(statement)


def _version_number(version: AgentVersion) -> int:
    match = _VERSION_NUMBER.fullmatch(version.name or "")
    return int(match.group(1)) if match else -1


def _specification(text: str) -> str:
    """Print a specification as indented JSON, as SST renders one; text that is not JSON as it is."""
    try:
        return json.dumps(json.loads(text), indent=2, ensure_ascii=False)
    except ValueError:
        return text.rstrip("\n")

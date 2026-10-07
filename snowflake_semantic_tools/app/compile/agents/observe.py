"""Check compiled agents and tools against the live objects they publish over or call.

Connected validation runs `ObserveLiveObjects` once Snowflake is reachable. It only reads --
SHOW, DESCRIBE, SHOW GRANTS and SHOW PARAMETERS through the `CatalogPort` -- and the
comparisons are pure, in `domain.validate.agent_live`. A read Snowflake refuses is no
evidence either way, so the check that needed it reports nothing -- except that an extension
the project consumes, which Snowflake reports does not exist or is not authorized, is one
the agent cannot reach, and that is reported.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TypeVar

from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent
from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import SessionFailure, session_failure
from snowflake_semantic_tools.domain.model.agent import AgentSkill, ResolvedAgentTool
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from snowflake_semantic_tools.domain.model.tool import ToolKind
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.agent import render_agent_spec
from snowflake_semantic_tools.domain.validate.agent_live import (
    first_difference,
    live_signature,
    live_spec,
    signature_disagreement,
)

_Read = TypeVar("_Read")
# The object a tool's resources name, by tool type: the resource key and the type SHOW lists it as.
_EXTERNAL = {
    "cortex_search": ("search_service", "CORTEX SEARCH SERVICE"),
    "generic": ("identifier", None),
    "agent": ("identifier", "AGENT"),
}
_SHARED_GRANTEES = frozenset(("SHARE", "APPLICATION_ROLE", "APPLICATION ROLE"))
# The skill reference resolver of an extension this project consumes rather than publishes.
_CONSUMED = "extension"
# A version selector that names no fixed version, which SST-VAL538 refuses offline.
_MOVING_VERSION = "live"
_HOLDS = frozenset(("USAGE", "OWNERSHIP"))


class ObserveLiveObjects:
    """Read what Snowflake holds for each compiled agent and tool, and report where it disagrees.

    Args:
        target: The `profiles.yml` target being validated, which SST-VAL531 names.
    """

    def __init__(self, port: CatalogPort, *, target: str) -> None:
        self._port = port
        self._target = target

    def run(self, compiled: CompileResult) -> tuple[Diagnostic, ...]:
        """Check every compiled agent, then every compiled tool, in compile order.

        Diagnostics:
            SST-VAL506: a secure agent's live object is owned by a role other than the session's.
            SST-VAL507: a live secure agent granted to a share or application role is authored
                non-secure.
            SST-VAL508: an agent's tag names no tag object.
            SST-VAL509: an agent SST last published unchanged no longer matches its rendered spec.
            SST-VAL533: a generic tool's resource key is not confirmed by a live spec.
            SST-VAL531: an object a tool calls, which SST does not publish, does not exist;
                or an extension the agent consumes, or the version it pins, does not exist
                or is not visible to the session.
            SST-VAL532: such a routine's live signature disagrees with the tool's input schema.
            SST-VAL616: the session or a consuming role lacks USAGE on such an object.
            SST-VAL534: a tool's query timeout exceeds its warehouse's statement timeout.
            SST-VAL542: the session or a consuming role lacks READ on an extension the agent pins.
            SST-VAL525: a search tool marks searchable a vector column of the relation indexed.
            SST-VAL613: a search service declares a column its live service does not have.
            SST-VAL618: a search service's source relation has change tracking off.
            SST-VAL619: a search service being replaced holds explicit grants to replay.
        """
        role = self._read(self._port.current_role) or ""
        tools = tuple(item for item in compiled.compiled if isinstance(item, CompiledTool))
        sources = {tool.artifact_key: relation for tool in tools for _, relation in tool.dbt_relations}
        found: list[Diagnostic] = []
        for agent in (item for item in compiled.compiled if isinstance(item, CompiledAgent)):
            found.extend(self._agent(agent, role, sources))
        for tool in tools:
            found.extend(self._tool(tool))
        return tuple(found)

    def _read(self, read: Callable[[], _Read]) -> _Read | None:
        try:
            return read()
        except SnowflakePortError:
            return None

    def _exists(self, object_type: str, qualified: QualifiedName) -> bool | None:
        """Report whether an object exists; None when the lookup failed, which proves nothing."""
        return self._read(lambda: self._port.object_exists(object_type, qualified))

    def _agent(self, agent: CompiledAgent, role: str, sources: Mapping[str, str]) -> list[Diagnostic]:
        """Check one agent's live object, then each tool and skill it reaches."""
        model, name, subject = agent.resolved.model, agent.resolved.model.name, agent.artifact_key
        live = self._read(lambda: self._port.show_row("AGENT", agent.target))
        found: list[Diagnostic] = []
        owner_only = model.secure and live is not None and live.get("owner", "").upper() != role.upper()
        if owner_only:
            found.append(D("SST-VAL506", artifact=name, subject=subject))
        grants = self._read(lambda: self._port.show_grants("AGENT", agent.target)) if live is not None else ()
        if live is not None and not model.secure and live.get("is_secure", "").casefold() in ("true", "y", "yes"):
            shared = sorted(
                {grant.grantee_name for grant in grants or () if grant.granted_to.upper() in _SHARED_GRANTEES}
            )
            if shared:
                found.append(D("SST-VAL507", artifact=name, value=", ".join(shared), subject=subject))
        found.extend(self._tags(agent))
        spec = None if live is None or owner_only else self._live_spec(agent)
        found.extend(self._round_trip(agent, spec))
        consumers = (role, *(grant.grantee_name for grant in grants or () if _uses(grant)))
        for tool in agent.resolved.tools:
            found.extend(self._agent_tool(agent, tool, spec, consumers, sources))
        for skill in model.skills:
            found.extend(self._extension(agent, skill, consumers))
        return found

    def _tags(self, agent: CompiledAgent) -> list[Diagnostic]:
        """Report each tag the agent sets that names no tag object; a tag lookup that fails reports nothing."""
        found: list[Diagnostic] = []
        for tag, _ in agent.resolved.model.tags:
            qualified = _qualified(tag, agent.target)
            if qualified is not None and self._exists("TAG", qualified) is False:
                found.append(D("SST-VAL508", artifact=agent.name, field=tag, subject=agent.artifact_key))
        return found

    def _live_spec(self, agent: CompiledAgent) -> Mapping[str, object] | None:
        properties = self._read(lambda: self._port.describe_properties("AGENT", agent.target))
        return live_spec(properties.get("agent_spec", "")) if properties is not None else None

    def _round_trip(self, agent: CompiledAgent, spec: Mapping[str, object] | None) -> list[Diagnostic]:
        """Compare the live spec with the rendered one, when the live object's marker says nothing changed.

        With a change pending the two differ by design; the comparison only means something
        when SST believes the live agent already carries this definition.
        """
        if spec is None:
            return []
        marker = self._read(lambda: self._port.describe_marker(agent.target, "AGENT"))
        if marker is None or marker.fingerprint != agent.definition_fingerprint:
            return []
        difference = first_difference(render_agent_spec(agent.resolved.model, agent.resolved.tools), spec)
        if difference is None:
            return []
        return [D("SST-VAL509", artifact=agent.name, value=difference, subject=agent.artifact_key)]

    def _agent_tool(
        self,
        agent: CompiledAgent,
        tool: ResolvedAgentTool,
        spec: Mapping[str, object] | None,
        consumers: tuple[str, ...],
        sources: Mapping[str, str],
    ) -> list[Diagnostic]:
        """Check one tool: its resource key, the external object it calls, its timeout, its columns."""
        found: list[Diagnostic] = []
        if tool.type == "generic" and not _confirmed(spec, tool.name, "identifier"):
            found.append(D("SST-VAL533", artifact=agent.name, key="identifier", subject=agent.artifact_key))
        if tool.external:
            found.extend(self._external(agent, tool, consumers))
        found.extend(self._timeout(agent, tool))
        if tool.type == "cortex_search":
            source = next((sources[key] for key in tool.depends_on if key in sources), None)
            found.extend(self._vector_columns(agent, tool, source))
        return found

    def _external(self, agent: CompiledAgent, tool: ResolvedAgentTool, consumers: tuple[str, ...]) -> list[Diagnostic]:
        """Look up the object an external tool calls: that it exists, its signature, and who may use it."""
        key, object_type = _EXTERNAL.get(tool.type, ("", None))
        qualified = _qualified(str(tool.resources.get(key, "")), None)
        object_type = object_type or str(tool.resources.get("type", "")).upper()
        if qualified is None or not object_type:
            return []
        exists = self._exists(object_type, qualified)
        if exists is False:
            return [
                D(
                    "SST-VAL531",
                    artifact=agent.name,
                    value=qualified.sql,
                    target=self._target,
                    subject=agent.artifact_key,
                )
            ]
        if not exists:
            return []
        found: list[Diagnostic] = []
        if tool.type == "generic":
            row = self._read(lambda: self._port.show_row(object_type, qualified))
            types = live_signature(row.get("arguments", "")) if row is not None else None
            disagreement = signature_disagreement(types, tool.input_schema) if types is not None else None
            if disagreement is not None:
                found.append(
                    D(
                        "SST-VAL532",
                        artifact=agent.name,
                        value=qualified.sql,
                        found=disagreement[0],
                        expected=disagreement[1],
                        subject=agent.artifact_key,
                    )
                )
        grants = self._read(lambda: self._port.show_grants(object_type, qualified))
        if grants is not None:
            holders = {grant.grantee_name.upper() for grant in grants if grant.privilege.upper() in _HOLDS}
            for consumer in dict.fromkeys(role for role in consumers if role and role.upper() not in holders):
                found.append(
                    D(
                        "SST-VAL616",
                        name=tool.member or tool.name,
                        value=consumer,
                        detail=f"USAGE on {object_type} {qualified.sql}",
                        subject=agent.artifact_key,
                    )
                )
        return found

    def _timeout(self, agent: CompiledAgent, tool: ResolvedAgentTool) -> list[Diagnostic]:
        """Compare a tool's query timeout with the statement timeout its warehouse enforces."""
        environment = tool.resources.get("execution_environment")
        if not isinstance(environment, Mapping):
            return []
        warehouse, timeout = environment.get("warehouse"), environment.get("query_timeout")
        if not isinstance(warehouse, str) or not isinstance(timeout, int):
            return []
        limit = self._read(lambda: self._port.object_parameter("WAREHOUSE", warehouse, "STATEMENT_TIMEOUT_IN_SECONDS"))
        # Zero is no limit at all.
        if limit is None or not limit.isdigit() or int(limit) == 0 or timeout <= int(limit):
            return []
        return [D("SST-VAL534", artifact=agent.name, found=timeout, expected=int(limit), subject=agent.artifact_key)]

    def _vector_columns(self, agent: CompiledAgent, tool: ResolvedAgentTool, source: str | None) -> list[Diagnostic]:
        """Report each searchable column of a search tool that is a vector column of the indexed table."""
        columns = tool.resources.get("columns_and_descriptions")
        qualified = _qualified(source or "", None)
        if not isinstance(columns, Mapping) or qualified is None:
            return []
        live = self._read(lambda: self._port.table_columns(qualified)) or ()
        vectors = {name.casefold() for name, kind in live if kind.upper().startswith("VECTOR")}
        return [
            D("SST-VAL525", artifact=agent.name, name=tool.name, column=str(column), subject=agent.artifact_key)
            for column, descriptor in columns.items()
            if isinstance(descriptor, Mapping)
            and descriptor.get("searchable") is True
            and str(column).casefold() in vectors
        ]

    def _extension(self, agent: CompiledAgent, skill: AgentSkill, consumers: tuple[str, ...]) -> list[Diagnostic]:
        """Check an extension the agent pins: who may read it and, when consumed, that it exists.

        Reports each consuming role, the session's included, with no READ on it. An extension
        the project publishes may not exist before its first apply, so only a consumed one is
        reported absent: when SHOW GRANTS says it does not exist or is not authorized -- a
        missing schema reads the same -- or when it lists no version or alias the agent pins.
        """
        qualified = _qualified(skill.path, None)
        if qualified is None:
            return []
        try:
            grants = self._port.show_grants("CORTEX EXTENSION", qualified)
        except SnowflakePortError as exc:
            if skill.ref == _CONSUMED and _not_visible(exc):
                return [self._absent(agent, qualified.sql)]
            return []
        holders = {grant.grantee_name.upper() for grant in grants if grant.privilege.upper() in ("READ", "OWNERSHIP")}
        found = [
            D("SST-VAL542", artifact=agent.name, value=consumer, name=qualified.sql, subject=agent.artifact_key)
            for consumer in dict.fromkeys(role for role in consumers if role and role.upper() not in holders)
        ]
        found.extend(self._pinned_version(agent, skill, qualified))
        return found

    def _pinned_version(self, agent: CompiledAgent, skill: AgentSkill, qualified: QualifiedName) -> list[Diagnostic]:
        """Report a consumed extension's pinned version that SHOW VERSIONS lists by neither name nor alias."""
        pinned = skill.version.strip()
        if skill.ref != _CONSUMED or not pinned or pinned.casefold() == _MOVING_VERSION:
            return []
        versions = self._read(lambda: self._port.extension_versions(qualified))
        if versions is None:
            return []
        listed = {version.name.casefold() for version in versions}
        listed.update(alias.strip().casefold() for version in versions for alias in (version.alias or "").split(","))
        if pinned.casefold() in listed:
            return []
        return [self._absent(agent, f"{qualified.sql} version {pinned}")]

    def _absent(self, agent: CompiledAgent, value: str) -> Diagnostic:
        return D("SST-VAL531", artifact=agent.name, value=value, target=self._target, subject=agent.artifact_key)

    def _tool(self, tool: CompiledTool) -> list[Diagnostic]:
        """Check one compiled search service against its live service, its source, and its grants."""
        member, target, subject = tool.member, tool.rendered_artifact.target, tool.artifact_key
        if member.type != ToolKind.CORTEX_SEARCH_SERVICE.value:
            return []
        found: list[Diagnostic] = []
        properties = self._read(lambda: self._port.describe_properties("CORTEX SEARCH SERVICE", target))
        if properties is not None and properties.get("columns"):
            live = {column.strip().casefold() for column in properties["columns"].split(",")}
            found.extend(
                D("SST-VAL613", name=member.name, column=column.name, subject=subject)
                for column in member.columns
                if column.name.casefold() not in live
            )
        for _, relation in tool.dbt_relations:
            found.extend(self._change_tracking(tool, relation))
        if properties is not None:
            grants = self._read(lambda: self._port.show_grants("CORTEX SEARCH SERVICE", target)) or ()
            explicit = sum(grant.is_explicit for grant in grants)
            if explicit:
                found.append(D("SST-VAL619", name=member.name, detail=explicit, subject=subject))
        return found

    def _change_tracking(self, tool: CompiledTool, relation: str) -> list[Diagnostic]:
        """Report a source table or view whose change tracking is off, unless the service refreshes FULL."""
        qualified = _qualified(relation, None)
        if qualified is None or (tool.member.refresh_mode or "").upper() == "FULL":
            return []
        row = self._read(lambda: self._port.show_row("TABLE", qualified)) or self._read(
            lambda: self._port.show_row("VIEW", qualified)
        )
        if row is None or row.get("change_tracking", "").upper() != "OFF":
            return []
        return [D("SST-VAL618", value=qualified.sql, name=tool.member.name, subject=tool.artifact_key)]


def _not_visible(error: SnowflakePortError) -> bool:
    """Report whether Snowflake refused a read because the object does not exist or is not granted."""
    return session_failure(str(error), errno=error.errno, sqlstate=error.sqlstate) is SessionFailure.NOT_VISIBLE


def _uses(grant: GrantRow) -> bool:
    return grant.privilege.upper() == "USAGE" and grant.granted_to.upper() == "ROLE"


def _confirmed(spec: Mapping[str, object] | None, tool: str, key: str) -> bool:
    """Report whether a live spec carries `key` in the tool's resources, which settles the key's name."""
    resources = spec.get("tool_resources") if spec is not None else None
    entry = resources.get(tool) if isinstance(resources, Mapping) else None
    return isinstance(entry, Mapping) and key in entry


def _qualified(name: str, beside: QualifiedName | None) -> QualifiedName | None:
    """Parse a three-part name, or qualify a single name in `beside`'s schema; None when neither works."""
    try:
        return QualifiedName.parse(name)
    except ValueError:
        pass
    if beside is None or not name:
        return None
    try:
        return QualifiedName.from_parts(beside.database.folded, beside.schema.folded, name)
    except ValueError:
        return None

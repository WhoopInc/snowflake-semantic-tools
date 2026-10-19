"""Scratch copies of what an agent calls, so `sst ask` runs it on your edits, publishing nothing.

An agent names its views, tools, skills and sub-agents by their published names. Each of them
that compiles differently from what the target's state records -- the set SST-ASK001 warns of
-- is copied into the agent's scratch schema as `<NAME>__SST_ASK_<id>`, and the agent's
specification is pointed at the copies (`domain.ask.spec`). A sub-agent is copied when it is
edited or anything it calls is, its own specification pointed at the copies too, so an edit
reaches the agent through every agent between them. Copies are made in publish order -- views,
tools, skills, then sub-agents, deepest first -- and dropped in reverse when the session ends.

Nothing is recorded in state, and no copy carries an ownership marker: a view or tool keeps its
own comment. What a session that died left behind is found by its name and its age (`sweep`).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from snowflake_semantic_tools.app.ask import compiled_agents, unpublished_reason
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.compile.skills import CompiledExtension
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.app.lifecycle.composite import create_sse_stage_sql
from snowflake_semantic_tools.domain.ask.spec import Rename, rewrite_spec
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ORIGIN, AgentStreamPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort
from snowflake_semantic_tools.domain.sql import (
    Sql,
    dollar_quoted,
    ident,
    keyword,
    literal,
    qname,
    renamed,
    routine_ref,
    scope,
    sql,
    stage_path,
)
from snowflake_semantic_tools.domain.state import AppliedEntry

# What every scratch copy's name ends with, before the session's id.
SCRATCH_MARK = "__SST_ASK_"
# How old a copy or thread must be before a later session removes it as left behind.
DEFAULT_TTL = timedelta(hours=12)
# The object types a copy can be, as SHOW lists them, with the keyword DROP takes.
_SWEPT = (
    ("AGENTS", "AGENT"),
    ("CORTEX EXTENSIONS", "CORTEX EXTENSION"),
    ("CORTEX SEARCH SERVICES", "CORTEX SEARCH SERVICE"),
    ("SEMANTIC VIEWS", "SEMANTIC VIEW"),
    ("STAGES", "STAGE"),
    ("USER FUNCTIONS", "FUNCTION"),
    ("USER PROCEDURES", "PROCEDURE"),
)
_ARGUMENTS = re.compile(r"\((?P<types>[^)]*)\)")


class ScratchPort(ExecutionPort, StagePort, Protocol):
    """What building and removing scratch copies uses: statements, reads, and uploads."""


@dataclass(frozen=True, slots=True)
class ScratchCopy:
    """One scratch object a session made, and how to drop it.

    Attributes:
        key: The artifact it copies; empty for a skill copy's upload stage.
    """

    key: str
    name: QualifiedName
    drop: Sql


@dataclass(frozen=True, slots=True)
class Edited:
    """What an agent calls that a run must copy, in the order the copies are made."""

    artifacts: tuple[CompiledArtifact, ...]

    @property
    def keys(self) -> tuple[str, ...]:
        """Return each artifact's key, in copy order."""
        return tuple(item.artifact_key for item in self.artifacts)


def edited_dependencies(
    agent: CompiledAgent, compiled: CompileResult, applied: Mapping[str, AppliedEntry] | None
) -> Edited:
    """Return what `agent` reaches that a run must copy: each edited dependency, and the agents between.

    A dependency is edited when `unpublished_reason` gives a reason: not published to the
    target, published from another compile, or state unreadable. A sub-agent is copied when it
    is edited or something it reaches is.
    """
    artifacts = {item.artifact_key: item for item in compiled.compiled}
    agents = {item.artifact_key: item for item in compiled_agents(compiled)}
    edited: dict[str, CompiledArtifact] = {}

    def edited_in(key: str, path: tuple[str, ...]) -> bool:
        artifact = artifacts.get(key)
        if artifact is None or key in path:
            return False
        rendered = artifact.rendered_artifact
        own = unpublished_reason(applied, key, rendered.fingerprint, rendered.target) is not None
        # Every child is visited, not just up to the first edited one, so each agent below is collected.
        children = agents[key].resolved.depends_on if key in agents else ()
        visited = [edited_in(child, (*path, key)) for child in children]
        below = any(visited)
        if own or below:
            edited.setdefault(key, artifact)
        return own or below

    for key in agent.resolved.depends_on:
        edited_in(key, (agent.artifact_key,))
    ordered = sorted(edited.values(), key=lambda item: (_copy_rank(item), _depth(item, agents)))
    return Edited(tuple(ordered))


def _copy_rank(artifact: CompiledArtifact) -> int:
    """Rank a copy by type: views, then tools, skills, and agents last."""
    if isinstance(artifact, CompiledView):
        return 0
    if isinstance(artifact, CompiledTool):
        return 1
    if isinstance(artifact, CompiledExtension):
        return 2
    return 3


def _depth(artifact: CompiledArtifact, agents: Mapping[str, CompiledAgent]) -> int:
    """Return how many agents deep an agent's calls go, itself included: the one it calls is copied first."""

    def depth(key: str, path: tuple[str, ...]) -> int:
        if key not in agents or key in path:
            return 0
        return 1 + max((depth(child, (*path, key)) for child in agents[key].resolved.depends_on), default=0)

    return depth(artifact.artifact_key, ())


def scratch_name(published: QualifiedName, where: SchemaScope, session: str) -> QualifiedName:
    """Return a copy's name: the published object's, marked with the session, in the scratch schema."""
    stem = published.name.value if published.name.quoted else published.name.folded
    name = f"{stem}{SCRATCH_MARK}{session.upper()}"
    return QualifiedName(where.database, where.schema, Identifier(name, quoted=published.name.quoted))


@dataclass
class ScratchSession:
    """The scratch copies one `sst ask` session made, built in copy order and dropped in reverse.

    Args:
        where: The scratch schema, `agents.+scratch_schema` or the agent's own.
        session: The session's id, which every copy's name ends with.
        progress: Told what is being built, as a line, before each copy.
    """

    port: ScratchPort
    where: SchemaScope
    session: str
    progress: Callable[[str], None] = lambda line: None
    copies: list[ScratchCopy] = field(default_factory=list)
    renames: dict[str, Rename] = field(default_factory=dict)

    def build(self, edited: Edited) -> Diagnostic | None:
        """Make a copy of each edited artifact, in order; on the first refusal, drop what was made.

        Diagnostics:
            SST-ASK008: Snowflake refused a copy's statement; the copies made before are dropped.
        """
        for artifact in edited.artifacts:
            target = artifact.rendered_artifact.target
            copy = scratch_name(target, self.where, self.session)
            self.progress(f"copying {artifact.artifact_key} to {copy.sql}")
            try:
                version = self._build_one(artifact, copy)
            except (SnowflakePortError, ValueError) as exc:
                self.drop()
                return D("SST-ASK008", artifact=artifact.artifact_key, value=copy.sql, detail=str(exc).splitlines()[0])
            self.renames[target.sql.casefold()] = Rename(copy.sql, version)
        return None

    def _build_one(self, artifact: CompiledArtifact, copy: QualifiedName) -> str | None:
        """Create one copy; return a skill copy's version alias, else None.

        Raises:
            SnowflakePortError: Snowflake refused a statement, or an upload failed.
        """
        if isinstance(artifact, CompiledExtension):
            return self._build_extension(artifact, copy)
        if isinstance(artifact, CompiledAgent):
            spec, _ = rewrite_spec(artifact.payload, self.renames)
            statements: Sequence[Sql] = (
                sql(
                    "CREATE OR REPLACE AGENT {name} FROM SPECIFICATION {spec}",
                    name=qname(copy),
                    spec=dollar_quoted(spec),
                ),
            )
            drop = sql("DROP AGENT IF EXISTS {name}", name=qname(copy))
        else:
            rendered = artifact.rendered_artifact
            statements = tuple(renamed(statement, rendered.target, copy) for statement in rendered.statements)
            drop = _drop(rendered.object_type, copy, rendered.routine_signature)
        self._run(artifact.artifact_key, copy, statements, drop)
        return None

    def _build_extension(self, artifact: CompiledExtension, copy: QualifiedName) -> str:
        """Upload a skill's bundle to a stage of the session's own, then create the copy from it."""
        release = artifact.release
        stage = QualifiedName(
            self.where.database,
            self.where.schema,
            Identifier.parse(f"SKILL_BUNDLE{SCRATCH_MARK}{self.session.upper()}"),
        )
        if not any(item.name.folded == stage.folded for item in self.copies):
            self._run(
                "", stage, (create_sse_stage_sql(stage),), sql("DROP STAGE IF EXISTS {stage}", stage=qname(stage))
            )
        prefix = f"@{stage.sql}/{release.bundle.name}/{release.alias}/"
        for entry in release.bundle.entries:
            self.port.upload(f"{prefix}{entry.path}", entry.content)
        create = sql(
            "CREATE CORTEX EXTENSION IF NOT EXISTS {name} FROM {location} VERSION {alias} "
            "TYPE = {kind} COMMENT = {comment}",
            name=qname(copy),
            location=stage_path(stage, f"{release.bundle.name}/{release.alias}/"),
            alias=ident(Identifier.parse(release.alias)),
            kind=literal(release.extension_type),
            comment=literal(f"sst ask scratch copy of {release.target.sql}"),
        )
        self._run(
            artifact.artifact_key, copy, (create,), sql("DROP CORTEX EXTENSION IF EXISTS {name}", name=qname(copy))
        )
        return release.alias

    def _run(self, key: str, name: QualifiedName, statements: Sequence[Sql], drop: Sql) -> None:
        """Run a copy's statements, recording it to drop first, so a half-made one is dropped too."""
        self.copies.append(ScratchCopy(key, name, drop))
        result = self.port.execute_script(statements)
        if not result.ok:
            message = result.error.message if result.error else "no error reported"
            raise SnowflakePortError(message)

    def drop(self) -> tuple[Diagnostic, ...]:
        """Drop every copy, newest first; one that will not drop is reported, and the rest still dropped.

        Diagnostics:
            SST-ASK009: a copy could not be dropped.
        """
        left: list[Diagnostic] = []
        while self.copies:
            copy = self.copies.pop()
            result = self.port.try_execute(copy.drop)
            if not result.ok:
                left.append(
                    D("SST-ASK009", value=copy.name.sql, detail=result.error.message if result.error else "unknown")
                )
        self.renames.clear()
        return tuple(left)


def _drop(object_type: str, name: QualifiedName, signature: tuple[str, ...]) -> Sql:
    """Return the DROP of a copy of `object_type`: a routine by its argument types."""
    if object_type in ("FUNCTION", "PROCEDURE"):
        return sql("DROP {kind} IF EXISTS {routine}", kind=keyword(object_type), routine=routine_ref(name, signature))
    return sql("DROP {kind} IF EXISTS {name}", kind=keyword(object_type), name=qname(name))


@dataclass(frozen=True, slots=True)
class Swept:
    """What a sweep removed, and what it could not."""

    removed: tuple[str, ...] = ()
    diagnostics: tuple[Diagnostic, ...] = ()


def sweep(
    port: ScratchPort,
    stream: AgentStreamPort | None,
    where: Iterable[SchemaScope],
    *,
    now: datetime,
    ttl: timedelta = DEFAULT_TTL,
    keep: str = "",
) -> Swept:
    """Remove the scratch copies and threads older sessions left: older than `ttl`, never session `keep`'s.

    A copy is found by `SCRATCH_MARK` in its name, in each scratch schema, and its age by
    SHOW's `created_on`; a thread by its origin and its `updated_on`. `ttl` of zero removes
    every one but `keep`'s, as `sst ask --cleanup` asks.

    Diagnostics:
        SST-ASK009: a copy or thread could not be removed, or a schema could not be read.
    """
    removed: list[str] = []
    left: list[Diagnostic] = []
    cutoff = now - ttl
    for schema in dict.fromkeys(where):
        for plural, singular in _SWEPT:
            try:
                shown = port.query(
                    sql(
                        "SHOW {kind} LIKE {pattern} IN SCHEMA {schema}",
                        kind=keyword_plural(plural),
                        pattern=literal(f"%{SCRATCH_MARK}%"),
                        schema=scope(schema),
                    )
                )
            except SnowflakePortError as exc:
                left.append(D("SST-ASK009", value=f"{plural.lower()} in {schema.sql}", detail=str(exc).splitlines()[0]))
                continue
            for row in (dict(zip(shown.columns, values, strict=False)) for values in shown.rows):
                name = str(row.get("name") or "")
                if (
                    SCRATCH_MARK not in name
                    or (keep and name.endswith(keep.upper()))
                    or not _older(row.get("created_on"), cutoff)
                ):
                    continue
                copy = QualifiedName(schema.database, schema.schema, Identifier(name, quoted=name != name.upper()))
                drop = _swept_drop(singular, copy, str(row.get("arguments") or ""))
                if drop is None:
                    continue
                result = port.try_execute(drop)
                if result.ok:
                    removed.append(copy.sql)
                else:
                    left.append(
                        D("SST-ASK009", value=copy.sql, detail=result.error.message if result.error else "unknown")
                    )
    if stream is not None:
        removed.extend(_swept_threads(stream, now, ttl, left))
    return Swept(tuple(removed), tuple(left))


def keyword_plural(value: str) -> Sql:
    """Return a SHOW keyword: `USER FUNCTIONS` and `USER PROCEDURES` as written, else a type's plural."""
    if value == "USER FUNCTIONS":
        return sql("USER FUNCTIONS")
    if value == "USER PROCEDURES":
        return sql("USER PROCEDURES")
    return keyword(value.removesuffix("S"), plural=True)


def _swept_drop(kind: str, name: QualifiedName, arguments: str) -> Sql | None:
    """Return the DROP of a left-behind copy; a routine's types read from SHOW's `arguments`."""
    if kind not in ("FUNCTION", "PROCEDURE"):
        return _drop(kind, name, ())
    found = _ARGUMENTS.search(arguments)
    if found is None:
        return None
    types = tuple(part.strip() for part in found["types"].split(",") if part.strip())
    try:
        return _drop(kind, name, types)
    except ValueError:
        return None


def _older(created: object, cutoff: datetime) -> bool:
    """Report whether a SHOW `created_on` is before `cutoff`; a value that is no time is not."""
    if not isinstance(created, datetime):
        return False
    stamp = created if created.tzinfo else created.replace(tzinfo=UTC)
    return stamp < cutoff


def _swept_threads(stream: AgentStreamPort, now: datetime, ttl: timedelta, left: list[Diagnostic]) -> list[str]:
    """Delete the threads SST opened that have not changed in `ttl`; report each that will not go."""
    removed: list[str] = []
    try:
        threads = stream.list_threads(ORIGIN)
    except SnowflakePortError as exc:
        left.append(D("SST-ASK009", value="the sst_ask threads", detail=str(exc).splitlines()[0]))
        return removed
    cutoff = (now - ttl).timestamp() * 1000
    for thread in threads:
        if thread.updated_on >= cutoff:
            continue
        try:
            stream.delete_thread(thread.thread_id)
        except SnowflakePortError as exc:
            left.append(D("SST-ASK009", value=f"thread {thread.thread_id}", detail=str(exc).splitlines()[0]))
            continue
        removed.append(f"thread {thread.thread_id}")
    return removed

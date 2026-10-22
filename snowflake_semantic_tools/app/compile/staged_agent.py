"""The statements that publish one AGENT object from its specification, staged or temporary.

An agent and a CoWork profile are both AGENT objects, published by the same programs: CREATE
AGENT from a staged specification, a committed version per change, a live version beside it,
then the alias, tags, profile, and SECURE. This module owns those programs and the rendered
artifact that carries them; each kind supplies its key, specification, and metadata. It never
decides what a specification holds.

CoWork profiles are a Snowflake private preview (PrPr):
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    DesiredMetadata,
    ProbeKind,
    PublishShape,
    RenderedArtifact,
    SmokeProbe,
    StatementPlan,
    Upload,
    version_comment,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.render.agent import render_agent_profile
from snowflake_semantic_tools.domain.sql import (
    Sql,
    boolean,
    dollar_quoted,
    ident,
    join,
    literal,
    qname,
    sql,
    stage_path,
)
from snowflake_semantic_tools.domain.validate.agent import plain_alias


@dataclass(frozen=True, slots=True)
class AgentObjectMetadata:
    """The properties an AGENT object carries beside its specification, as its kind declares them.

    Attributes:
        alias: The version alias to move to the last committed version; None sets none.
        tags: `(tag name, value)` pairs, set in this order.
        profile: The display block `SET PROFILE` writes.
        secure: None and False both publish `SECURE = FALSE`.
    """

    alias: str | None
    tags: tuple[tuple[str, str], ...]
    profile: AgentProfile
    secure: bool | None


@dataclass(frozen=True, slots=True)
class StagedAgentObject:
    """One AGENT object to render: what is published, where, and from which stage directory.

    Attributes:
        stage_path: `<stage>/<folder>/<git sha>`; empty until publication sets it, and an
            object without one has metadata statements only.
        stage: The stage `stage_path` starts with; None until publication sets it.
        folder: The directory below the stage that holds this object's versions; a kind
            whose names could meet another kind's gives it a prefix of its own.
        definition_fingerprint: Replaces the rendered artifact's own fingerprint, so a change
            to any property the kind counts is a change.
    """

    key: str
    artifact_type: str
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    spec_fingerprint: str
    metadata: AgentObjectMetadata
    depends_on: tuple[str, ...]
    references: tuple[str, ...]
    folder: str
    stage_path: str = ""
    stage: QualifiedName | None = None
    temporary: bool = False
    dirty: bool = False

    @property
    def location(self) -> tuple[Sql, str] | None:
        """The stage directory the specification is staged in, with its git sha; None when unstaged."""
        if self.stage is None or not self.stage_path:
            return None
        git_sha = self.stage_path.rsplit("/", 1)[-1]
        return stage_path(self.stage, f"{self.folder}/{git_sha}/"), git_sha


def staged_path(stage: QualifiedName, folder: str, git_sha: str) -> str:
    """Return the stage path an AGENT object's specification is staged under for one commit."""
    return f"{stage.sql}/{folder}/{git_sha}"


def render_agent_object(staged: StagedAgentObject, comment: str | None) -> RenderedArtifact:
    """Render an AGENT object; `comment` is the marked comment to publish, None for no metadata.

    Raises:
        ValueError: as `agent_programs` does.
    """
    programs = agent_programs(
        staged.metadata,
        staged.target,
        staged.location,
        temporary=staged.temporary,
        payload=staged.payload,
        comment=comment,
        dirty=staged.dirty,
    )
    artifact = RenderedArtifact.create(
        key=staged.key,
        artifact_type=staged.artifact_type,
        target=staged.target,
        ddl=staged.payload,
        shape=PublishShape(
            "AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE, temporary=staged.temporary
        ),
        statements=StatementPlan(
            default=programs.create,
            create=programs.create,
            update=programs.update,
            update_live=programs.update_live,
            update_properties=programs.update_properties,
            properties_lead=programs.properties_lead,
        ),
        # Snowflake's docs name agent_spec.yaml only when reading a version back; the server
        # reads a staged spec from that file (docs/design/snowflake-behaviour.md).
        upload=(
            None
            if staged.temporary
            else Upload(
                f"@{staged.stage_path.rstrip('/')}/agent_spec.yaml", staged.payload.encode("utf-8"), staged.stage
            )
        ),
        metadata=DesiredMetadata(
            alias=staged.metadata.alias,
            tags=tuple(name for name, _ in staged.metadata.tags),
            spec_fingerprint=None if staged.temporary else staged.spec_fingerprint,
        ),
        depends_on=staged.depends_on,
        references=staged.references,
        smoke=(
            SmokeProbe(
                f"{staged.key}:describe",
                ProbeKind.DESCRIBE,
                sql("DESCRIBE AGENT {agent}", agent=qname(staged.target)),
            ),
        ),
    )
    return replace(artifact, fingerprint=staged.definition_fingerprint)


@dataclass(frozen=True, slots=True)
class AgentPrograms:
    """An AGENT object's statement programs, as `StatementPlan` names them."""

    create: tuple[Sql, ...] = ()
    update: tuple[Sql, ...] = ()
    update_live: tuple[Sql, ...] = ()
    update_properties: tuple[Sql, ...] = ()
    properties_lead: int = 0


def agent_programs(
    metadata: AgentObjectMetadata,
    target: QualifiedName,
    location: tuple[Sql, str] | None,
    *,
    temporary: bool,
    payload: str,
    comment: str | None,
    dirty: bool = False,
) -> AgentPrograms:
    """Return the statement programs that publish an AGENT object.

    A temporary agent is one CREATE OR REPLACE TEMPORARY AGENT from the inline
    specification for every action. A staged agent's create, update, and update-live
    programs end with exactly one committed version more than they began with and a live
    version holding the same specification, then its alias and tags are set. Snowsight
    shows the live version, and a request naming no version is served it, so an agent
    without one shows and runs with no instructions, tools, or skills
    (docs/design/snowflake-behaviour.md). Each version committed carries
    `git:<sha> sst-spec:<fingerprint>`, which plan reads back to tell the newest version
    already holds a specification:

    - create: CREATE AGENT commits VERSION$1 and also opens a live version of the same
      specification, which is kept. VERSION$1 takes the comment an added version carries, and
      the default is pinned to it by name, so no version committed later moves it.
    - update: ADD VERSION from the stage directory, then a live version from it.
    - update-live: an agent with a live version gets the specification written into it
      and committed, since ADD VERSION beside a live version fails and committing it first
      would publish the stale draft as a version of its own; then a live version is opened
      from the commit, since COMMIT closes the one it committed.
    - update-properties: for an agent whose newest version already holds the
      specification; only its tags here, and `for_action` points the alias, the default
      version and a live version at that version. It never adds or commits a version.

    With `comment`, each program also publishes the agent's metadata. The marked comment
    comes first -- in CREATE AGENT itself, or as the update's first statement -- and the
    profile and SECURE last. An agent with no stage directory has only that metadata.

    Args:
        location: The stage directory and the git sha it is named for; None when unstaged.
        comment: The comment carrying the ownership marker; None publishes no metadata.
        dirty: Label each committed version's commit `<sha>-dirty`, as `version_comment` does.

    Raises:
        ValueError: a temporary agent's specification holds `$$`, which would end the
            dollar-quoted literal it is inlined in; or the alias is not a plain identifier,
            which validation reports as SST-PRS025 or SST-PRS005.
    """
    agent = qname(target)
    lead = () if comment is None else (_set_comment(agent, comment),)
    tail = () if comment is None else _tail_metadata(metadata, agent)
    if temporary:
        if "$$" in payload:
            raise ValueError("temporary agent spec contains an unsupported dollar-quote delimiter")
        profile = render_agent_profile(metadata.profile)
        statement = sql(
            "CREATE OR REPLACE TEMPORARY AGENT {agent}{profile} FROM SPECIFICATION {specification}",
            agent=agent,
            profile=sql(" WITH PROFILE = {profile}", profile=literal(profile)) if profile else sql(""),
            specification=dollar_quoted(payload.rstrip()),
        )
        program = (statement, *lead, *tail)
        return AgentPrograms(program, program, program)
    if location is None:
        unstaged = (*lead, *tail)
        return AgentPrograms(unstaged, unstaged, unstaged)
    directory, git_sha = location
    create, update, update_live = _version_programs(agent, directory, git_sha, payload, comment, dirty)
    properties = (*_alias_statement(metadata, agent), *_tag_statements(metadata, agent))
    return AgentPrograms(
        create=(*create, *properties, *tail),
        update=(*lead, *update, *properties, *tail),
        update_live=(*lead, *update_live, *properties, *tail),
        update_properties=(*lead, *_tag_statements(metadata, agent), *tail),
        properties_lead=len(lead),
    )


def _version_programs(
    agent: Sql, directory: Sql, git_sha: str, payload: str, comment: str | None, dirty: bool
) -> tuple[tuple[Sql, ...], tuple[Sql, ...], tuple[Sql, ...]]:
    """Return the statements of the create, update, and update-live programs that commit a version.

    Each committed version's comment carries the git sha, `-dirty` after it for a `dirty`
    agent, and the specification's fingerprint; CREATE AGENT also sets the agent's marked
    `comment` when there is one. The create program ends by pinning the default to VERSION$1
    by name; an update's version is named only once Snowflake commits it, so apply pins that.
    """
    version_comment_text = literal(version_comment(git_sha, payload, dirty=dirty))
    created = (
        sql("CREATE AGENT {agent}\n  FROM {directory}", agent=agent, directory=directory)
        if comment is None
        else sql(
            "CREATE AGENT {agent}\n  COMMENT = {comment}\n  FROM {directory}",
            agent=agent,
            comment=literal(comment),
            directory=directory,
        )
    )
    create = (
        created,
        sql(
            "ALTER AGENT {agent} MODIFY VERSION VERSION$1 SET COMMENT = {comment}",
            agent=agent,
            comment=version_comment_text,
        ),
        # MEASURED: pinning VERSION$1 straight after CREATE AGENT ... FROM a stage works.
        sql("ALTER AGENT {agent} SET DEFAULT_VERSION = 'VERSION$1'", agent=agent),
    )
    # MEASURED: the live version this opens holds LAST's specification, and DESCRIBE AGENT
    # then reports it as `agent_spec`.
    open_live = sql("ALTER AGENT {agent} ADD LIVE VERSION FROM LAST", agent=agent)
    update = (
        sql(
            "ALTER AGENT {agent}\n  ADD VERSION FROM {directory}\n  COMMENT = {comment}",
            agent=agent,
            directory=directory,
            comment=version_comment_text,
        ),
        open_live,
    )
    update_live = (
        # Snowflake's docs do not say ADD VERSION fails beside a live version; the server refuses
        # it (docs/design/snowflake-behaviour.md).
        # `literal` escapes backslashes, so the JSON's own escapes survive; MEASURED that the
        # committed version's spec then reads back byte for byte.
        sql(
            "ALTER AGENT {agent}\n  MODIFY LIVE VERSION SET SPECIFICATION = {specification}",
            agent=agent,
            specification=literal(payload.rstrip()),
        ),
        sql("ALTER AGENT {agent} COMMIT COMMENT = {comment}", agent=agent, comment=version_comment_text),
        open_live,
    )
    return create, update, update_live


def _alias_statement(metadata: AgentObjectMetadata, agent: Sql) -> tuple[Sql, ...]:
    """Return the statement that moves the agent's alias to the last committed version, if it has one."""
    if not metadata.alias:
        return ()
    # Snowflake's docs list LAST as a version shortcut; the server rejects it unquoted in
    # MODIFY VERSION (docs/design/snowflake-behaviour.md).
    return (
        sql(
            'ALTER AGENT {agent}\n  MODIFY VERSION "LAST" SET ALIAS = {alias}',
            agent=agent,
            alias=_alias(metadata.alias),
        ),
    )


def _tag_statements(metadata: AgentObjectMetadata, agent: Sql) -> tuple[Sql, ...]:
    """Return the statement that sets the agent's tags, if it declares any."""
    if not metadata.tags:
        return ()
    pairs = join(
        ", ",
        (
            sql("{tag} = {value}", tag=_qualified_or_identifier(name), value=literal(value))
            for name, value in metadata.tags
        ),
    )
    return (sql("ALTER AGENT {agent}\n  SET TAG {pairs}", agent=agent, pairs=pairs),)


def _set_comment(agent: Sql, comment: str) -> Sql:
    return sql("ALTER AGENT {agent} SET COMMENT = {comment}", agent=agent, comment=literal(comment))


def _tail_metadata(metadata: AgentObjectMetadata, agent: Sql) -> tuple[Sql, ...]:
    """Return the statements that set the agent's profile and SECURE, which every program ends with."""
    profile = literal(render_agent_profile(metadata.profile))
    return (
        sql("ALTER AGENT {agent} SET PROFILE = {profile}", agent=agent, profile=profile),
        sql("ALTER AGENT {agent} SET SECURE = {secure}", agent=agent, secure=boolean(metadata.secure is True)),
    )


def _alias(value: str) -> Sql:
    """Render an alias unquoted; one that would need quoting, or is reserved, is never rendered.

    Raises:
        ValueError: the alias is not a plain identifier.
    """
    if not plain_alias(value):
        raise ValueError(f"agent alias {value!r} is not a plain, unreserved identifier")
    return ident(Identifier.parse(value))


def _qualified_or_identifier(value: str) -> Sql:
    """Render a tag name: three parts or one, each reserved word quoted, as `ident` renders it."""
    try:
        name = QualifiedName.parse(value)
    except ValueError:
        return ident(Identifier.parse(value))
    return qname(name)


def external_references(members: Sequence[str | None]) -> tuple[str, ...]:
    """Return the `tool_ref:` references of the tool members another team owns, sorted and unique."""
    return tuple(sorted({f"tool_ref:{member.casefold()}" for member in members if member}))

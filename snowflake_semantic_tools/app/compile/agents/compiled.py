"""A compiled Cortex Agent and the statements that publish it, staged or temporary."""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile.base import StandaloneArtifact
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    DesiredMetadata,
    OwnershipMarker,
    ProbeKind,
    PublishShape,
    RenderedArtifact,
    SmokeProbe,
    StatementPlan,
    Upload,
    marked_comment,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.setting import Setting
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
class CompiledAgent(StandaloneArtifact):
    """One agent whose tools and skills resolved, with its rendered specification.

    Attributes:
        payload: The specification JSON that is staged, or inlined for a temporary agent.
        definition_fingerprint: The SHA-256 of the whole definition -- the specification with
            the comment, profile, alias, tags, `secure`, `enabled` and `meta` -- which replaces
            the payload's own fingerprint, so a change to any of them is a change.
        stage_path: `<stage>/<agent>/<git sha>`, where the specification is staged; empty
            until `for_publication` sets it, and an agent without one has no statements.
        stage: The stage `stage_path` starts with; None until `for_publication` sets it.
        temporary: Publish as a TEMPORARY agent created from the inline specification,
            staging nothing.
    """

    resolved: ResolvedAgent
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    stage_path: str = ""
    temporary: bool = False
    stage: QualifiedName | None = None

    @property
    def name(self) -> str:
        return str(self.resolved.model.name)

    @property
    def artifact_key(self) -> str:
        return str(self.resolved.model.key)

    @property
    def artifact_type(self) -> str:
        return "agent"

    @property
    def source_files(self) -> tuple[str, ...]:
        return tuple(self.resolved.model.source_files)

    @property
    def settings(self) -> tuple[Setting, ...]:
        settings = self.resolved.model.settings
        return tuple(settings[key] for key in sorted(settings))

    @property
    def spec_fingerprint(self) -> str:
        """The fingerprint of the specification alone, which each version SST commits carries."""
        return spec_fingerprint(self.payload)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self._rendered(None)

    @property
    def _location(self) -> tuple[Sql, str] | None:
        """The stage directory the specification is staged in, with its git sha; None when unstaged."""
        if self.stage is None or not self.stage_path:
            return None
        git_sha = self.stage_path.rsplit("/", 1)[-1]
        return stage_path(self.stage, f"{self.name}/{git_sha}/"), git_sha

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Render every statement program with the profile, the marked comment, and SECURE set too.

        The comment carrying the ownership marker is written first -- with CREATE AGENT, or
        before an update writes anything else -- so a write that stops part way leaves the
        marker for the definition state records it was writing, which the next plan finishes.
        """
        marker = OwnershipMarker(manifest_id, self.definition_fingerprint)
        return replace(self._rendered(marked_comment(self.resolved.model.comment, marker.text)), expected_marker=marker)

    def _rendered(self, comment: str | None) -> RenderedArtifact:
        """Render the agent; `comment` is the marked comment to publish, None for no publish metadata."""
        programs = _agent_programs(
            self.resolved.model,
            self.target,
            self._location,
            temporary=self.temporary,
            payload=self.payload,
            comment=comment,
        )
        artifact = RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="agent",
            target=self.target,
            ddl=self.payload,
            shape=PublishShape(
                "AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE, temporary=self.temporary
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
                if self.temporary
                else Upload(f"@{self.stage_path.rstrip('/')}/agent_spec.yaml", self.payload.encode("utf-8"), self.stage)
            ),
            metadata=DesiredMetadata(
                alias=self.resolved.model.alias,
                tags=tuple(name for name, _ in self.resolved.model.tags),
                spec_fingerprint=None if self.temporary else self.spec_fingerprint,
            ),
            depends_on=self.resolved.depends_on,
            smoke=(
                SmokeProbe(
                    f"{self.artifact_key}:describe",
                    ProbeKind.DESCRIBE,
                    sql("DESCRIBE AGENT {agent}", agent=qname(self.target)),
                ),
            ),
        )
        return replace(artifact, fingerprint=self.definition_fingerprint)


def for_publication(
    compiled: CompiledAgent,
    *,
    stage: QualifiedName,
    git_sha: str,
    temporary: bool = False,
) -> CompiledAgent:
    """Return the agent ready to publish: staged under `<stage>/<agent>/<git_sha>`, or temporary.

    Compile leaves an agent without a stage path, so it has no statements; plan and apply
    call this first. A temporary agent keeps the stage path but stages nothing.
    """
    return replace(
        compiled,
        stage_path=f"{stage.sql}/{compiled.name}/{git_sha}",
        temporary=temporary,
        stage=stage,
    )


@dataclass(frozen=True, slots=True)
class _Programs:
    """An agent's statement programs, as `StatementPlan` names them."""

    create: tuple[Sql, ...] = ()
    update: tuple[Sql, ...] = ()
    update_live: tuple[Sql, ...] = ()
    update_properties: tuple[Sql, ...] = ()
    properties_lead: int = 0


def _agent_programs(
    model: AgentModel,
    target: QualifiedName,
    location: tuple[Sql, str] | None,
    *,
    temporary: bool,
    payload: str,
    comment: str | None,
) -> _Programs:
    """Return the statement programs that publish an agent.

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
      specification, which is kept. VERSION$1 takes the comment an added version carries.
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

    Raises:
        ValueError: a temporary agent's specification holds `$$`, which would end the
            dollar-quoted literal it is inlined in; or the alias is not a plain identifier,
            which validation reports as SST-PRS025 or SST-PRS005.
    """
    agent = qname(target)
    lead = () if comment is None else (_set_comment(agent, comment),)
    tail = () if comment is None else _tail_metadata(model, agent)
    if temporary:
        if "$$" in payload:
            raise ValueError("temporary agent spec contains an unsupported dollar-quote delimiter")
        profile = render_agent_profile(model.profile)
        statement = sql(
            "CREATE OR REPLACE TEMPORARY AGENT {agent}{profile} FROM SPECIFICATION {specification}",
            agent=agent,
            profile=sql(" WITH PROFILE = {profile}", profile=literal(profile)) if profile else sql(""),
            specification=dollar_quoted(payload.rstrip()),
        )
        program = (statement, *lead, *tail)
        return _Programs(program, program, program)
    if location is None:
        unstaged = (*lead, *tail)
        return _Programs(unstaged, unstaged, unstaged)
    directory, git_sha = location
    create, update, update_live = _version_programs(agent, directory, git_sha, payload, comment)
    metadata = (*_alias_statement(model, agent), *_tag_statements(model, agent))
    return _Programs(
        create=(*create, *metadata, *tail),
        update=(*lead, *update, *metadata, *tail),
        update_live=(*lead, *update_live, *metadata, *tail),
        update_properties=(*lead, *_tag_statements(model, agent), *tail),
        properties_lead=len(lead),
    )


def _version_programs(
    agent: Sql, directory: Sql, git_sha: str, payload: str, comment: str | None
) -> tuple[tuple[Sql, ...], tuple[Sql, ...], tuple[Sql, ...]]:
    """Return the statements of the create, update, and update-live programs that commit a version.

    Each committed version's comment carries the git sha and the specification's fingerprint;
    CREATE AGENT also sets the agent's marked `comment` when there is one.
    """
    version_comment_text = literal(version_comment(git_sha, payload))
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


def _alias_statement(model: AgentModel, agent: Sql) -> tuple[Sql, ...]:
    """Return the statement that moves the agent's alias to the last committed version, if it has one."""
    if not model.alias:
        return ()
    # Snowflake's docs list LAST as a version shortcut; the server rejects it unquoted in
    # MODIFY VERSION (docs/design/snowflake-behaviour.md).
    return (
        sql(
            'ALTER AGENT {agent}\n  MODIFY VERSION "LAST" SET ALIAS = {alias}',
            agent=agent,
            alias=_alias(model.alias),
        ),
    )


def _tag_statements(model: AgentModel, agent: Sql) -> tuple[Sql, ...]:
    """Return the statement that sets the agent's tags, if it declares any."""
    if not model.tags:
        return ()
    pairs = join(
        ", ",
        (
            sql("{tag} = {value}", tag=_qualified_or_identifier(name), value=literal(value))
            for name, value in model.tags
        ),
    )
    return (sql("ALTER AGENT {agent}\n  SET TAG {pairs}", agent=agent, pairs=pairs),)


def _set_comment(agent: Sql, comment: str) -> Sql:
    return sql("ALTER AGENT {agent} SET COMMENT = {comment}", agent=agent, comment=literal(comment))


def _tail_metadata(model: AgentModel, agent: Sql) -> tuple[Sql, ...]:
    """Return the statements that set the agent's profile and SECURE, which every program ends with."""
    profile = literal(render_agent_profile(model.profile))
    return (
        sql("ALTER AGENT {agent} SET PROFILE = {profile}", agent=agent, profile=profile),
        sql("ALTER AGENT {agent} SET SECURE = {secure}", agent=agent, secure=boolean(model.secure is True)),
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

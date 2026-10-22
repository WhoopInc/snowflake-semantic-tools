"""A rendered artifact: what compile produced for one artifact, ready for plan and apply.

`RenderedArtifact.create` canonicalizes the rendered text and fingerprints it. What a
caller sets besides arrives in small parameter objects: `PublishShape` for the object
Snowflake receives, `StatementPlan` for the statements each action runs, `Upload` for a
file staged first, `DesiredMetadata` for the alias, tags and specification an agent keeps, and
`CompositeFacts` for an artifact a lifecycle handler publishes. Once plan has chosen an
action, `RenderedArtifact.for_action` picks the statements apply runs, and `version_drift` says
how an agent's versions no longer point where SST published them. A version an eval-model
comparison committed is never SST's release, and its `EVAL_` alias is left alone.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from hashlib import sha256

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle.action import Action
from snowflake_semantic_tools.domain.model.lifecycle.marker import OwnershipMarker
from snowflake_semantic_tools.domain.model.lifecycle.observation import ArtifactKey, ObservedArtifact
from snowflake_semantic_tools.domain.model.lifecycle.version import is_eval_alias
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.sql import Sql, canonical, ident, join, qname, sql

# The type Snowflake reports for an internal stage with server-side encryption only: the one
# kind of stage SST creates, and the only kind it stages files on.
SSE_STAGE_TYPE = "INTERNAL NO CSE"


class ProbeKind(Enum):
    """What a smoke probe exercises: the view, one metric, one verified query, or a DESCRIBE."""

    VIEW = "view"
    METRIC = "metric"
    VERIFIED_QUERY = "verified_query"
    DESCRIBE = "describe"


@dataclass(frozen=True, slots=True)
class SmokeProbe:
    """One query the smoke suite runs against a published artifact; it passes when the query runs.

    Attributes:
        key: What a failure is reported under: a member's key, or the artifact's key with a
            `:view` or `:describe` suffix.
    """

    key: str
    kind: ProbeKind
    sql: Sql


@dataclass(frozen=True, slots=True)
class PublishShape:
    """How Snowflake receives an artifact: the object it becomes and how an update keeps grants.

    The defaults describe a semantic view rendered as DDL.

    Attributes:
        object_type: The Snowflake object type, such as AGENT or STAGE; empty for an artifact
            that spans several objects, which its lifecycle handler publishes.
        render_dialect: What the rendered text is, such as `ddl`, `json`, or `eval_yaml`; the
            manifest records it, and it picks the suffix of the file the text is written to.
        grant_preservation: How the object's explicit grants survive an update that replaces it.
        temporary: The object lives only as long as the session, so apply does not refuse a
            CREATE of one that already exists.
        routine_signature: The argument types of a procedure or function, which Snowflake
            needs to address it; empty for any other object.
    """

    object_type: str = "SEMANTIC VIEW"
    render_dialect: str = "ddl"
    grant_preservation: GrantPreservation = GrantPreservation.CLAUSE
    temporary: bool = False
    routine_signature: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StatementPlan:
    """The statements that publish an artifact, and the programs a CREATE or an UPDATE runs instead.

    An empty action program means the action runs `default`.

    Attributes:
        default: What apply runs when no action program applies. None means the canonical
            text as one statement; an empty tuple means nothing, for an artifact a lifecycle
            handler publishes.
        create: What a CREATE runs instead of `default`.
        update: What an UPDATE runs instead of `default`.
        update_live: What an UPDATE runs instead of `update` when the object has a live version.
        update_properties: What an UPDATE runs instead of either when the agent's newest
            committed version already holds the desired specification: it sets properties
            and never adds or commits a version. `for_action` inserts, after its first
            `properties_lead` statements, those that point the alias and the default version
            at that version and open a live version from it.
        properties_lead: How many of `update_properties` run before the pointing statements.
    """

    default: tuple[Sql, ...] | None = None
    create: tuple[Sql, ...] = ()
    update: tuple[Sql, ...] = ()
    update_live: tuple[Sql, ...] = ()
    update_properties: tuple[Sql, ...] = ()
    properties_lead: int = 0


@dataclass(frozen=True, slots=True)
class Upload:
    """A file apply stages before it runs an artifact's statements.

    Attributes:
        path: The stage path the file is written to, such as `@DB.S.STAGE/name/sha/spec.yaml`.
        content: The file's bytes, exactly as they are staged.
        stage: The stage `path` is on, which apply creates when it is absent; None when apply
            leaves the stage to whoever owns it.
    """

    path: str
    content: bytes
    stage: QualifiedName | None = None


@dataclass(frozen=True, slots=True)
class DesiredMetadata:
    """The version alias, tags and specification an artifact keeps.

    An agent's update unsets every other alias and tag it observes; a semantic view's replace
    sets exactly these tags, so plan warns about any other one the view carries. Both compare
    casefolded with what Snowflake shows.

    Attributes:
        alias: The one version alias to keep; None keeps none.
        tags: The tags to keep, by name.
        spec_fingerprint: The fingerprint of the specification an agent's served version must
            hold, which its committed versions carry in their comments; None for anything else.
    """

    alias: str | None = None
    tags: tuple[str, ...] = ()
    spec_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class CompositeFacts:
    """What the manifest and state record for an artifact a dedicated lifecycle handler publishes.

    Passing one marks the artifact composite, which the generic apply path refuses.

    Attributes:
        component_fingerprints: One `(component, fingerprint)` pair per part the handler
            publishes separately, such as an eval's dataset and configuration.
        physical_resources: The Snowflake objects the artifact spans, as `(object type, name)`.
    """

    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, QualifiedName], ...] = ()


@dataclass(frozen=True, slots=True)
class RenderedArtifact:
    """What compile rendered for one artifact: its canonical text, fingerprint, and statements.

    Build one with `create`. A compiler whose fingerprint covers more than the text, such as
    an agent's full definition, replaces `fingerprint` afterwards.

    Attributes:
        ddl: The canonical rendered text: every line right-stripped, ending in one newline. It
            is DDL for a semantic view or tool, and a document in `render_dialect` otherwise.
        statements: What apply runs; `for_action` narrows it to the action plan chose.
        fingerprint: The SHA-256 hex digest of `ddl`, unless the compiler replaced it.
        object_type, render_dialect, grant_preservation, temporary, routine_signature: As
            `PublishShape` describes them.
        upload_path, upload_content, upload_stage: As `Upload` describes its `path`, `content`,
            and `stage`; all None when nothing is staged.
        create_statements, update_statements, update_live_statements,
            update_properties_statements, properties_lead: As `StatementPlan` describes its
            `create`, `update`, `update_live`, `update_properties`, and `properties_lead`.
        expected_marker: The ownership marker the write must leave, which apply reads back;
            None skips the check.
        desired_alias, desired_tags, spec_fingerprint: As `DesiredMetadata` describes its
            `alias`, `tags`, and `spec_fingerprint`.
        depends_on: The keys of the artifacts that must be published before this one.
        references: What the artifact names but does not wait on: a plugin's or profile's
            members, as artifact keys and component keys such as `command:<name>`,
            `hook:<name>` and `mcp:<name>`, and the tool members another team owns that an
            agent calls, as `tool_ref:<name>`. Recorded in the manifest; never a publish edge.
        smoke: The probes the smoke suite runs once the artifact is published.
        required_relations: The tables and views apply checks exist before it writes anything.
        component_fingerprints, physical_resources: As `CompositeFacts` describes them.
        generic_apply_safe: False for a composite artifact, which the generic apply path refuses.
        version_metadata: The METADATA the dataset version an eval mints carries, commit and all,
            set only on what is published; None otherwise. It is neither a component nor in the
            fingerprint, so the plan and the compiled manifest report the same components.
        member_fingerprints: One `(member, fingerprint)` pair per member of the artifact's
            definition, as `member_fingerprints` hashes them, keyed `<kind>:<name>`; empty for an
            artifact plan does not compare. Not in the fingerprint: state records them, so a later
            plan can name which members an update changes without reading the object.
        adds_version: Whether the program `for_action` chose commits a new version of a
            permanent agent: False for one that only sets its properties; None for anything
            else, and before `for_action` chose.
        wants_attachment: The AGENT is a CoWork profile, published where CoWork lists profiles
            with `attach` on, so plan attaches it to the account's CoWork object; False for
            anything else, a rehearsal of a profile included. Snowflake private preview (PrPr).
            Not in the fingerprint: attaching is no part of the definition.
        attaches_to: The CoWork object the last of `statements` attaches the AGENT to, as SHOW
            SNOWFLAKE INTELLIGENCES names it; set by plan, and None when nothing attaches it.
    """

    key: ArtifactKey
    artifact_type: str
    target: QualifiedName
    ddl: str
    statements: tuple[Sql, ...]
    fingerprint: str
    object_type: str = "SEMANTIC VIEW"
    render_dialect: str = "ddl"
    grant_preservation: GrantPreservation = GrantPreservation.CLAUSE
    upload_path: str | None = None
    upload_content: bytes | None = None
    upload_stage: QualifiedName | None = None
    temporary: bool = False
    routine_signature: tuple[str, ...] = ()
    create_statements: tuple[Sql, ...] = ()
    update_statements: tuple[Sql, ...] = ()
    update_live_statements: tuple[Sql, ...] = ()
    update_properties_statements: tuple[Sql, ...] = ()
    properties_lead: int = 0
    expected_marker: OwnershipMarker | None = None
    desired_alias: str | None = None
    desired_tags: tuple[str, ...] = ()
    spec_fingerprint: str | None = None
    depends_on: tuple[ArtifactKey, ...] = ()
    references: tuple[str, ...] = ()
    smoke: tuple[SmokeProbe, ...] = ()
    required_relations: tuple[QualifiedName, ...] = ()
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, QualifiedName], ...] = ()
    generic_apply_safe: bool = True
    version_metadata: str | None = None
    member_fingerprints: tuple[tuple[str, str], ...] = ()
    adds_version: bool | None = None
    wants_attachment: bool = False
    attaches_to: str | None = None

    @classmethod
    def create(
        cls,
        *,
        key: ArtifactKey,
        artifact_type: str,
        target: QualifiedName,
        ddl: Sql | str,
        shape: PublishShape = PublishShape(),
        statements: StatementPlan = StatementPlan(),
        upload: Upload | None = None,
        metadata: DesiredMetadata = DesiredMetadata(),
        composite: CompositeFacts | None = None,
        depends_on: tuple[ArtifactKey, ...] = (),
        references: tuple[str, ...] = (),
        smoke: tuple[SmokeProbe, ...] = (),
        required_relations: tuple[QualifiedName, ...] = (),
    ) -> RenderedArtifact:
        """Render an artifact from its text, which this canonicalizes and fingerprints.

        `expected_marker` is not a parameter: the marker embeds the manifest id, which exists
        only once the manifest is built, so the publish step sets it.

        Args:
            ddl: The rendered statement, or the document a non-DDL dialect renders; canonicalized
                into `ddl` before it is fingerprinted. A statement is also what apply runs
                when `statements` names no default.
            upload: The file to stage first; None stages nothing.
            composite: Given for an artifact a lifecycle handler publishes, which sets
                `generic_apply_safe` to False; None for one the generic apply path publishes.

        Raises:
            TypeError: `ddl` is a document and `statements` names no default to run.
        """
        if isinstance(ddl, Sql):
            statement = canonical(ddl)
            text = statement.text + "\n"
            default = (statement,) if statements.default is None else statements.default
        elif statements.default is None:
            raise TypeError("a rendered document needs the statements that publish it")
        else:
            # Lines end only at `\n`: a document's text may hold a separator such as U+2028.
            text = "\n".join(line.rstrip() for line in ddl.split("\n")).strip() + "\n"
            default = statements.default
        return cls(
            key=key,
            artifact_type=artifact_type,
            target=target,
            ddl=text,
            statements=default,
            fingerprint=sha256(text.encode("utf-8")).hexdigest(),
            object_type=shape.object_type,
            render_dialect=shape.render_dialect,
            grant_preservation=shape.grant_preservation,
            upload_path=upload.path if upload is not None else None,
            upload_content=upload.content if upload is not None else None,
            upload_stage=upload.stage if upload is not None else None,
            temporary=shape.temporary,
            routine_signature=shape.routine_signature,
            create_statements=statements.create,
            update_statements=statements.update,
            update_live_statements=statements.update_live,
            update_properties_statements=statements.update_properties,
            properties_lead=statements.properties_lead,
            desired_alias=metadata.alias,
            desired_tags=metadata.tags,
            spec_fingerprint=metadata.spec_fingerprint,
            depends_on=depends_on,
            references=references,
            smoke=smoke,
            required_relations=required_relations,
            component_fingerprints=composite.component_fingerprints if composite is not None else (),
            physical_resources=composite.physical_resources if composite is not None else (),
            generic_apply_safe=composite is None,
        )

    @property
    def content(self) -> str:
        """Return the rendered text, whatever its dialect: the same string as `ddl`."""
        return self.ddl

    @property
    def keeps_grants(self) -> bool:
        """Report whether running `statements` keeps the object's explicit grants.

        A type that re-applies its grants (REPLAY) or is never replaced (NONE) keeps them. A
        type that keeps them by clause does only when every statement that replaces the object
        says COPY GRANTS, however it is spaced or cased.
        """
        if self.grant_preservation is not GrantPreservation.CLAUSE:
            return True
        for statement in self.statements:
            normalized = " ".join(str(statement).upper().split())
            if "CREATE OR REPLACE" in normalized and "COPY GRANTS" not in normalized:
                return False
        return True

    def for_action(self, action: Action, observed: ObservedArtifact | None) -> RenderedArtifact:
        """Return the artifact with `statements` set to what the action runs.

        A CREATE runs `create_statements` when there are any. An UPDATE runs
        `update_properties_statements` when the observed agent's newest release already holds
        the desired specification and there are any, with the statements that point the alias
        and the default version at it, and open a live version from it, where missing; else
        `update_live_statements` when the observed object has a live version and there are any;
        else `update_statements` when there are any; either of those also leaves the default
        following the new version. An agent with no live version whose newest committed version
        is an eval-model version never takes the properties program: the live version it would
        open `FROM LAST` would serve that version's candidate specification, so it commits a new
        release instead. It then unsets the aliases and tags the observed agent carries but this
        artifact does not desire, except an alias on the version the properties program just set
        the desired alias on, which that replaced, and an eval-model version's. A permanent
        agent's UPDATE also says whether it commits a new version, in `adds_version`. Any other
        action, or an UPDATE with no program, returns this artifact unchanged.
        """
        if action is Action.CREATE and self.create_statements:
            return replace(self, statements=self.create_statements)
        if action is Action.UPDATE:
            carrier = _spec_carrier(self, observed)
            replaced: str | None = None
            adds_version = True
            if (
                observed is not None
                and carrier is not None
                and self.update_properties_statements
                and (observed.has_live_version or not observed.last_is_eval_model)
            ):
                lead, rest = (
                    self.update_properties_statements[: self.properties_lead],
                    self.update_properties_statements[self.properties_lead :],
                )
                statements = (*lead, *_pointing(self, observed, carrier), *rest)
                replaced = carrier if _moves_alias(self, observed, carrier) else None
                adds_version = False
            elif observed is not None and observed.has_live_version and self.update_live_statements:
                statements = (*self.update_live_statements, *_unpinned_default(self, observed))
            elif self.update_statements:
                statements = (*self.update_statements, *_unpinned_default(self, observed))
            else:
                return self
            versioned = self.object_type == "AGENT" and not self.temporary
            return replace(
                self,
                statements=(*statements, *_metadata_removals(self, observed, replaced)),
                adds_version=adds_version if versioned else None,
            )
        return self


def _unpinned_default(artifact: RenderedArtifact, observed: ObservedArtifact | None) -> tuple[Sql, ...]:
    """Leave the default following the version an update adds, as `'LAST'`.

    Snowflake keeps a default set by name where it is when a version is committed, so a
    version-adding update would leave it serving the old one. SHOW VERSIONS marks which version
    is the default but not whether it was set by name, so a default on the newest version may be
    pinned there too; every version-adding update of a permanent agent therefore sets it to
    `'LAST'`, which then follows each version committed after.
    """
    if observed is None or artifact.object_type != "AGENT" or artifact.temporary:
        return ()
    return (_default_to_last(artifact),)


def _default_to_last(artifact: RenderedArtifact) -> Sql:
    """Set the agent's default to its newest committed version, as a default that follows it."""
    # MEASURED: the shortcut is taken only as a string literal; unquoted, it is a syntax error. A
    # default set to it follows each version committed afterwards.
    return sql("ALTER AGENT {agent} SET DEFAULT_VERSION = 'LAST'", agent=qname(artifact.target))


def version_drift(artifact: RenderedArtifact, observed: ObservedArtifact) -> tuple[str, ...]:
    """Describe how an agent's versions no longer serve what SST published; empty when they do.

    Only a permanent agent whose desired specification is known, and that lists a release, is
    checked: it must have a live version and, when its newest release holds the desired
    specification, carry the desired alias there and have no other version marked the default,
    an eval-model version included. A newest release holding another specification, or one that
    cannot be read, is not drift by itself: whatever changed it is not told apart here. An
    eval-model version and its `EVAL_` alias are never drift: a comparison commits them beside
    the release, which keeps its alias and default.

    Returns:
        One phrase per finding, in that order, such as `alias PROMOTED is on VERSION$9, not
        VERSION$10`.
    """
    if (
        artifact.object_type != "AGENT"
        or artifact.temporary
        or artifact.spec_fingerprint is None
        or observed.newest_version is None
    ):
        return ()
    found: list[str] = []
    if not observed.has_live_version:
        found.append("it has no live version")
    carrier = _spec_carrier(artifact, observed)
    if carrier is None:
        return tuple(found)
    if artifact.desired_alias:
        holder = _alias_holder(observed, artifact.desired_alias)
        if holder is None or holder.casefold() != carrier.casefold():
            found.append(f"alias {artifact.desired_alias.upper()} is on {holder or 'no version'}, not {carrier}")
    default = observed.default_version
    if default is not None and default.casefold() != carrier.casefold():
        found.append(f"the default version is {default}, not {carrier}")
    return tuple(found)


def _spec_carrier(artifact: RenderedArtifact, observed: ObservedArtifact | None) -> str | None:
    """Name the observed agent's newest release when it holds the desired specification."""
    if observed is None or artifact.spec_fingerprint is None or observed.newest_spec != artifact.spec_fingerprint:
        return None
    return observed.newest_version


def _alias_holder(observed: ObservedArtifact, alias: str) -> str | None:
    """Name the committed version that carries `alias`, casefolded alike; None when none does."""
    return next(
        (version for shown, version in observed.alias_versions if shown.casefold() == alias.casefold()),
        None,
    )


def _pointing(artifact: RenderedArtifact, observed: ObservedArtifact, carrier: str) -> tuple[Sql, ...]:
    """Point the alias and default version at `carrier`, and open a live version from it, where missing.

    `carrier` is the newest committed version, so the default is set to `'LAST'`, which serves it
    now and follows each version committed after, and the live version opens `FROM LAST`:
    Snowflake opens one from no named version (docs/design/snowflake-behaviour.md). A name no
    identifier can spell is not one Snowflake showed or SST renders, so nothing is pointed at it.
    """
    version = _safe_identifier(carrier)
    if version is None:
        return ()
    agent = qname(artifact.target)
    statements: list[Sql] = []
    alias = _plain_identifier(artifact.desired_alias)
    if alias is not None and _moves_alias(artifact, observed, carrier):
        statements.append(
            sql(
                "ALTER AGENT {agent}\n  MODIFY VERSION {version} SET ALIAS = {alias}",
                agent=agent,
                version=version,
                alias=alias,
            )
        )
    default = observed.default_version
    if default is not None and default.casefold() != carrier.casefold():
        statements.append(_default_to_last(artifact))
    if not observed.has_live_version:
        statements.append(sql("ALTER AGENT {agent} ADD LIVE VERSION FROM LAST", agent=agent))
    return tuple(statements)


def _moves_alias(artifact: RenderedArtifact, observed: ObservedArtifact, carrier: str) -> bool:
    """Report whether the desired alias is set on `carrier`, which no version or another one carries."""
    alias = artifact.desired_alias
    if alias is None or _plain_identifier(alias) is None:
        return False
    holder = _alias_holder(observed, alias)
    return holder is None or holder.casefold() != carrier.casefold()


def _metadata_removals(
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
    replaced: str | None = None,
) -> tuple[Sql, ...]:
    """Unset each alias and tag the observed agent carries that the artifact does not desire.

    Both compare casefolded. An alias on `replaced`, the version the update just set the desired
    alias on, is left alone: a version carries one alias, so setting another removed it, and an
    UNSET naming it would find no version. An alias starting `EVAL_` is an eval-model version's,
    which a comparison evaluates by, so it is left alone too. Only an observed agent has any;
    for anything else this is empty.
    """
    if observed is None or artifact.object_type != "AGENT":
        return ()
    statements: list[Sql] = []
    agent = qname(artifact.target)
    desired_alias = artifact.desired_alias.casefold() if artifact.desired_alias else None
    for alias in observed.aliases:
        name = _safe_identifier(alias)
        if alias.casefold() == desired_alias or name is None or is_eval_alias(alias):
            continue
        holder = _alias_holder(observed, alias)
        if replaced is not None and holder is not None and holder.casefold() == replaced.casefold():
            continue
        statements.append(sql("ALTER AGENT {agent} MODIFY VERSION {alias} UNSET ALIAS", agent=agent, alias=name))
    desired_tags = {value.casefold() for value in artifact.desired_tags}
    # A name no identifier can spell is not one Snowflake showed, so it is left alone.
    stale_tags = tuple(
        name
        for name in (
            _safe_qualified_identifier(tag) for tag in observed.tags or () if tag.casefold() not in desired_tags
        )
        if name is not None
    )
    if stale_tags:
        statements.append(
            sql(
                "ALTER AGENT {agent} UNSET TAG {tags}",
                agent=agent,
                tags=join(", ", stale_tags),
            )
        )
    return tuple(statements)


def _plain_identifier(value: str | None) -> Sql | None:
    """Spell an authored name as SQL, as it parses unquoted; None for no name or one that does not parse."""
    if not value:
        return None
    try:
        return ident(Identifier.parse(value))
    except ValueError:
        return None


def _safe_identifier(value: str) -> Sql | None:
    """Spell a shown name as SQL: unquoted only when that reads back as exactly the same name.

    A shown reserved word, such as an alias `SELECT`, is quoted as `ident` quotes every one.

    Returns:
        The identifier; None for a name no identifier can spell, such as an empty one.
    """
    try:
        return ident(Identifier.shown(value))
    except ValueError:
        return None


def _safe_qualified_identifier(value: str) -> Sql | None:
    """Spell a shown dotted name as SQL, quoting part by part when it does not parse whole.

    Returns:
        The name; None when a part is one no identifier can spell.
    """
    try:
        return qname(QualifiedName.parse(value))
    except ValueError:
        parts = tuple(_safe_identifier(part) for part in value.split("."))
    if any(part is None for part in parts):
        return None
    return join(".", (part for part in parts if part is not None))

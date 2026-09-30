"""A rendered artifact: what compile produced for one artifact, ready for plan and apply.

`RenderedArtifact.create` canonicalizes the rendered text and fingerprints it. What a
caller sets besides arrives in small parameter objects: `PublishShape` for the object
Snowflake receives, `StatementPlan` for the statements each action runs, `Upload` for a
file staged first, `DesiredMetadata` for the alias and tags an agent keeps, and
`CompositeFacts` for an artifact a lifecycle handler publishes. Once plan has chosen an
action, `RenderedArtifact.for_action` picks the statements apply runs.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from hashlib import sha256

from ..identifier import Identifier, QualifiedName
from ..registry import GrantPreservation
from .action import Action
from .marker import OwnershipMarker
from .observation import ArtifactKey, ObservedArtifact


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
    sql: str


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
    """

    default: tuple[str, ...] | None = None
    create: tuple[str, ...] = ()
    update: tuple[str, ...] = ()
    update_live: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Upload:
    """A file apply stages before it runs an artifact's statements.

    Attributes:
        path: The stage path the file is written to, such as `@DB.S.STAGE/name/sha/spec.yaml`.
        content: The file's bytes, exactly as they are staged.
    """

    path: str
    content: bytes


@dataclass(frozen=True, slots=True)
class DesiredMetadata:
    """The version alias and tags an agent keeps; an update unsets every other one it observes.

    Both compare casefolded with what Snowflake shows.

    Attributes:
        alias: The one version alias to keep; None keeps none.
        tags: The tags to keep, by name.
    """

    alias: str | None = None
    tags: tuple[str, ...] = ()


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
        upload_path, upload_content: As `Upload` describes them; both None when nothing is staged.
        create_statements, update_statements, update_live_statements: As `StatementPlan`
            describes its `create`, `update`, and `update_live`.
        expected_marker: The ownership marker the write must leave, which apply reads back;
            None skips the check.
        desired_alias, desired_tags: As `DesiredMetadata` describes them.
        depends_on: The keys of the artifacts that must be published before this one.
        smoke: The probes the smoke suite runs once the artifact is published.
        required_relations: The tables and views apply checks exist before it writes anything.
        component_fingerprints, physical_resources: As `CompositeFacts` describes them.
        generic_apply_safe: False for a composite artifact, which the generic apply path refuses.
    """

    key: ArtifactKey
    artifact_type: str
    target: QualifiedName
    ddl: str
    statements: tuple[str, ...]
    fingerprint: str
    object_type: str = "SEMANTIC VIEW"
    render_dialect: str = "ddl"
    grant_preservation: GrantPreservation = GrantPreservation.CLAUSE
    upload_path: str | None = None
    upload_content: bytes | None = None
    temporary: bool = False
    routine_signature: tuple[str, ...] = ()
    create_statements: tuple[str, ...] = ()
    update_statements: tuple[str, ...] = ()
    update_live_statements: tuple[str, ...] = ()
    expected_marker: OwnershipMarker | None = None
    desired_alias: str | None = None
    desired_tags: tuple[str, ...] = ()
    depends_on: tuple[ArtifactKey, ...] = ()
    smoke: tuple[SmokeProbe, ...] = ()
    required_relations: tuple[QualifiedName, ...] = ()
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, QualifiedName], ...] = ()
    generic_apply_safe: bool = True

    @classmethod
    def create(
        cls,
        *,
        key: ArtifactKey,
        artifact_type: str,
        target: QualifiedName,
        ddl: str,
        shape: PublishShape = PublishShape(),
        statements: StatementPlan = StatementPlan(),
        upload: Upload | None = None,
        metadata: DesiredMetadata = DesiredMetadata(),
        composite: CompositeFacts | None = None,
        depends_on: tuple[ArtifactKey, ...] = (),
        smoke: tuple[SmokeProbe, ...] = (),
        required_relations: tuple[QualifiedName, ...] = (),
    ) -> RenderedArtifact:
        """Render an artifact from its text, which this canonicalizes and fingerprints.

        `expected_marker` is not a parameter: the marker embeds the manifest id, which exists
        only once the manifest is built, so the publish step sets it.

        Args:
            ddl: The rendered text, canonicalized into `ddl` before it is fingerprinted.
            upload: The file to stage first; None stages nothing.
            composite: Given for an artifact a lifecycle handler publishes, which sets
                `generic_apply_safe` to False; None for one the generic apply path publishes.
        """
        canonical = "\n".join(line.rstrip() for line in ddl.splitlines()).strip() + "\n"
        return cls(
            key=key,
            artifact_type=artifact_type,
            target=target,
            ddl=canonical,
            statements=(canonical.rstrip("\n"),) if statements.default is None else statements.default,
            fingerprint=sha256(canonical.encode("utf-8")).hexdigest(),
            object_type=shape.object_type,
            render_dialect=shape.render_dialect,
            grant_preservation=shape.grant_preservation,
            upload_path=upload.path if upload is not None else None,
            upload_content=upload.content if upload is not None else None,
            temporary=shape.temporary,
            routine_signature=shape.routine_signature,
            create_statements=statements.create,
            update_statements=statements.update,
            update_live_statements=statements.update_live,
            desired_alias=metadata.alias,
            desired_tags=metadata.tags,
            depends_on=depends_on,
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

    def for_action(self, action: Action, observed: ObservedArtifact | None) -> RenderedArtifact:
        """Return the artifact with `statements` set to what the action runs.

        A CREATE runs `create_statements` when there are any. An UPDATE runs
        `update_live_statements` when the observed object has a live version and there are
        any, else `update_statements` when there are any, and then unsets the aliases and tags
        the observed agent carries but this artifact does not desire. Any other action, or an
        UPDATE with neither program, returns this artifact unchanged.
        """
        if action is Action.CREATE and self.create_statements:
            return replace(self, statements=self.create_statements)
        if action is Action.UPDATE:
            if observed is not None and observed.has_live_version and self.update_live_statements:
                statements = self.update_live_statements
            elif self.update_statements:
                statements = self.update_statements
            else:
                return self
            return replace(self, statements=(*statements, *_metadata_removals(self, observed)))
        return self


def _metadata_removals(
    artifact: RenderedArtifact,
    observed: ObservedArtifact | None,
) -> tuple[str, ...]:
    """Unset each alias and tag the observed agent carries that the artifact does not desire.

    Both compare casefolded. Only an observed agent has any; for anything else this is empty.
    """
    if observed is None or artifact.object_type != "AGENT":
        return ()
    statements: list[str] = []
    desired_alias = artifact.desired_alias.casefold() if artifact.desired_alias else None
    for alias in observed.aliases:
        if alias.casefold() == desired_alias:
            continue
        statements.append(f"ALTER AGENT {artifact.target.sql} MODIFY VERSION {_safe_identifier(alias)} UNSET ALIAS")
    stale_tags = tuple(
        tag for tag in observed.tags if tag.casefold() not in {value.casefold() for value in artifact.desired_tags}
    )
    if stale_tags:
        statements.append(
            f"ALTER AGENT {artifact.target.sql} UNSET TAG "
            + ", ".join(_safe_qualified_identifier(tag) for tag in stale_tags)
        )
    return tuple(statements)


def _safe_identifier(value: str) -> str:
    """Spell a shown name as SQL: unquoted only when that reads back as exactly the same name."""
    try:
        parsed = Identifier.parse(value)
    except ValueError:
        return Identifier(value, quoted=True).sql
    return parsed.sql if parsed.folded == value else Identifier(value, quoted=True).sql


def _safe_qualified_identifier(value: str) -> str:
    """Spell a shown dotted name as SQL, quoting part by part when it does not parse whole."""
    try:
        return QualifiedName.parse(value).sql
    except ValueError:
        return ".".join(_safe_identifier(part) for part in value.split("."))

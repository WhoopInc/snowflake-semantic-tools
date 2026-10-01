"""What every typed compiler shares: the compiled-artifact contract, its result, and its loop.

A typed compiler turns one kind of authored input into `CompiledArtifact` values and
reports what kept the rest back as diagnostics; `CompileResult` carries both.
`StandaloneArtifact` supplies the contract's defaults for an artifact that carries no
semantic members and reads no dbt model, and `compile_each` is the render loop the
compilers of views, tools, and evals share.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Protocol, TypeVar

from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact

# What rendering an input that validated may still raise; anything else escapes.
RENDER_ERRORS: tuple[type[Exception], ...] = (KeyError, TypeError, ValueError)


class CompiledArtifact(Protocol):
    """Manifest and lifecycle projection shared by compiled artifact types.

    Every member is a pure read of the compiled value: calling one again returns an
    equal result, and none of them caches or mutates anything.
    """

    __slots__ = ()

    @property
    def name(self) -> str:
        """Return the artifact's unqualified name, as authored.

        The manifest records it casefolded, and `compile --emit-ddl` names the payload file
        after it. It is unique within one artifact type, not across types.
        """
        ...

    @property
    def artifact_key(self) -> str:
        """Return the `<kind>:<name>` key that identifies the artifact in state, plans, and selectors.

        Unique within one compile result; every diagnostic about the artifact carries it as
        its subject.
        """
        ...

    @property
    def artifact_type(self) -> str:
        """Return the registry artifact type, which places the artifact in the combined stream.

        One of the types `SEMANTIC_REGISTRY` declares; `CompileArtifacts` orders artifacts by
        that type's position, then by key.
        """
        ...

    @property
    def source_files(self) -> tuple[str, ...]:
        """Return the project-relative files the artifact was compiled from, each once.

        The manifest checksums each file and indexes the artifact under it, so the order is
        stable from one compile to the next.
        """
        ...

    @property
    def member_keys(self) -> tuple[str, ...]:
        """Return the keys of the semantic members compiled into the artifact, in manifest order.

        Empty for an artifact that carries no members. The manifest indexes the artifact
        under each key, which is how a change to a member finds the views it lands in.
        """
        ...

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Return the names of the dbt models the artifact reads, each once.

        Empty when it reads none. Each name has its relation in `dbt_relations`.
        """
        ...

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        """Return a `(model, relation)` pair for each dbt model the artifact reads.

        The relation is the fully qualified name the model resolved to, or empty when it
        resolved to none. Empty when the artifact reads no model.
        """
        ...

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        """Return the artifact as rendered, carrying no ownership marker.

        Plan compares its fingerprint with what is published. Deterministic for one compiled
        value; raises ValueError only for a value no statement can express, such as a
        temporary agent whose specification holds a `$$` delimiter.
        """
        ...

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Return the artifact as apply writes it, marked as owned by the manifest `manifest_id`.

        The fingerprint is `rendered_artifact`'s, so publishing never reads as a change. An
        artifact whose object holds no marker ignores `manifest_id`. Raises as
        `rendered_artifact` does.
        """
        ...


class StandaloneArtifact(CompiledArtifact):
    """Contract defaults for an artifact that carries no semantic members and reads no dbt model.

    A subclass overrides what it does carry: a tool over a dbt model overrides
    `referenced_models` and `dbt_relations`, and an artifact whose object holds an
    ownership marker overrides `rendered_for_publish`.
    """

    __slots__ = ()

    @property
    def member_keys(self) -> tuple[str, ...]:
        """Return no keys: the artifact is compiled from no semantic members."""
        return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Return no model names: the artifact reads no dbt model."""
        return ()

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        """Return no relations: the artifact reads no dbt model."""
        return ()

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Return `rendered_artifact` unchanged: the published object holds no ownership marker."""
        del manifest_id
        return self.rendered_artifact


@dataclass(frozen=True)
class CompileResult:
    """Compiled artifacts plus the diagnostics about them, which callers format once.

    Attributes:
        compiled: In the order the compiler documents; `CompileArtifacts` sorts the stream.
        diagnostics: In the order they were found.
    """

    compiled: tuple[CompiledArtifact, ...]
    diagnostics: DiagnosticBag = DiagnosticBag()

    @property
    def success(self) -> bool:
        """Return whether no diagnostic is an error."""
        return not self.diagnostics.has_errors

    @property
    def rendered(self) -> tuple[RenderedArtifact, ...]:
        """Return each compiled artifact's `rendered_artifact`, in compile order."""
        return tuple(compiled.rendered_artifact for compiled in self.compiled)

    def rendered_for_publish(self, manifest_id: str) -> tuple[RenderedArtifact, ...]:
        """Return each compiled artifact as apply writes it for `manifest_id`, in compile order."""
        return tuple(compiled.rendered_for_publish(manifest_id) for compiled in self.compiled)


class ArtifactCompiler(Protocol):
    """A typed compiler: one kind of authored input in, compiled artifacts and diagnostics out."""

    def run_result(self) -> CompileResult:
        """Compile every input the compiler was built with.

        A problem in the project is reported as a diagnostic, never raised, and the inputs
        it does not concern still compile. Calling it again with unchanged inputs returns
        an equal result.
        """
        ...


Member = TypeVar("Member")
Compiled = TypeVar("Compiled", bound=CompiledArtifact)


def has_error(subject: str, diagnostics: Iterable[Diagnostic]) -> bool:
    """Return whether an error-severity diagnostic names `subject`."""
    return any(item.severity is Severity.ERROR and item.subject == subject for item in diagnostics)


def compile_each(
    members: Iterable[Member],
    key: Callable[[Member], str],
    render: Callable[[Member], Compiled],
    diagnostics: DiagnosticBag,
    *,
    skip: Callable[[str, DiagnosticBag], bool] | None = has_error,
    origin: Callable[[Member], Origin | None] | None = None,
    errors: tuple[type[Exception], ...] = RENDER_ERRORS,
) -> CompileResult:
    """Render each member in order, reporting an unexpected rendering failure as SST-INT902.

    `skip` sees each member's key with the diagnostics so far; a member it accepts is not
    rendered. The default skips a member an error already names, which includes an
    SST-INT902 reported for an earlier member with the same key. `skip=None` renders every
    member. Only `errors` are caught; any other exception escapes.

    Args:
        key: The member's artifact key: what `skip` checks and what SST-INT902 names.
        render: Compiles one member; raising one of `errors` reports SST-INT902 instead.
        diagnostics: What was reported before rendering, which the default `skip` reads.
        origin: Where SST-INT902 points for a member; None points nowhere.

    Returns:
        The rendered members in member order, with `diagnostics` followed by one SST-INT902
        per failure, in member order. `diagnostics` itself is returned when nothing fails.

    Diagnostics:
        SST-INT902: rendering a member raised one of `errors`, although it validated.
    """
    compiled: list[Compiled] = []
    for member in members:
        if skip is not None and skip(key(member), diagnostics):
            continue
        try:
            compiled.append(render(member))
        except errors as exc:
            failure = D(
                "SST-INT902",
                subject=key(member),
                detail=str(exc),
                origin=origin(member) if origin is not None else None,
            )
            diagnostics = DiagnosticBag((*diagnostics, failure))
    return CompileResult(tuple(compiled), diagnostics)

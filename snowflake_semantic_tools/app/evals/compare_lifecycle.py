"""Commit a comparison's versions of one published agent or CoWork profile, and always restore it.

The sequence never exposes a candidate to a caller that names no version, and never moves the
default or the released alias (docs/design/snowflake-behaviour.md):

1. Hold: COMMIT the live version, which holds the released specification. With no live version,
   an unversioned caller is served the default, which `sst apply` pinned by name to the release.
2. Each candidate model: stage the released specification with only `models.orchestration`
   changed, ADD VERSION from it, read the version it committed by its comment's model and run id,
   take `EVAL_<MODEL>` off the version that holds it (an alias names one version), and set it on
   the new one.
3. Restore, whenever step 1 may have committed: stage the released specification, ADD VERSION
   from it, then ADD LIVE VERSION FROM LAST, so the live version holds it again.

A candidate that fails is reported and left out; the others still commit. The restore runs in a
`finally`, so an interrupt or a failure part way still restores. Every version committed carries
`eval_model_comment`'s comment, so plan never takes one for a release.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    RenderedArtifact,
    eval_model_alias,
    eval_model_comment,
    eval_model_run,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort
from snowflake_semantic_tools.domain.render.compare import (
    HOLD_LABEL,
    RESTORE_LABEL,
    add_version_statement,
    hold_statement,
    model_slug,
    open_live_statement,
    set_alias_statement,
    staged_spec_path,
    unset_alias_statement,
    version_directory,
    with_orchestration_model,
)
from snowflake_semantic_tools.domain.sql import Sql


class CompareLifecyclePort(CatalogPort, ExecutionPort, StagePort, Protocol):
    """The Snowflake roles a comparison's versions are committed through: reads, statements, uploads."""


@dataclass(frozen=True, slots=True)
class CompareSubject:
    """One published agent or CoWork profile a comparison commits versions of.

    Attributes:
        kind: `agent` or `cowork_profile`.
        payload: The released specification: the one the compiled manifest renders, which plan
            compares by its `sst-spec:` fingerprint.
        pinned_model: The orchestration model `payload` names.
        stage: The agent specification stage the subject's own versions are staged on.
        folder: The subject's directory on `stage`: its name, or `cowork-profiles/<name>`.
        rendered: The subject as plan publishes it, which the precheck classifies.
    """

    key: str
    kind: str
    name: str
    target: QualifiedName
    payload: str
    pinned_model: str
    stage: QualifiedName
    folder: str
    rendered: RenderedArtifact


@dataclass(frozen=True, slots=True)
class CandidateVersion:
    """The version one candidate model was committed as, and the alias it carries.

    Attributes:
        moved_from: The version that held the alias before, from an earlier comparison; None
            when no version held it.
    """

    model: str
    version: str
    alias: str
    moved_from: str | None = None


@dataclass(frozen=True, slots=True)
class LifecycleResult:
    """What one subject's comparison committed, and what went wrong.

    Attributes:
        hold: The hold copy's version; None when the hold did not commit, or its name was not read.
        candidates: Each candidate committed, in the order asked.
        failed: `(model, reason)` of each candidate that did not commit, in the order asked; every
            candidate, when the hold failed.
        restore: The restore copy's version; None when none committed or its name was not read.
        restored: Whether a live version holds the released specification again, or never closed.
        diagnostics: SST-APL109 for each failed step, then SST-APL110 for a failed restore.
    """

    subject_key: str
    hold: str | None = None
    candidates: tuple[CandidateVersion, ...] = ()
    failed: tuple[tuple[str, str], ...] = ()
    restore: str | None = None
    restored: bool = True
    diagnostics: tuple[Diagnostic, ...] = ()


class _StepFailed(Exception):
    """One statement, upload or read of the sequence failed; its message says which and why."""


@dataclass(slots=True)
class _Progress:
    """How far one subject's sequence got.

    Attributes:
        held: The hold copy committed, so candidates may be added.
        closed: The live version may be closed, so the restore must run: the hold committed,
            or it failed in a way that may still have committed it.
    """

    held: bool = False
    closed: bool = False
    hold: str | None = None
    candidates: list[CandidateVersion] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)


class CompareLifecycle:
    """Commit the hold, candidate and restore versions of a subject, as the module docstring says.

    Args:
        run_id: The comparison's run id, which every version's comment and stage path carry.
        git_sha: The commit the project is at, which every version's comment names.
        dirty: The project had uncommitted changes, so each comment reads `git:<sha>-dirty`.
    """

    def __init__(self, port: CompareLifecyclePort, *, run_id: str, git_sha: str, dirty: bool = False) -> None:
        self._port = port
        self._run_id = run_id
        self._git_sha = git_sha
        self._dirty = dirty

    def commit(self, subject: CompareSubject, models: tuple[str, ...]) -> LifecycleResult:
        """Hold the subject, commit a version per model, then restore it, whatever happened between.

        Never raises for Snowflake: a failed step is a diagnostic. An interrupt during the
        candidates still restores before it propagates. With no model to commit, nothing is
        held or restored, and nothing is sent.

        Diagnostics:
            SST-APL109: the hold, or one candidate's upload, ADD VERSION, version read or alias
                move, failed; once per failure. A failed hold commits no candidate.
            SST-APL110: the restore's upload, ADD VERSION or ADD LIVE VERSION failed, so the
                subject is left with no live version until `sst plan` and `sst apply` repair it.
        """
        if not models:
            return LifecycleResult(subject.key)
        progress = _Progress()
        try:
            self._hold(subject, progress)
            for model in models if progress.held else ():
                try:
                    progress.candidates.append(self._candidate(subject, model))
                except _StepFailed as exc:
                    progress.failed.append((model, str(exc)))
                    progress.diagnostics.append(_commit_failed(subject, f"the candidate {model}", str(exc)))
        finally:
            restore, restored = self._restore(subject, progress) if progress.closed else (None, True)
        if not progress.held:
            progress.failed.extend((model, "the hold copy did not commit") for model in models)
        return LifecycleResult(
            subject.key,
            progress.hold,
            tuple(progress.candidates),
            tuple(progress.failed),
            restore,
            restored,
            tuple(progress.diagnostics),
        )

    def _hold(self, subject: CompareSubject, progress: _Progress) -> None:
        """Commit the live version as the hold copy, recording whether the restore must run.

        A hold that failed may still have committed, as a statement whose reply was lost does,
        so the versions are read: no live version means it did, and a read that fails is taken
        to mean it may have, since a restore beside a live version only fails, harmlessly.
        Either way no candidate is added after a failed hold.
        """
        comment = eval_model_comment(self._git_sha, subject.payload, HOLD_LABEL, self._run_id, dirty=self._dirty)
        try:
            # MEASURED: COMMIT leaves no live version, so an unversioned caller is served the
            # default, pinned by name to the release; ADD VERSION then succeeds beside none.
            self._run(hold_statement(subject.target, comment))
        except _StepFailed as exc:
            progress.diagnostics.append(_commit_failed(subject, "the hold copy", str(exc)))
            try:
                progress.closed = not self._port.observe_agent_versions(subject.target).has_live_version
            except SnowflakePortError:
                progress.closed = True
            return
        progress.held = progress.closed = True
        progress.hold = self._committed_name(subject, HOLD_LABEL)

    def _candidate(self, subject: CompareSubject, model: str) -> CandidateVersion:
        """Stage and commit one candidate's version, then move its alias onto it.

        Raises:
            _StepFailed: the upload, ADD VERSION, the read of its name, or an alias move failed.
        """
        slug = model_slug(model)
        alias = eval_model_alias(model)
        try:
            specification = with_orchestration_model(subject.payload, model)
        except ValueError as exc:
            raise _StepFailed(f"the released specification is not one SST rendered: {exc}") from exc
        self._add_version(subject, specification, model, slug)
        versions = self._versions(subject)
        version = _committed(versions, model, self._run_id)
        if version is None:
            raise _StepFailed(f"the agent's versions list none this comparison committed for {model}")
        holder = next((name for shown, name in versions.alias_versions if shown.casefold() == alias.casefold()), None)
        moved_from = holder if holder is not None and holder.casefold() != version.casefold() else None
        # MEASURED: an alias names one version of an agent; SET ALIAS of one another version
        # carries fails, so it is unset there first.
        if moved_from is not None:
            self._run(unset_alias_statement(subject.target, moved_from))
        self._run(set_alias_statement(subject.target, version, alias))
        return CandidateVersion(model, version, alias, moved_from)

    def _restore(self, subject: CompareSubject, progress: _Progress) -> tuple[str | None, bool]:
        """Commit the restore copy and open a live version from it; report its name and success.

        Diagnostics:
            SST-APL110: a step failed; the subject has no live version.
        """
        try:
            self._add_version(subject, subject.payload, RESTORE_LABEL, RESTORE_LABEL)
            restore = self._committed_name(subject, RESTORE_LABEL)
            # MEASURED: the live version this opens holds LAST's specification, the restore copy's.
            self._run(open_live_statement(subject.target))
        except _StepFailed as exc:
            progress.diagnostics.append(
                D("SST-APL110", artifact=subject.key, detail=str(exc), subject=subject.key, value=subject.target.sql)
            )
            return None, False
        return restore, True

    def _add_version(self, subject: CompareSubject, specification: str, label: str, slug: str) -> None:
        """Stage a specification in its comparison directory and commit a version from it.

        Raises:
            _StepFailed: the upload or ADD VERSION failed.
        """
        path = staged_spec_path(subject.stage, subject.folder, self._run_id, slug)
        try:
            self._port.upload(path, specification.encode("utf-8"))
        except SnowflakePortError as exc:
            raise _StepFailed(f"staging {path} failed: {exc}") from exc
        comment = eval_model_comment(self._git_sha, specification, label, self._run_id, dirty=self._dirty)
        directory = version_directory(subject.folder, self._run_id, slug)
        self._run(add_version_statement(subject.target, subject.stage, directory, comment))

    def _committed_name(self, subject: CompareSubject, label: str) -> str | None:
        """Name the version this comparison committed under `label`; None when it cannot be read."""
        try:
            return _committed(self._port.observe_agent_versions(subject.target), label, self._run_id)
        except SnowflakePortError:
            return None

    def _versions(self, subject: CompareSubject) -> AgentVersions:
        try:
            return self._port.observe_agent_versions(subject.target)
        except SnowflakePortError as exc:
            raise _StepFailed(f"reading SHOW VERSIONS IN AGENT failed: {exc}") from exc

    def _run(self, statement: Sql) -> None:
        """Run one statement as its own script.

        Raises:
            _StepFailed: Snowflake refused it, or the session failed.
        """
        try:
            result = self._port.execute_script((statement,))
        except SnowflakePortError as exc:
            raise _StepFailed(f"{_verb(statement)} failed: {exc}") from exc
        if not result.ok:
            reason = result.error.message if result.error is not None else "unknown Snowflake failure"
            raise _StepFailed(f"{_verb(statement)} failed: {reason}")


def _committed(versions: AgentVersions, label: str, run_id: str) -> str | None:
    """Name the committed version whose comment carries `label` and `run_id`, casefolded alike."""
    wanted = (label.casefold(), run_id.casefold())
    return next(
        (
            version.name
            for version in versions.versions
            if version.name is not None
            and (found := eval_model_run(version.comment)) is not None
            and (found[0].casefold(), found[1].casefold()) == wanted
        ),
        None,
    )


# The clause that names each statement of the sequence, checked in this order: UNSET ALIAS
# before SET ALIAS, and ADD LIVE VERSION before ADD VERSION, since each holds the next.
_CLAUSES = ("COMMIT", "ADD LIVE VERSION", "ADD VERSION", "UNSET ALIAS", "SET ALIAS")


def _verb(statement: Sql) -> str:
    """Name a statement by its clause, as a failure says which step it was."""
    text = " ".join(str(statement).split())
    return next((clause for clause in _CLAUSES if clause in text), "the statement")


def _commit_failed(subject: CompareSubject, what: str, detail: str) -> Diagnostic:
    return D("SST-APL109", artifact=subject.key, value=what, detail=detail, subject=subject.key)

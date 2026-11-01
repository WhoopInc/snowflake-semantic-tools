"""A project loaded once and asked many questions: the library interface to SST.

`ProjectSession.open` reads a dbt + SST project the way `sst compile` does, and keeps what it
read: a session answers what the project declares (`catalog`), what an artifact publishes
(`render`), and what `sst validate --no-snowflake-syntax-check` reports (`validate`), without
reading the project again. A session is a snapshot: `reload` reads the files again into a new
one, and `with_overlay` reads them with unsaved edits over them, in a private copy of the
project, which is never written to. Nothing here prints, exits, or connects to Snowflake.
"""

from __future__ import annotations

import dataclasses
import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.dbt.parse_stamp import dbt_inputs
from snowflake_semantic_tools.adapters.dbt.profiles import read_plain_yaml
from snowflake_semantic_tools.adapters.dbt.run_cache import DbtCache
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, read_baseline
from snowflake_semantic_tools.adapters.fs.working_copy import WorkingCopy
from snowflake_semantic_tools.adapters.git import git_dirty, git_sha
from snowflake_semantic_tools.adapters.locations import (
    PROFILES_FILE,
    SST_TARGET,
    ProjectPaths,
    locate_project,
    ran_under_1_0,
)
from snowflake_semantic_tools.adapters.project_source import DbtOutcome, YamlProjectInputs
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache
from snowflake_semantic_tools.adapters.yaml.semantic.reuse import ViewBuilds
from snowflake_semantic_tools.app.baseline import gate_baseline
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult, RenderedViews
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.diagnostics_json import diagnostic_json, summary_counts
from snowflake_semantic_tools.app.policy import hold_to_policy, severity_policy, strict_disagreement
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, stable_fingerprint
from snowflake_semantic_tools.domain.model.authored_schema.locations import DIRECTORY_DEFAULTS, schema_kind
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_FILE,
    config_block,
    config_bool,
    configured_dir,
    governance_config,
)


@dataclass(frozen=True, slots=True)
class ObjectSummary:
    """One artifact the project declares, as a catalog lists it.

    Attributes:
        key: Its `<type>:<name>` artifact key, unique in the project.
        type: Its registry artifact type, such as `semantic_view` or `agent`.
        name: Its name, as authored.
        target: The fully qualified name it publishes as, as Snowflake SQL.
        source_files: The project files it is compiled from, relative to the project.
        depends_on: The keys of the artifacts that must publish before it.
        member_keys: The keys of the members it holds, such as a view's metrics.
        referenced_models: The dbt models it reads.
        fingerprint: The digest of what it publishes; equal fingerprints publish the same thing.
    """

    key: str
    type: str
    name: str
    target: str
    source_files: tuple[str, ...]
    depends_on: tuple[str, ...]
    member_keys: tuple[str, ...]
    referenced_models: tuple[str, ...]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class Rendered:
    """What one artifact publishes: its target and the statements that create it.

    Attributes:
        key: Its artifact key.
        target: The fully qualified name it publishes as, as Snowflake SQL.
        object_type: The Snowflake object type, such as `SEMANTIC VIEW` or `AGENT`.
        ddl: Its canonical definition, which `fingerprint` digests.
        statements: The statements that publish it, in order.
        fingerprint: The digest of `ddl`.
    """

    key: str
    target: str
    object_type: str
    ddl: str
    statements: tuple[str, ...]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class Validation:
    """What `sst validate --no-snowflake-syntax-check --output json` reports, as data.

    Attributes:
        passed: Whether the run passes: exit 0 rather than 1.
        diagnostics: Each diagnostic as the envelope lists it, in report order.
        summary: The counts by severity, and of what the baseline holds, as the envelope's.
        promoted: How many warnings strict mode made errors.
        rules_run: How many validation rules ran.
        artifacts_checked: How many compiled artifacts were validated.
    """

    passed: bool
    diagnostics: tuple[Mapping[str, object], ...]
    summary: Mapping[str, int]
    promoted: int
    rules_run: int
    artifacts_checked: int


class ProjectSession:
    """A project's files read once and compiled, answering questions about what it declares.

    Build one with `open`. A session never reads the project again: `reload` returns a new one
    from the files as they are then, and `with_overlay` one from them with unsaved edits.
    """

    def __init__(self, base: _Base, overlay: Mapping[str, bytes | None] | None = None) -> None:
        self._base = base
        self._overlay = None if overlay is None else dict(overlay)
        if self._overlay is None:
            read_dir, reuse, outcome = base.paths.project_dir, base.reuse, None
            # Read before compiling, as the CLI reads it before its command runs.
            base.first_run = not ran_under_1_0(read_dir)
        else:
            read_dir, reuse = base.copy.sync(self._overlay), base.overlay_reuse
            outcome = None if _touches_dbt(base.paths.project_dir, self._overlay) else base.outcome
        self._paths = _read_from(base.paths, read_dir)
        project = base.paths.project_dir
        dirty = tuple(sorted(self._overlay or ()))
        self._inputs = YamlProjectInputs(
            self._paths,
            target_name=base.target,
            manifest_path=base.manifest_path,
            git_sha=lambda: git_sha(project),
            git_dirty=lambda: tuple(sorted({*git_dirty(project), *dirty})),
            load_cache=reuse.load_cache,
            dbt_cache=reuse.dbt_cache,
            view_builds=reuse.view_builds,
            dbt_outcome=outcome,
        )
        self._first_run = base.first_run
        self._compiled = CompileProject(self._inputs, renders=reuse.renders).run()
        self._by_key = {item.artifact_key: item for item in self._compiled.compiled}
        self._baseline = _read_baseline(read_dir)
        if self._overlay is None:
            # Compiling read dbt before anything else, so this is what it read, not a new read.
            base.outcome = self._inputs.dbt_outcome()

    @classmethod
    def open(cls, project_dir: Path, *, target: str | None = None, manifest_path: Path | None = None) -> ProjectSession:
        """Read and compile the project in `project_dir`, as `sst compile` would.

        Args:
            target: The `profiles.yml` target to compile for; None takes the profile's default.
            manifest_path: A dbt `manifest.json` to read instead of running `dbt parse`, as
                `--manifest` gives one.

        Raises:
            ProjectError: the project cannot be read at all: no configuration file, a file that
                cannot be read, or dbt failing, each with the diagnostic `sst` reports.
        """
        paths = dataclasses.replace(locate_project(project_dir.resolve()), target_name=target)
        return cls(_Base(paths, target, manifest_path))

    def reload(self, *, dbt: bool = True) -> ProjectSession:
        """Read and compile the project again, as a new session; this one is unchanged.

        A file whose bytes did not change is not parsed again, and a view none of whose inputs
        changed is neither built nor rendered again: what this returns is what `open` would. A
        session `with_overlay` returned is reloaded with the same edits over the project.

        Args:
            dbt: Run `dbt parse` again, as a session must once a dbt file changed; False reads
                the models as this session read them.
        """
        if dbt:
            self._base.reuse = dataclasses.replace(self._base.reuse, dbt_cache=DbtCache())
            self._base.overlay_reuse = dataclasses.replace(self._base.overlay_reuse, dbt_cache=DbtCache())
        if self._overlay is not None:
            return ProjectSession(self._base, self._overlay)
        return ProjectSession(self._base)

    def with_overlay(self, files: Mapping[str, bytes | None]) -> ProjectSession:
        """Return a session for the project's files as they are now, with unsaved edits over them.

        Each key is a path relative to the project: its bytes replace the file, or create it, and
        None deletes it; edits this session already reads over the project are kept, and a key
        given again replaces them. What it returns is what `open` would return for the project
        with the edits saved, but the project is never written to: the session reads a private
        copy of it, made the first time an overlay is asked for and brought back in step with
        the project for each one after.

        dbt is read again only when an edit touches what it reads: a model, macro, seed,
        snapshot, analysis or test file, a dbt project or package file, `profiles.yml`, or
        `sst_config.yml`; otherwise the models are this session's. Overlays of one project share
        what they parse and build, so each reads again only what its edits changed.

        Raises:
            ProjectError: as `open` raises, for the project with the edits over it.
            UnsafeWrite: an edit's path is absolute, leaves the project, or names a folder.
        """
        return ProjectSession(self._base, {**(self._overlay or {}), **files})

    @property
    def project_dir(self) -> Path:
        """The project directory this session describes, resolved; an overlay's is the project's."""
        return self._base.paths.project_dir

    @property
    def overlay(self) -> Mapping[str, bytes | None]:
        """The unsaved edits this session reads over the project; empty for one `open` returned."""
        return MappingProxyType(dict(self._overlay or {}))

    @property
    def compiled(self) -> CompileResult:
        """The compile result, for a caller that needs more than the summaries give."""
        return self._compiled

    def catalog(self) -> tuple[ObjectSummary, ...]:
        """Return every artifact the project compiled, in the order apply publishes them.

        An artifact whose files hold an error is not compiled, so it is not listed; `validate`
        reports why.
        """
        return tuple(_summary(item) for item in self._compiled.compiled)

    def render(self, key: str) -> Rendered:
        """Return what the artifact `key` publishes.

        Raises:
            KeyError: the project compiled no artifact with that key.
        """
        artifact = self._by_key[key].rendered_artifact
        return Rendered(
            key=artifact.key,
            target=artifact.target.sql,
            object_type=artifact.object_type,
            ddl=artifact.ddl,
            statements=tuple(str(statement) for statement in artifact.statements),
            fingerprint=artifact.fingerprint,
        )

    def schema_for(self, path: str) -> str | None:
        """Return the kind, a key of `schemas()`, of the schema that checks the project file at `path`.

        `path` is relative to the project, with `/` separators; None when no schema checks it. A
        file in an `evals/` folder is read, overlay first, to tell a dataset, which lists
        `questions:`, from a config; one that cannot be read is taken for a config.
        """
        config = resolved_config(self._paths, self._base.target).tree
        directories = {key: configured_dir(config, key, default) for key, default in DIRECTORY_DEFAULTS.items()}
        return schema_kind(path, directories, lists_questions=self._lists_questions(path))

    def _lists_questions(self, path: str) -> bool:
        clean = posixpath.normpath(path)
        if "evals" not in clean.split("/")[:-1] or clean.startswith(".."):
            return False
        try:
            return "questions" in read_plain_yaml(self._paths.project_dir / clean)
        except ProjectError:
            return False

    def validate(self, *, strict: bool | None = None, baseline: bool = True) -> Validation:
        """Validate the project offline: what `sst validate --no-snowflake-syntax-check` reports.

        The diagnostics are those the command's JSON envelope lists, in its order, with the
        project's severity overrides, strict mode and baseline applied as the command applies them.

        Args:
            strict: `--strict` or `--no-strict`; None follows `validation.strict`.
            baseline: Hold the diagnostics `.sst/baseline.json` lists, when it exists, as `sst`
                does; False is `--no-baseline`.

        Raises:
            ProjectError: the baseline file cannot be read.
        """
        config = resolved_config(self._paths, self._base.target)
        configured = config_bool(config_block(config.tree.get("validation")).get("strict"))
        effective = strict if strict is not None else bool(configured)
        result = ValidateArtifacts(governance=governance_config(config.tree), target=self._base.target or "").run(
            self._compiled,
            strict=effective,
            connected=False,
            overrides=severity_policy(config.tree).overrides,
        )
        diagnostics = DiagnosticBag((*strict_disagreement(config, strict), *result.diagnostics))
        held_baseline = self._baseline.read() if baseline else None
        held = hold_to_policy(
            "validate",
            diagnostics,
            config,
            gated=True,
            promoted=result.promoted,
            baselined=held_baseline is not None,
            notice_due=self._first_run,
            strict=strict,
        )
        diagnostics = held.diagnostics
        passed = result.success if held.blocks is None else not held.blocks
        promoted = result.promoted
        baselined: frozenset[str] = frozenset()
        if held_baseline is not None:
            gate = gate_baseline(diagnostics, held_baseline, clock=SystemClock(), strict=effective)
            diagnostics, baselined, promoted = gate.diagnostics, gate.baselined, promoted + gate.promoted
            if not passed and gate.forgiven:
                passed = True
            if passed and gate.expired:
                passed = False
        return Validation(
            passed=passed,
            diagnostics=tuple(
                diagnostic_json(item, baselined=stable_fingerprint(item) in baselined) for item in diagnostics
            ),
            summary=_summary_counts(diagnostics, baselined, promoted),
            promoted=promoted,
            rules_run=result.rules_run,
            artifacts_checked=result.artifacts_checked,
        )


def _read_from(paths: ProjectPaths, read_dir: Path) -> ProjectPaths:
    """The paths of the project as read from `read_dir`: the project's, or its working copy's.

    A configuration file inside the project is read from the same place in the copy; one the
    run named outside it is read where it is.
    """
    config_file = paths.config_file
    if config_file is not None and config_file.is_relative_to(paths.project_dir):
        config_file = read_dir / config_file.relative_to(paths.project_dir)
    candidates = tuple(
        read_dir / item.relative_to(paths.project_dir) if item.is_relative_to(paths.project_dir) else item
        for item in paths.candidates
    )
    return dataclasses.replace(paths, project_dir=read_dir, config_file=config_file, candidates=candidates, resolved={})


@dataclass(frozen=True, slots=True)
class _HeldBaseline:
    """`.sst/baseline.json` as a session read it: the baseline, none, or why it cannot be read."""

    baseline: Baseline | None = None
    refused: ProjectError | None = None

    def read(self) -> Baseline | None:
        """Return the baseline the session read; None when there is none.

        Raises:
            ProjectError: the file could not be read as a baseline when the session read it.
        """
        if self.refused is not None:
            raise self.refused
        return self.baseline


def _read_baseline(project_dir: Path) -> _HeldBaseline:
    """Read `.sst/baseline.json` as `sst` does with no `--baseline`, keeping a refusal for later."""
    path = project_dir / BASELINE_FILE
    if not path.is_file():
        return _HeldBaseline()
    try:
        return _HeldBaseline(read_baseline(path, BASELINE_FILE.as_posix()))
    except ProjectError as exc:
        return _HeldBaseline(refused=exc)


def _summary_counts(diagnostics: DiagnosticBag, baselined: frozenset[str], promoted: int) -> dict[str, int]:
    """The envelope's `summary`, its keys in its order."""
    counts = summary_counts(diagnostics, baselined)
    return {
        "error": counts["error"],
        "warning": counts["warning"],
        "info": counts["info"],
        "promoted": promoted,
        "suppressed_cascade": sum(item.cascaded for item in diagnostics),
        "baselined": counts["baselined"],
    }


@dataclass(frozen=True, slots=True)
class _Reuse:
    """What one session's loads share, so a load reads and builds again only what changed."""

    load_cache: LoadCache = field(default_factory=LoadCache)
    dbt_cache: DbtCache = field(default_factory=DbtCache)
    view_builds: ViewBuilds = field(default_factory=ViewBuilds)
    renders: RenderedViews = field(default_factory=RenderedViews)


class _Base:
    """What every session of one project shares: how it was opened, its caches, and its copy.

    `reuse` is the project's own sessions'; `overlay_reuse` is the overlays', which read the copy
    and so hold views read from another directory. `outcome` is what the last session of the
    project itself read from dbt, which an overlay that touches nothing dbt reads answers from.
    `first_run` is whether the project had run under SST 1.0 when that session opened.
    """

    def __init__(self, paths: ProjectPaths, target: str | None, manifest_path: Path | None) -> None:
        self.paths = paths
        self.target = target
        self.manifest_path = manifest_path
        self.reuse = _Reuse()
        self.overlay_reuse = _Reuse()
        self.outcome: DbtOutcome | None = None
        self.first_run = True
        self.copy = WorkingCopy(paths.project_dir, skipped=_unread_paths(paths.project_dir))


def _unread_paths(project_dir: Path) -> frozenset[str]:
    """The project paths no compile reads, so a working copy leaves out: dbt's and SST's output."""
    skipped = {"logs", "target", f"{SST_TARGET}/manifest.json"}
    try:
        project = read_plain_yaml(project_dir / "dbt_project.yml")
    except ProjectError:
        return frozenset(skipped)
    for key in ("target-path", "log-path"):
        if isinstance(project.get(key), str):
            skipped.add(Path(str(project[key])).as_posix())
    return frozenset(skipped)


def _touches_dbt(project_dir: Path, overlay: Mapping[str, bytes | None]) -> bool:
    """Report whether an edit is to something dbt reads, or SST's own `dbt:` and `defer:` settings.

    A project whose `dbt_project.yml` cannot be read is treated as touched, so dbt reports it.
    """
    try:
        files, folders = dbt_inputs(project_dir)
    except ProjectError:
        return True
    named = {*files, PROFILES_FILE, CONFIG_FILE}
    for relative in overlay:
        path = Path(relative).as_posix()
        if path in named or any(path == folder or path.startswith(f"{folder}/") for folder in folders):
            return True
    return False


def _summary(item: CompiledArtifact) -> ObjectSummary:
    artifact = item.rendered_artifact
    return ObjectSummary(
        key=item.artifact_key,
        type=item.artifact_type,
        name=item.name,
        target=artifact.target.sql,
        source_files=tuple(item.source_files),
        depends_on=tuple(artifact.depends_on),
        member_keys=tuple(item.member_keys),
        referenced_models=tuple(item.referenced_models),
        fingerprint=artifact.fingerprint,
    )

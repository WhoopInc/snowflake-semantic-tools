"""A project loaded once and asked many questions: the library interface to SST.

`ProjectSession.open` reads a dbt + SST project the way `sst compile` does, and keeps what it
read: a session answers what the project declares (`catalog`), what an artifact publishes
(`render`), and what `sst validate --no-snowflake-syntax-check` reports (`validate`), without
reading the project again. A session is a snapshot: `reload` reads the files again into a new
one, sharing the parse cache, so a file left unchanged is not parsed twice. Nothing here prints,
exits, or connects to Snowflake.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.dbt.run_cache import DbtCache
from snowflake_semantic_tools.adapters.fs.baseline import BASELINE_FILE, read_baseline
from snowflake_semantic_tools.adapters.git import git_dirty, git_sha
from snowflake_semantic_tools.adapters.locations import ProjectPaths, locate_project, ran_under_1_0
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache
from snowflake_semantic_tools.app.baseline import gate_baseline
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.diagnostics_json import diagnostic_json, summary_counts
from snowflake_semantic_tools.app.policy import hold_to_policy, severity_policy, strict_disagreement
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, stable_fingerprint
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool, governance_config


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

    Build one with `open`. A session never reads the project again; `reload` returns a new one
    from the files as they are then.
    """

    def __init__(
        self,
        paths: ProjectPaths,
        *,
        target: str | None,
        manifest_path: Path | None,
        load_cache: LoadCache,
        dbt_cache: DbtCache,
    ) -> None:
        self._paths = paths
        self._target = target
        self._manifest_path = manifest_path
        self._load_cache = load_cache
        self._dbt_cache = dbt_cache
        project = paths.project_dir
        inputs = YamlProjectInputs(
            paths,
            target_name=target,
            manifest_path=manifest_path,
            git_sha=lambda: git_sha(project),
            git_dirty=lambda: git_dirty(project),
            load_cache=load_cache,
            dbt_cache=dbt_cache,
        )
        # Read before compiling, as the CLI reads it before its command runs.
        self._first_run = not ran_under_1_0(project)
        self._compiled = CompileProject(inputs).run()
        self._by_key = {item.artifact_key: item for item in self._compiled.compiled}

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
        return cls(paths, target=target, manifest_path=manifest_path, load_cache=LoadCache(), dbt_cache=DbtCache())

    def reload(self, *, dbt: bool = True) -> ProjectSession:
        """Read and compile the project again, as a new session; this one is unchanged.

        Files whose bytes did not change are not parsed again.

        Args:
            dbt: Run `dbt parse` again, as a session must once a dbt file changed; False reads
                the models as this session read them.
        """
        return ProjectSession(
            dataclasses.replace(self._paths, resolved={}),
            target=self._target,
            manifest_path=self._manifest_path,
            load_cache=self._load_cache,
            dbt_cache=DbtCache() if dbt else self._dbt_cache,
        )

    @property
    def project_dir(self) -> Path:
        """The project directory this session read, resolved."""
        return self._paths.project_dir

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
        config = resolved_config(self._paths, self._target)
        configured = config_bool(config_block(config.tree.get("validation")).get("strict"))
        effective = strict if strict is not None else bool(configured)
        result = ValidateArtifacts(governance=governance_config(config.tree), target=self._target or "").run(
            self._compiled,
            strict=effective,
            connected=False,
            overrides=severity_policy(config.tree).overrides,
        )
        diagnostics = DiagnosticBag((*strict_disagreement(config, strict), *result.diagnostics))
        held_baseline = self._baseline() if baseline else None
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

    def _baseline(self) -> Baseline | None:
        """Read `.sst/baseline.json` as `sst` does with no `--baseline`; None when there is none."""
        path = self._paths.project_dir / BASELINE_FILE
        return read_baseline(path, BASELINE_FILE.as_posix()) if path.is_file() else None


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

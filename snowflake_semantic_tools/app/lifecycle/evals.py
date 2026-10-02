"""Composite plan and publication lifecycle for immutable evaluation datasets."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from hashlib import md5, sha256
from threading import Lock
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.evals import METRIC_COMPONENT_PREFIX
from snowflake_semantic_tools.app.lifecycle.composite import (
    CompositeHandler,
    PublicationRun,
    blocked,
    failed,
)
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE, EVAL_MINT_NEVER
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOutcome,
    Change,
    ChangeReason,
    ClassifiedError,
    CompositeObservation,
    CompositePlan,
    ErrorKind,
    ExecResult,
    OutcomeStatus,
    PhysicalResource,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry, ResourceStatus

_EVAL_STAGE_FILE_FORMAT = sql(
    "TYPE='CSV' FIELD_DELIMITER=NONE RECORD_DELIMITER='\\n' SKIP_HEADER=0 "
    "FIELD_OPTIONALLY_ENCLOSED_BY=NONE ESCAPE_UNENCLOSED_FIELD=NONE"
)
EVAL_STAGE_FILE_FORMAT = str(_EVAL_STAGE_FILE_FORMAT)


@dataclass(frozen=True, slots=True)
class EvalLifecycleConfig:
    """Where evals stage their run configuration: one stage per target schema, named here."""

    config_stage_name: str = DEFAULT_EVAL_CONFIG_STAGE


class EvalLifecycleHandler(CompositeHandler[RenderedArtifact, CompositeObservation, CatalogPublicationPort]):
    """Plan and publish one agent's evaluation: its source table, dataset, and staged config.

    The table and dataset are immutable: a changed dataset publishes new ones, and state
    retains the old ones, which prune only reports.
    """

    artifact_type = "eval"
    _prune_detail = "keep them while an evaluation run or baseline references them"
    _prune_order = 400

    def __init__(self, port: CatalogPublicationPort, config: EvalLifecycleConfig = EvalLifecycleConfig()) -> None:
        super().__init__(port)
        self._config = config
        # Every eval in a schema shares its config stage, and the first to apply
        # creates it; the others' plans saw it absent.
        self._stage_lock = Lock()
        self._created_stages: set[tuple[str, str, str]] = set()

    @staticmethod
    def merge_physical_resources(
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[tuple[str, str] | tuple[str, str, ResourceStatus], ...]:
        """Return the resources verified now, retaining the tables and datasets they superseded."""
        if previous is None:
            return current
        current_identities = {
            (object_type.upper(), qualified_name.casefold()) for object_type, qualified_name in current
        }
        retained = tuple(
            (resource.object_type, resource.qualified_name, ResourceStatus.RETAINED)
            for resource in previous.applied_resources
            if resource.object_type.upper() in {"TABLE", "DATASET"}
            and (resource.object_type.upper(), resource.qualified_name.casefold()) not in current_identities
        )
        return (*current, *retained)

    def config_path(self, artifact: RenderedArtifact) -> str:
        """Return the stage path the eval's config is uploaded to, below its agent's segment.

        Raises:
            ValueError: the artifact carries no config fingerprint.
        """
        return self._config_path(artifact)

    def config_stage(self, artifact: RenderedArtifact) -> QualifiedName:
        """Return the stage in the eval's target schema that holds its run configs."""
        return self._config_stage(artifact)

    def _subject(self, artifact: RenderedArtifact) -> RenderedArtifact:
        return artifact

    def _observe(self, artifact: RenderedArtifact) -> CompositeObservation:
        """Read the table, dataset, config stage, and staged config the eval publishes."""
        resources = tuple(
            PhysicalResource(object_type, name, self._port.object_exists(object_type, name))
            for object_type, name in artifact.physical_resources
        )
        stage = self._config_stage(artifact)
        stage_exists = self._port.object_exists("STAGE", stage)
        stage_format = self._port.describe_stage_file_format(stage) if stage_exists else None
        problem = _format_problem(stage, stage_format) if stage_exists else None
        config_path = self._config_path(artifact)
        staged_config = self._port.observe_staged_file(config_path) if stage_exists else None
        staged_content = self._port.read_staged_file(config_path) if staged_config is not None else None
        return CompositeObservation(
            key=artifact.key,
            resources=resources,
            stage_exists=stage_exists,
            stage_file_format=stage_format,
            config_path=config_path,
            config_exists=staged_config is not None,
            diagnostics=DiagnosticBag(() if problem is None else (problem,)),
            config_size=len(staged_content) if staged_content is not None else None,
            config_md5=(md5(staged_content, usedforsecurity=False).hexdigest() if staged_content is not None else None),
        )

    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        subject: RenderedArtifact,
        observation: CompositeObservation,
    ) -> CompositePlan:
        """Decide an eval's change; a non-minting eval is decided by `_decide_unminted`.

        Diagnostics:
            SST-VAL744: a custom metric is rendered under the name state recorded with another
                definition, as `edited_metrics` reports.
            SST-PLN024: an object the eval publishes exists, and state does not record it as SST's.
        """
        del subject
        if observation.diagnostics.has_errors:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, observation.diagnostics)
        edited = edited_metrics(artifact, state_entry)
        if edited:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, DiagnosticBag(edited))
        if not mints(artifact):
            return self._decide_unminted(artifact, state_entry, observation)
        recorded_resources = self._recorded_resource_identity(state_entry)
        unmanaged_live_resources = tuple(
            item
            for item in observation.resources
            if item.exists and (item.object_type.upper(), item.qualified_name.folded) not in recorded_resources
        )
        if unmanaged_live_resources:
            live = ", ".join(item.qualified_name.sql for item in unmanaged_live_resources)
            return blocked(
                observation, D("SST-PLN024", artifact=artifact.key, value=live), ChangeReason.UNMANAGED_OBJECT
            )
        if state_entry is None:
            return CompositePlan(Action.CREATE, ChangeReason.NOT_PRESENT, observation)
        return self._decide_update(artifact, state_entry, observation, recorded_resources)

    def _decide_unminted(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        observation: CompositeObservation,
    ) -> CompositePlan:
        """Decide an eval whose dataset someone else minted: it must exist, and only its config publishes.

        The dataset is not SST's, so it is never reported unmanaged and never recorded.

        Diagnostics:
            SST-SNO003: the dataset the eval runs against does not exist, or the role cannot see it.
        """
        dataset = self._resources_by_type(artifact)["DATASET"]
        if not all(item.exists for item in observation.resources):
            return blocked(observation, D("SST-SNO003", value=dataset.sql, subject=artifact.key))
        if state_entry is None:
            return CompositePlan(Action.CREATE, ChangeReason.NOT_PRESENT, observation)
        recorded = dict(state_entry.component_fingerprints)
        if (
            recorded.get("config") != dict(artifact.component_fingerprints).get("config")
            or not observation.config_exists
            or observation.config_size != len(artifact.ddl.encode("utf-8"))
            or observation.config_md5 != recorded.get("config_stage_md5")
        ):
            return CompositePlan(Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, observation)
        return CompositePlan(Action.NOOP, ChangeReason.UNCHANGED, observation)

    def _decide_update(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry,
        observation: CompositeObservation,
        recorded_resources: frozenset[tuple[str, tuple[str, str, str]]],
    ) -> CompositePlan:
        """Decide an eval state records: a new dataset, a missing resource, a new config, or nothing.

        Diagnostics:
            SST-PLN025: state records resources this eval no longer publishes to.
        """
        desired_components = dict(artifact.component_fingerprints)
        recorded_components = dict(state_entry.component_fingerprints)
        if not recorded_components:
            return CompositePlan(Action.UPDATE, ChangeReason.NO_PRIOR_STATE, observation)
        if recorded_components.get("dataset") != desired_components.get("dataset"):
            return CompositePlan(Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, observation)
        desired_resources = self._resource_identity(artifact.physical_resources)
        if recorded_resources - desired_resources:
            diagnostic = D(
                "SST-PLN025",
                artifact=artifact.key,
                found=", ".join(sorted(resource.qualified_name for resource in state_entry.applied_resources)),
                expected=", ".join(name.sql for _, name in artifact.physical_resources),
            )
            return blocked(observation, diagnostic, ChangeReason.TARGET_MOVED)
        observed_resources = {
            (item.object_type.upper(), item.qualified_name.folded): item.exists for item in observation.resources
        }
        if not all(observed_resources.get(identity, False) for identity in desired_resources):
            return CompositePlan(Action.UPDATE, ChangeReason.NOT_PRESENT, observation)
        if (
            recorded_components.get("config") != desired_components.get("config")
            or not observation.config_exists
            or observation.config_size != len(artifact.ddl.encode("utf-8"))
            or observation.config_md5 != recorded_components.get("config_stage_md5")
        ):
            return CompositePlan(Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, observation)
        return CompositePlan(Action.NOOP, ChangeReason.UNCHANGED, observation)

    def _publish(self, change: Change, artifact: RenderedArtifact) -> ApplyOutcome:
        observation = change.composite_observation or self._observe(artifact)
        if observation.diagnostics.has_errors:
            return _diagnosed(change, observation.diagnostics[0])
        if self._changed_since_plan(observation, artifact):
            return failed(change, "composite resources changed since plan", code="SST-APL012")
        return _EvalRun(self, change, artifact).publish()

    def _apply_unrendered(self, change: Change) -> ApplyOutcome:
        """Fail a change without its rendered eval: there is no dataset or config to publish."""
        return failed(change, "missing rendered eval artifact")

    def _config_stage(self, artifact: RenderedArtifact) -> QualifiedName:
        return QualifiedName(
            artifact.target.database, artifact.target.schema, Identifier.parse(self._config.config_stage_name)
        )

    def _config_path(self, artifact: RenderedArtifact) -> str:
        config = dict(artifact.component_fingerprints).get("config")
        if config is None:
            raise ValueError("eval artifact requires a config fingerprint")
        stage = self._config_stage(artifact)
        agent_prefix = artifact_key("agent", "")
        dependency = next((key for key in artifact.depends_on if key.startswith(agent_prefix)), artifact.key)
        agent_name = split_artifact_key(dependency)[1]
        segment = _safe_stage_segment(agent_name)
        return f"@{stage.sql}/{segment}/{config}.yaml"

    def _changed_since_plan(self, planned: CompositeObservation, artifact: RenderedArtifact) -> bool:
        """Report whether an eval changed since its plan, allowing for a config stage this run created."""
        # Under the lock, a sibling creating the stage is seen before it starts or
        # once the stage is recorded, never in between.
        with self._stage_lock:
            current = self._observe(artifact)
            if (
                not planned.stage_exists
                and current.stage_exists
                and self._config_stage(artifact).folded in self._created_stages
            ):
                # A sibling eval's apply created the shared stage after this plan saw
                # it absent. Its format is still checked before the config is uploaded.
                planned = replace(planned, stage_exists=True, stage_file_format=current.stage_file_format)
        return _observation_identity(current) != _observation_identity(planned)

    @staticmethod
    def _resources_by_type(artifact: RenderedArtifact) -> MappingProxyType[str, QualifiedName]:
        resources = {object_type.upper(): name for object_type, name in artifact.physical_resources}
        if mints(artifact) and set(resources) != {"TABLE", "DATASET"}:
            raise ValueError("eval artifact requires one TABLE and one DATASET resource")
        if not mints(artifact) and set(resources) != {"DATASET"}:
            raise ValueError("an eval that does not mint requires one DATASET resource")
        return MappingProxyType(resources)

    @staticmethod
    def _resource_identity(
        resources: tuple[tuple[str, QualifiedName], ...],
    ) -> frozenset[tuple[str, tuple[str, str, str]]]:
        return frozenset((object_type.upper(), name.folded) for object_type, name in resources)

    @staticmethod
    def _recorded_resource_identity(
        state_entry: AppliedEntry | None,
    ) -> frozenset[tuple[str, tuple[str, str, str]]]:
        if state_entry is None:
            return frozenset()
        values = []
        for resource in state_entry.applied_resources:
            if resource.status is ResourceStatus.RETAINED:
                continue
            object_type = resource.object_type
            raw_name = resource.qualified_name
            if object_type.upper() not in {"TABLE", "DATASET"}:
                continue
            if not raw_name:
                continue
            values.append((object_type.upper(), QualifiedName.parse(raw_name).folded))
        return frozenset(values)

    @staticmethod
    def _source_statements(artifact: RenderedArtifact) -> tuple[Sql, ...]:
        statements = tuple(statement for statement in artifact.create_statements if str(statement).strip())
        if len(statements) < 2:
            raise ValueError("eval artifact requires CREATE TABLE and INSERT statements")
        return statements[:-1]

    @staticmethod
    def _dataset_statement(artifact: RenderedArtifact) -> Sql:
        statements = tuple(statement for statement in artifact.create_statements if str(statement).strip())
        if len(statements) < 2:
            raise ValueError("eval artifact requires a dataset statement")
        return statements[-1]

    def _source_row_count(self, source_table: QualifiedName) -> int:
        result = self._port.query(sql("SELECT COUNT(*) AS ROW_COUNT FROM {table}", table=qname(source_table)))
        if not result.rows or not result.rows[0]:
            raise SnowflakePortError("COUNT(*) returned no row")
        value = result.rows[0][0]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise SnowflakePortError(f"COUNT(*) returned {type(value).__name__}")
        return int(value)

    @staticmethod
    def _expected_row_count(statements: tuple[Sql, ...]) -> int:
        if len(statements) < 2:
            return 0
        insert = str(statements[1])
        return insert.count("\nUNION ALL\n") + 1

    @staticmethod
    def _create_stage_sql(stage: QualifiedName) -> Sql:
        return sql(
            "CREATE STAGE IF NOT EXISTS {stage} FILE_FORMAT = ({file_format})",
            stage=qname(stage),
            file_format=_EVAL_STAGE_FILE_FORMAT,
        )


class _EvalRun(PublicationRun[CatalogPublicationPort]):
    """One eval publication: each resource created only when absent, and verified before the next.

    The steps run in order: the source table, its row count, the dataset, the config
    stage and its file format, then the config file and its read-back. Resources are
    recorded as each is verified; a failure reports those, or the table alone once its
    statements ran. A port error that interrupts the run also reports what it created.
    """

    def __init__(self, handler: EvalLifecycleHandler, change: Change, artifact: RenderedArtifact) -> None:
        super().__init__(handler._port, change, artifact)
        self._handler = handler
        resources = handler._resources_by_type(artifact)
        self._mints = mints(artifact)
        self._table = resources.get("TABLE", resources["DATASET"])
        self._dataset = resources["DATASET"]
        self._stage = handler._config_stage(artifact)
        self._table_exists = False
        self._dataset_exists = False
        self._statements: tuple[Sql, ...] = ()
        # What this run's CREATE statements made, verified or not: SST's once created.
        self._created: list[tuple[str, str]] = []

    def publish(self) -> ApplyOutcome:
        """Publish the eval as the class describes; a port error fails it, keeping what it wrote."""
        # A read-back error after a CREATE must still report what the run created, or
        # state would forget it and every later plan would call it unmanaged.
        try:
            # Both are read before any write, so what this run creates is verified, not found.
            self._dataset_exists = self._port.object_exists("DATASET", self._dataset)
            if not self._mints:
                failure = self._run_steps(self._require_dataset, self._ensure_config_stage, self._verify_stage_format)
                return failure if failure is not None else self._upload_config()
            self._table_exists = self._port.object_exists("TABLE", self._table)
            self._statements = self._handler._source_statements(self._artifact)
            failure = self._run_steps(
                self._create_source_table,
                self._verify_source_rows,
                self._create_dataset,
                self._ensure_config_stage,
                self._verify_stage_format,
            )
            return failure if failure is not None else self._upload_config()
        except SnowflakePortError as exc:
            return self._interrupted(exc)

    def _interrupted(self, error: SnowflakePortError) -> ApplyOutcome:
        """Fail on a port error with each resource verified or created so far, partial once it wrote."""
        return failed(
            self._change,
            f"{error}",
            code="SST-APL016" if self._written else "SST-APL001",
            write_succeeded=self._written,
            attempts=self._attempts,
            physical_resources=tuple(dict.fromkeys((*self._verified, *self._created))),
        )

    def _create_source_table(self) -> ApplyOutcome | None:
        if self._table_exists:
            return None
        for index, statement in enumerate(self._statements):
            result = self._run_statement(statement)
            if not result.ok:
                return self._source_failure(index, result)
            if index == 0:
                self._created.append(("TABLE", self._table.sql))
                if not self._port.object_exists("TABLE", self._table):
                    return self.fail(f"source table {self._table.sql} is absent after publication", "SST-APL016")
        return None

    def _source_failure(self, index: int, result: ExecResult) -> ApplyOutcome:
        """Fail a source statement, as a partial write once it or an earlier statement wrote."""
        wrote = bool(result.query_ids or result.rows_affected) or index > 0
        # After the CREATE TABLE ran, or when a failed one still left the table, it is SST's.
        table = (("TABLE", self._table.sql),) if index > 0 or self._port.object_exists("TABLE", self._table) else ()
        return failed(
            self._change,
            result.error.message if result.error else "source-table publication failed",
            code="SST-APL016" if wrote else "SST-APL001",
            write_succeeded=wrote,
            attempts=self._attempts,
            physical_resources=table,
        )

    def _verify_source_rows(self) -> ApplyOutcome | None:
        """Require the source table to hold one row per rendered question (SST-APL016)."""
        try:
            found_rows = self._handler._source_row_count(self._table)
        except SnowflakePortError as exc:
            detail = f"source table {self._table.sql} row count is unreadable: {exc}"
            return self._table_failure(detail, "SST-APL016" if self._written else "SST-APL001")
        expected_rows = self._handler._expected_row_count(self._statements)
        if found_rows != expected_rows:
            detail = f"source table {self._table.sql} has {found_rows} rows, expected {expected_rows}"
            return self._table_failure(detail, "SST-APL016")
        self._verified.append(("TABLE", self._table.sql))
        return None

    def _table_failure(self, detail: str, code: str) -> ApplyOutcome:
        # The table exists by now, created or found, so it is reported though unverified.
        return failed(
            self._change,
            detail,
            code=code,
            write_succeeded=self._written,
            attempts=self._attempts,
            physical_resources=(("TABLE", self._table.sql),),
        )

    def _create_dataset(self) -> ApplyOutcome | None:
        if not self._dataset_exists:
            result = self._run_statement(self._handler._dataset_statement(self._artifact))
            if not result.ok:
                return self.fail(result.error.message if result.error else "dataset publication failed", "SST-APL022")
            self._created.append(("DATASET", self._dataset.sql))
            if not self._port.object_exists("DATASET", self._dataset):
                return self.fail(f"dataset {self._dataset.sql} is absent after publication", "SST-APL016")
        self._verified.append(("DATASET", self._dataset.sql))
        return None

    def _require_dataset(self) -> ApplyOutcome | None:
        """Require the dataset a non-minting eval runs against to exist; SST never creates it."""
        if self._dataset_exists:
            return None
        return self.fail(f"dataset {self._dataset.sql} does not exist, and the eval does not mint it")

    def _ensure_config_stage(self) -> ApplyOutcome | None:
        """Create the shared config stage unless it exists, and record it for sibling evals."""
        stage = self._stage
        with self._handler._stage_lock:
            if self._port.object_exists("STAGE", stage):
                return None
            result = self._run_statement(self._handler._create_stage_sql(stage))
            if not result.ok:
                # The table and dataset are verified by now, so state keeps them even
                # when this eval then fails on the stage it shares.
                return failed(
                    self._change,
                    result.error.message if result.error else "config-stage creation failed",
                    write_succeeded=True,
                    attempts=self._attempts,
                    physical_resources=self._recorded_resources(),
                )
            self._created.append(("STAGE", stage.sql))
            if not self._port.object_exists("STAGE", stage):
                return self.fail(f"config stage {stage.sql} is absent after creation", "SST-APL016")
            self._handler._created_stages.add(stage.folded)
            self._verified.append(("STAGE", stage.sql))
        return None

    def _verify_stage_format(self) -> ApplyOutcome | None:
        """Require the config stage to declare exactly the file format evals read (SST-APL028)."""
        problem = _format_problem(self._stage, self._port.describe_stage_file_format(self._stage))
        if problem is None:
            return None
        return _diagnosed(
            self._change,
            problem,
            attempts=self._attempts,
            write_succeeded=self._written,
            component_fingerprints=self._artifact.component_fingerprints,
            physical_resources=self._recorded_resources(),
        )

    def _upload_config(self) -> ApplyOutcome:
        """Upload the rendered config and read it back; then the eval is published."""
        artifact = self._artifact
        config_path = self._handler._config_path(artifact)
        if dict(artifact.component_fingerprints).get("config") is None:
            return failed(self._change, "eval artifact has no config fingerprint", write_succeeded=self._written)
        content = artifact.ddl.encode("utf-8")
        self._attempts += 1
        try:
            self._port.upload(config_path, content)
        except SnowflakePortError as exc:
            return self.fail(str(exc))
        self._written = True
        failure = self._verify_config(config_path, content)
        if failure is not None:
            return failure
        return self.applied(
            write_succeeded=self._written or self._change.action is Action.UPDATE,
            component_fingerprints=(
                *artifact.component_fingerprints,
                ("config_stage_md5", md5(content, usedforsecurity=False).hexdigest()),
            ),
            # A dataset SST did not mint is not SST's to record.
            physical_resources=(
                tuple((object_type, name.sql) for object_type, name in artifact.physical_resources)
                if self._mints
                else ()
            ),
        )

    def _verify_config(self, config_path: str, content: bytes) -> ApplyOutcome | None:
        """Require the staged config to read back as exactly the uploaded bytes (SST-APL016)."""
        if self._port.observe_staged_file(config_path) is None:
            return self.fail(f"staged config {config_path} is absent after upload", "SST-APL016")
        staged = self._port.read_staged_file(config_path)
        if staged is None:
            return self.fail(f"staged config {config_path} is unreadable after upload", "SST-APL016")
        if len(staged) != len(content):
            return self.fail(
                f"staged config {config_path} has {len(staged)} bytes, expected {len(content)}", "SST-APL016"
            )
        if staged != content:
            return self.fail(f"staged config {config_path} bytes do not match the rendered config", "SST-APL016")
        return None


def _diagnosed(
    change: Change,
    diagnostic: Diagnostic,
    *,
    attempts: int = 0,
    write_succeeded: bool = False,
    component_fingerprints: tuple[tuple[str, str], ...] = (),
    physical_resources: tuple[tuple[str, str], ...] = (),
) -> ApplyOutcome:
    """Return a failed outcome carrying a diagnostic's code and message, unclassified."""
    artifact = change.rendered
    return ApplyOutcome(
        change.key,
        change.action,
        OutcomeStatus.FAILED,
        attempts,
        0,
        artifact.ddl if artifact is not None else "",
        ClassifiedError(diagnostic.code, diagnostic.message, ErrorKind.UNKNOWN),
        write_succeeded=write_succeeded,
        component_fingerprints=component_fingerprints,
        physical_resources=physical_resources,
    )


def edited_metrics(artifact: RenderedArtifact, state_entry: AppliedEntry | None) -> tuple[Diagnostic, ...]:
    """Report each custom metric the eval renders under a name state recorded with another definition.

    A custom metric has no Snowflake version: its name is the score's column and chart, so an
    edited prompt published under the same name would read as a continuous trend across a change
    of judge. A metric versions by taking a new name.

    Diagnostics:
        SST-VAL744: the metric's definition digest differs from the one state recorded for its name.
    """
    if state_entry is None:
        return ()
    recorded = dict(state_entry.component_fingerprints)
    edited = (
        key.removeprefix(METRIC_COMPONENT_PREFIX)
        for key, digest in artifact.component_fingerprints
        if key.startswith(METRIC_COMPONENT_PREFIX) and recorded.get(key, digest) != digest
    )
    return tuple(D("SST-VAL744", subject=f"eval_metric:{name}", artifact=name) for name in edited)


def mints(artifact: RenderedArtifact) -> bool:
    """Report whether an eval artifact's apply creates its source table and dataset (`dataset.mint`)."""
    return dict(artifact.component_fingerprints).get("mint") != EVAL_MINT_NEVER


def eval_stage_format_matches(stage_format: str | None) -> bool:
    """Report whether a stage's declared file format is the one evals read, compared key by key."""
    return _normalize_file_format(stage_format) == _normalize_file_format(EVAL_STAGE_FILE_FORMAT)


def _format_problem(stage: QualifiedName, stage_format: str | None) -> Diagnostic | None:
    """Return SST-APL028 when the config stage declares another file format than evals read."""
    if eval_stage_format_matches(stage_format):
        return None
    return D("SST-APL028", value=stage.sql, found=stage_format or "absent", expected=EVAL_STAGE_FILE_FORMAT)


def _normalize_file_format(value: str | None) -> str:
    if value is None:
        return ""
    pairs = re.findall(r"([A-Z_]+)\s*=\s*(?:'([^']*)'|([^\s]+))", value.upper())
    normalized = {key: quoted if quoted != "" else raw for key, quoted, raw in pairs}
    return " ".join(f"{key}={normalized[key]}" for key in sorted(normalized))


def _observation_identity(value: CompositeObservation) -> tuple[object, ...]:
    return (
        tuple((item.object_type, item.qualified_name.folded, item.exists) for item in value.resources),
        value.stage_exists,
        _normalize_file_format(value.stage_file_format),
        value.config_path,
        value.config_exists,
        value.config_size,
        value.config_md5,
    )


def _safe_stage_segment(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    if safe == value and safe not in {"", ".", ".."}:
        return safe
    digest = sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"{safe or 'agent'}_{digest}"

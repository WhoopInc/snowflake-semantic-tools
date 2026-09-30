"""Composite plan and publication lifecycle for immutable evaluation datasets."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from hashlib import md5, sha256
from threading import Lock
from types import MappingProxyType

from ..domain.model.artifact_key import artifact_key, split_artifact_key
from ..domain.model.diagnostic import D, DiagnosticBag
from ..domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from ..domain.model.identifier import Identifier, QualifiedName
from ..domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeReason,
    ClassifiedError,
    CompositeObservation,
    CompositePlan,
    ErrorKind,
    OutcomeStatus,
    PhysicalResource,
    RenderedArtifact,
)
from ..domain.ports.snowflake import SnowflakePort, SnowflakePortError
from ..domain.state.model import AppliedEntry, Manifest, ResourceStatus
from .apply import classify_error

EVAL_STAGE_FILE_FORMAT = (
    "TYPE='CSV' FIELD_DELIMITER=NONE RECORD_DELIMITER='\\n' SKIP_HEADER=0 "
    "FIELD_OPTIONALLY_ENCLOSED_BY=NONE ESCAPE_UNENCLOSED_FIELD=NONE"
)


@dataclass(frozen=True, slots=True)
class EvalLifecycleConfig:
    config_stage_name: str = DEFAULT_EVAL_CONFIG_STAGE


class EvalLifecycleHandler:
    artifact_type = "eval"

    def __init__(self, port: SnowflakePort, config: EvalLifecycleConfig = EvalLifecycleConfig()) -> None:
        self._port = port
        self._config = config
        # Every eval in a schema shares its config stage, and the first to apply
        # creates it; the others' plans saw it absent.
        self._stage_lock = Lock()
        self._created_stages: set[tuple[str, str, str]] = set()

    def plan(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        manifest: Manifest,
    ) -> CompositePlan:
        del manifest
        try:
            observation = self._observe(artifact)
        except SnowflakePortError as exc:
            diagnostic = D("SST-PLN001", value=artifact.key, detail=str(exc))
            empty = CompositeObservation(artifact.key, diagnostics=DiagnosticBag((diagnostic,)))
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, empty, empty.diagnostics)
        if observation.diagnostics.has_errors:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, observation.diagnostics)

        desired_components = dict(artifact.component_fingerprints)
        recorded_components = dict(state_entry.component_fingerprints) if state_entry is not None else {}
        desired_resources = self._resource_identity(artifact.physical_resources)
        observed_resources = {
            (item.object_type.upper(), item.qualified_name.folded): item.exists for item in observation.resources
        }
        resources_exist = all(observed_resources.get(identity, False) for identity in desired_resources)
        recorded_resources = self._recorded_resource_identity(state_entry)
        unmanaged_live_resources = tuple(
            item
            for item in observation.resources
            if item.exists and (item.object_type.upper(), item.qualified_name.folded) not in recorded_resources
        )

        if unmanaged_live_resources:
            live = ", ".join(item.qualified_name.sql for item in unmanaged_live_resources)
            diagnostic = D("SST-PLN024", artifact=artifact.key, value=live)
            return CompositePlan(
                Action.BLOCKED,
                ChangeReason.UNMANAGED_OBJECT,
                observation,
                DiagnosticBag((diagnostic,)),
            )
        if state_entry is None:
            action = Action.CREATE
            reason = ChangeReason.NOT_PRESENT
        elif not recorded_components:
            action = Action.UPDATE
            reason = ChangeReason.NO_PRIOR_STATE
        elif recorded_components.get("dataset") != desired_components.get("dataset"):
            action = Action.UPDATE
            reason = ChangeReason.FINGERPRINT_DIFFERS
        elif recorded_resources - desired_resources:
            diagnostic = D(
                "SST-PLN025",
                artifact=artifact.key,
                found=", ".join(sorted(resource.qualified_name for resource in state_entry.applied_resources)),
                expected=", ".join(name.sql for _, name in artifact.physical_resources),
            )
            return CompositePlan(
                Action.BLOCKED,
                ChangeReason.TARGET_MOVED,
                observation,
                DiagnosticBag((diagnostic,)),
            )
        elif not resources_exist:
            action = Action.UPDATE
            reason = ChangeReason.NOT_PRESENT
        elif (
            recorded_components.get("config") != desired_components.get("config")
            or not observation.config_exists
            or observation.config_size != len(artifact.ddl.encode("utf-8"))
            or observation.config_md5 != recorded_components.get("config_stage_md5")
        ):
            action = Action.UPDATE
            reason = ChangeReason.FINGERPRINT_DIFFERS
        else:
            action = Action.NOOP
            reason = ChangeReason.UNCHANGED
        return CompositePlan(action, reason, observation)

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        del options
        if change.action is Action.PRUNE:
            return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, "")
        artifact = change.rendered
        if artifact is None:
            return self._failure(change, "missing rendered eval artifact", write_succeeded=False)
        if change.action in (Action.NOOP, Action.BLOCKED):
            return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, artifact.ddl)

        observation = change.composite_observation or self._observe(artifact)
        if observation.diagnostics.has_errors:
            diagnostic = observation.diagnostics[0]
            error = ClassifiedError(diagnostic.code, diagnostic.message, ErrorKind.UNKNOWN)
            return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, 0, artifact.ddl, error)

        if self._changed_since_plan(observation, artifact):
            return self._failure(
                change,
                "composite resources changed since plan",
                write_succeeded=False,
                code="SST-APL012",
            )

        resources = self._resources_by_type(artifact)
        source_table = resources["TABLE"]
        dataset = resources["DATASET"]
        verified_resources: list[tuple[str, str]] = []
        write_succeeded = False
        attempts = 0

        dataset_exists = self._port.object_exists("DATASET", dataset)
        source_exists = self._port.object_exists("TABLE", source_table)
        source_statements = self._source_statements(artifact)
        if not source_exists:
            for index, statement in enumerate(source_statements):
                attempts += 1
                source_result = self._port.execute_script((statement,))
                if not source_result.ok:
                    wrote = bool(source_result.query_ids or source_result.rows_affected) or index > 0
                    resources_after_failure = (
                        (("TABLE", source_table.sql),)
                        if index > 0 or self._port.object_exists("TABLE", source_table)
                        else ()
                    )
                    return self._execution_failure(
                        change,
                        source_result.error.message if source_result.error else "source-table publication failed",
                        attempts,
                        wrote,
                        resources_after_failure,
                        code="SST-APL016" if wrote else "SST-APL001",
                    )
                write_succeeded = True
                if index == 0 and not self._port.object_exists("TABLE", source_table):
                    return self._failure(
                        change,
                        f"source table {source_table.sql} is absent after publication",
                        write_succeeded=True,
                        code="SST-APL016",
                        attempts=attempts,
                    )
            source_exists = True
        if source_exists:
            try:
                found_rows = self._source_row_count(source_table)
            except SnowflakePortError as exc:
                return self._failure(
                    change,
                    f"source table {source_table.sql} row count is unreadable: {exc}",
                    write_succeeded=write_succeeded,
                    code="SST-APL016" if write_succeeded else "SST-APL001",
                    attempts=attempts,
                    physical_resources=(("TABLE", source_table.sql),),
                )
            expected_rows = self._expected_row_count(source_statements)
            if found_rows != expected_rows:
                return self._failure(
                    change,
                    f"source table {source_table.sql} has {found_rows} rows, expected {expected_rows}",
                    write_succeeded=write_succeeded,
                    code="SST-APL016",
                    attempts=attempts,
                    physical_resources=(("TABLE", source_table.sql),),
                )
            verified_resources.append(("TABLE", source_table.sql))

        if not dataset_exists:
            attempts += 1
            dataset_result = self._port.execute_script((self._dataset_statement(artifact),))
            if not dataset_result.ok:
                return self._execution_failure(
                    change,
                    dataset_result.error.message if dataset_result.error else "dataset publication failed",
                    attempts,
                    write_succeeded,
                    tuple(verified_resources),
                    code="SST-APL016" if write_succeeded else "SST-APL001",
                )
            write_succeeded = True
            if not self._port.object_exists("DATASET", dataset):
                return self._failure(
                    change,
                    f"dataset {dataset.sql} is absent after publication",
                    write_succeeded=True,
                    code="SST-APL016",
                    attempts=attempts,
                    physical_resources=tuple(verified_resources),
                )
        verified_resources.append(("DATASET", dataset.sql))

        stage = self._config_stage(artifact)
        with self._stage_lock:
            if not self._port.object_exists("STAGE", stage):
                attempts += 1
                stage_result = self._port.execute_script((self._create_stage_sql(stage),))
                if not stage_result.ok:
                    return self._execution_failure(
                        change,
                        stage_result.error.message if stage_result.error else "config-stage creation failed",
                        attempts,
                        True,
                        tuple(verified_resources),
                    )
                write_succeeded = True
                if not self._port.object_exists("STAGE", stage):
                    return self._failure(
                        change,
                        f"config stage {stage.sql} is absent after creation",
                        write_succeeded=True,
                        code="SST-APL016",
                        attempts=attempts,
                        physical_resources=tuple(verified_resources),
                    )
                self._created_stages.add(stage.folded)
                verified_resources.append(("STAGE", stage.sql))
        stage_format = self._port.describe_stage_file_format(stage)
        if _normalize_file_format(stage_format) != _normalize_file_format(EVAL_STAGE_FILE_FORMAT):
            diagnostic = D(
                "SST-APL028",
                value=stage.sql,
                found=stage_format or "absent",
                expected=EVAL_STAGE_FILE_FORMAT,
            )
            error = ClassifiedError(diagnostic.code, diagnostic.message, ErrorKind.UNKNOWN)
            return ApplyOutcome(
                change.key,
                change.action,
                OutcomeStatus.FAILED,
                attempts,
                0,
                artifact.ddl,
                error,
                write_succeeded=write_succeeded,
                component_fingerprints=artifact.component_fingerprints,
                physical_resources=tuple(verified_resources),
            )

        config_path = self._config_path(artifact)
        expected_config = dict(artifact.component_fingerprints).get("config")
        if expected_config is None:
            return self._failure(change, "eval artifact has no config fingerprint", write_succeeded=write_succeeded)
        expected_content = artifact.ddl.encode("utf-8")
        attempts += 1
        try:
            self._port.upload(config_path, expected_content)
        except SnowflakePortError as exc:
            return self._failure(
                change,
                str(exc),
                write_succeeded=write_succeeded,
                attempts=attempts,
                physical_resources=tuple(verified_resources),
            )
        write_succeeded = True
        staged_config = self._port.observe_staged_file(config_path)
        if staged_config is None:
            return self._failure(
                change,
                f"staged config {config_path} is absent after upload",
                write_succeeded=True,
                code="SST-APL016",
                attempts=attempts,
                physical_resources=tuple(verified_resources),
            )
        staged_content = self._port.read_staged_file(config_path)
        if staged_content is None:
            return self._failure(
                change,
                f"staged config {config_path} is unreadable after upload",
                write_succeeded=True,
                code="SST-APL016",
                attempts=attempts,
                physical_resources=tuple(verified_resources),
            )
        expected_size = len(expected_content)
        if len(staged_content) != expected_size:
            return self._failure(
                change,
                f"staged config {config_path} has {len(staged_content)} bytes, expected {expected_size}",
                write_succeeded=write_succeeded,
                code="SST-APL016",
                attempts=attempts,
                physical_resources=tuple(verified_resources),
            )
        if staged_content != expected_content:
            return self._failure(
                change,
                f"staged config {config_path} bytes do not match the rendered config",
                write_succeeded=write_succeeded,
                code="SST-APL016",
                attempts=attempts,
                physical_resources=tuple(verified_resources),
            )

        complete_resources = tuple((object_type, name.sql) for object_type, name in artifact.physical_resources)
        content_md5 = md5(staged_content, usedforsecurity=False).hexdigest()
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.APPLIED,
            max(attempts, 1),
            0,
            artifact.ddl,
            write_succeeded=write_succeeded or change.action is Action.UPDATE,
            component_fingerprints=(*artifact.component_fingerprints, ("config_stage_md5", content_md5)),
            physical_resources=complete_resources,
        )

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        resources = ", ".join(resource.qualified_name for resource in state_entry.applied_resources) or artifact_key
        diagnostic = D(
            "SST-PLN021",
            subject=artifact_key,
            artifact=artifact_key,
            value=resources,
            detail="keep them while an evaluation run or baseline references them",
        )
        return Change(
            artifact_key,
            self.artifact_type,
            Action.PRUNE,
            ChangeReason.ORPHANED,
            None,
            None,
            (),
            400,
            DiagnosticBag((diagnostic,)),
            prune_executable=False,
        )

    @staticmethod
    def merge_physical_resources(
        current: tuple[tuple[str, str], ...],
        previous: AppliedEntry | None,
    ) -> tuple[tuple[str, str] | tuple[str, str, ResourceStatus], ...]:
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

    def _observe(self, artifact: RenderedArtifact) -> CompositeObservation:
        diagnostics = []
        resources = tuple(
            PhysicalResource(object_type, name, self._port.object_exists(object_type, name))
            for object_type, name in artifact.physical_resources
        )
        stage = self._config_stage(artifact)
        stage_exists = self._port.object_exists("STAGE", stage)
        stage_format = self._port.describe_stage_file_format(stage) if stage_exists else None
        if stage_exists and _normalize_file_format(stage_format) != _normalize_file_format(EVAL_STAGE_FILE_FORMAT):
            diagnostics.append(
                D(
                    "SST-APL028",
                    value=stage.sql,
                    found=stage_format or "absent",
                    expected=EVAL_STAGE_FILE_FORMAT,
                )
            )
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
            diagnostics=DiagnosticBag(diagnostics),
            config_size=len(staged_content) if staged_content is not None else None,
            config_md5=(md5(staged_content, usedforsecurity=False).hexdigest() if staged_content is not None else None),
        )

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

    def config_path(self, artifact: RenderedArtifact) -> str:
        return self._config_path(artifact)

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
        if set(resources) != {"TABLE", "DATASET"}:
            raise ValueError("eval artifact requires one TABLE and one DATASET resource")
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
    def _source_statements(artifact: RenderedArtifact) -> tuple[str, ...]:
        statements = tuple(statement.strip() for statement in artifact.create_statements if statement.strip())
        if len(statements) < 2:
            raise ValueError("eval artifact requires CREATE TABLE and INSERT statements")
        return statements[:-1]

    @staticmethod
    def _dataset_statement(artifact: RenderedArtifact) -> str:
        statements = tuple(statement.strip() for statement in artifact.create_statements if statement.strip())
        if len(statements) < 2:
            raise ValueError("eval artifact requires a dataset statement")
        return statements[-1]

    def _source_row_count(self, source_table: QualifiedName) -> int:
        result = self._port.query(f"SELECT COUNT(*) AS ROW_COUNT FROM {source_table.sql}")
        if not result.rows or not result.rows[0]:
            raise SnowflakePortError("COUNT(*) returned no row")
        value = result.rows[0][0]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise SnowflakePortError(f"COUNT(*) returned {type(value).__name__}")
        return int(value)

    @staticmethod
    def _expected_row_count(statements: tuple[str, ...]) -> int:
        if len(statements) < 2:
            return 0
        insert = statements[1]
        return insert.count("\nUNION ALL\n") + 1

    @staticmethod
    def _create_stage_sql(stage: QualifiedName) -> str:
        return f"CREATE STAGE IF NOT EXISTS {stage.sql} FILE_FORMAT = ({EVAL_STAGE_FILE_FORMAT})"

    def _execution_failure(
        self,
        change: Change,
        detail: str,
        attempts: int,
        write_succeeded: bool,
        physical_resources: tuple[tuple[str, str], ...],
        *,
        code: str = "SST-APL001",
    ) -> ApplyOutcome:
        return self._failure(
            change,
            detail,
            write_succeeded=write_succeeded,
            code=code,
            attempts=attempts,
            physical_resources=physical_resources,
        )

    def _failure(
        self,
        change: Change,
        detail: str,
        *,
        write_succeeded: bool,
        code: str = "SST-APL001",
        attempts: int = 0,
        physical_resources: tuple[tuple[str, str], ...] = (),
    ) -> ApplyOutcome:
        artifact = change.rendered
        classified = classify_error(detail)
        error = ClassifiedError(code, detail, classified.kind, classified.retryable, classified.sqlstate)
        return ApplyOutcome(
            change.key,
            change.action,
            OutcomeStatus.FAILED,
            attempts,
            0,
            artifact.ddl if artifact is not None else "",
            error,
            write_succeeded=write_succeeded,
            component_fingerprints=artifact.component_fingerprints if artifact is not None else (),
            physical_resources=physical_resources,
        )


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

"""One eval publication, step by step: the source table, the dataset and its version, the config stage.

`EvalLifecycleHandler` in `evals` plans an eval and hands each publication to an `EvalRun`.
"""

from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING

from snowflake_semantic_tools.app.lifecycle.composite import (
    PublicationRun,
    failed,
)
from snowflake_semantic_tools.app.lifecycle.eval_rules import (
    dataset_version,
    format_problem,
    mints,
)
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOutcome,
    Change,
    ClassifiedError,
    ErrorKind,
    ExecResult,
    OutcomeStatus,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql

if TYPE_CHECKING:
    # evals builds an EvalRun, so this module may not import it at run time.
    from snowflake_semantic_tools.app.lifecycle.evals import EvalLifecycleHandler


class EvalRun(PublicationRun[CatalogPublicationPort]):
    """One eval publication: each resource created only when absent, and verified before the next.

    The steps run in order: the source table, its row count, the dataset and its version, the
    config stage and its file format, then the config file and its read-back. Resources are
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
                self._add_dataset_version,
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

    def _add_dataset_version(self) -> ApplyOutcome | None:
        """Add SST's version to the dataset unless it holds it, then require it there.

        A dataset this run found rather than minted may have changed owner, so its ownership
        is checked first (SST-VAL713). A refused statement is SST-APL022, and a version absent
        after it SST-APL016.
        """
        version = dataset_version(self._artifact)
        if version is None or self._handler._holds_version(self._dataset, version):
            return None
        if self._dataset_exists:
            problem = self._handler.ownership_problem(self._artifact, self._dataset)
            if problem is not None:
                return diagnosed(
                    self._change,
                    problem,
                    attempts=self._attempts,
                    write_succeeded=self._written,
                    physical_resources=self._recorded_resources(),
                )
        result = self._run_statement(self._handler._version_statement(self._artifact))
        if not result.ok:
            return self.fail(result.error.message if result.error else "ADD VERSION failed", "SST-APL022")
        if not self._handler._holds_version(self._dataset, version):
            return self.fail(
                f"version {version} is absent from dataset {self._dataset.sql} after ADD VERSION", "SST-APL016"
            )
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
        problem = format_problem(self._stage, self._port.describe_stage_file_format(self._stage))
        if problem is None:
            return None
        return diagnosed(
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


def diagnosed(
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

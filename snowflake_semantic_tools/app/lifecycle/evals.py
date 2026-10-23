"""Composite plan and publication lifecycle for immutable evaluation datasets."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import md5
from threading import Lock
from types import MappingProxyType

from snowflake_semantic_tools.app.lifecycle.composite import (
    CompositeHandler,
    blocked,
    failed,
)
from snowflake_semantic_tools.app.lifecycle.eval_rules import (
    EVAL_STAGE_FILE_FORMAT as EVAL_STAGE_FILE_FORMAT,
)
from snowflake_semantic_tools.app.lifecycle.eval_rules import (
    EVAL_STAGE_SQL,
    dataset_version,
    edited_metrics,
    format_problem,
    mints,
    observation_identity,
    provenance,
    safe_stage_segment,
    with_provenance,
)
from snowflake_semantic_tools.app.lifecycle.eval_rules import (
    eval_stage_format_matches as eval_stage_format_matches,
)
from snowflake_semantic_tools.app.lifecycle.eval_run import EvalRun, diagnosed
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.eval import DEFAULT_EVAL_CONFIG_STAGE
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    ABSENT,
    CONFIG_DIFFERS,
    OTHER_DEFINITION,
    PROVENANCE_DIFFERS,
    RECORDED,
    UNOBSERVABLE,
    Action,
    ApplyOutcome,
    Change,
    ChangeReason,
    CompositeObservation,
    CompositePlan,
    PhysicalResource,
    RecoveryProof,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry, ResourceStatus
from snowflake_semantic_tools.domain.validate.publication import eval_publication_diagnostics


@dataclass(frozen=True, slots=True)
class EvalLifecycleConfig:
    """Where evals stage their run configuration: one stage per target schema, named here."""

    config_stage_name: str = DEFAULT_EVAL_CONFIG_STAGE


def config_folder(artifact: RenderedArtifact, stage_name: str) -> str:
    """Return where an eval's configs are staged: `@<stage>/<agent segment>`, in the eval's schema.

    The agent segment names the agent the eval depends on, else the eval itself.
    """
    stage = QualifiedName(artifact.target.database, artifact.target.schema, Identifier.parse(stage_name))
    agent_prefix = artifact_key("agent", "")
    dependency = next((key for key in artifact.depends_on if key.startswith(agent_prefix)), artifact.key)
    return f"@{stage.sql}/{safe_stage_segment(split_artifact_key(dependency)[1])}"


class EvalLifecycleHandler(CompositeHandler[RenderedArtifact, CompositeObservation, CatalogPublicationPort]):
    """Plan and publish one agent's evaluation: its source table, dataset, and staged config.

    The table and dataset are immutable: a changed dataset publishes new ones, and state
    retains the old ones, which prune only reports.
    """

    artifact_type = "eval"
    _prune_detail = "keep them while an evaluation run or baseline references them"

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
        problem = format_problem(stage, stage_format) if stage_exists else None
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
            details=(("dataset_version", self._version_state(artifact, resources)),),
        )

    def _version_state(self, artifact: RenderedArtifact, resources: tuple[PhysicalResource, ...]) -> str:
        """Say whether the dataset holds SST's version: `present`, `absent`, or empty with no dataset to read."""
        version = dataset_version(artifact)
        dataset = next((item for item in resources if item.object_type.upper() == "DATASET"), None)
        if version is None or dataset is None or not dataset.exists:
            return ""
        return "present" if self._holds_version(dataset.qualified_name, version) else "absent"

    def _holds_version(self, dataset: QualifiedName, version: str) -> bool:
        return version.casefold() in {name.casefold() for name in self._port.dataset_versions(dataset)}

    def ownership_problem(self, artifact: RenderedArtifact, dataset: QualifiedName) -> Diagnostic | None:
        """Return SST-VAL713 when the session's role does not own the dataset a version is added to.

        SST mints a dataset as the session's role, so that role owns it unless ownership moved
        since -- a future grant, or a hand transfer -- and only the owner may add a version.
        Snowflake lists no grants on a dataset, so the owner is read from SHOW DATASETS and
        compared with the role as the agent observation compares an agent's owner.
        """
        role = self._port.current_role()
        row = self._port.show_row("DATASET", dataset)
        owner = (row or {}).get("owner", "")
        if owner.upper() == role.upper():
            return None
        return D(
            "SST-VAL713",
            subject=artifact.key,
            artifact=artifact.key,
            value=role,
            found=f"no ownership ({owner} owns it)" if owner else "no ownership",
        )

    def _decide(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        subject: RenderedArtifact,
        observation: CompositeObservation,
    ) -> CompositePlan:
        """Decide an eval's change: `_decide_unminted` decides one SST does not mint, else `_decide_minted`.

        Diagnostics:
            SST-VAL744: a custom metric is rendered under the name state recorded with another
                definition, as `edited_metrics` reports.
            SST-VAL715, SST-VAL716: as `eval_publication_diagnostics` reports them; the eval is
                blocked.
            SST-VAL714: the change adds a dataset version whose METADATA carries no commit.
        """
        del subject
        if observation.diagnostics.has_errors:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, observation.diagnostics)
        edited = edited_metrics(artifact, state_entry)
        if edited:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, DiagnosticBag(edited))
        problems = eval_publication_diagnostics(artifact, observation)
        if problems:
            return CompositePlan(Action.BLOCKED, ChangeReason.VALIDATION_ERRORS, observation, DiagnosticBag(problems))
        if not mints(artifact):
            return self._decide_unminted(artifact, state_entry, observation)
        return with_provenance(artifact, self._decide_minted(artifact, state_entry, observation))

    def _decide_minted(
        self,
        artifact: RenderedArtifact,
        state_entry: AppliedEntry | None,
        observation: CompositeObservation,
    ) -> CompositePlan:
        """Decide an eval SST mints: refuse an object it does not own, else create, update, or leave it.

        SST owns what state records for the eval, the tables and datasets an earlier publish left
        behind included, so a revert to earlier questions updates the eval onto the dataset SST
        minted for them. An object state does not record is SST's too when it is exactly what
        this eval mints and its dataset's version carries this eval's provenance: an apply that
        stopped before recording it left it, and publishing again records it.

        Diagnostics:
            SST-PLN024: an object the eval publishes exists, and state does not record it as SST's.
        """
        owned = self._recorded_resource_identity(state_entry, retained=True)
        unmanaged_live_resources = tuple(
            item
            for item in observation.resources
            if item.exists and (item.object_type.upper(), item.qualified_name.folded) not in owned
        )
        if unmanaged_live_resources and self._minted_unrecorded(artifact, unmanaged_live_resources, observation):
            return CompositePlan(Action.UPDATE, ChangeReason.UNRECORDED_PUBLISH, observation)
        if unmanaged_live_resources:
            live = ", ".join(item.qualified_name.sql for item in unmanaged_live_resources)
            return blocked(
                observation, D("SST-PLN024", artifact=artifact.key, value=live), ChangeReason.UNMANAGED_OBJECT
            )
        if state_entry is None:
            return CompositePlan(Action.CREATE, ChangeReason.NOT_PRESENT, observation)
        return self._decide_update(artifact, state_entry, observation, self._recorded_resource_identity(state_entry))

    def _minted_unrecorded(
        self,
        artifact: RenderedArtifact,
        unrecorded: tuple[PhysicalResource, ...],
        observation: CompositeObservation,
    ) -> bool:
        """Report whether objects state does not record are the ones this eval mints, left by SST.

        Each must be one of the eval's own resources, and the dataset must hold the version
        this eval adds, whose METADATA names the same agent, source table and questions' digest.
        The commit is not compared: the stopped apply may have run from another one. A
        METADATA that cannot be read proves nothing.
        """
        own = self._resource_identity(artifact.physical_resources)
        if any((item.object_type.upper(), item.qualified_name.folded) not in own for item in unrecorded):
            return False
        version = dataset_version(artifact)
        if version is None or artifact.version_metadata is None:
            return False
        if dict(observation.details).get("dataset_version") != "present":
            return False
        try:
            found = self._port.dataset_version_metadata(self._resources_by_type(artifact)["DATASET"], version)
        except SnowflakePortError:
            return False
        expected = provenance(artifact.version_metadata)
        return found is not None and expected is not None and provenance(found) == expected

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

        A dataset missing SST's version counts as a missing resource.

        Diagnostics:
            SST-PLN025: state records resources this eval no longer publishes to.
            SST-VAL713: the dataset lacks SST's version, and the session's role does not own it.
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
        if dict(observation.details).get("dataset_version") == "absent":
            # A publish that stopped after minting, or a dataset minted with no version of SST's.
            problem = self.ownership_problem(artifact, self._resources_by_type(artifact)["DATASET"])
            if problem is not None:
                return blocked(observation, problem)
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
            return diagnosed(change, observation.diagnostics[0])
        if self._changed_since_plan(observation, artifact):
            return failed(change, "composite resources changed since plan", code="SST-APL012")
        return EvalRun(self, change, artifact).publish()

    def _apply_unrendered(self, change: Change) -> ApplyOutcome:
        """Fail a change without its rendered eval: there is no dataset or config to publish."""
        return failed(change, "missing rendered eval artifact")

    def recover(self, change: Change) -> RecoveryProof:
        """Prove an unrecorded eval's objects are those it publishes, with exactly its config staged.

        An eval SST mints must have every table and dataset it publishes, and the dataset the
        version this eval adds, whose METADATA names its agent, source table and full questions'
        digest, as `_minted_unrecorded` reads it; one that does not mint proves its config
        alone. Either way the staged config must be the rendered bytes, by size and MD5. The
        digest prefix in the objects' names is never proof: it is a few characters long. A
        METADATA that cannot be read proves nothing.
        """
        artifact = change.rendered
        observation = change.composite_observation
        assert artifact is not None
        if observation is None or not observation.details:
            return RecoveryProof(UNOBSERVABLE)
        if observation.diagnostics.has_errors:
            return RecoveryProof(OTHER_DEFINITION)
        minted = mints(artifact)
        if minted:
            live = tuple(item for item in observation.resources if item.exists)
            if not live:
                # Absent under these names, as an eval of earlier questions is: nothing to recover.
                return RecoveryProof(ABSENT)
            if len(live) != len(observation.resources) or not self._minted_unrecorded(artifact, live, observation):
                return RecoveryProof(PROVENANCE_DIFFERS)
        content = artifact.ddl.encode("utf-8")
        digest = md5(content, usedforsecurity=False).hexdigest()
        if not observation.config_exists:
            return RecoveryProof(CONFIG_DIFFERS if minted else ABSENT)
        if observation.config_size != len(content) or observation.config_md5 != digest:
            return RecoveryProof(CONFIG_DIFFERS)
        return RecoveryProof(
            RECORDED,
            (*artifact.component_fingerprints, ("config_stage_md5", digest)),
            tuple((object_type, name.sql) for object_type, name in artifact.physical_resources) if minted else (),
        )

    def _config_stage(self, artifact: RenderedArtifact) -> QualifiedName:
        return QualifiedName(
            artifact.target.database, artifact.target.schema, Identifier.parse(self._config.config_stage_name)
        )

    def _config_path(self, artifact: RenderedArtifact) -> str:
        config = dict(artifact.component_fingerprints).get("config")
        if config is None:
            raise ValueError("eval artifact requires a config fingerprint")
        return f"{config_folder(artifact, self._config.config_stage_name)}/{config}.yaml"

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
        return observation_identity(current) != observation_identity(planned)

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
        *,
        retained: bool = False,
    ) -> frozenset[tuple[str, tuple[str, str, str]]]:
        """Return the tables and datasets state records for the eval, by type and folded name.

        Args:
            retained: Include those an earlier publish superseded, which are still SST's, as
                the ownership check does; without it, only those the latest publish wrote,
                which is where the eval is now published.
        """
        if state_entry is None:
            return frozenset()
        values = []
        for resource in state_entry.applied_resources:
            if resource.status is ResourceStatus.RETAINED and not retained:
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
    def _minting_statements(artifact: RenderedArtifact) -> tuple[Sql, ...]:
        """The source table's statements, then the dataset's, then its version's.

        Raises:
            ValueError: there are not at least those three.
        """
        statements = tuple(statement for statement in artifact.create_statements if str(statement).strip())
        if len(statements) < 3:
            raise ValueError("eval artifact requires source-table, dataset and version statements")
        return statements

    @classmethod
    def _source_statements(cls, artifact: RenderedArtifact) -> tuple[Sql, ...]:
        return cls._minting_statements(artifact)[:-2]

    @classmethod
    def _dataset_statement(cls, artifact: RenderedArtifact) -> Sql:
        return cls._minting_statements(artifact)[-2]

    @classmethod
    def _version_statement(cls, artifact: RenderedArtifact) -> Sql:
        return cls._minting_statements(artifact)[-1]

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
            file_format=EVAL_STAGE_SQL,
        )

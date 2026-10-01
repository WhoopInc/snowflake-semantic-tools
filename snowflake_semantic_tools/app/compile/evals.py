"""Compile resolved evaluation authoring into deterministic composite artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256

from ...domain.model.eval import EvalCatalog, EvalDefaults, ResolvedEval, render_eval_name_template
from ...domain.model.identifier import QualifiedName
from ...domain.model.lifecycle import CompositeFacts, PublishShape, RenderedArtifact, StatementPlan
from ...domain.model.registry import GrantPreservation
from ...domain.render.eval import (
    RenderedEval,
    render_create_dataset_sql,
    render_dataset_payload,
    render_eval_config,
    render_source_table_sql,
)
from .base import CompileResult, StandaloneArtifact, compile_each


@dataclass(frozen=True, slots=True)
class CompiledEval(StandaloneArtifact):
    """One agent's evaluation: a dataset over a source table, and the config that runs it.

    The eval publishes into its agent's schema. Its fingerprint combines the dataset's and
    the config's, so a change to either is a change.

    Attributes:
        resolved: The eval with its effective agent version applied.
        source_table: The table the dataset payload is loaded into.
        dataset_target: The dataset created over `source_table`; the artifact publishes here.
    """

    resolved: ResolvedEval
    agent_target: QualifiedName
    source_table: QualifiedName
    dataset_target: QualifiedName
    rendered: RenderedEval

    @property
    def name(self) -> str:
        return self.resolved.name

    @property
    def artifact_key(self) -> str:
        return self.resolved.key

    @property
    def artifact_type(self) -> str:
        return "eval"

    @property
    def source_files(self) -> tuple[str, ...]:
        return self.resolved.source_files

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        combined = sha256(
            f"dataset:{self.rendered.dataset_fingerprint}\nconfig:{self.rendered.config_fingerprint}\n".encode("utf-8")
        ).hexdigest()
        artifact = RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="eval",
            target=self.dataset_target,
            ddl=self.rendered.config_yaml,
            shape=PublishShape("", render_dialect="eval_yaml", grant_preservation=GrantPreservation.NONE),
            statements=StatementPlan(default=()),
            composite=CompositeFacts(
                component_fingerprints=(
                    ("dataset", self.rendered.dataset_fingerprint),
                    ("config", self.rendered.config_fingerprint),
                ),
                physical_resources=(("TABLE", self.source_table), ("DATASET", self.dataset_target)),
            ),
            depends_on=self.resolved.depends_on,
        )
        return replace(artifact, fingerprint=combined)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Add the statements that create the source table and then the dataset; nothing is marked."""
        del manifest_id
        artifact = self.rendered_artifact
        return replace(
            artifact,
            create_statements=(
                *_sql_statements(self.rendered.source_table_sql),
                self.rendered.create_dataset_sql.strip(),
            ),
        )


class CompileEvals:
    """Compile each eval into its agent's schema.

    `agent_targets` maps each enabled agent's casefolded name to the name it publishes
    under; an eval whose agent is not in it reports SST-INT902.
    """

    def __init__(self, catalog: EvalCatalog, *, agent_targets: dict[str, QualifiedName]) -> None:
        self._catalog = catalog
        self._agent_targets = agent_targets

    def run_result(self) -> CompileResult:
        """Compile the evals in key order, skipping each one an error already names.

        The diagnostics are the catalog's, then any SST-INT902.

        Diagnostics:
            SST-INT902: rendering an eval raised KeyError, TypeError or ValueError, such as one
                whose agent has no target.
        """
        return compile_each(
            sorted(self._catalog.evals, key=lambda item: item.key),
            key=lambda resolved: resolved.key,
            render=self._compile,
            diagnostics=self._catalog.diagnostics,
            origin=lambda resolved: resolved.config.origin,
        )

    def _compile(self, resolved: ResolvedEval) -> CompiledEval:
        agent_target = self._agent_targets[resolved.agent.name.casefold()]
        return _render(resolved, agent_target, self._catalog.defaults)


def _render(
    resolved: ResolvedEval,
    agent_target: QualifiedName,
    defaults: EvalDefaults,
) -> CompiledEval:
    """Render one eval's dataset and config into `agent_target`'s schema.

    The dataset and source table names come from the config's templates, filled with the
    agent name and the first seven characters of the dataset payload's digest, so a new
    dataset publishes under a new name.

    Raises:
        ValueError: the config has no dataset name or source table template.
    """
    effective_agent_version = resolved.config.agent_version or defaults.agent_version
    resolved = replace(
        resolved,
        config=replace(resolved.config, agent_version=effective_agent_version),
    )
    dataset_config = resolved.config.dataset
    if dataset_config is None or dataset_config.name_template is None or dataset_config.source_table_template is None:
        raise ValueError("eval dataset requires name_template and source_table_template")
    payload = render_dataset_payload(resolved.dataset)
    dataset_fingerprint = sha256(payload.encode("utf-8")).hexdigest()
    sha7 = dataset_fingerprint[:7]
    dataset_name = render_eval_name_template(dataset_config.name_template, agent=resolved.name, sha7=sha7)
    source_name = render_eval_name_template(dataset_config.source_table_template, agent=resolved.name, sha7=sha7)
    dataset_target = QualifiedName(
        agent_target.database, agent_target.schema, QualifiedName.from_parts("X", "X", dataset_name).name
    )
    source_table = QualifiedName(
        agent_target.database, agent_target.schema, QualifiedName.from_parts("X", "X", source_name).name
    )
    config_yaml = render_eval_config(
        resolved.config,
        resolved.custom_metrics,
        agent_target=agent_target,
        dataset_target=dataset_target,
    )
    rendered = RenderedEval(
        payload,
        render_source_table_sql(payload, source_table),
        render_create_dataset_sql(resolved.config, source_table, dataset_target),
        config_yaml,
        dataset_fingerprint,
        sha256(config_yaml.encode("utf-8")).hexdigest(),
    )
    return CompiledEval(resolved, agent_target, source_table, dataset_target, rendered)


def _sql_statements(value: str) -> tuple[str, ...]:
    return tuple(
        statement.strip() for statement in value.rstrip().removesuffix(";").split(";\n\n") if statement.strip()
    )

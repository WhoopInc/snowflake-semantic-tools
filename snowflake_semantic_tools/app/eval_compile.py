"""Compile resolved evaluation authoring into deterministic composite artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256

from ..domain.model.diagnostic import D, DiagnosticBag, Severity
from ..domain.model.eval import EvalCatalog, EvalDefaults, ResolvedEval, render_eval_name_template
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import RenderedArtifact
from ..domain.model.registry import GrantPreservation
from ..domain.render.eval import (
    RenderedEval,
    render_create_dataset_sql,
    render_dataset_payload,
    render_eval_config_with_metrics,
    render_source_table_sql,
)
from .compile import CompileResult


@dataclass(frozen=True, slots=True)
class CompiledEval:
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
    def member_keys(self) -> tuple[str, ...]:
        return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        return ()

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        return ()

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
            object_type="",
            render_dialect="eval_yaml",
            grant_preservation=GrantPreservation.NONE,
            statements=(),
            depends_on=self.resolved.depends_on,
            component_fingerprints=(
                ("dataset", self.rendered.dataset_fingerprint),
                ("config", self.rendered.config_fingerprint),
            ),
            physical_resources=(("TABLE", self.source_table), ("DATASET", self.dataset_target)),
            generic_apply_safe=False,
        )
        return replace(artifact, fingerprint=combined)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
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
    def __init__(self, catalog: EvalCatalog, *, agent_targets: dict[str, QualifiedName]) -> None:
        self._catalog = catalog
        self._agent_targets = agent_targets

    def run_result(self) -> CompileResult:
        diagnostics = self._catalog.diagnostics
        compiled: list[CompiledEval] = []
        for resolved in sorted(self._catalog.evals, key=lambda item: item.key):
            if any(
                diagnostic.severity is Severity.ERROR and diagnostic.subject == resolved.key
                for diagnostic in diagnostics
            ):
                continue
            try:
                agent_target = self._agent_targets[resolved.agent.name.casefold()]
                rendered = _render(resolved, agent_target, self._catalog.defaults)
                compiled.append(rendered)
            except (KeyError, TypeError, ValueError) as exc:
                diagnostics = DiagnosticBag(
                    (
                        *diagnostics,
                        D("SST-INT902", subject=resolved.key, detail=str(exc), origin=resolved.config.origin),
                    )
                )
        return CompileResult(tuple(compiled), diagnostics)


def _render(
    resolved: ResolvedEval,
    agent_target: QualifiedName,
    defaults: EvalDefaults,
) -> CompiledEval:
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
    config_yaml = render_eval_config_with_metrics(
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

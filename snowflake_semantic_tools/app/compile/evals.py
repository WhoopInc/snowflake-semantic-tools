"""Compile resolved evaluation authoring into deterministic composite artifacts.

An eval compiles into the schema of what it evaluates: an agent, or a CoWork profile. CoWork
profiles are a Snowflake private preview (PrPr) feature; a profile's eval follows the preview's
contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from hashlib import sha256
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact, compile_checked
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EVAL_MINT_NEVER, EvalCatalog, ResolvedEval, SubjectTarget
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    CompositeFacts,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.render.eval import (
    RenderedEval,
    dataset_version_metadata,
    dataset_version_name,
    eval_render_checks,
    render_add_version_statement,
    render_create_dataset_statement,
    render_dataset_payload,
    render_eval_config,
    render_source_table_statements,
)
from snowflake_semantic_tools.domain.resolve.eval_name import render_eval_name_template
from snowflake_semantic_tools.domain.validate.eval import eval_placement
from snowflake_semantic_tools.domain.validate.eval_subject import cowork_listed_placement

# The component fingerprint prefix that names one custom metric an eval renders.
METRIC_COMPONENT_PREFIX = "metric:"


@dataclass(frozen=True, slots=True)
class CompiledEval(StandaloneArtifact):
    """One agent's or CoWork profile's evaluation: a dataset over a source table, and its config.

    The eval publishes into its subject's schema. Its fingerprint combines the dataset's and
    the config's, so a change to either is a change.

    Attributes:
        resolved: The eval, with the settings it resolved.
        agent_target: The AGENT object the eval runs against: the agent, or the profile.
        source_table: The table the dataset payload is loaded into.
        dataset_target: The dataset created over `source_table`; the artifact publishes here.
        git_sha: The commit the dataset version's METADATA records; plan sets it for
            publication, and it never enters the artifact's fingerprint.
    """

    resolved: ResolvedEval
    agent_target: QualifiedName
    source_table: QualifiedName
    dataset_target: QualifiedName
    rendered: RenderedEval
    git_sha: str = ""

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
    def settings(self) -> tuple[Setting, ...]:
        return tuple(self.resolved.settings[key] for key in sorted(self.resolved.settings))

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        combined = sha256(
            f"dataset:{self.rendered.dataset_fingerprint}\nconfig:{self.rendered.config_fingerprint}\n".encode()
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
                    *((("dataset_version", self.dataset_version),) if self.mints else (("mint", EVAL_MINT_NEVER),)),
                    *self.metric_fingerprints,
                ),
                physical_resources=(
                    (("TABLE", self.source_table), ("DATASET", self.dataset_target))
                    if self.mints
                    else (("DATASET", self.dataset_target),)
                ),
            ),
            depends_on=self.resolved.depends_on,
        )
        return replace(artifact, fingerprint=combined)

    @property
    def metric_fingerprints(self) -> tuple[tuple[str, str], ...]:
        """Each custom metric the config renders, as `metric:<casefolded name>` and its definition digest.

        State keeps them, so a later plan can tell a metric edited under its own name.
        """
        return tuple(
            (f"{METRIC_COMPONENT_PREFIX}{metric.name.casefold()}", metric.definition_digest)
            for metric in self.resolved.custom_metrics
            if metric.enabled
        )

    @property
    def dataset_version(self) -> str:
        """Name the version SST adds to the dataset it mints, from the questions' digest."""
        return dataset_version_name(self.rendered.dataset_fingerprint)

    @property
    def version_metadata(self) -> str:
        """The dataset version's METADATA: the eval's provenance, with `git_sha` when it is a commit."""
        return dataset_version_metadata(
            agent_target=self.agent_target,
            source_table=self.source_table,
            dataset_fingerprint=self.rendered.dataset_fingerprint,
            git_sha=self.git_sha,
        )

    @property
    def mints(self) -> bool:
        """Report whether SST creates the eval's source table and dataset, which `dataset.mint: never` forbids."""
        dataset = self.resolved.config.dataset
        return dataset is None or dataset.mint != EVAL_MINT_NEVER

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Add the statements that create the source table, the dataset, then its version, when SST mints them.

        The version's METADATA carries the commit, so it rides on the published artifact alone:
        never a component, and never in what the fingerprint covers.
        """
        del manifest_id
        artifact = self.rendered_artifact
        if not self.mints:
            return artifact
        add_version = render_add_version_statement(
            self.dataset_target, self.source_table, version=self.dataset_version, metadata=self.version_metadata
        )
        return replace(
            artifact,
            create_statements=(
                *self.rendered.source_table_statements,
                self.rendered.create_dataset_statement,
                add_version,
            ),
            version_metadata=self.version_metadata,
        )


class CompileEvals:
    """Compile each eval into its subject's schema.

    `agent_targets` maps each enabled agent's casefolded name to the name it publishes
    under; an agent's eval whose agent is not in it reports SST-INT902. `profile_targets` maps
    each compiled CoWork profile's casefolded name to where it publishes, with the subagents
    its router names there; a profile's eval whose profile did not compile is left out, since
    the profile's own diagnostics say why. Snowflake private preview (PrPr): CoWork profiles.
    """

    def __init__(
        self,
        catalog: EvalCatalog,
        *,
        agent_targets: Mapping[str, QualifiedName],
        profile_targets: Mapping[str, SubjectTarget] = MappingProxyType({}),
    ) -> None:
        self._catalog = catalog
        self._agent_targets = agent_targets
        self._profile_targets = profile_targets

    def run_result(self) -> CompileResult:
        """Compile the evals in key order, skipping each one an error already names.

        The diagnostics are the catalog's, then each eval's placement, then each eval's render
        checks or SST-INT902. An eval a placement or render check refuses is left out, and so
        is a CoWork profile's eval on a target that publishes the profile where CoWork lists it.

        Diagnostics:
            SST-VAL704: an eval names a schema other than its subject's; it does not compile.
            SST-VAL886: a CoWork profile's eval, on a target that publishes the profile to
                SNOWFLAKE_INTELLIGENCE.PROFILES; it does not compile.
            SST-RND020, SST-RND021, SST-RND022: as `eval_render_checks` reports them.
            SST-INT902: rendering an eval raised KeyError, TypeError or ValueError, such as an
                agent's eval whose agent has no target.
        """
        evals: list[ResolvedEval] = []
        placement: list[Diagnostic] = []
        for resolved in sorted(self._catalog.evals, key=lambda item: item.key):
            target = self._target(resolved)
            if target is None:
                # An agent's eval renders, and reports SST-INT902; a profile's is left out.
                if not resolved.subject.is_profile:
                    evals.append(resolved)
                continue
            listed = cowork_listed_placement(resolved, target.target)
            placement.extend(listed)
            if not listed:
                evals.append(resolved)
                placement.extend(eval_placement(resolved, target.target))
        return compile_checked(
            evals,
            key=lambda resolved: resolved.key,
            render=self._compile,
            diagnostics=DiagnosticBag((*self._catalog.diagnostics, *placement)),
            origin=lambda resolved: resolved.config.origin,
        )

    def _target(self, resolved: ResolvedEval) -> SubjectTarget | None:
        """Return where the eval's subject publishes, looked up by its kind and name; None when unknown."""
        name = resolved.subject.name.casefold()
        if resolved.subject.is_profile:
            return self._profile_targets.get(name)
        agent_target = self._agent_targets.get(name)
        return SubjectTarget(agent_target) if agent_target is not None else None

    def _compile(self, resolved: ResolvedEval) -> tuple[CompiledEval | None, tuple[Diagnostic, ...]]:
        found = eval_render_checks(resolved)
        if any(item.blocks for item in found):
            return None, found
        target = self._target(resolved)
        if target is None:
            raise KeyError(f"{resolved.key} has no target")
        return _render(resolved, target, self._catalog.defaults.agent_version), found


def _render(resolved: ResolvedEval, subject_target: SubjectTarget, root_version: str | None) -> CompiledEval:
    """Render one eval's dataset and config into the schema of `subject_target`.

    The dataset and source table names come from the config's templates, filled with the
    subject's name and the first seven characters of the dataset payload's digest, so a new
    dataset publishes under a new name. A CoWork profile's expected subagents render as the
    qualified names its router gives them on this target.

    The agent version is the eval's `agent_version` setting, else `root_version`, the block
    root's.

    Raises:
        ValueError: the config has no dataset name or source table template, or neither names
            an agent version.
        KeyError: an expected subagent is not one of the profile's on this target.
    """
    agent_target = subject_target.target
    dataset_config = resolved.config.dataset
    if dataset_config is None or dataset_config.name_template is None or dataset_config.source_table_template is None:
        raise ValueError("eval dataset requires name_template and source_table_template")
    payload = render_dataset_payload(resolved.dataset, subject_target.subagents)
    dataset_fingerprint = sha256(payload.encode("utf-8")).hexdigest()
    sha7 = dataset_fingerprint[:7]
    dataset_name = render_eval_name_template(dataset_config.name_template, agent=resolved.name, sha7=sha7)
    source_name = render_eval_name_template(dataset_config.source_table_template, agent=resolved.name, sha7=sha7)
    dataset_target = _in_agent_schema(agent_target, dataset_name)
    source_table = _in_agent_schema(agent_target, source_name)
    config_yaml = render_eval_config(
        resolved.config,
        resolved.custom_metrics,
        agent_target=agent_target,
        dataset_target=dataset_target,
        agent_version=resolved.options.agent_version or root_version,
    )
    rendered = RenderedEval(
        payload,
        render_source_table_statements(payload, source_table),
        render_create_dataset_statement(resolved.config, source_table, dataset_target),
        config_yaml,
        dataset_fingerprint,
        sha256(config_yaml.encode("utf-8")).hexdigest(),
    )
    return CompiledEval(resolved, agent_target, source_table, dataset_target, rendered)


def _in_agent_schema(agent_target: QualifiedName, rendered: str) -> QualifiedName:
    """Place a rendered eval object name in the agent's schema, dropping a qualifier that names it."""
    name = rendered.rsplit(".", 1)[-1]
    return QualifiedName(agent_target.database, agent_target.schema, QualifiedName.from_parts("X", "X", name).name)

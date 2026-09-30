"""A `SemanticViewSource` backed by YAML files on disk.

Thin by design: it binds a project directory to the loader functions so `app/` can
hold a source without holding a path. All the reading lives in the loaders it calls.
It is also where the dbt side meets the semantic layer: it loads the dbt target and
models through `dbt.project` and passes them to the semantic pipeline, which reads no
dbt file itself.
"""

from __future__ import annotations

from pathlib import Path

from ..domain.model.agent import AgentModel
from ..domain.model.diagnostic import DiagnosticBag
from ..domain.model.eval import EvalCatalog
from ..domain.model.project import SemanticViewProject
from ..domain.model.tool import ToolCatalog
from .dbt.manifest import load_manifest_catalog
from .dbt.profiles import resolve_profile_name
from .dbt.project import load_models, resolve_target, run_dbt_parse, target_path
from .yaml.agents import load_agents
from .yaml.evals import load_eval_catalog, parse_eval_defaults
from .yaml.fields import strings
from .yaml.parse import read_yaml_mapping
from .yaml.semantic import load_semantic_views_result, read_semantic_inputs
from .yaml.tools import load_tool_catalog


class YamlProjectSource:
    """Reads semantic views from a dbt + SST project directory.

    Satisfies `domain.ports.semantic_view_source.SemanticViewSource` structurally;
    it does not import the Protocol, which is what keeps the dependency pointing
    one way.
    """

    def __init__(
        self,
        project_dir: Path,
        *,
        target_name: str | None = None,
        manifest_path: Path | None = None,
        invoke_dbt: bool = True,
    ) -> None:
        self._project_dir = project_dir
        self._target_name = target_name
        self._manifest_path = manifest_path
        self._invoke_dbt = invoke_dbt

    @property
    def project_dir(self) -> Path:
        return self._project_dir

    def load_project(self) -> SemanticViewProject:
        # SST's own files first, then the dbt target and models: a problem in either is
        # reported in that order, and before dbt is run.
        inputs = read_semantic_inputs(self._project_dir)
        target = resolve_target(self._project_dir, self._target_name)
        models = load_models(
            self._project_dir,
            read_yaml=read_yaml_mapping,
            target_name=self._target_name,
            manifest_path=self._manifest_path,
            invoke_dbt=self._invoke_dbt,
        )
        return load_semantic_views_result(self._project_dir, inputs, target=target, models=models)

    def load_tools(self) -> ToolCatalog:
        if self._invoke_dbt and self._manifest_path is None:
            run_dbt_parse(self._project_dir, self._target_name)
        manifest_path = self._manifest_path or target_path(self._project_dir, read_yaml_mapping)
        dbt = load_manifest_catalog(manifest_path)
        config = read_yaml_mapping(self._project_dir / "sst_config.yml")
        project = config.get("project")
        tools_dir = str(project.get("tools_dir") or "tools") if isinstance(project, dict) else "tools"
        profile_name = resolve_profile_name(self._project_dir)
        profiles = read_yaml_mapping(self._project_dir / "profiles.yml")
        profile = profiles.get(profile_name)
        outputs = profile.get("outputs") if isinstance(profile, dict) else None
        declared_targets = frozenset(str(key) for key in outputs) if isinstance(outputs, dict) else frozenset()
        selected = self._target_name or (profile.get("target") if isinstance(profile, dict) else None)
        if not isinstance(selected, str) or selected not in declared_targets:
            raise ValueError(f"profiles.yml has no target {selected!r}")
        return load_tool_catalog(
            self._project_dir,
            dbt,
            target_name=selected,
            declared_targets=declared_targets,
            tools_dir=tools_dir,
        )

    def load_evals(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_diagnostics: DiagnosticBag = DiagnosticBag(),
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        config = read_yaml_mapping(self._project_dir / "sst_config.yml")
        project = config.get("project")
        agents_dir = str(project.get("agents_dir") or "agents") if isinstance(project, dict) else "agents"
        metrics_dir = (
            str(project.get("eval_metrics_dir") or "eval_metrics") if isinstance(project, dict) else "eval_metrics"
        )
        if agents is None:
            agents, agent_diagnostics = load_agents(self._project_dir, agents_dir=agents_dir)
        defaults, default_diagnostics = parse_eval_defaults(config.get("evals"))
        snowflake = config.get("snowflake")
        raw_models = snowflake.get("orchestration_models") if isinstance(snowflake, dict) else None
        allowed_models = strings(raw_models)
        return load_eval_catalog(
            self._project_dir,
            agents,
            eval_metrics_dir=metrics_dir,
            defaults=defaults,
            initial_diagnostics=DiagnosticBag((*agent_diagnostics, *default_diagnostics)),
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        )

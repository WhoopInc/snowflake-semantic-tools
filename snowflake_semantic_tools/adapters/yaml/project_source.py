"""A `SemanticViewSource` backed by YAML files on disk.

Thin by design: it binds a project directory to the loader functions so `app/` can
hold a source without holding a path. All the reading lives in `loader.py`.
"""

from __future__ import annotations

from pathlib import Path

from ...domain.model.agent import AgentModel
from ...domain.model.diagnostic import DiagnosticBag
from ...domain.model.eval import EvalCatalog
from ...domain.model.project import SemanticViewProject
from ...domain.model.tool import ToolCatalog
from ..dbt.manifest import load_manifest_catalog
from ..profile import resolve_profile_name
from .agents import load_agents
from .evals import load_eval_catalog, parse_eval_defaults
from .loader import _read_yaml, _run_dbt_parse, _target_path, load_semantic_views_result
from .tools import load_tool_catalog


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
        return load_semantic_views_result(
            self._project_dir,
            target_name=self._target_name,
            manifest_path=self._manifest_path,
            invoke_dbt=self._invoke_dbt,
        )

    def load_tools(self) -> ToolCatalog:
        if self._invoke_dbt and self._manifest_path is None:
            _run_dbt_parse(self._project_dir, self._target_name)
        manifest_path = self._manifest_path or _target_path(self._project_dir)
        dbt = load_manifest_catalog(manifest_path)
        config = _read_yaml(self._project_dir / "sst_config.yml")
        project = config.get("project")
        tools_dir = str(project.get("tools_dir") or "tools") if isinstance(project, dict) else "tools"
        profile_name = resolve_profile_name(self._project_dir)
        profiles = _read_yaml(self._project_dir / "profiles.yml")
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
        config = _read_yaml(self._project_dir / "sst_config.yml")
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
        allowed_models = tuple(str(model) for model in raw_models) if isinstance(raw_models, list) else ()
        return load_eval_catalog(
            self._project_dir,
            agents,
            eval_metrics_dir=metrics_dir,
            defaults=defaults,
            initial_diagnostics=DiagnosticBag((*agent_diagnostics, *default_diagnostics)),
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        )

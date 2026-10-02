"""The project directory on disk as `SemanticViewSource` and `ProjectInputs`.

Thin by design: `YamlProjectSource` binds a project directory to the loader functions so
`app/` can hold a source without holding a path, and `YamlProjectInputs` adds everything
else a use case reads -- the config, the target, the catalogs, the commit, and what a
manifest records about the files. All the reading lives in the loaders they call. It is
also where the dbt side meets the semantic layer: the dbt target and models are loaded
through `dbt.project` and passed to the semantic pipeline, which reads no dbt file itself.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import DbtRunner, parse_project, subprocess_runner
from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.dbt.profiles import load_profile_target, profile_output, resolve_profile_name
from snowflake_semantic_tools.adapters.dbt.project import (
    check_model_paths,
    dbt_project_name,
    model_paths,
    resolve_target,
    stale_models,
    target_path,
)
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.config import load_project_config, read_config_document
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache, load_documents
from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog, parse_eval_defaults
from snowflake_semantic_tools.adapters.yaml.fields import strings
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes, read_yaml_mapping
from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.adapters.yaml.semantic import load_semantic_views_result, read_semantic_inputs
from snowflake_semantic_tools.adapters.yaml.skills import _published, load_skill_catalog
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.config_schema import config_block, configured_dir, skills_configured
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog
from snowflake_semantic_tools.domain.ports.project import (
    ManifestSources,
    ProjectConfig,
    ProjectInputs,
    ProjectTarget,
    ValidationDefaults,
)
from snowflake_semantic_tools.domain.state import canonical_json
from snowflake_semantic_tools.domain.validate.dbt_seam import empty_catalog


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
        dbt_runner: DbtRunner = subprocess_runner,
        allow_unsupported_manifest_schema: bool = False,
        load_cache: LoadCache | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._target_name = target_name
        self._manifest_path = manifest_path
        self._invoke_dbt = invoke_dbt
        self._load_cache = load_cache
        self._runner = dbt_runner
        self._allow_unsupported = allow_unsupported_manifest_schema
        self._parsed = False
        self._parse_warnings: tuple[Diagnostic, ...] = ()

    @property
    def project_dir(self) -> Path:
        """Return the project directory this source reads, as it was given, unresolved."""
        return self._project_dir

    def manifest_file(self) -> Path:
        """Return the manifest this source reads: the one given, else dbt's under its `target-path`.

        Raises:
            ProjectError: `dbt_project.yml` cannot be read.
        """
        return self._manifest_path or target_path(self._project_dir, read_yaml_mapping)

    def _parsed_manifest(self) -> Path:
        """Return the manifest to read, running `dbt parse` first the first time dbt may be run.

        The manifest path is resolved before dbt runs, and dbt runs at most once per source, so a
        command that reads both the models and the tools parses the project once.

        Raises:
            ProjectError: `dbt_project.yml` cannot be read or names a `model-paths` entry that
                is not a directory (SST-DIS009), or dbt fails.
        """
        path = self.manifest_file()
        if self._manifest_path is None and self._invoke_dbt and not self._parsed:
            check_model_paths(self._project_dir, read_yaml_mapping)
            self._parse_warnings = parse_project(
                self._project_dir, self._target_name, path, runner=self._runner, auto_compile=self._auto_compile()
            )
            self._parsed = True
        return path

    def _auto_compile(self) -> bool:
        """Read `defer.auto_compile`, the 0.3 key that asked SST to build the manifest for a target."""
        config = read_config_document(self._project_dir)
        defer = config.get("defer") if isinstance(config, dict) else None
        return isinstance(defer, dict) and defer.get("auto_compile") is True

    def dbt_catalog(self) -> DbtCatalog:
        """Return the dbt manifest's models, running `dbt parse` first as `load_project` does.

        Raises:
            ProjectError: dbt fails, or the manifest is absent, unreadable, or of another schema.
        """
        return load_manifest_catalog(self._parsed_manifest(), allow_unsupported_schema=self._allow_unsupported)

    def _seam_diagnostics(self, catalog: DbtCatalog) -> tuple[Diagnostic, ...]:
        """What the dbt seam found before any semantic file is checked against the models.

        A manifest given with `--manifest` is checked against the model files, since SST did not
        produce it; one `dbt parse` just wrote cannot be stale.

        Diagnostics:
            SST-DBT020: from running dbt.
            SST-DBT013, SST-DBT014, SST-DBT012: from reading the manifest.
            SST-DBT001: the manifest holds no model.
            SST-DBT022: `model-paths` cannot be read.
            SST-DBT005: under `--manifest`, a model file changed after the manifest was written.
        """
        found = [*self._parse_warnings, *catalog.diagnostics, *empty_catalog(catalog)]
        if self._manifest_path is not None and (self._project_dir / "dbt_project.yml").is_file():
            paths, unreadable = model_paths(self._project_dir, read_yaml_mapping)
            found.extend((*unreadable, *stale_models(self._project_dir, catalog, paths)))
        return tuple(found)

    def load_project(self) -> SemanticViewProject:
        """Load the semantic views, with every diagnostic the semantic load collected.

        Without a manifest path, and when dbt may be invoked, `dbt parse` runs before the models
        are read.

        Raises:
            ProjectError: `sst_config.yml`, the semantic-models directory or a member cannot be
                read at all, the dbt target cannot be resolved, or dbt or its manifest fails.
        """
        # SST's own files first, then the dbt target and models: a problem in either is
        # reported in that order, and before dbt is run.
        inputs = read_semantic_inputs(self._project_dir, self._load_cache)
        target = resolve_target(self._project_dir, self._target_name)
        catalog = self.dbt_catalog()
        models = {model.name.casefold(): model for model in catalog.models}
        project = load_semantic_views_result(self._project_dir, inputs, target=target, models=models, catalog=catalog)
        return replace(project, diagnostics=DiagnosticBag((*self._seam_diagnostics(catalog), *project.diagnostics)))

    def load_tools(self) -> ToolCatalog:
        """Load the tool groups under `project.tools_dir`, checked against the dbt manifest and target.

        Without a manifest path, and when dbt may be invoked, `dbt parse` runs first. The target
        is the one this source was given, else the profile's default `target`.

        Raises:
            ProjectError: dbt fails, or the dbt manifest, `dbt_project.yml`, `sst_config.yml` or
                `profiles.yml` cannot be read.
            ValueError: The profile cannot be resolved, or `profiles.yml` does not declare the target.
        """
        dbt = self.dbt_catalog()
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
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        """Load the evals of `agents`, reading every agent again when `agents` is None.

        The agents and the custom metrics come from `project.agents_dir` and
        `project.eval_metrics_dir`, the defaults from `evals:`, and the judge models a custom
        metric may name from `snowflake.orchestration_models`. Agents read here bring their own
        diagnostics, ahead of the eval diagnostics, and raise what `load_agents` raises; agents
        passed in do not, since whoever loaded them reports theirs.

        Args:
            agent_tool_names: Each agent's tool names, by casefolded agent name.

        Raises:
            ProjectError: `sst_config.yml` cannot be read.
        """
        config = read_yaml_mapping(self._project_dir / "sst_config.yml")
        project = config.get("project")
        agents_dir = str(project.get("agents_dir") or "agents") if isinstance(project, dict) else "agents"
        metrics_dir = (
            str(project.get("eval_metrics_dir") or "eval_metrics") if isinstance(project, dict) else "eval_metrics"
        )
        agent_diagnostics = DiagnosticBag()
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


class YamlProjectInputs(ProjectInputs):
    """Everything a use case reads from a project directory, read from disk when asked.

    Implements `domain.ports.project.ProjectInputs`, whose methods document the contract.
    Without a dbt manifest path, the semantic views and tools run `dbt parse` first. The
    commit comes from `git_sha`, which the CLI supplies, so the CLI decides how git is asked.
    """

    def __init__(
        self,
        project_dir: Path,
        *,
        target_name: str | None,
        manifest_path: Path | None,
        git_sha: Callable[[], str],
        dbt_runner: DbtRunner = subprocess_runner,
        allow_unsupported_manifest_schema: bool = False,
        load_cache: LoadCache | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._target_name = target_name
        self._allow_unsupported = allow_unsupported_manifest_schema
        self._load_cache = load_cache or LoadCache()
        self._source = YamlProjectSource(
            project_dir,
            target_name=target_name,
            manifest_path=manifest_path,
            invoke_dbt=manifest_path is None,
            dbt_runner=dbt_runner,
            allow_unsupported_manifest_schema=allow_unsupported_manifest_schema,
            load_cache=self._load_cache,
        )
        self._git_sha = git_sha

    def config(self) -> ProjectConfig:
        return load_project_config(self._project_dir)

    def target(self) -> ProjectTarget:
        profile = load_profile_target(self._project_dir, self._target_name)
        return ProjectTarget(profile.identity, profile.diagnostics)

    def load_project(self) -> SemanticViewProject:
        return self._source.load_project()

    def skill_catalog(self, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
        return load_skill_catalog(self._project_dir, skills_dir=skills_dir, plugins_dir=plugins_dir)

    def profile_catalog(
        self, *, profiles_dir: str, hooks_dir: str, mcp_servers_dir: str, commands_dir: str
    ) -> ProfileCatalog:
        return load_profile_catalog(
            self._project_dir,
            profiles_dir=profiles_dir,
            hooks_dir=hooks_dir,
            mcp_servers_dir=mcp_servers_dir,
            commands_dir=commands_dir,
        )

    def dbt_catalog(self) -> DbtCatalog:
        return self._source.dbt_catalog()

    def tool_catalog(self) -> ToolCatalog:
        return self._source.load_tools()

    def agents(self, *, agents_dir: str) -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
        return load_agents(self._project_dir, agents_dir=agents_dir)

    def eval_catalog(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        return self._source.load_evals(agents, agent_tool_names)

    def git_sha(self) -> str:
        return self._git_sha()

    def validation_defaults(self) -> ValidationDefaults:
        config = read_config_document(self._project_dir)
        validation = config.get("validation") if isinstance(config, dict) else None
        if not isinstance(validation, dict):
            return ValidationDefaults()
        return ValidationDefaults(
            bool(validation.get("strict", False)), bool(validation.get("snowflake_syntax_check", True))
        )

    def manifest_sources(self) -> ManifestSources:
        # The reads run in this order on every run, so a broken input is reported at one point.
        dbt_name, catalog, dbt_path = self._dbt_sources()
        config_checksum, semantic_path = self._config_sources()
        projection = _dbt_projection(catalog)
        return ManifestSources(
            semantic_path=semantic_path,
            dbt_project_name=dbt_name,
            config_checksum=config_checksum,
            dbt_manifest_path=dbt_path,
            dbt_schema_version=catalog.schema_version,
            dbt_digest=sha256(canonical_json(projection)).hexdigest(),
            model_count=len(catalog.models),
            file_checksums=_file_checksums(self._project_dir, self._load_cache),
            target_name=self._selected_target(),
        )

    def _selected_target(self) -> str:
        """Return the `profiles.yml` target this source compiles for, by name; empty when none resolves."""
        try:
            return profile_output(self._project_dir, self._target_name)[1]
        except (ProjectError, ValueError, OSError):
            return ""

    def _dbt_sources(self) -> tuple[str, DbtCatalog, str]:
        """Read the dbt project's name and manifest; empty values for a project without dbt.

        The manifest path is recorded relative to the project, or as given when it lies outside.
        """
        if not (self._project_dir / "dbt_project.yml").is_file():
            return "", DbtCatalog(schema_version="", dbt_version=None, project_name=None, models=()), ""
        dbt_path = self._source.manifest_file()
        name = dbt_project_name(self._project_dir)
        catalog = load_manifest_catalog(dbt_path, allow_unsupported_schema=self._allow_unsupported)
        try:
            recorded = dbt_path.resolve().relative_to(self._project_dir.resolve()).as_posix()
        except ValueError:
            recorded = str(dbt_path)
        return name, catalog, recorded

    def _config_sources(self) -> tuple[str, str]:
        """Read `sst_config.yml` as written: its checksum, and the semantic models path it names."""
        config_value = read_config_document(self._project_dir)
        if config_value is None:
            return "", "semantic_models"
        semantic_path = "semantic_models"
        if isinstance(config_value, dict) and isinstance(config_value.get("project"), dict):
            semantic_path = str(config_value["project"].get("semantic_models_dir") or semantic_path)
        return sha256(canonical_json(config_value)).hexdigest(), semantic_path


def _dbt_projection(catalog: DbtCatalog) -> list[dict[str, object]]:
    """The dbt models as the manifest digests them: by casefolded name, with their keys and columns."""
    return [
        {
            "name": model.name,
            "relation": model.relation_name,
            "checksum": model.checksum,
            "primary_key": list(model.primary_key),
            "unique_keys": [list(key) for key in model.unique_keys],
            "columns": [
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "column_type": column.column_type,
                }
                for column in model.columns
            ],
        }
        for model in sorted(catalog.models, key=lambda item: item.name.casefold())
    ]


def _file_checksums(project_dir: Path, cache: LoadCache | None = None) -> dict[str, str]:
    """Checksum every input file the manifest records, by project-relative path.

    With a dbt project, each semantic model document; then every file under the tools,
    agents, and eval metric roots; then, when a channel publishes them, the skill and plugin
    roots, and with the stage channel the profile, hook, and MCP server roots. Those bundled
    roots skip what publication skips: hidden entries and caches.
    """
    config = dict(load_project_config(project_dir).tree)
    checksums: dict[str, str] = {}
    if (project_dir / "dbt_project.yml").is_file():
        semantic_models_dir = configured_dir(config, "semantic_models_dir", "semantic_models")
        documents = load_documents(discover_yaml(project_dir, semantic_models_dir), parse_yaml_bytes, cache)
        checksums.update({document.path: document.checksum for document in documents.documents})
    directories = [
        configured_dir(config, "tools_dir", "tools"),
        configured_dir(config, "agents_dir", "agents"),
        configured_dir(config, "eval_metrics_dir", "eval_metrics"),
    ]
    bundled: tuple[str, ...] = (
        (configured_dir(config, "skills_dir", "skills"), configured_dir(config, "plugins_dir", "plugins"))
        if skills_configured(config)
        else ()
    )
    if isinstance(config_block(config.get("skills")).get("stage"), dict):
        bundled = (
            *bundled,
            configured_dir(config, "profiles_dir", "profiles"),
            configured_dir(config, "hooks_dir", "hooks"),
            configured_dir(config, "mcp_servers_dir", "mcp-servers"),
        )
    for directory in (*directories, *bundled):
        root = project_dir / directory
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if directory in bundled and not _published(path, root):
                continue
            if path.is_file():
                checksums[path.relative_to(project_dir).as_posix()] = sha256(path.read_bytes()).hexdigest()
    return checksums

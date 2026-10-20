"""The project directory on disk as `SemanticViewSource` and `ProjectInputs`.

Thin by design: `YamlProjectSource` binds a project directory to the loader functions so
`app/` can hold a source without holding a path, and `YamlProjectInputs` adds everything
else a use case reads -- the config, the target, the catalogs, the commit, and what a
manifest records about the files. All the reading lives in the loaders they call. It is
also where the dbt side meets the semantic layer: the dbt target and models are loaded
through `dbt.project` and passed to the semantic pipeline, which reads no dbt file itself.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from hashlib import sha256
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import DbtRunner, parse_project, subprocess_runner
from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.dbt.profiles import declared_targets, load_profile_target, profile_output
from snowflake_semantic_tools.adapters.dbt.project import (
    check_model_paths,
    dbt_project_name,
    model_paths,
    resolve_target,
    stale_models,
    target_path,
)
from snowflake_semantic_tools.adapters.deferral import resolve_deferral
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths, sst_target_dir
from snowflake_semantic_tools.adapters.paths import walk_refusal
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.config import read_config_document
from snowflake_semantic_tools.adapters.yaml.discover import directory_refused, discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache, load_documents
from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog, parse_eval_defaults
from snowflake_semantic_tools.adapters.yaml.fields import strings
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes, read_yaml_mapping
from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic import load_semantic_views_result, read_semantic_inputs
from snowflake_semantic_tools.adapters.yaml.skills import _published, load_skill_catalog
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.config_schema import (
    config_block,
    config_bool,
    configured_dir,
    dbt_settings,
)
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.identifier import Identifier
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
from snowflake_semantic_tools.domain.resolve.defer import defer_relations
from snowflake_semantic_tools.domain.state import canonical_json
from snowflake_semantic_tools.domain.validate.dbt_seam import empty_catalog


class YamlProjectSource:
    """Reads semantic views from a dbt + SST project directory.

    Satisfies `domain.ports.semantic_view_source.SemanticViewSource` structurally;
    it does not import the Protocol, which is what keeps the dependency pointing
    one way. `allow_stale_manifest` reports a stale manifest SST did not write (SST-DBT005) as a
    warning instead of an error, as `--allow-stale-manifest` asks.
    """

    def __init__(
        self,
        files: ProjectPaths,
        *,
        target_name: str | None = None,
        manifest_path: Path | None = None,
        invoke_dbt: bool = True,
        database: str | None = None,
        dbt_runner: DbtRunner = subprocess_runner,
        load_cache: LoadCache | None = None,
        allow_stale_manifest: bool = False,
    ) -> None:
        self._files = files
        self._database = database
        self._project_dir = files.project_dir
        self._target_name = target_name
        self._manifest_path = manifest_path
        self._allow_stale_manifest = allow_stale_manifest
        self._invoke_dbt = invoke_dbt
        self._runner = dbt_runner
        self._load_cache = load_cache
        # True once SST has run dbt for this source: only then did SST write the manifest it reads.
        self._parsed = False
        self._parse_warnings: tuple[Diagnostic, ...] = ()
        self._deferred: DbtCatalog | None = None

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
        """Return the manifest to read, running dbt first the first time dbt may be run.

        dbt runs only when no manifest was given and `dbt.invoke` is not false, as `dbt.command`
        says. The manifest path is resolved before dbt runs, and dbt runs at most once per source,
        so a command that reads both the models and the tools parses the project once.

        Raises:
            ProjectError: `dbt_project.yml` cannot be read or names a `model-paths` entry that
                is not a directory (SST-DIS009), or dbt fails.
        """
        path = self.manifest_file()
        settings = dbt_settings(self._config_tree())
        if self._manifest_path is None and self._invoke_dbt and settings.invoke and not self._parsed:
            check_model_paths(self._project_dir, read_yaml_mapping)
            self._parse_warnings = parse_project(
                self._project_dir,
                self._target_name,
                path,
                runner=self._runner,
                auto_compile=self._auto_compile(),
                profiles_dir=self._files.profiles_directory(),
                command=settings.command,
                installation_cache=self._installation_cache(),
            )
            self._parsed = True
        return path

    def _installation_cache(self) -> Path | None:
        """Where what `dbt --version` answered is kept: `dbt_installation.json` in SST's build directory.

        Only for the `dbt` on `PATH`: a runner passed in answers for itself, so None.
        """
        if self._runner is not subprocess_runner:
            return None
        return sst_target_dir(self._project_dir) / "dbt_installation.json"

    def _auto_compile(self) -> bool:
        """Read `defer.auto_compile`: SST produces the deferred target's manifest itself."""
        return config_bool(config_block(self._config_tree().get("defer")).get("auto_compile")) is True

    def dbt_catalog(self) -> DbtCatalog:
        """Return the dbt manifest's models, running `dbt parse` first as `load_project` does.

        With a deferral, each model and source the deferred target's manifest holds reads that
        target's relation, as `defer_relations` says.

        Raises:
            ProjectError: dbt fails, or a manifest is absent, unreadable, or of another schema;
                or the deferred target is not declared (SST-CFG010).
        """
        return self.deferred(self._read_catalog(self._parsed_manifest()))

    def deferred(self, catalog: DbtCatalog) -> DbtCatalog:
        """Return `catalog` with its relations resolved to the deferred target's; itself without a deferral.

        The deferral is the one the configuration names for this source's target, which `sst diff`
        compiles each side for. The deferred target's manifest is read once per source, and
        produced first with `defer.auto_compile` when dbt may be run.

        Raises:
            ProjectError: as `dbt_catalog` raises.
        """
        deferral = resolve_deferral(self._files, self._target_name)
        if deferral is None:
            return catalog
        if self._deferred is None:
            if deferral.produce and self._invoke_dbt and dbt_settings(self._config_tree()).invoke:
                parse_project(
                    self._project_dir,
                    deferral.target,
                    deferral.manifest,
                    runner=self._runner,
                    auto_compile=True,
                    profiles_dir=self._files.profiles_directory(),
                    target_path=deferral.state_dir,
                    command=dbt_settings(self._config_tree()).command,
                    installation_cache=self._installation_cache(),
                )
            self._deferred = self._read_catalog(deferral.manifest)
        return defer_relations(catalog, self._deferred)

    def _read_catalog(self, path: Path) -> DbtCatalog:
        return load_manifest_catalog(
            path,
            allow_unsupported_schema=self._files.allow_unsupported_manifest_schema,
            supported_versions=dbt_settings(self._config_tree()).manifest_schema_versions,
        )

    def _seam_diagnostics(self, catalog: DbtCatalog) -> tuple[Diagnostic, ...]:
        """What the dbt seam found before any semantic file is checked against the models.

        A manifest SST did not produce, given with `--manifest` or read from disk under
        `dbt.invoke: false`, is checked against the model files; one `dbt parse` just wrote cannot
        be stale.

        Diagnostics:
            SST-DBT020: from running dbt.
            SST-DBT013, SST-DBT014, SST-DBT012: from reading the manifest.
            SST-DBT001: the manifest holds no model.
            SST-DBT022: `model-paths` cannot be read.
            SST-DBT005: SST did not write the manifest and a model file changed after it was
                written; a warning under `--allow-stale-manifest`.
        """
        found = [*self._parse_warnings, *catalog.diagnostics, *empty_catalog(catalog)]
        if not self._parsed and (self._project_dir / "dbt_project.yml").is_file():
            paths, unreadable = model_paths(self._project_dir, read_yaml_mapping)
            if self._files.model_paths is not None:
                paths, unreadable = self._files.model_paths, ()
            stale = stale_models(self._project_dir, catalog, paths)
            if self._allow_stale_manifest:
                # Recorded as an override, so the severity stays one the policy could give.
                stale = tuple(
                    dataclasses.replace(item, severity=Severity.WARNING, override=Severity.WARNING) for item in stale
                )
            found.extend((*unreadable, *stale))
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
        config = resolved_config(self._files, self._target_name)
        inputs = read_semantic_inputs(
            self._files, dict(config.tree), self._load_cache, config_diagnostics=config.diagnostics
        )
        target = resolve_target(self._files, self._target_name)
        if self._database is not None:
            target = dataclasses.replace(target, database=self._database)
        catalog = self.dbt_catalog()
        models = {model.name.casefold(): model for model in catalog.models}
        project = load_semantic_views_result(self._project_dir, inputs, target=target, models=models, catalog=catalog)
        seam = self._seam_diagnostics(catalog)
        return dataclasses.replace(project, diagnostics=DiagnosticBag((*seam, *project.diagnostics)))

    def load_tools(self) -> ToolCatalog:
        """Load the tool members under `project.tools_dir`, checked against the dbt manifest and target.

        Without a manifest path, and when dbt may be invoked, `dbt parse` runs first. The target
        is the one this source was given, else the profile's default `target`.

        Raises:
            ProjectError: dbt fails, or the dbt manifest, `dbt_project.yml`, `sst_config.yml` or
                `profiles.yml` cannot be read.
            ValueError: The profile cannot be resolved, or `profiles.yml` does not declare the target.
        """
        dbt = self.dbt_catalog()
        config = self._config_tree()
        tools_dir = configured_dir(config, "tools_dir", "tools")
        _profile, targets, default = declared_targets(self._files)
        selected = self._target_name or default
        if not isinstance(selected, str) or selected not in targets:
            raise ValueError(f"profiles.yml has no target {selected!r}")
        return load_tool_catalog(
            self._project_dir,
            dbt,
            target_name=selected,
            declared_targets=targets,
            tools_dir=tools_dir,
            config=config,
        )

    def _config_tree(self) -> dict[str, object]:
        """Return the run's resolved configuration as a plain tree; empty when the run has none.

        Raises:
            ProjectError: the file cannot be read or parsed.
        """
        return dict(resolved_config(self._files, self._target_name).tree)

    def load_evals(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        """Load the evals of `agents`, reading every agent again when `agents` is None.

        The agents and the custom metrics come from `project.agents_dir` and
        `project.eval_metrics_dir`, each eval's settings from `evals:` and its folder routes,
        whose folders are checked under `project.agents_dir`, and the judge models a custom
        metric may name from `snowflake.orchestration_models`. Agents read here bring their own
        diagnostics, ahead of the eval diagnostics, and raise what `load_agents` raises; agents
        passed in do not, since whoever loaded them reports theirs.

        Args:
            agent_tool_names: Each agent's tool names, by casefolded agent name.

        Raises:
            ProjectError: `sst_config.yml` cannot be read.
        """
        config = self._config_tree()
        agents_dir = configured_dir(config, "agents_dir", "agents")
        metrics_dir = configured_dir(config, "eval_metrics_dir", "eval_metrics")
        agent_diagnostics = DiagnosticBag()
        if agents is None:
            agents, agent_diagnostics = load_agents(self._project_dir, agents_dir=agents_dir, config=config)
        defaults, default_diagnostics = parse_eval_defaults(config.get("evals"), file=self._files.config_name)
        routes = routed_diagnostics(config, self._project_dir, "eval")
        snowflake = config.get("snowflake")
        raw_models = snowflake.get("orchestration_models") if isinstance(snowflake, dict) else None
        allowed_models = strings(raw_models)
        return load_eval_catalog(
            self._project_dir,
            agents,
            eval_metrics_dir=metrics_dir,
            defaults=defaults,
            block=config.get("evals"),
            initial_diagnostics=DiagnosticBag((*agent_diagnostics, *routes, *default_diagnostics)),
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        )


class YamlProjectInputs(ProjectInputs):
    """Everything a use case reads from a project directory, read from disk when asked.

    Implements `domain.ports.project.ProjectInputs`, whose methods document the contract.
    Without a dbt manifest path, the semantic views and tools run `dbt parse` first. The
    commit comes from `git_sha`, and the uncommitted paths from `git_dirty`, which the CLI
    supplies, so the CLI decides how git is asked; without `git_dirty`, none are.
    `allow_stale_manifest` is passed to the source, as `YamlProjectSource` describes.
    """

    def __init__(
        self,
        files: ProjectPaths,
        *,
        target_name: str | None,
        manifest_path: Path | None,
        git_sha: Callable[[], str],
        git_dirty: Callable[[], tuple[str, ...]] = tuple,
        database: str | None = None,
        dbt_runner: DbtRunner = subprocess_runner,
        load_cache: LoadCache | None = None,
        allow_stale_manifest: bool = False,
    ) -> None:
        self._files = files
        self._project_dir = files.project_dir
        self._target_name = target_name
        self._database = database
        self._load_cache = load_cache or LoadCache()
        self._source = YamlProjectSource(
            files,
            target_name=target_name,
            manifest_path=manifest_path,
            invoke_dbt=manifest_path is None,
            database=database,
            dbt_runner=dbt_runner,
            load_cache=self._load_cache,
            allow_stale_manifest=allow_stale_manifest,
        )
        self._git_sha = git_sha
        self._git_dirty = git_dirty

    def config(self) -> ProjectConfig:
        return resolved_config(self._files, self._target_name)

    def target(self) -> ProjectTarget:
        profile = load_profile_target(self._files, self._target_name)
        identity = profile.identity
        if self._database is not None:
            identity = dataclasses.replace(identity, database=Identifier.parse(self._database))
        return ProjectTarget(identity, profile.diagnostics)

    def load_project(self) -> SemanticViewProject:
        return self._source.load_project()

    def skill_catalog(self, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
        return load_skill_catalog(
            self._project_dir, skills_dir=skills_dir, plugins_dir=plugins_dir, config=self.config().tree
        )

    def profile_catalog(
        self, *, profiles_dir: str, hooks_dir: str, mcp_servers_dir: str, commands_dir: str
    ) -> ProfileCatalog:
        return load_profile_catalog(
            self._project_dir,
            profiles_dir=profiles_dir,
            hooks_dir=hooks_dir,
            mcp_servers_dir=mcp_servers_dir,
            commands_dir=commands_dir,
            config=self.config().tree,
        )

    def dbt_catalog(self) -> DbtCatalog:
        return self._source.dbt_catalog()

    def tool_catalog(self) -> ToolCatalog:
        return self._source.load_tools()

    def agents(self, *, agents_dir: str) -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
        return load_agents(self._project_dir, agents_dir=agents_dir, config=self.config().tree)

    def eval_catalog(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        return self._source.load_evals(agents, agent_tool_names)

    def git_sha(self) -> str:
        return self._git_sha()

    def git_dirty(self) -> tuple[str, ...]:
        return self._git_dirty()

    def validation_defaults(self) -> ValidationDefaults:
        validation = config_block(self.config().tree.get("validation"))
        strict = config_bool(validation.get("strict"))
        connected = config_bool(validation.get("snowflake_syntax_check"))
        return ValidationDefaults(bool(strict), True if connected is None else connected)

    def manifest_sources(self) -> ManifestSources:
        # The reads run in this order on every run, so a broken input is reported at one point.
        dbt_name, catalog, dbt_path = self._dbt_sources()
        config_checksum, semantic_path = self._config_sources()
        config = self.config()
        projection = _dbt_projection(catalog)
        return ManifestSources(
            semantic_path=semantic_path,
            dbt_project_name=dbt_name,
            config_checksum=config_checksum,
            dbt_manifest_path=dbt_path,
            dbt_schema_version=catalog.schema_version,
            dbt_digest=sha256(canonical_json(projection)).hexdigest(),
            model_count=len(catalog.models),
            file_checksums=_file_checksums(
                self._files, config.tree, self._load_cache, refused=directory_refused(config.diagnostics)
            ),
            target_name=self._selected_target(),
            dbt_graph=catalog.nodes,
        )

    def _selected_target(self) -> str:
        """Return the `profiles.yml` target this source compiles for, by name; empty when none resolves."""
        try:
            return profile_output(self._files, self._target_name)[1]
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
        catalog = self._source.deferred(
            load_manifest_catalog(
                dbt_path,
                allow_unsupported_schema=self._files.allow_unsupported_manifest_schema,
                supported_versions=dbt_settings(self.config().tree).manifest_schema_versions,
            )
        )
        try:
            recorded = dbt_path.resolve().relative_to(self._project_dir.resolve()).as_posix()
        except ValueError:
            recorded = str(dbt_path)
        return name, catalog, recorded

    def _config_sources(self) -> tuple[str, str]:
        """Return the checksum of `sst_config.yml` as written, and the semantic models path the run reads.

        The checksum hashes the document as written, so it is the one value read off the file
        rather than off the resolved configuration.
        """
        config_value = read_config_document(self._files)
        if config_value is None:
            return "", "semantic_models"
        semantic_path = configured_dir(self.config().tree, "semantic_models_dir", "semantic_models")
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


def _file_checksums(
    files: ProjectPaths, tree: Mapping[str, object], cache: LoadCache | None = None, *, refused: bool = False
) -> dict[str, str]:
    """Checksum every input file the manifest records, by project-relative path.

    With a dbt project, each semantic model document discovery finds; then every file under the
    tools, agents, and eval metric roots; then under the skill, plugin, profile, hook, MCP
    server, and command roots. Those bundled roots skip what publication skips: hidden entries
    and caches.

    A symbolic link, or a file reached through one, is skipped, as every loader refuses it: no
    checksum is read from outside the project.
    """
    project_dir = files.project_dir
    config = dict(tree)
    checksums: dict[str, str] = {}
    # A semantic-models directory the configuration refused is reported there, not looked for.
    if (project_dir / "dbt_project.yml").is_file() and not refused:
        semantic_models_dir = configured_dir(config, "semantic_models_dir", "semantic_models")
        found = discover_yaml(project_dir, semantic_models_dir, config=config)
        documents = load_documents(found, parse_yaml_bytes, cache)
        checksums.update({document.path: document.checksum for document in documents.documents})
    directories = [
        configured_dir(config, "tools_dir", "tools"),
        configured_dir(config, "agents_dir", "agents"),
        configured_dir(config, "eval_metrics_dir", "eval_metrics"),
    ]
    # Any skill, plugin, or profile may publish from its own `config:`, so every root they are
    # read from is an input whatever `sst_config.yml` says.
    bundled = (
        configured_dir(config, "skills_dir", "skills"),
        configured_dir(config, "plugins_dir", "plugins"),
        configured_dir(config, "profiles_dir", "profiles"),
        configured_dir(config, "hooks_dir", "hooks"),
        configured_dir(config, "mcp_servers_dir", "mcp-servers"),
        configured_dir(config, "commands_dir", "commands"),
    )
    for directory in (*directories, *bundled):
        root = project_dir / directory
        refused = not root.is_relative_to(project_dir) or walk_refusal(project_dir, root) is not None
        if refused or not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if directory in bundled and not _published(path, root):
                continue
            if walk_refusal(project_dir, path) is None and path.is_file():
                checksums[path.relative_to(project_dir).as_posix()] = sha256(path.read_bytes()).hexdigest()
    return checksums

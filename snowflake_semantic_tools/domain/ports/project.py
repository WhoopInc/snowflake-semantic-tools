"""The port `app/` reads a project through, and the values it returns.

`ProjectInputs` is everything a compile, a plan, or a test suite reads from the project
directory -- `sst_config.yml`, the dbt target, the authored catalogs, the dbt manifest, the
commit -- returned as domain values, so a use case never touches a file. Each method reads
when it is called, and the use cases call them in a fixed order, so a broken input is
reported at the same point on every run. An adapter satisfies the protocol structurally.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.diagnostic import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog
from snowflake_semantic_tools.domain.ports.semantic_view_source import SemanticViewSource


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    """`sst_config.yml` as checked against the declared schema.

    Attributes:
        tree: The parsed document, empty when the project has no `sst_config.yml`.
        diagnostics: What checking the document and its `project.*_dir` roots reported.
        has_dbt_project: Whether the project directory holds a `dbt_project.yml`.
    """

    tree: Mapping[str, Any]
    diagnostics: DiagnosticBag
    has_dbt_project: bool


@dataclass(frozen=True, slots=True)
class ProjectTarget:
    """The dbt target a compile resolves `{{ target.* }}` against, as `profiles.yml` declares it.

    Attributes:
        identity: The target as declared, before any connection confirms its account or role.
        diagnostics: What resolving the target reported without refusing it.
    """

    identity: TargetIdentity
    diagnostics: tuple[Diagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidationDefaults:
    """The `validation:` block: what validate, plan, and apply do when no flag overrides it.

    Attributes:
        strict: Whether every warning is promoted to an error.
        snowflake_syntax_check: Whether expressions are compiled against Snowflake.
    """

    strict: bool = False
    snowflake_syntax_check: bool = True

    def resolve(self, strict: bool | None, connected: bool | None) -> tuple[bool, bool]:
        """Return `(strict, connected)`, where a flag that is not None overrides the configured value."""
        return (
            self.strict if strict is None else strict,
            self.snowflake_syntax_check if connected is None else connected,
        )


@dataclass(frozen=True, slots=True)
class ManifestSources:
    """What a manifest records about the project's inputs, besides the artifacts compiled from them.

    Attributes:
        semantic_path: `project.semantic_models_dir` as written, else `semantic_models`.
        dbt_project_name: The dbt project's name; empty without a dbt project.
        config_checksum: The SHA-256 of `sst_config.yml` as written; empty without one.
        dbt_manifest_path: The dbt manifest's path relative to the project root, else as given
            when it lies outside the project; empty without a dbt project.
        dbt_schema_version, dbt_digest, model_count: The dbt manifest's schema version, the
            SHA-256 of its model projection, and its model count.
        file_checksums: The SHA-256 of every input file, by project-relative path.
    """

    semantic_path: str
    dbt_project_name: str
    config_checksum: str
    dbt_manifest_path: str
    dbt_schema_version: str
    dbt_digest: str
    model_count: int
    file_checksums: Mapping[str, str]


class ProjectInputs(SemanticViewSource, Protocol):
    """Everything a use case reads from the project directory, as domain values.

    Every method reads when called and returns a fresh value; none caches and none writes.
    A problem in what was authored is a diagnostic in the value returned; an input that
    cannot be read at all raises. `load_project`, from `SemanticViewSource`, returns the
    semantic views.
    """

    def config(self) -> ProjectConfig:
        """Return `sst_config.yml` parsed and checked against the declared schema.

        Returns:
            The configuration; an empty tree when the project has no `sst_config.yml`.

        Raises:
            ProjectError: the file cannot be read or is not valid YAML.
        """
        ...

    def target(self) -> ProjectTarget:
        """Return the dbt target the compile resolves against, as `profiles.yml` declares it.

        Raises:
            ProjectError: the target is absent or holds a value SST cannot use.
            ValueError: the profile or a required environment variable cannot be resolved.
            OSError: `profiles.yml` cannot be read.
        """
        ...

    def skill_catalog(self, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
        """Return every skill folder under `skills_dir` and every plugin under `plugins_dir`.

        A directory that does not exist contributes nothing.

        Raises:
            ProjectError: a skill or plugin file cannot be read.
        """
        ...

    def profile_catalog(
        self, *, profiles_dir: str, hooks_dir: str, mcp_servers_dir: str, commands_dir: str
    ) -> ProfileCatalog:
        """Return the Desktop profiles, and the hooks, MCP configs, and commands they can name.

        A directory that does not exist contributes nothing.

        Raises:
            ProjectError: a profile input cannot be read.
        """
        ...

    def dbt_catalog(self) -> DbtCatalog:
        """Return the models of the dbt manifest the compile reads.

        Without a manifest path, `dbt parse` runs first, at most once for everything these
        inputs read.

        Raises:
            ProjectError: dbt fails, or the manifest is absent or unreadable.
        """
        ...

    def tool_catalog(self) -> ToolCatalog:
        """Return the tool groups the project declares, resolved against the dbt target.

        Raises:
            ProjectError: the dbt manifest or a tool file cannot be read.
            ValueError: `profiles.yml` does not declare the target.
        """
        ...

    def agents(self, *, agents_dir: str) -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
        """Return every agent under `agents_dir`, enabled or not, with what loading them reported.

        Raises:
            ProjectError: an agent file cannot be read.
        """
        ...

    def eval_catalog(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        """Return the evals of `agents`, with the `evals:` defaults and the custom metrics they use.

        The diagnostics of loading `agents` stay with the agents; only agents read here, when
        `agents` is None, bring theirs.

        Args:
            agents: The agents whose evals to load; None loads every agent again.
            agent_tool_names: Each agent's tool names, by casefolded agent name, which an
                eval's expected tools must be among.

        Raises:
            ProjectError: an eval or metric file cannot be read.
        """
        ...

    def git_sha(self) -> str:
        """Return the project's commit, as its first seven characters.

        Returns:
            The abbreviated commit; `WORKTREE` when the project is not in a git work tree.
        """
        ...

    def validation_defaults(self) -> ValidationDefaults:
        """Return the `validation:` block, with each key the document omits at its default.

        Raises:
            yaml.YAMLError: `sst_config.yml` is not valid YAML.
        """
        ...

    def manifest_sources(self) -> ManifestSources:
        """Return what a manifest records about the project's files, read now.

        Raises:
            ProjectError: the dbt project or its manifest cannot be read.
            OSError: an input file cannot be read for its checksum.
        """
        ...

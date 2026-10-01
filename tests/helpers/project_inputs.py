"""An in-memory `ProjectInputs` for application tests: every value given up front, every read recorded."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.diagnostic import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.identifier import Identifier, TargetIdentity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog
from snowflake_semantic_tools.domain.ports.project import (
    ManifestSources,
    ProjectConfig,
    ProjectTarget,
    ValidationDefaults,
)


def dev_target() -> TargetIdentity:
    """The target the doubles compile against: `DB.SCH` on warehouse `WH`."""
    return TargetIdentity("dev", "ACCOUNT", Identifier.parse("DB"), Identifier.parse("SCH"), None, "WH")


EMPTY_SOURCES = ManifestSources("semantic_models", "", "", "", "", "", 0, MappingProxyType({}))


@dataclass
class InMemoryProjectInputs:
    """Holds what `YamlProjectInputs` would read from disk, and records every read in order.

    Each entry of `reads` names the method, and the directories it was asked for where it
    takes any; `eval_requests` holds the arguments of each `eval_catalog` call.
    """

    tree: Mapping[str, object] = field(default_factory=dict)
    config_diagnostics: DiagnosticBag = DiagnosticBag()
    has_dbt_project: bool = True
    identity: TargetIdentity = field(default_factory=dev_target)
    target_diagnostics: tuple[Diagnostic, ...] = ()
    skills: SkillCatalog = field(default_factory=SkillCatalog)
    profiles: ProfileCatalog = field(default_factory=ProfileCatalog)
    views: SemanticViewProject = field(default_factory=lambda: SemanticViewProject(()))
    dbt: DbtCatalog = field(default_factory=lambda: DbtCatalog("v12", None, None, ()))
    tools: ToolCatalog = field(default_factory=lambda: ToolCatalog((), "dev", frozenset(("dev",))))
    agent_models: tuple[AgentModel, ...] = ()
    agent_diagnostics: DiagnosticBag = DiagnosticBag()
    evals: EvalCatalog = field(default_factory=lambda: EvalCatalog((), ()))
    revision: str = "abc1234"
    validation: ValidationDefaults = field(default_factory=ValidationDefaults)
    sources: ManifestSources = EMPTY_SOURCES
    reads: list[str] = field(default_factory=list)
    eval_requests: list[tuple[object, ...]] = field(default_factory=list)

    def config(self) -> ProjectConfig:
        self.reads.append("config")
        return ProjectConfig(MappingProxyType(dict(self.tree)), self.config_diagnostics, self.has_dbt_project)

    def target(self) -> ProjectTarget:
        self.reads.append("target")
        return ProjectTarget(self.identity, self.target_diagnostics)

    def load_project(self) -> SemanticViewProject:
        self.reads.append("load_project")
        return self.views

    def skill_catalog(self, *, skills_dir: str, plugins_dir: str) -> SkillCatalog:
        self.reads.append(f"skill_catalog:{skills_dir},{plugins_dir}")
        return self.skills

    def profile_catalog(
        self, *, profiles_dir: str, hooks_dir: str, mcp_servers_dir: str, commands_dir: str
    ) -> ProfileCatalog:
        self.reads.append(f"profile_catalog:{profiles_dir},{hooks_dir},{mcp_servers_dir},{commands_dir}")
        return self.profiles

    def dbt_catalog(self) -> DbtCatalog:
        self.reads.append("dbt_catalog")
        return self.dbt

    def tool_catalog(self) -> ToolCatalog:
        self.reads.append("tool_catalog")
        return self.tools

    def agents(self, *, agents_dir: str) -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
        self.reads.append(f"agents:{agents_dir}")
        return self.agent_models, self.agent_diagnostics

    def eval_catalog(
        self,
        agents: tuple[AgentModel, ...] | None = None,
        agent_diagnostics: DiagnosticBag = DiagnosticBag(),
        agent_tool_names: dict[str, tuple[str, ...]] | None = None,
    ) -> EvalCatalog:
        self.reads.append("eval_catalog")
        self.eval_requests.append((agents, agent_diagnostics, agent_tool_names))
        return self.evals

    def git_sha(self) -> str:
        self.reads.append("git_sha")
        return self.revision

    def validation_defaults(self) -> ValidationDefaults:
        self.reads.append("validation_defaults")
        return self.validation

    def manifest_sources(self) -> ManifestSources:
        self.reads.append("manifest_sources")
        return self.sources

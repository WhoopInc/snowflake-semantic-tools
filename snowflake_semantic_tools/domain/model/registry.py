"""Closed artifact registry and the members each artifact type owns.

Types are registered on a `RegistryBuilder`, whose `freeze` runs every integrity check and
returns the read-only `Registry`; a registration after `freeze` is refused. The checks run in a
fixed order and the first failure raises, so a registry with several faults always reports the
same one. Each raises `RegistryIntegrityError` carrying an SST-REG code.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import RULE_SETS, ErrorSpec
from snowflake_semantic_tools.domain.diagnostics.integrity import registry_fault
from snowflake_semantic_tools.domain.model.artifact_types import (
    ArtifactLifecycle as ArtifactLifecycle,
)
from snowflake_semantic_tools.domain.model.artifact_types import (
    ArtifactType as ArtifactType,
)
from snowflake_semantic_tools.domain.model.artifact_types import (
    AttachRule as AttachRule,
)
from snowflake_semantic_tools.domain.model.artifact_types import (
    GrantPreservation as GrantPreservation,
)
from snowflake_semantic_tools.domain.model.artifact_types import (
    MemberSource as MemberSource,
)
from snowflake_semantic_tools.domain.model.artifact_types import (
    MemberType as MemberType,
)


@dataclass(frozen=True, slots=True)
class Registry:
    """The artifact and member types, each map keyed by type name.

    A registry `RegistryBuilder.freeze` returns has passed every integrity check, and its maps
    are read-only.
    """

    artifacts: Mapping[str, ArtifactType]
    members: Mapping[str, MemberType]

    def shared_object_types(self) -> frozenset[str]:
        """Return the object types more than one artifact type publishes, such as AGENT.

        Observation lists such a type in each kind's own schemas only, so an object is keyed
        by the one kind that published it.
        """
        claims = Counter(
            object_type
            for artifact in self.artifacts.values()
            for object_type in {*artifact.object_types, artifact.object_type}
            if object_type
        )
        return frozenset(object_type for object_type, count in claims.items() if count > 1)


# Fields every registration fills, because the generated references and discovery read them.
REQUIRED_ARTIFACT_FIELDS = ("name", "summary", "authored_in", "publishes", "dir_key", "validation_rules")
REQUIRED_MEMBER_FIELDS = ("name", "root_key", "owner_type", "validation_rules")
# Only these artifact types may own members (SST-REG022). Widening the set is the act of
# merging the member and artifact registries, so the decision is recorded here, by name.
MEMBER_OWNING_TYPES = frozenset({"semantic_view"})
# Every template function some resolver in the package handles: `domain.resolve.template` for
# `ref`, `metric`, `var`, `custom_instructions` and tags, refusing the legacy `table` and
# `column`, and for `filter`, `relationship` and `verified_query` in custom-instruction text;
# the semantic loader for `custom_instructions` entries and a table entry's `source`; the agent
# loader for `file`, `skill`, `plugin` and a skill source's `extension`; agent tool resolution for
# `semantic_view`, `tool` and `agent`; the eval config reader for `eval_metric`; and the agent
# loader for `eval_dataset` and `eval_config`, which the eval catalog resolves; and the plugin
# and profile loaders for `skill`, `plugin`, `command`, `mcp_server` and `hook` members.
RESOLVED_FUNCTIONS = frozenset(
    {
        *("ref", "table", "column", "source", "metric", "var", "tag", "custom_instructions", "file"),
        *("eval_metric", "semantic_view", "tool", "agent", "skill", "plugin", "extension"),
        *("eval_dataset", "eval_config", "command", "mcp_server", "hook"),
        *("filter", "relationship", "verified_query"),
    }
)
# Resolved functions that address something outside this registry: dbt models, columns and
# sources, project variables and files, tags, the shared eval metrics, an agent's eval dataset and
# config files, a profile's commands, MCP servers and hooks, and extensions published elsewhere.
NON_ARTIFACT_FUNCTIONS = frozenset(
    {
        *("ref", "table", "column", "source", "var", "tag", "file", "eval_metric", "eval_dataset", "eval_config"),
        *("command", "mcp_server", "hook", "extension"),
    }
)
# Artifact types nothing references, by design: an eval, a Desktop profile, and a CoWork profile
# are leaves, so they need no template function. Named rather than inferred, so a type that
# forgot one still fails.
DELIBERATELY_UNREFERENCED = frozenset({"eval", "profile", "cowork_profile"})


class RegistryBuilder:
    """Collects artifact and member type registrations until `freeze` checks them once.

    A name registered twice is refused at once; everything else is checked by `freeze`.
    """

    def __init__(self) -> None:
        self._artifacts: dict[str, ArtifactType] = {}
        self._members: dict[str, MemberType] = {}
        self._frozen = False

    def artifact(self, artifact: ArtifactType) -> None:
        """Register one artifact type.

        Raises:
            RegistryIntegrityError: SST-REG900 after `freeze`; SST-REG002 for a repeated name.
        """
        self._open(f"artifact type {artifact.name}")
        if artifact.name in self._artifacts:
            raise registry_fault("SST-REG002", type=artifact.name)
        self._artifacts[artifact.name] = artifact

    def member(self, member: MemberType) -> None:
        """Register one member type.

        Raises:
            RegistryIntegrityError: SST-REG900 after `freeze`; SST-REG002 for a repeated name.
        """
        self._open(f"member type {member.name}")
        if member.name in self._members:
            raise registry_fault("SST-REG002", type=member.name)
        self._members[member.name] = member

    def freeze(
        self,
        *,
        rule_sets: Mapping[str, tuple[ErrorSpec, ...]] = RULE_SETS,
        functions: frozenset[str] = RESOLVED_FUNCTIONS,
    ) -> Registry:
        """Check every registration against the others, and return the read-only registry.

        The checks run in this order: required fields, root keys, positions, the names types
        refer to, member ownership, rules, the dependency graph's cycles and then its order,
        grant handling and lifecycle, reference functions, and the resolver set.

        Args:
            rule_sets: The validation rule sets a type's `validation_rules` may name.
            functions: The template functions the package resolves.

        Raises:
            RegistryIntegrityError: SST-REG900 when already frozen; otherwise the first of
                SST-REG001, 003, 004, 007, 022, 006, 011, 010, 021, 005 and 020 that applies.
        """
        self._open("freeze")
        artifacts, members = tuple(self._artifacts.values()), tuple(self._members.values())
        _check_required(artifacts, members)
        _check_root_keys(artifacts, members)
        _check_positions(artifacts, members)
        _check_names(self._artifacts, self._members)
        _check_ownership(artifacts, members)
        _check_rules((*artifacts, *members), rule_sets)
        _check_cycles(self._artifacts)
        _check_dependency_order(self._artifacts)
        for artifact in artifacts:
            _check_grant_preservation(artifact)
        _check_ref_functions(artifacts, members)
        _check_resolvers(artifacts, members, functions)
        self._frozen = True
        return Registry(MappingProxyType(dict(self._artifacts)), MappingProxyType(dict(self._members)))

    def _open(self, what: str) -> None:
        if self._frozen:
            raise registry_fault("SST-REG900", detail=f"{what} after the registry was frozen")


def build_registry(
    artifact_types: tuple[ArtifactType, ...],
    member_types: tuple[MemberType, ...],
    *,
    rule_sets: Mapping[str, tuple[ErrorSpec, ...]] = RULE_SETS,
    functions: frozenset[str] = RESOLVED_FUNCTIONS,
) -> Registry:
    """Register every type on a new `RegistryBuilder`, artifacts first, and freeze it.

    Raises:
        RegistryIntegrityError: As `RegistryBuilder.artifact`, `member` and `freeze` do.
    """
    builder = RegistryBuilder()
    for artifact in artifact_types:
        builder.artifact(artifact)
    for member in member_types:
        builder.member(member)
    return builder.freeze(rule_sets=rule_sets, functions=functions)


def check_member_index(covered: Iterable[str], registry: Registry) -> None:
    """Refuse a member index that does not hold exactly the member types `semantic_view` declares.

    Raises:
        RegistryIntegrityError: SST-REG023.
    """
    declared = sorted(registry.artifacts["semantic_view"].member_types)
    indexed = sorted(covered)
    if indexed != declared:
        raise registry_fault("SST-REG023", covered=indexed, declared=declared)


def _check_required(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a registration that leaves a required field empty (SST-REG001).

    A type authored in files under a list root also names the singular root a one-item file
    uses; an artifact type with no list root names none.
    """
    for kind, fields in ((artifacts, REQUIRED_ARTIFACT_FIELDS), (members, REQUIRED_MEMBER_FIELDS)):
        for entry in kind:
            for field in fields:
                if getattr(entry, field) in (None, "", ()):
                    raise registry_fault("SST-REG001", type=entry.name or "<unnamed>", field=field)
    for artifact in artifacts:
        if (artifact.root_key is None) != (artifact.singular_root is None):
            raise registry_fault("SST-REG001", type=artifact.name, field="singular_root")
    for member in members:
        if member.source is MemberSource.FILES and not member.singular_root:
            raise registry_fault("SST-REG001", type=member.name, field="singular_root")


def _check_root_keys(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a YAML root key, list or singular, claimed twice; a type without one claims nothing.

    Raises:
        RegistryIntegrityError: SST-REG003.
    """
    claims: dict[str, list[str]] = {}
    entries: tuple[ArtifactType | MemberType, ...] = (*artifacts, *members)
    for entry in entries:
        for root in (entry.root_key, entry.singular_root):
            if root is not None:
                claims.setdefault(root, []).append(entry.name)
    for root_key, types in claims.items():
        if len(types) > 1:
            raise registry_fault("SST-REG003", root_key=root_key, types=types)


def _check_positions(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse two members at one clause position of one owner, then two artifacts at one DDL position."""
    clauses: dict[tuple[str, int], list[str]] = {}
    for member in members:
        clauses.setdefault((member.owner_type, member.clause_position), []).append(member.name)
    positions: dict[int, list[str]] = {}
    for artifact in artifacts:
        positions.setdefault(artifact.ddl_position, []).append(artifact.name)
    for position, types in (*((key[1], value) for key, value in clauses.items()), *positions.items()):
        if len(types) > 1:
            raise registry_fault("SST-REG004", position=position, types=types)


def _check_names(artifacts: Mapping[str, ArtifactType], members: Mapping[str, MemberType]) -> None:
    """Refuse an owner, member type, or dependency that names an unregistered type (SST-REG007)."""
    for member in members.values():
        if member.owner_type not in artifacts:
            raise registry_fault("SST-REG007", type=member.name, member_type=member.owner_type)
    for artifact in artifacts.values():
        for name in (*artifact.member_types, *artifact.dependency_types):
            if name not in members and name not in artifacts:
                raise registry_fault("SST-REG007", type=artifact.name, member_type=name)
        for name in artifact.pins_versions_of:
            if name not in artifact.dependency_types:
                raise registry_fault("SST-REG001", type=artifact.name, field=f"dependency_types: {name}")


def _check_ownership(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse members on any type but `semantic_view`, and declared members that do not match owners."""
    for artifact in artifacts:
        if artifact.member_types and artifact.name not in MEMBER_OWNING_TYPES:
            raise registry_fault("SST-REG022", artifact=artifact.name, member_types=list(artifact.member_types))
        owned = {member.name for member in members if member.owner_type == artifact.name}
        for name in sorted(owned - set(artifact.member_types)):
            raise registry_fault("SST-REG001", type=artifact.name, field=f"member_types: {name}")
        for name in sorted(set(artifact.member_types) - owned):
            raise registry_fault("SST-REG007", type=artifact.name, member_type=name)


def _check_rules(entries: tuple[ArtifactType | MemberType, ...], rule_sets: Mapping[str, object]) -> None:
    """Refuse a type naming a validation rule set that is not registered (SST-REG006)."""
    for entry in entries:
        for rule_id in entry.validation_rules:
            if rule_id not in rule_sets:
                raise registry_fault("SST-REG006", type=entry.name, rule_id=rule_id)


def _check_cycles(artifacts: Mapping[str, ArtifactType]) -> None:
    """Refuse a cycle in the type dependency graph, named from its first type back to it (SST-REG011)."""
    done: set[str] = set()

    def visit(name: str, path: tuple[str, ...]) -> None:
        if name in path:
            cycle = (*path[path.index(name) :], name)
            raise registry_fault("SST-REG011", cycle=" -> ".join(cycle))
        if name in done:
            return
        for dependency in artifacts[name].dependency_types:
            visit(dependency, (*path, name))
        done.add(name)

    for name in artifacts:
        visit(name, ())


def _check_dependency_order(artifacts: Mapping[str, ArtifactType]) -> None:
    """Refuse an artifact whose DDL position does not follow every type it depends on (SST-REG010)."""
    for artifact in artifacts.values():
        for dependency in artifact.dependency_types:
            if artifacts[dependency].ddl_position >= artifact.ddl_position:
                raise registry_fault(
                    "SST-REG010", type=artifact.name, position=artifact.ddl_position, blocker=dependency
                )


def _check_grant_preservation(artifact: ArtifactType) -> None:
    """Refuse grant handling, or object lifecycle metadata, that contradicts how the type publishes.

    Raises:
        RegistryIntegrityError: SST-REG021 for a contradiction; SST-REG001 for an object type
            nothing can observe.
    """
    reason = _grant_contradiction(artifact)
    if reason is not None:
        raise registry_fault("SST-REG021", artifact=artifact.name, reason=reason)
    if artifact.lifecycle is ArtifactLifecycle.OBJECT and not artifact.object_type and not artifact.object_types:
        raise registry_fault("SST-REG001", type=artifact.name, field="object_type")


def _grant_contradiction(artifact: ArtifactType) -> str | None:
    """Say how the type's grant handling or composite lifecycle contradicts itself; None when it does not.

    A type that keeps grants per object kind declares them for exactly its object types and none
    for the type; it replaces on update when any kind keeps grants across a replace.
    """
    by_kind = dict(artifact.object_grant_preservation)
    if artifact.grant_preservation is None:
        if not by_kind or set(by_kind) != set(artifact.object_types):
            return "its per-kind grant preservation does not list exactly its object types"
        kept = set(by_kind.values())
    elif by_kind:
        return "it declares grant preservation both for the type and per object kind"
    else:
        kept = {artifact.grant_preservation}
    if artifact.replaces_on_update and kept == {GrantPreservation.NONE}:
        return "it replaces its object and declares no grant preservation"
    if not artifact.replaces_on_update and kept != {GrantPreservation.NONE}:
        return "it never replaces its object and declares grant preservation"
    if artifact.lifecycle is ArtifactLifecycle.COMPOSITE:
        if artifact.object_type or artifact.object_types:
            return "a composite type's grants are its handler's, so it cannot declare an object type"
        if artifact.prunable or artifact.replaces_on_update:
            return "a composite type is pruned and replaced by its handler, not generically"
    return None


def _check_ref_functions(artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...]) -> None:
    """Refuse a reference function claimed by two types; types without one claim nothing (SST-REG005)."""
    seen: set[str] = set()
    entries: tuple[ArtifactType | MemberType, ...] = (*artifacts, *members)
    for entry in entries:
        if entry.ref_function is None:
            continue
        if entry.ref_function in seen:
            raise registry_fault("SST-REG005", ref_function=entry.ref_function)
        seen.add(entry.ref_function)


def _check_resolvers(
    artifacts: tuple[ArtifactType, ...], members: tuple[MemberType, ...], functions: frozenset[str]
) -> None:
    """Refuse a resolver and a registry that disagree in any of the three directions (SST-REG020).

    A resolved function that addresses no registered type, a type's function that no resolver
    handles, and an artifact type with no function that is not deliberately unreferenced.
    """
    entries: tuple[ArtifactType | MemberType, ...] = (*artifacts, *members)
    declared = {entry.ref_function for entry in entries if entry.ref_function is not None}
    for function in sorted(functions - NON_ARTIFACT_FUNCTIONS - declared):
        raise registry_fault("SST-REG020", ref_function=function, direction="a resolver addresses no registered type")
    for function in sorted(declared - functions):
        raise registry_fault("SST-REG020", ref_function=function, direction="no resolver handles the type's function")
    for artifact in artifacts:
        if artifact.ref_function is None and artifact.name not in DELIBERATELY_UNREFERENCED:
            raise registry_fault("SST-REG020", ref_function=artifact.name, direction="the type has no ref_function")


ARTIFACT_REGISTRY = build_registry(
    (
        ArtifactType(
            name="semantic_view",
            root_key="semantic_views",
            singular_root="semantic_view",
            ddl_position=100,
            ref_function="semantic_view",
            member_types=(
                "relationship",
                "fact",
                "dimension",
                "metric",
                "filter",
                "verified_query",
                "custom_instruction",
            ),
            object_type="SEMANTIC VIEW",
            summary="A Snowflake semantic view built from dbt models and the semantic members attached to them.",
            authored_in=(
                "a `semantic_view:` file, or an entry of a `semantic_views:` list, in `project.semantic_models_dir`"
            ),
            publishes="SEMANTIC VIEW",
            validation_rules=("shared", "semantic_view"),
            dir_key="semantic_models_dir",
            routed_block="semantic_views",
            route_dir="semantic_views",
        ),
        ArtifactType(
            name="tool",
            root_key="tools",
            singular_root="tool",
            ddl_position=200,
            ref_function="tool",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=True,
            grant_preservation=None,
            dependency_types=("semantic_view",),
            object_types=("CORTEX SEARCH SERVICE", "PROCEDURE", "FUNCTION", "STAGE"),
            object_grant_preservation=(
                ("CORTEX SEARCH SERVICE", GrantPreservation.REPLAY),
                ("PROCEDURE", GrantPreservation.CLAUSE),
                ("FUNCTION", GrantPreservation.CLAUSE),
                ("STAGE", GrantPreservation.NONE),
            ),
            summary=(
                "One defined tool member: a Snowflake object an agent tool calls, published before any "
                "agent that uses it. Grants depend on the kind: a search service is replaced and its "
                "explicit grants re-applied, a procedure or function is replaced with `COPY GRANTS`, "
                "and a stage is only ever created when absent. A member that writes `relations:` names "
                "an object SST never writes, and is no artifact."
            ),
            authored_in="a `tool:` file, or an entry of a `tools:` list, under `project.tools_dir`",
            publishes="CORTEX SEARCH SERVICE, PROCEDURE, FUNCTION, or STAGE",
            validation_rules=("shared", "tool"),
            dir_key="tools_dir",
            routed_block="tools",
        ),
        ArtifactType(
            name="skill",
            root_key=None,
            singular_root=None,
            ddl_position=250,
            ref_function="skill",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "One `SKILL.md` folder, flattened and published as a version of a skill-type Cortex "
                "Extension wherever its `+bundle_stage` resolves. The version alias is its "
                "`+version_prefix` and 12 hex characters of the bundle digest, so a version is never "
                "rewritten: a change adds a new one, every apply makes the declared one the default "
                "unless a certified version outranks it, and `--prune` only reports one whose folder "
                "was deleted."
            ),
            authored_in="a `SKILL.md` folder in `project.skills_dir`",
            publishes="CORTEX EXTENSION (TYPE = 'SKILL') and its bundle stage",
            validation_rules=("shared", "skill"),
            dir_key="skills_dir",
            routed_block="skills",
        ),
        ArtifactType(
            name="plugin",
            root_key=None,
            singular_root=None,
            ddl_position=260,
            ref_function="plugin",
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "A named set of project skills, published together as a version of one plugin-type "
                "Cortex Extension, content-addressed as a skill's versions are."
            ),
            authored_in="`plugin.yml` in `project.plugins_dir`",
            publishes="CORTEX EXTENSION (TYPE = 'PLUGIN') and its bundle stage",
            validation_rules=("shared", "skill"),
            dir_key="plugins_dir",
            routed_block="plugins",
        ),
        ArtifactType(
            name="profile",
            root_key=None,
            singular_root=None,
            ddl_position=270,
            ref_function=None,
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "A CoCo Desktop profile: content-addressed skill, prompt, MCP, and hook trees on a stage, "
                "behind one row of the profile registry table. `--prune` deactivates the row; nothing "
                "is deleted."
            ),
            authored_in="`profile.yml` in `project.profiles_dir`",
            publishes="profile stage trees and one profile registry row",
            validation_rules=("shared", "skill"),
            dir_key="profiles_dir",
            routed_block="profiles",
        ),
        ArtifactType(
            name="agent",
            root_key=None,
            singular_root=None,
            ddl_position=300,
            ref_function="agent",
            member_types=(),
            object_type="AGENT",
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            dependency_types=("semantic_view", "tool", "skill", "plugin"),
            pins_versions_of=("skill", "plugin"),
            summary=(
                "A Cortex Agent whose specification SST renders and pins to exactly what it uses. It is "
                "created once from a staged specification, and each change to that specification adds a "
                "version; `apply --temporary` publishes a session-scoped temporary agent instead."
            ),
            authored_in="`agent.yml` in `project.agents_dir`",
            publishes="AGENT",
            validation_rules=("shared", "agent"),
            dir_key="agents_dir",
            routed_block="agents",
        ),
        # Snowflake private preview (PrPr): https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
        ArtifactType(
            name="cowork_profile",
            root_key=None,
            singular_root=None,
            ddl_position=350,
            ref_function=None,
            member_types=(),
            object_type="AGENT",
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            dependency_types=("agent", "tool", "skill", "plugin"),
            pins_versions_of=("skill", "plugin"),
            summary=(
                "A Snowflake CoWork profile (Snowflake private preview): an AGENT that routes each question "
                "to one of the agents it names. SST renders its agent_router tool and subagent list; it "
                "publishes and versions like an agent. Prod targets publish it to "
                "SNOWFLAKE_INTELLIGENCE.PROFILES, where CoWork lists it."
            ),
            authored_in="`cowork_profile.yml` in `project.cowork_profiles_dir`",
            publishes="AGENT with an agent_router tool",
            validation_rules=("shared", "cowork_profile"),
            dir_key="cowork_profiles_dir",
            routed_block="cowork_profiles",
        ),
        ArtifactType(
            name="eval",
            root_key=None,
            singular_root=None,
            ddl_position=400,
            ref_function=None,
            member_types=(),
            object_type="",
            prunable=False,
            replaces_on_update=False,
            grant_preservation=GrantPreservation.NONE,
            # Snowflake private preview (PrPr): CoWork profiles. A profile's eval depends on the
            # profile, and its folder routes name a folder under `project.cowork_profiles_dir`.
            dependency_types=("agent", "cowork_profile"),
            lifecycle=ArtifactLifecycle.COMPOSITE,
            summary=(
                "One agent's, or CoWork profile's, evaluation, published into its schema: a source table "
                "and dataset of its questions, named by default for their digest, and its staged run "
                "configuration. A changed question set publishes a new table and dataset and keeps the "
                "old ones; with `dataset.mint: never`, only the configuration publishes, against a "
                "dataset that must already exist."
            ),
            authored_in=(
                "the dataset and config files an agent's or CoWork profile's `evals:` names, by convention in `evals/`"
            ),
            publishes="TABLE, DATASET, and a staged run configuration",
            validation_rules=("shared", "eval"),
            dir_key="agents_dir",
            nested_under="agent",
            routed_block="evals",
        ),
    ),
    (
        # A relationship, filter, or verified query is attached by the tables it reads; its
        # template function names it only from custom-instruction text.
        MemberType(
            "relationship",
            "snowflake_relationships",
            "relationship",
            MemberSource.FILES,
            10,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "relationship",
            ("relationship",),
        ),
        MemberType(
            "fact",
            "facts",
            None,
            MemberSource.DBT_META,
            30,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("semantic_view",),
        ),
        MemberType(
            "dimension",
            "dimensions",
            None,
            MemberSource.DBT_META,
            40,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            None,
            ("semantic_view",),
        ),
        MemberType(
            "metric",
            "snowflake_metrics",
            "metric",
            MemberSource.FILES,
            50,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "metric",
            ("metric",),
        ),
        MemberType(
            "filter",
            "snowflake_filters",
            "filter",
            MemberSource.FILES,
            60,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "filter",
            ("filter",),
        ),
        MemberType(
            "verified_query",
            "snowflake_verified_queries",
            "verified_query",
            MemberSource.FILES,
            70,
            AttachRule.TABLE_MEMBERSHIP,
            "semantic_view",
            "verified_query",
            ("filter",),
        ),
        MemberType(
            "custom_instruction",
            "snowflake_custom_instructions",
            "custom_instruction",
            MemberSource.FILES,
            80,
            AttachRule.VIEW_NAME,
            "semantic_view",
            "custom_instructions",
            ("filter",),
        ),
    ),
)

# The name the semantic-view compiler uses; the registry describes every artifact type.
SEMANTIC_REGISTRY = ARTIFACT_REGISTRY

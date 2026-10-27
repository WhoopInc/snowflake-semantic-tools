"""Resolve and check Snowflake CoWork profiles, then render each one's specification.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile compiles to the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile is an AGENT object whose router hands each question to one of the agents it names, so
it compiles after the agents and against what they compiled against: `subagents` resolves the
agents it routes to, and its skills, display block, alias, tags, and orchestration model are
resolved and checked by the code that resolves and checks an agent's. `compiled` holds the
compiled profile with the statements that publish it; `settled` applies its settings.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext
from snowflake_semantic_tools.app.compile.agents.resolve_skills import resolve_skills
from snowflake_semantic_tools.app.compile.base import CompileResult, has_error
from snowflake_semantic_tools.app.compile.cowork_profiles.compiled import (
    COWORK_STAGE_FOLDER,
    CompiledCoworkProfile,
    for_cowork_publication,
)
from snowflake_semantic_tools.app.compile.cowork_profiles.settled import settled_cowork_profile
from snowflake_semantic_tools.app.compile.cowork_profiles.subagents import CoworkCompileContext, resolve_subagents
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel, ResolvedCoworkProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.render.cowork_profile import (
    cowork_profile_render_checks,
    desired_cowork_profile_definition,
    render_cowork_profile_json,
    render_cowork_profile_spec,
)
from snowflake_semantic_tools.domain.resolve.tags import DeclaredTags
from snowflake_semantic_tools.domain.validate.agent import AgentIdentities, published_agent_rules, spec_size
from snowflake_semantic_tools.domain.validate.cowork_profile import unlisted_profile
from snowflake_semantic_tools.domain.validate.shared import unmodeled_key_diagnostics

__all__ = [
    "COWORK_STAGE_FOLDER",
    "CompileCoworkProfiles",
    "CompiledCoworkProfile",
    "CoworkCompileContext",
    "compile_cowork_profiles",
    "for_cowork_publication",
    "pinned_extensions",
    "settled_cowork_profile",
]


def compile_cowork_profiles(
    loaded: tuple[CoworkProfileModel, ...],
    diagnostics: DiagnosticBag,
    agents: tuple[AgentModel, ...],
    context: AgentCompileContext,
    *,
    target: TargetIdentity,
    tags: DeclaredTags,
    place: Callable[[tuple[CoworkProfileModel, ...]], Mapping[str, QualifiedName]],
) -> CompileResult:
    """Compile the enabled profiles of `loaded` against the enabled `agents` and their context.

    Each profile's settings are applied first, as an agent's are, for every profile loaded;
    then each enabled one compiles where `place` puts it.

    Args:
        diagnostics: What loading the profiles reported; they come first.
        context: The context the agents compiled against, with the `agents:` block root's
            environment.
        place: Where each enabled profile publishes, by casefolded name, as its
            `cowork_profiles:` settings resolve.

    Diagnostics:
        Every code `settled_cowork_profile` lists, for any profile loaded.
        Every code `CompileCoworkProfiles.run_result` lists.
    """
    settled = tuple(settled_cowork_profile(model, target, tags) for model in loaded)
    enabled = tuple(model for model, _ in settled if model.enabled)
    found = DiagnosticBag((*diagnostics, *(item for _, problems in settled for item in problems)))
    by_name: dict[str, AgentModel] = {}
    for model in agents:
        by_name.setdefault(model.name.casefold(), model)
    profiles = CoworkCompileContext(context, by_name, place(enabled), target.database.folded, target.schema.folded)
    return CompileCoworkProfiles(enabled, found, profiles).run_result()


def pinned_extensions(profiles: tuple[CoworkProfileModel, ...]) -> frozenset[str]:
    """Return the artifact keys of the skills and plugins an enabled profile pins, as authored.

    Agents compile first, so these count as consumed when the agents check that each published
    extension has one.
    """
    return frozenset(
        artifact_key(skill.ref, skill.path)
        for model in profiles
        if model.enabled
        for skill in model.skills
        if skill.ref in ("skill", "plugin")
    )


class CompileCoworkProfiles:
    """Compile each CoWork profile: resolve its subagents and skills, check it, render its spec.

    Each model arrives enabled, with its settings applied (`settled_cowork_profile`).
    `diagnostics` are the loader's; they come first, and an error among them that names a
    profile keeps that profile back like one found here.
    """

    def __init__(
        self, models: tuple[CoworkProfileModel, ...], diagnostics: DiagnosticBag, context: CoworkCompileContext
    ) -> None:
        self._models = models
        self._diagnostics = diagnostics
        self._context = context

    def run_result(self) -> CompileResult:
        """Compile the profiles in authored order; a profile compiles unless an error names it.

        Each profile reports, in order: its name and display-name clashes, what
        `resolve_subagents` reports, the rules every published AGENT object shares, what
        `resolve_skills` reports, the spec keys SST does not model, then its rendered spec's
        size and what `cowork_profile_render_checks` reports, as an agent's are. Each that
        compiles then reports that it relies on the preview, and whether CoWork lists it where
        it publishes.

        Diagnostics:
            SST-VAL001: a profile name repeats, ignoring case.
            SST-VAL549: a profile's display name repeats, ignoring case.
            Every code `resolve_subagents` lists.
            Every code `published_agent_rules` lists: an alias, a tag name, the orchestration
                model, the display block, or a deprecated profile's alias.
            Every code `resolve_skills` lists.
            SST-VAL014, SST-VAL013: the spec renders `spec.passthrough` keys SST does not model,
                with a warning when `snowflake.allow_unknown_keys` is true and an error per key
                otherwise.
            SST-VAL511: a rendered spec is over the 100,000-byte limit.
            SST-VAL512: a rendered spec is over 80% of that limit.
            Every code `cowork_profile_render_checks` lists, such as SST-RND011 for a `$$`.
            SST-VAL870: a profile compiled, relying on CoWork profiles, a Snowflake private preview.
            SST-VAL878: a compiled profile publishes outside SNOWFLAKE_INTELLIGENCE.PROFILES.
        """
        diagnostics: list[Diagnostic] = list(self._diagnostics)
        identities = AgentIdentities("cowork profile")
        compiled: list[CompiledCoworkProfile] = []
        for model in self._models:
            diagnostics.extend(identities.claim(model))
            resolved, payload, problems = self._resolve(model)
            diagnostics.extend(problems)
            if has_error(model.key, diagnostics):
                continue
            profile = self._compile(resolved, payload)
            compiled.append(profile)
            diagnostics.append(D("SST-VAL870", origin=model.origin, subject=model.key, artifact=model.name))
            diagnostics.extend(unlisted_profile(model, profile.target))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))

    def _resolve(self, model: CoworkProfileModel) -> tuple[ResolvedCoworkProfile, str, tuple[Diagnostic, ...]]:
        """Resolve the profile's subagents and skills, check it whole, then render and check its payload."""
        context = self._context.agent
        subagents, subagent_problems = resolve_subagents(model, self._context)
        rules = published_agent_rules(model, context.allowed_models, context.avatar_allowlist)
        skills, dependencies, skill_problems = resolve_skills(model, context)
        unmodeled = unmodeled_key_diagnostics(
            "cowork profile",
            model.name,
            (f"spec.passthrough.{key}" for key in model.passthrough),
            allow=context.allow_unknown_keys,
            subject=model.key,
        )
        resolved = ResolvedCoworkProfile(replace(model, skills=skills), subagents, dependencies)
        payload = render_cowork_profile_json(resolved)
        rendered = (*spec_size(model, payload), *cowork_profile_render_checks(resolved, payload))
        return resolved, payload, (*subagent_problems, *rules, *skill_problems, *unmodeled, *rendered)

    def _compile(self, resolved: ResolvedCoworkProfile, payload: str) -> CompiledCoworkProfile:
        spec = render_cowork_profile_spec(resolved)
        definition_fingerprint = sha256(desired_cowork_profile_definition(resolved.model, spec)).hexdigest()
        return CompiledCoworkProfile(resolved, self._context.target(resolved.model), payload, definition_fingerprint)

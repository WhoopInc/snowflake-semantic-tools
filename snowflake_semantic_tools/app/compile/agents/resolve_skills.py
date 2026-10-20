"""Resolve an agent's `skills:` entries to pinned Cortex Extension versions.

An entry reaches its extension one of three ways. `skill()` and `plugin()` name an
extension this project publishes, and resolve to its published alias. `extension()`
names one this project only consumes, and must pin a version itself. A STAGE source,
a path into a mutable bundle, is kept as authored and reported. A `plugin()` entry that
names no member loads every member: Snowflake refuses a plugin entry without a name, so it
resolves to one named entry per member.

Its diagnostics are reported as each entry resolves against the `AgentCompileContext`, so
they stay with resolution here rather than in `domain.validate`. A CoWork profile pins its
`skills:` entries through the same resolution, as a `SkillOwner`.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Protocol

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext, ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key


class SkillOwner(Protocol):
    """What resolving skills reads of the agent, or CoWork profile, whose `skills:` they are."""

    @property
    def name(self) -> str:
        """Return the owner's name, as written, which each diagnostic names as its artifact."""
        ...

    @property
    def key(self) -> str:
        """Return the owner's artifact key, which each diagnostic names as its subject."""
        ...

    @property
    def origin(self) -> Origin:
        """Return where the owner is declared."""
        ...

    @property
    def skills(self) -> tuple[AgentSkill, ...]:
        """Return the `skills:` entries as authored, in order."""
        ...


def resolve_skills(
    model: SkillOwner,
    context: AgentCompileContext,
) -> tuple[tuple[AgentSkill, ...], tuple[str, ...], tuple[Diagnostic, ...]]:
    """Pin owned extensions to their published alias; check consumed ones are pinned.

    Returns:
        The entries that resolve, in authored order, with the path and version they pin, a
        `plugin()` entry naming no member as one entry per member, in the plugin's order;
        the artifact keys of the owned extensions they pin, first reference first, each
        once; and every diagnostic, in authored order.

    Diagnostics:
        SST-VAL539: an entry's source is a STAGE path into a mutable bundle; the entry is kept.
        SST-VAL839: an entry pins `var('sha_version')`, which names no published version.
        SST-VAL856: an entry names a declared extension that has no version to pin.
        SST-REF032: `skill()` names a skill this project does not declare.
        SST-REF036: `plugin()` names a plugin this project does not declare.
        SST-VAL838: an owned extension's entry sets a version, which SST pins itself.
        SST-VAL540: a `skill()` entry writes a name; it is named after the skill.
        SST-VAL840: a `plugin()` entry's name is not a member of the plugin.
        SST-REF037: `extension()` names an extension this project publishes.
        SST-REF013: `extension()` names no entry of the `extensions:` block.
        SST-VAL538: a consumed extension's entry pins no version, or pins LIVE; a `var()`
            version that `vars:` does not declare pins none.
        SST-VAL541: a resolved entry contributes a member name an earlier entry contributed.
    """
    diagnostics: list[Diagnostic] = []
    resolved: list[AgentSkill] = []
    dependencies: list[str] = []
    contributors: dict[str, str] = {}
    for skill in model.skills:
        pinned, dependency = _resolve_skill(model, skill, context, diagnostics)
        if pinned:
            resolved.extend(pinned)
            diagnostics.extend(_shadowed(model, skill, context, contributors))
        if dependency is not None:
            dependencies.append(dependency)
    return tuple(resolved), tuple(dict.fromkeys(dependencies)), tuple(diagnostics)


def _shadowed(
    model: SkillOwner, skill: AgentSkill, context: AgentCompileContext, contributors: dict[str, str]
) -> list[Diagnostic]:
    """Claim the member names one resolved entry contributes, reporting each an earlier entry claimed.

    A named entry contributes its name, and a `skill()` entry the skill's; an unnamed plugin
    entry, every member of the plugin.
    """
    label = f"{skill.ref}('{skill.path}')" if not skill.name else skill.name
    pin = context.plugins.get(skill.path) if skill.ref == "plugin" and not skill.name else None
    if skill.name:
        names: tuple[str, ...] = (skill.name,)
    elif skill.ref == "skill":
        names = (skill.path,)
    else:
        names = pin.members if pin is not None else ()
    found: list[Diagnostic] = []
    for name in names:
        earlier = contributors.get(name)
        if earlier is not None:
            found.append(D("SST-VAL541", artifact=model.name, a=earlier, b=label, name=name, subject=model.key))
        contributors[name] = label
    return found


def _resolve_skill(
    model: SkillOwner,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> tuple[tuple[AgentSkill, ...], str | None]:
    """Resolve one entry, appending what it reports; return the entries it pins, and the key of its pin.

    The STAGE and `sha_version` checks come first, so they apply whichever function the
    entry's path uses. An entry that does not resolve pins none.
    """
    if skill.source_type == "STAGE":
        diagnostics.append(D("SST-VAL539", artifact=model.name, name=skill.name or skill.path, subject=model.key))
        return (skill,), None
    if skill.version_var == "sha_version":
        diagnostics.append(D("SST-VAL839", artifact=model.name, name=skill.name or skill.path, subject=model.key))
        return (), None
    if skill.ref in ("skill", "plugin"):
        return _pin_owned(model, skill, context, diagnostics)
    consumed = _pin_consumed(model, skill, context, diagnostics)
    return ((consumed,) if consumed is not None else ()), None


def _pin_owned(
    model: SkillOwner,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> tuple[tuple[AgentSkill, ...], str | None]:
    """Pin a `skill()` or `plugin()` entry to the extension's published target and alias.

    A `skill()` entry is named after the skill it names. A `plugin()` entry naming no member
    becomes one entry per member, each named after it; a plugin with no members keeps the
    one entry, unnamed.
    """
    pin = (context.skills if skill.ref == "skill" else context.plugins).get(skill.path)
    if pin is None:
        diagnostics.append(_unpublished_or_undeclared(model, skill, context))
        return (), None
    problem = _owned_entry_problem(model, skill, pin)
    if problem is not None:
        diagnostics.append(problem)
        return (), None
    if skill.ref == "skill":
        names: tuple[str, ...] = (skill.path,)
    elif skill.name:
        names = (skill.name,)
    else:
        names = pin.members or ("",)
    return tuple(replace(skill, name=name, path=pin.target.sql, version=pin.alias) for name in names), pin.key


def _unpublished_or_undeclared(model: SkillOwner, skill: AgentSkill, context: AgentCompileContext) -> Diagnostic:
    reason = context.unpublished.get(artifact_key(skill.ref, skill.path))
    if reason is not None:
        return D(
            "SST-VAL856",
            artifact=model.name,
            kind=skill.ref,
            name=skill.path,
            reason=reason,
            subject=model.key,
            origin=model.origin,
        )
    code = "SST-REF032" if skill.ref == "skill" else "SST-REF036"
    return D(code, name=skill.path, subject=model.key, origin=model.origin)


def _owned_entry_problem(model: SkillOwner, skill: AgentSkill, pin: ExtensionPin) -> Diagnostic | None:
    """What keeps an owned entry from pinning: a version of its own, a name on a skill, or a wrong name."""
    if skill.version or skill.version_var:
        return D(
            "SST-VAL838",
            artifact=model.name,
            name=skill.name or skill.path,
            kind=skill.ref,
            path=skill.path,
            subject=model.key,
        )
    if skill.ref == "skill" and skill.name:
        return D("SST-VAL540", artifact=model.name, path=skill.path, name=skill.name, subject=model.key)
    if skill.name and skill.name not in pin.members:
        expected = "one of the plugin's members: " + ", ".join(pin.members)
        return D("SST-VAL840", artifact=model.name, name=skill.name, expected=expected, subject=model.key)
    return None


def _pin_consumed(
    model: SkillOwner,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> AgentSkill | None:
    """Pin an `extension()` entry to its `extensions:` target and the version it names."""
    label = skill.name or skill.path
    owned = _owned_kind(skill.path, context)
    if owned is not None:
        diagnostics.append(D("SST-REF037", artifact=model.name, name=skill.path, kind=owned, subject=model.key))
        return None
    target = context.extensions.get(skill.path.casefold())
    if target is None:
        reason = context.unpublished.get(f"extension:{skill.path.casefold()}")
        if reason is not None:
            diagnostics.append(
                D(
                    "SST-VAL856",
                    artifact=model.name,
                    kind="extension",
                    name=skill.path,
                    reason=reason,
                    subject=model.key,
                    origin=model.origin,
                )
            )
        else:
            diagnostics.append(D("SST-REF013", name=skill.path or label, subject=model.key))
        return None
    version = context.variables.get(skill.version_var, "") if skill.version_var else skill.version
    if not version or version.upper() == "LIVE":
        diagnostics.append(D("SST-VAL538", artifact=model.name, name=label, subject=model.key))
        return None
    return replace(skill, path=target.sql, version=version)


def _owned_kind(path: str, context: AgentCompileContext) -> str | None:
    """Return `skill` or `plugin` when this project declares an extension named `path`, else None.

    A declared extension counts even when it has no version to pin, so `extension()` on it
    is still reported as a reference to an owned extension.
    """
    return next(
        (
            kind
            for kind, pins in (("skill", context.skills), ("plugin", context.plugins))
            if path in pins or artifact_key(kind, path) in context.unpublished
        ),
        None,
    )

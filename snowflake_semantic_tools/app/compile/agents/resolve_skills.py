"""Resolve an agent's `skills:` entries to pinned Cortex Extension versions.

An entry reaches its extension one of three ways. `skill()` and `plugin()` name an
extension this project publishes, and resolve to its published alias. `extension()`
names one this project only consumes, and must pin a version itself. A STAGE source,
a path into a mutable bundle, is kept as authored and reported.

Its diagnostics are reported as each entry resolves against the `AgentCompileContext`, so
they stay with resolution here rather than in `domain.validate`.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext, ExtensionPin
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentSkill
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key


def resolve_skills(
    model: AgentModel,
    context: AgentCompileContext,
) -> tuple[tuple[AgentSkill, ...], tuple[str, ...], tuple[Diagnostic, ...]]:
    """Pin owned extensions to their published alias; check consumed ones are pinned.

    Returns:
        The entries that resolve, in authored order, with the path and version they pin;
        the artifact keys of the owned extensions they pin, first reference first, each
        once; and every diagnostic, in authored order.

    Diagnostics:
        SST-VAL539: an entry's source is a STAGE path into a mutable bundle; the entry is kept.
        SST-VAL839: an entry pins `var('sha_version')`, which names no published version.
        SST-VAL856: an entry names a declared extension that has no version to pin.
        SST-REF032: `skill()` names a skill this project does not declare.
        SST-REF036: `plugin()` names a plugin this project does not declare.
        SST-VAL838: an owned extension's entry sets a version, which SST pins itself.
        SST-VAL540: a `skill()` entry has no name.
        SST-VAL840: an entry's name is not the skill, or not a member of the plugin.
        SST-REF037: `extension()` names an extension this project publishes.
        SST-REF013: `extension()` names no `skills.extensions` entry.
        SST-VAL538: a consumed extension's entry pins no version, or pins LIVE.
    """
    diagnostics: list[Diagnostic] = []
    resolved: list[AgentSkill] = []
    dependencies: list[str] = []
    for skill in model.skills:
        pinned, dependency = _resolve_skill(model, skill, context, diagnostics)
        if pinned is not None:
            resolved.append(pinned)
        if dependency is not None:
            dependencies.append(dependency)
    return tuple(resolved), tuple(dict.fromkeys(dependencies)), tuple(diagnostics)


def _resolve_skill(
    model: AgentModel,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> tuple[AgentSkill | None, str | None]:
    """Resolve one entry, appending what it reports; return it pinned, and the key of its pin.

    The STAGE and `sha_version` checks come first, so they apply whichever function the
    entry's path uses.
    """
    if skill.source_type == "STAGE":
        diagnostics.append(D("SST-VAL539", artifact=model.name, name=skill.name or skill.path, subject=model.key))
        return skill, None
    if skill.version_var == "sha_version":
        diagnostics.append(D("SST-VAL839", artifact=model.name, name=skill.name or skill.path, subject=model.key))
        return None, None
    if skill.ref in ("skill", "plugin"):
        return _pin_owned(model, skill, context, diagnostics)
    return _pin_consumed(model, skill, context, diagnostics), None


def _pin_owned(
    model: AgentModel,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> tuple[AgentSkill | None, str | None]:
    """Pin a `skill()` or `plugin()` entry to the extension's published target and alias."""
    pin = (context.skills if skill.ref == "skill" else context.plugins).get(skill.path)
    if pin is None:
        diagnostics.append(_unpublished_or_undeclared(model, skill, context))
        return None, None
    problem = _owned_entry_problem(model, skill, pin)
    if problem is not None:
        diagnostics.append(problem)
        return None, None
    return replace(skill, path=pin.target.sql, version=pin.alias), pin.key


def _unpublished_or_undeclared(model: AgentModel, skill: AgentSkill, context: AgentCompileContext) -> Diagnostic:
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


def _owned_entry_problem(model: AgentModel, skill: AgentSkill, pin: ExtensionPin) -> Diagnostic | None:
    """What keeps an owned entry from pinning: a version of its own, no name, or the wrong name."""
    if skill.version or skill.version_var:
        return D(
            "SST-VAL838",
            artifact=model.name,
            name=skill.name or skill.path,
            kind=skill.ref,
            path=skill.path,
            subject=model.key,
        )
    if skill.ref == "skill" and not skill.name:
        return D("SST-VAL540", artifact=model.name, path=skill.path, subject=model.key)
    if skill.name and skill.name not in pin.members:
        expected = (
            f"'{skill.path}'" if skill.ref == "skill" else "one of the plugin's members: " + ", ".join(pin.members)
        )
        return D("SST-VAL840", artifact=model.name, name=skill.name, expected=expected, subject=model.key)
    return None


def _pin_consumed(
    model: AgentModel,
    skill: AgentSkill,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
) -> AgentSkill | None:
    """Pin an `extension()` entry to its `skills.extensions` target and the version it names."""
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

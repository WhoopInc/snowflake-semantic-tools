"""Check Desktop profiles against the skills, plugins, commands, hooks, and MCP configs they name.

Rules run in a fixed order, which is the order diagnostics are reported in: the shared
layer's references, then each profile's name and references, then literal credentials in
MCP configs, then staged file names. A hook, MCP config, or command that failed to load
already carries its own error, so a profile naming it is not also told it is unknown.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Container, Iterable, Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.profile.model import (
    SHARED_PROFILE,
    DesktopProfile,
    McpConfig,
    ProfileCatalog,
    SharedProfile,
)
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill
from snowflake_semantic_tools.domain.model.stage_path import ALLOWED_DESCRIPTION, unsafe_segment
from snowflake_semantic_tools.domain.model.validation import PROFILE_NAMES, Emitter

_PLACEHOLDER = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*\}$")
_CREDENTIAL_KEY = re.compile(r"(?i)(token|secret|password|passwd|api[_-]?key|private[_-]?key)")


@dataclass(frozen=True, slots=True)
class _Known:
    """What the catalog defines, by name, and the subjects whose loader error explains a miss."""

    skills: Mapping[str, Skill]
    plugins: Mapping[str, Plugin]
    commands: frozenset[str]
    hooks: frozenset[str]
    configs: Mapping[str, McpConfig]
    shared_skills: frozenset[str]
    shared_commands: frozenset[str]
    broken: frozenset[str | None]


@dataclass(frozen=True, slots=True)
class _Names:
    """One list of names a profile draws from, and how to check each name in it.

    Attributes:
        code: The diagnostic for a name nothing defines.
        loaded_as: The artifact kind whose loader error, under the same name, already explains
            the miss; None when no loader error can.
        on_defined: More checks for a name that is defined, run in place so the diagnostics
            keep the list's order.
    """

    code: str
    names: tuple[str, ...]
    defined: Container[str]
    loaded_as: str | None = None
    on_defined: Callable[[str], None] | None = None


def validate_profile_catalog(
    catalog: ProfileCatalog,
    skills: Mapping[str, Skill],
    plugins: Mapping[str, Plugin] | None = None,
) -> DiagnosticBag:
    """Check what every profile names, then MCP credentials and staged file names.

    The loader's diagnostics come first, unchanged.

    Diagnostics:
        SST-VAL844: the shared layer or a profile names a skill the project does not have.
        SST-VAL858: the shared layer or a profile names a command that is not defined.
        SST-VAL801: a profile's name is not lowercase letters, digits, '-' or '_', or is 'shared'.
        SST-VAL845: a profile repeats a skill or command the shared layer already carries.
        SST-VAL860: a profile names a plugin that is not defined.
        SST-VAL846: a profile names a hook that is not defined.
        SST-VAL847: a profile names an MCP config that is not defined.
        SST-VAL848: an MCP server a profile uses is not a configuration object.
        SST-VAL849: two MCP configs a profile uses define one server.
        SST-VAL850: an MCP config sets a credential-like key to a literal value.
        SST-VAL857: a hook script or command file has a path a stage rejects.
    """
    shared = catalog.shared
    known = _Known(
        skills=skills,
        plugins=plugins or {},
        commands=frozenset(command.name for command in catalog.commands),
        hooks=frozenset(hook.name for hook in catalog.hooks),
        configs={config.name: config for config in catalog.mcp_configs},
        shared_skills=frozenset(shared.skills if shared is not None else ()),
        shared_commands=frozenset(shared.commands if shared is not None else ()),
        broken=frozenset(item.subject for item in catalog.diagnostics if item.severity is Severity.ERROR),
    )
    diagnostics: list[Diagnostic] = [*catalog.diagnostics, *_shared_rules(shared, known)]
    for profile in catalog.profiles:
        diagnostics.extend(_profile_rules(profile, known))
    diagnostics.extend(_credential_rules(catalog.mcp_configs))
    diagnostics.extend(_staged_path_rules(catalog))
    return DiagnosticBag(diagnostics)


def _shared_rules(shared: SharedProfile | None, known: _Known) -> tuple[Diagnostic, ...]:
    """Check what `shared/` names: its skills, then its commands."""
    if shared is None:
        return ()
    emit = Emitter(subject=artifact_key("profile", SHARED_PROFILE), origin=shared.origin, artifact=SHARED_PROFILE)
    _report_unknown(
        emit,
        (
            _Names("SST-VAL844", shared.skills, known.skills),
            _Names("SST-VAL858", shared.commands, known.commands, "command"),
        ),
        known.broken,
    )
    return emit.diagnostics


def _profile_rules(profile: DesktopProfile, known: _Known) -> tuple[Diagnostic, ...]:
    """Check one profile: its name, then its skills, commands, plugins, hooks, and MCP configs."""
    emit = Emitter(subject=profile.key, origin=profile.origin, artifact=profile.name)
    problem = PROFILE_NAMES.problem(profile.name)
    if problem is not None:
        emit("SST-VAL801", artifact=profile.key, detail=f"profile name '{profile.name}' must be {problem}")
    lists = (
        _Names("SST-VAL844", profile.skills, known.skills, None, _shared_repeat(emit, "skill", known.shared_skills)),
        _Names(
            "SST-VAL858",
            profile.commands,
            known.commands,
            "command",
            _shared_repeat(emit, "command", known.shared_commands),
        ),
        _Names("SST-VAL860", profile.plugins, known.plugins),
        _Names("SST-VAL846", profile.hooks, known.hooks, "hook"),
        _Names("SST-VAL847", profile.mcp_servers, known.configs, "mcp", _server_check(emit, known.configs)),
    )
    _report_unknown(emit, lists, known.broken)
    return emit.diagnostics


def _report_unknown(emit: Emitter, lists: Iterable[_Names], broken: Container[str | None]) -> None:
    """Report each name no list defines, list by list and in each list's order.

    A defined name gets its list's further checks instead, in the same place.
    """
    for names in lists:
        for name in names.names:
            if name in names.defined:
                if names.on_defined is not None:
                    names.on_defined(name)
            elif names.loaded_as is None or artifact_key(names.loaded_as, name) not in broken:
                emit(names.code, name=name)


def _shared_repeat(emit: Emitter, kind: str, shared: Container[str]) -> Callable[[str], None]:
    """Build the check that warns when a profile names what the shared layer already carries."""

    def check(name: str) -> None:
        if name in shared:
            emit("SST-VAL845", kind=kind, name=name)

    return check


def _server_check(emit: Emitter, configs: Mapping[str, McpConfig]) -> Callable[[str], None]:
    """Build the check for the MCP configs one profile names, in the order it names them.

    A server another config already defined is reported against the config that defined it last.
    """
    servers: dict[str, str] = {}

    def check(name: str) -> None:
        config = configs[name]
        for server, value in config.servers.items():
            if not isinstance(value, Mapping):
                emit("SST-VAL848", origin=config.origin, name=server)
            other = servers.get(server)
            if other is not None:
                emit("SST-VAL849", origin=config.origin, name=server, a=other, b=config.name)
            servers[server] = config.name

    return check


def _credential_rules(configs: Iterable[McpConfig]) -> list[Diagnostic]:
    return [
        D(
            "SST-VAL850",
            origin=config.origin,
            subject=artifact_key("mcp", config.name),
            artifact=config.name,
            name=server,
            key=key,
        )
        for config in configs
        for server, key in _literal_credentials(config.servers)
    ]


def _literal_credentials(servers: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    """Find (server, key) for each string set literally under a credential-like key, at any depth.

    A list's items are judged by the key that holds the list. An empty string or a `${VAR}`
    placeholder is not a literal credential.
    """
    found: list[tuple[str, str]] = []

    def walk(server: str, value: object, key: str) -> None:
        if isinstance(value, Mapping):
            for child_key, child in value.items():
                walk(server, child, str(child_key))
        elif isinstance(value, list):
            for item in value:
                walk(server, item, key)
        elif isinstance(value, str) and _CREDENTIAL_KEY.search(key) and value and not _PLACEHOLDER.fullmatch(value):
            found.append((server, key))

    for server, value in servers.items():
        walk(server, value, "")
    return tuple(found)


def _staged_path_rules(catalog: ProfileCatalog) -> list[Diagnostic]:
    """Report each hook script, then each command file, whose path a stage would reject."""
    staged = (
        *((artifact_key("hook", hook.name), hook.origin, hook.tree_path) for hook in catalog.hooks),
        *((command.key, command.origin, command.path) for command in catalog.commands),
    )
    return [
        D(
            "SST-VAL857",
            origin=origin,
            subject=subject,
            artifact=subject,
            value=path,
            found=unsafe,
            expected=ALLOWED_DESCRIPTION,
        )
        for subject, origin, path in staged
        if (unsafe := unsafe_segment(path)) is not None
    ]


def unreached_skills(
    skills: Mapping[str, Skill],
    catalog: ProfileCatalog,
    *,
    catalog_channel: bool,
    plugins: Mapping[str, Plugin] | None = None,
) -> tuple[Diagnostic, ...]:
    """Report each skill no channel publishes, when the catalog channel is off.

    A skill reaches Desktop through the shared layer, a profile, or a plugin a profile names.

    Diagnostics:
        SST-VAL830: a skill no channel publishes, in name order.
    """
    if catalog_channel:
        return ()
    plugins = plugins or {}
    reached = set(catalog.shared.skills) if catalog.shared is not None else set()
    for profile in catalog.profiles:
        reached.update(profile.skills)
        reached.update(member for name in profile.plugins if name in plugins for member in plugins[name].members)
    return tuple(
        D("SST-VAL830", origin=skill.origin, subject=skill.key, artifact=skill.name)
        for name, skill in sorted(skills.items())
        if name not in reached
    )

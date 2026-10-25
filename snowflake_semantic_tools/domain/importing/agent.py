"""Map a live agent or CoWork profile back to the files that would publish it.

The inverse of `domain.render.agent.render_agent_spec` and
`domain.render.cowork_profile.render_cowork_profile_spec`. A spec's settings become the
artifact's own `config:` where they differ from what its folder inherits; multi-line
instructions become files read with `{{ file() }}`; each physical name a tool, skill or
subagent holds becomes the project's reference to what publishes it, or a new tool member
that references it. What SST cannot write as Snowflake holds it is reported (SST-IMP013), and
what the project does not compile (SST-IMP012) is listed as a dependency to import too.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.model import Dependency, ImportedFile, ImportedObject, artifact_name
from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.references import ProjectIndex
from snowflake_semantic_tools.domain.model.agent import BUILTIN_AGENT_TOOLS
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

# The spec sections SST renders; any other top-level key is kept in `spec.passthrough`.
_RENDERED_SECTIONS = frozenset(("models", "orchestration", "instructions", "tools", "tool_resources", "skills"))
_INSTRUCTION_KEYS = frozenset(("orchestration", "response", "sample_questions"))
_TOOL_SPEC_KEYS = frozenset(("type", "name", "description", "input_schema"))
_ROUTER_TYPE = "agent_router"
# The orchestration settings an agent models, by where its spec holds each.
_ORCHESTRATION_SETTINGS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("budget_seconds", ("budget", "seconds")),
    ("budget_tokens", ("budget", "tokens")),
    ("tool_not_accessible", ("tool_not_accessible",)),
    ("analytical_search", ("capabilities", "analytical_search")),
)
_ORCHESTRATION_KEYS = frozenset(("budget", "tool_not_accessible", "capabilities"))
# Search resources a tool writes as its own keys; the rest come from its member or are kept.
# A tool's id and title columns are `ref()` calls (SST-REF050); its member declares them instead.
_SEARCH_KEYS = ("max_results", "filter", "stage_path", "relative_path_column")
_MEMBER_KEYS = ("id_column", "title_column", "columns_and_descriptions")
_SEARCH_RESOURCES = frozenset(("search_service", "execution_environment", *_SEARCH_KEYS, *_MEMBER_KEYS))
_GENERIC_RESOURCES = frozenset(("identifier", "type", "execution_environment"))


@dataclass(frozen=True, slots=True)
class LiveAgent:
    """An agent or CoWork profile as Snowflake shows it.

    Attributes:
        name: The object.
        comment: Its comment without SST's marker; None when it has none.
        profile: Its `SHOW AGENTS` profile, parsed; empty when it sets none.
        secure: Its `is_secure`; None when the role cannot see it.
        spec: The served version's specification, parsed.
        alias: The served version's alias; None when it carries none.
    """

    name: QualifiedName
    comment: str | None
    profile: Mapping[str, object]
    secure: bool | None
    spec: Mapping[str, object]
    alias: str | None = None


@dataclass(frozen=True, slots=True)
class AgentPlacement:
    """Where an agent's files go, and what they inherit there.

    Attributes:
        root: The type's directory, `project.agents_dir` or `project.cowork_profiles_dir`.
        tools_root: `project.tools_dir`, where a new referencing tool member goes.
        destination: The folder below `root`.
        inherited: Each setting the folder's routes give an artifact there, rendered.
    """

    root: str
    tools_root: str
    destination: Destination
    inherited: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


def is_cowork_profile(spec: Mapping[str, object]) -> bool:
    """Report whether a spec is a CoWork profile's: its tools include an `agent_router`."""
    return any(_tool_spec(tool).get("type") == _ROUTER_TYPE for tool in _list(spec.get("tools")))


class _Mapping:
    """One object's mapping: the files it adds and what it reports, shared by its parts."""

    def __init__(self, live: LiveAgent, index: ProjectIndex, placement: AgentPlacement, artifact_type: str) -> None:
        self.live = live
        self.index = index
        self.placement = placement
        self.artifact_type = artifact_type
        self.name = artifact_name(live.name)
        self.key = f"{artifact_type}:{self.name}"
        self.folder = posixpath.join(placement.root, *placement.destination.folder, file_name(self.name))
        self.files: list[ImportedFile] = []
        self.members: dict[str, ImportedFile] = {}
        self.dependencies: list[Dependency] = []
        self.diagnostics: list[Diagnostic] = []

    def differs(self, detail: str) -> None:
        self.diagnostics.append(D("SST-IMP013", subject=self.key, artifact=self.key, detail=detail))

    def unresolved(self, kind: str, name: str, written: str) -> None:
        self.diagnostics.append(
            D("SST-IMP012", subject=self.key, artifact=self.key, type=kind, value=name, found=written)
        )
        self.dependencies.append(Dependency(kind, name))

    def text(self, key: str, value: str) -> str:
        """Return `value` as written in the file: inline, or a file of its own when it spans lines."""
        if "\n" not in value.strip():
            return value
        path = f"instructions/{key}.md"
        self.files.append(ImportedFile(posixpath.join(self.folder, path), text=value.rstrip("\n") + "\n"))
        return "{{ file('" + path + "') }}"

    def tool_member(self, name: QualifiedName, member_type: str, description: str, extra: Mapping[str, object]) -> str:
        """Return the tool member that references `name`, adding one when the project has none."""
        existing = self.index.tool(name)
        if existing is not None:
            return existing
        member = artifact_name(name)
        if member in self.members:
            return member
        body: dict[str, object] = {"name": member, "type": member_type}
        if description:
            body["description"] = description
        body.update(extra)
        body["relations"] = {self.index.target_name: name.sql}
        folder = posixpath.join(self.placement.tools_root, *self.placement.destination.folder, file_name(member))
        self.members[member] = ImportedFile(posixpath.join(folder, f"{file_name(member)}.yml"), {"tool": body})
        self.differs(
            f"tool member '{member}' references {name.sql} on target {self.index.target_name} only; "
            "add its relation for each other target that compiles the agent"
        )
        return member

    def result(self, document: Mapping[str, object], file_stem: str) -> ImportedObject:
        main = ImportedFile(posixpath.join(self.folder, f"{file_stem}.yml"), document)
        return ImportedObject(
            self.artifact_type,
            self.live.name,
            self.name,
            (main, *self.files, *self.members.values()),
            dependencies=tuple(dict.fromkeys(self.dependencies)),
            diagnostics=tuple(self.diagnostics),
        )


def import_agent(live: LiveAgent, index: ProjectIndex, placement: AgentPlacement) -> ImportedObject:
    """Map a live agent to its `agent.yml`, its instruction files, and any tool member it needs.

    Diagnostics:
        SST-IMP012: a tool, skill or agent the spec names is not compiled by the project.
        SST-IMP013: a property SST writes otherwise, or cannot write: an analyst tool named
            other than its view, a STAGE skill, a setting the folder sets that the agent does not.
    """
    mapping = _Mapping(live, index, placement, "agent")
    document: dict[str, object] = {"name": mapping.name}
    if live.comment:
        document["comment"] = live.comment
    profile = _profile(mapping, live.profile)
    if profile:
        document["profile"] = profile
    config = _location(mapping)
    passthrough: dict[str, object] = {}
    _orchestration(mapping, live.spec, config, passthrough, modelled=True)
    if live.secure is not None and live.secure != bool(placement.inherited.get("secure", False)):
        config["secure"] = live.secure
    _setting(mapping, config, "alias", live.alias)
    if config:
        document["config"] = config
    spec: dict[str, object] = {}
    instructions = _instructions(mapping, live.spec, passthrough)
    if instructions:
        spec["instructions"] = instructions
    resources = _mapping_of(live.spec.get("tool_resources"))
    tools = [entry for tool in _list(live.spec.get("tools")) if (entry := _agent_tool(mapping, tool, resources))]
    if tools:
        spec["tools"] = tools
    skills = _skills(mapping, live.spec)
    if skills:
        spec["skills"] = skills
    passthrough.update({key: value for key, value in live.spec.items() if key not in _RENDERED_SECTIONS})
    if passthrough:
        spec["passthrough"] = passthrough
    if spec:
        document["spec"] = spec
    return mapping.result(document, "agent")


def import_cowork_profile(live: LiveAgent, index: ProjectIndex, placement: AgentPlacement) -> ImportedObject:
    """Map a live CoWork profile to its `cowork_profile.yml` and instruction files.

    Snowflake private preview (PrPr). Each agent the router routes to becomes an `agent()`
    of the project, else a tool member of type `agent` that references it.

    Diagnostics:
        SST-IMP012: an agent the profile routes to, or a skill it loads, is not compiled by the project.
        SST-IMP013: a property SST writes otherwise, or cannot write.
    """
    mapping = _Mapping(live, index, placement, "cowork_profile")
    document: dict[str, object] = {"name": mapping.name}
    if live.comment:
        document["comment"] = live.comment
    display = _profile(mapping, live.profile)
    if display:
        document["display"] = display
    config = _location(mapping)
    passthrough: dict[str, object] = {}
    _orchestration(mapping, live.spec, config, passthrough, modelled=False)
    _setting(mapping, config, "alias", live.alias)
    if config:
        document["config"] = config
    spec: dict[str, object] = {}
    instructions = _instructions(mapping, live.spec, passthrough)
    if instructions:
        spec["instructions"] = instructions
    resources = _mapping_of(live.spec.get("tool_resources"))
    subagents: list[object] = []
    tools: list[object] = []
    for tool in _list(live.spec.get("tools")):
        tool_spec = _tool_spec(tool)
        tool_type = str(tool_spec.get("type") or "")
        tool_name = str(tool_spec.get("name") or tool_type)
        if tool_type == _ROUTER_TYPE:
            router = _mapping_of(resources.get(tool_name))
            subagents.extend(_subagent(mapping, entry) for entry in _list(router.get("subagents")))
        elif tool_type in BUILTIN_AGENT_TOOLS:
            tools.append({"type": tool_type, **({"name": tool_name} if tool_name != tool_type else {})})
        else:
            mapping.differs(f"tool '{tool_name}' of type '{tool_type}' is not one a profile renders; it was left out")
    if subagents:
        spec["subagents"] = subagents
    if tools:
        spec["tools"] = tools
    skills = _skills(mapping, live.spec)
    if skills:
        spec["skills"] = skills
    passthrough.update({key: value for key, value in live.spec.items() if key not in _RENDERED_SECTIONS})
    if passthrough:
        spec["passthrough"] = passthrough
    if spec:
        document["spec"] = spec
    return mapping.result(document, "cowork_profile")


def _profile(mapping: _Mapping, profile: Mapping[str, object]) -> dict[str, object]:
    shown = {key: profile[key] for key in ("display_name", "avatar", "color") if profile.get(key)}
    for key in sorted(set(profile) - {"display_name", "avatar", "color"}):
        mapping.differs(f"profile key '{key}' is not one SST sets; it was left out")
    return shown


def _location(mapping: _Mapping) -> dict[str, object]:
    """Start the file's `config:`: the object's database and schema, when no folder routes there."""
    if not mapping.placement.destination.overridden:
        return {}
    database, schema, _ = mapping.live.name.folded
    return {"database": database, "schema": schema}


def _orchestration(
    mapping: _Mapping,
    spec: Mapping[str, object],
    config: dict[str, object],
    passthrough: dict[str, object],
    *,
    modelled: bool,
) -> None:
    """Write the orchestration model and settings into `config`, or the sections into `passthrough`."""
    models = _mapping_of(spec.get("models"))
    if set(models) - {"orchestration"}:
        passthrough["models"] = dict(models)
        mapping.differs("`models` holds keys SST does not model; the whole section is kept in spec.passthrough")
    _setting(mapping, config, "orchestration_model", models.get("orchestration"))
    orchestration = _mapping_of(spec.get("orchestration"))
    unmodelled = set(orchestration) - _ORCHESTRATION_KEYS if modelled else set(orchestration)
    if unmodelled:
        passthrough["orchestration"] = dict(orchestration)
        mapping.differs(
            f"`orchestration` holds {', '.join(sorted(unmodelled))}, which SST does not model for "
            f"a {mapping.artifact_type}; the whole section is kept in spec.passthrough"
        )
        return
    if not modelled:
        return
    for key, path in _ORCHESTRATION_SETTINGS:
        _setting(mapping, config, key, _at(orchestration, path))


def _setting(mapping: _Mapping, config: dict[str, object], key: str, live: object) -> None:
    """Write `key` into `config` when the live value differs from the inherited one."""
    inherited = mapping.placement.inherited.get(key)
    if live is None:
        if inherited is not None:
            mapping.differs(
                f"its folder sets {key} {inherited}, which the live {mapping.artifact_type} does not set; "
                "the next apply sets it"
            )
        return
    if inherited is None or str(inherited).casefold() != str(live).casefold():
        config[key] = live


def _instructions(mapping: _Mapping, spec: Mapping[str, object], passthrough: dict[str, object]) -> dict[str, object]:
    """Map the instructions, each long one to a file; a section SST does not model goes in passthrough."""
    instructions = _mapping_of(spec.get("instructions"))
    if not instructions:
        return {}
    if set(instructions) - _INSTRUCTION_KEYS:
        passthrough["instructions"] = dict(instructions)
        mapping.differs("`instructions` holds keys SST does not model; the whole section is kept in spec.passthrough")
        return {}
    written: dict[str, object] = {}
    for key in ("orchestration", "response"):
        value = instructions.get(key)
        if isinstance(value, str) and value:
            written[key] = mapping.text(key, value)
    questions = []
    for entry in _list(instructions.get("sample_questions")):
        question = entry.get("question") if isinstance(entry, Mapping) else entry
        if isinstance(question, str) and question:
            questions.append({"question": question})
        if isinstance(entry, Mapping) and set(entry) - {"question"}:
            mapping.differs("a sample question holds keys besides `question`; only the question was kept")
    if questions:
        written["sample_questions"] = questions
    return written


def _agent_tool(mapping: _Mapping, tool: object, resources: Mapping[str, object]) -> dict[str, object] | None:
    """Map one `tools` entry, with its resources, to the tool an agent file declares."""
    tool_spec = _tool_spec(tool)
    tool_type = str(tool_spec.get("type") or "")
    name = str(tool_spec.get("name") or tool_type)
    resource = _mapping_of(resources.get(name))
    entry: dict[str, object] = {}
    if tool_type == "cortex_analyst_text_to_sql":
        entry = _analyst(mapping, name, tool_spec, resource)
    elif tool_type == "cortex_search":
        entry = _search(mapping, name, tool_spec, resource)
    elif tool_type == "generic":
        entry = _generic(mapping, name, tool_spec, resource)
    elif tool_type == "agent_toolset":
        entry = _toolset(mapping, name, tool_spec, resource)
    elif tool_type in BUILTIN_AGENT_TOOLS:
        entry = {"type": tool_type, **({"name": name} if name != tool_type else {})}
        if tool_spec.get("description"):
            entry["description"] = tool_spec["description"]
        if resource:
            mapping.differs(f"built-in tool '{name}' has resources, which SST does not render; they were left out")
    else:
        mapping.differs(f"tool '{name}' of type '{tool_type}' is not one SST renders; it was left out")
        return None
    extra = {key: value for key, value in tool_spec.items() if key not in _TOOL_SPEC_KEYS}
    if extra:
        entry["tool_spec_passthrough"] = extra
    return entry


def _analyst(
    mapping: _Mapping, name: str, tool_spec: Mapping[str, object], resource: Mapping[str, object]
) -> dict[str, object]:
    """Map a Cortex Analyst tool to the `semantic_view()` it queries; SST names it after the view."""
    entry: dict[str, object] = {"type": "cortex_analyst_text_to_sql"}
    view = _qualified(resource.get("semantic_view"))
    if view is None:
        mapping.differs(f"Cortex Analyst tool '{name}' names no semantic view; written with none")
    else:
        placed = mapping.index.artifact("semantic_view", view)
        view_name = placed.name if placed is not None else artifact_name(view)
        reference = "{{ semantic_view('" + view_name + "') }}"
        entry["semantic_view"] = reference
        if placed is None:
            mapping.unresolved("semantic_view", view.sql, reference)
        if name.casefold() != view.name.folded.casefold():
            mapping.differs(
                f"Cortex Analyst tool '{name}' is named after its view, {view.name.folded}, as SST names "
                "every analyst tool; instructions that name the tool need the new name"
            )
    _describe(entry, tool_spec)
    _environment(mapping, entry, name, resource)
    if tool_spec.get("input_schema"):
        mapping.differs(f"Cortex Analyst tool '{name}' has an input schema, which SST does not render; it was left out")
    extra = sorted(set(resource) - {"semantic_view", "execution_environment"})
    if extra:
        mapping.differs(f"Cortex Analyst tool '{name}' has resources SST does not render: {', '.join(extra)}")
    return entry


def _search(
    mapping: _Mapping, name: str, tool_spec: Mapping[str, object], resource: Mapping[str, object]
) -> dict[str, object]:
    """Map a Cortex Search tool; its columns belong to the member, which a new one declares."""
    entry: dict[str, object] = {"type": "cortex_search", "name": name}
    service = _qualified(resource.get("search_service"))
    if service is None:
        mapping.differs(f"Cortex Search tool '{name}' names no search service; written with none")
    else:
        extra: dict[str, object] = {
            key: str(resource[key]).casefold() for key in ("id_column", "title_column") if resource.get(key)
        }
        extra.update(_search_columns(resource.get("columns_and_descriptions")))
        member = mapping.tool_member(service, "cortex_search_service", str(tool_spec.get("description") or ""), extra)
        entry["search_service"] = "{{ tool('" + member + "') }}"
    _describe(entry, tool_spec)
    for key in _SEARCH_KEYS:
        value = resource.get(key)
        if value not in (None, "", {}):
            entry[key] = value
    _environment(mapping, entry, name, resource)
    kept = {key: value for key, value in resource.items() if key not in _SEARCH_RESOURCES}
    if kept:
        entry["passthrough"] = kept
    return entry


def _generic(
    mapping: _Mapping, name: str, tool_spec: Mapping[str, object], resource: Mapping[str, object]
) -> dict[str, object]:
    entry: dict[str, object] = {"type": "generic", "name": name}
    routine = _qualified(resource.get("identifier"))
    if routine is None:
        mapping.differs(f"generic tool '{name}' names no procedure or function; written with none")
    else:
        kind = str(resource.get("type") or "procedure").casefold()
        member = mapping.tool_member(routine, "function" if kind == "function" else "procedure", "", {})
        entry["identifier"] = "{{ tool('" + member + "') }}"
    _describe(entry, tool_spec)
    if isinstance(tool_spec.get("input_schema"), Mapping):
        entry["input_schema"] = tool_spec["input_schema"]
    _environment(mapping, entry, name, resource)
    kept = {key: value for key, value in resource.items() if key not in _GENERIC_RESOURCES}
    if kept:
        entry["passthrough"] = kept
    return entry


def _toolset(
    mapping: _Mapping, name: str, tool_spec: Mapping[str, object], resource: Mapping[str, object]
) -> dict[str, object]:
    entry: dict[str, object] = {"type": "agent_toolset", "name": name}
    other = _qualified(resource.get("agent_name"))
    if other is None:
        mapping.differs(f"agent toolset '{name}' names no agent; written with none")
    else:
        placed = mapping.index.artifact("agent", other)
        if placed is not None:
            entry["agent"] = "{{ agent('" + placed.name + "') }}"
        else:
            member = mapping.tool_member(other, "agent", "", {})
            entry["identifier"] = "{{ tool('" + member + "') }}"
    _describe(entry, tool_spec)
    return entry


def _subagent(mapping: _Mapping, entry: object) -> dict[str, object]:
    subagent = _mapping_of(entry)
    source = _mapping_of(subagent.get("source"))
    fqn = _qualified(source.get("fqn") or subagent.get("name"))
    written: dict[str, object] = {}
    if fqn is None:
        mapping.differs("a subagent names no agent; written with none")
    else:
        placed = mapping.index.artifact("agent", fqn)
        if placed is not None:
            written["agent"] = "{{ agent('" + placed.name + "') }}"
        else:
            member = mapping.tool_member(fqn, "agent", "", {})
            written["agent"] = "{{ tool('" + member + "') }}"
    for key in ("display_name", "description"):
        if subagent.get(key):
            written[key] = subagent[key]
    questions = [str(item) for item in _list(source.get("sample_questions")) if item]
    if questions:
        written["sample_questions"] = questions
    return written


def _skills(mapping: _Mapping, spec: Mapping[str, object]) -> list[object]:
    """Map each skill a spec loads to the project's extension that publishes it."""
    written: list[object] = []
    for entry in _list(spec.get("skills")):
        skill = _mapping_of(entry)
        source = _mapping_of(skill.get("source"))
        source_type = str(source.get("type") or "").upper()
        path = str(source.get("path") or "")
        live_name = str(skill.get("name") or "")
        if source_type == "STAGE":
            written.append(_stage_skill(mapping, live_name, path))
            continue
        extension = _qualified(path)
        if extension is None:
            mapping.differs(f"skill '{live_name or path}' has a source SST cannot read; it was left out")
            continue
        for kind in ("skill", "plugin"):
            placed = mapping.index.artifact(kind, extension)
            if placed is not None:
                item: dict[str, object] = {
                    "source": {"type": "CORTEX_EXTENSION", "path": f"{{{{ {kind}('{placed.name}') }}}}"}
                }
                if kind == "plugin" and live_name:
                    item = {"name": "{{ skill('" + live_name + "') }}", **item}
                written.append(item)
                break
        else:
            reference = "{{ skill('" + (live_name or artifact_name(extension)) + "') }}"
            written.append({"source": {"type": "CORTEX_EXTENSION", "path": reference}})
            mapping.unresolved("skill", extension.sql, reference)
    return written


def _stage_skill(mapping: _Mapping, live_name: str, path: str) -> dict[str, object]:
    """Map a STAGE skill, which SST refuses (SST-VAL539), to the project skill of its folder's name."""
    folder = path.rstrip("/").rsplit("/", 1)[-1]
    candidate = live_name or folder
    reference = "{{ skill('" + candidate + "') }}"
    mapping.differs(
        f"skill '{candidate}' is read from the stage path {path}; it is written as the project's "
        "skill of that name, which publishes as a Cortex Extension"
    )
    if candidate.casefold() not in mapping.index.skill_names:
        mapping.unresolved("skill", path, reference)
    return {"source": {"type": "CORTEX_EXTENSION", "path": reference}}


def _describe(entry: dict[str, object], tool_spec: Mapping[str, object]) -> None:
    description = tool_spec.get("description")
    if isinstance(description, str) and description:
        entry["description"] = description


def _environment(mapping: _Mapping, entry: dict[str, object], name: str, resource: Mapping[str, object]) -> None:
    """Write a tool's warehouse and query timeout where they differ from what the agent inherits."""
    environment = _mapping_of(resource.get("execution_environment"))
    for key in ("warehouse", "query_timeout"):
        live = environment.get(key)
        inherited = mapping.placement.inherited.get(key)
        if live in (None, ""):
            if inherited not in (None, ""):
                mapping.differs(
                    f"tool '{name}' sets no {key}, and the agent's folder sets {inherited}; the next apply sets it"
                )
            continue
        if inherited is None or str(inherited).casefold() != str(live).casefold():
            entry[key] = live


def _search_columns(value: object) -> dict[str, object]:
    """Return a search tool's live columns as its new tool member declares them."""
    columns = _mapping_of(value)
    if not columns:
        return {}
    written = []
    for column, described in columns.items():
        details = _mapping_of(described)
        item: dict[str, object] = {"column": str(column).casefold()}
        for key in ("description", "type", "searchable", "filterable"):
            if details.get(key) is not None:
                item[key] = details[key]
        written.append(item)
    return {"columns": written}


def _tool_spec(tool: object) -> Mapping[str, Any]:
    return _mapping_of(_mapping_of(tool).get("tool_spec"))


def _mapping_of(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _at(value: Mapping[str, Any], path: tuple[str, ...]) -> object:
    current: object = value
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _qualified(value: object) -> QualifiedName | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return QualifiedName.parse(value)
    except ValueError:
        return None

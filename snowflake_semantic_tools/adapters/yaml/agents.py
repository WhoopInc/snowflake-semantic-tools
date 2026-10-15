"""Parse complete agent documents and resolve authored sidecars."""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_list,
    checked_mapping,
    mapping,
    optional_int,
    optional_string,
    read_sidecar,
    refused,
    unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentModel, AgentProfile, AgentSkill, AgentTool
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings, setting_value
from snowflake_semantic_tools.domain.parse.fields import unknown_field
from snowflake_semantic_tools.domain.parse.passthrough import AGENT_SPEC_KEYS, TOOL_SPEC_KEYS, passthrough_diagnostics
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.calls import literal_problem, required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions
from snowflake_semantic_tools.domain.validate.agent_tool import TOOL_KEYS
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics

_Block = TypeVar("_Block", bound=Mapping[Any, Any])


def load_agents(
    project_dir: Path, *, agents_dir: str = "agents", config: Mapping[str, Any] | None = None
) -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
    """Read each `agent.yml` or `agent.yaml` in a folder below `agents_dir` into an agent, in path order.

    An agent's folder may sit in grouping folders, `<agents_dir>/<domain>/<agent>/agent.yml`, which
    the `agents:` folder routes name; the agent records them as its `folder`. A missing
    `agents_dir` holds no agents. A file that cannot be read or parsed, or names no
    agent, contributes its diagnostics and no agent; any other problem is reported and the
    agent kept without what could not be read. Sidecars and eval files resolve against the
    agent's own folder. An agent file that is a symbolic link, or is reached through one, is
    reported and not read, wherever the link points.

    Args:
        config: The run's resolved configuration, whose `agents:` folder routes are checked
            against the folders under `agents_dir`, and whose `agents:` block each agent's
            settings resolve through, below its own `config:`; None checks no route and resolves
            each agent's settings from its `config:` alone. An agent whose `enabled` resolves to
            anything but false is enabled.

    Diagnostics:
        SST-CFG041: when an `agents:` folder route names no folder under `agents_dir`; reported
            first.
        SST-PRT009: when an agent file, or a folder between the project and it, is a symbolic
            link, or it resolves outside the project; the agent is left out.
        SST-LOD004: when a template in a file is malformed.
        SST-LOD006: when a file or an instruction sidecar is not UTF-8.
        SST-LOD001: when a file is not valid YAML.
        SST-LOD005: when a file writes a key twice in one mapping.
        SST-LOD003: when a file holds only whitespace or comments.
        SST-LOD008: when a file holds more than one document.
        SST-LOD002: when a file's root is not a mapping.
        SST-PRS002: when the agent has no `name`, a tool no `type`, a skill no `source.type`, or
            `evals:` no `dataset` or `config`.
        SST-PRS003: when `evals:` is not a mapping, or one of its paths not a non-empty string;
            when `spec.tools`, `spec.skills` or the sample questions are not a list;
            when `spec.instructions`, `meta`, `spec.passthrough` or a tool's mapping field, such
            as `filter`, is not a mapping; or when an `orchestration` or `response` instruction
            is not a string. The agent is kept back, and the field reads as empty.
        SST-PRS004: when a block SST models holds a key it does not model.
        SST-PRS022: when that key is within edit distance of one the block models.
        SST-PRS126: when the agent writes a setting where it is no longer read, outside `config:`.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports the agent's `config:` block.
        SST-PRS018: when a `spec.tools` or `spec.skills` entry has the wrong shape.
        SST-PRS118: when a sample question is not a mapping with a string `question`.
        Every code `required_call` lists, when a reference field, or an instruction holding a
            template, is not one call of the function it takes.
        Every code `resolve_file` lists, for an instruction's sidecar, which is read relative to
            the agent file's folder and never through a symbolic link.
        SST-REF008: when a consumed extension's skill `name` holds a template.
        SST-LOD018: when an agent file cannot be read.
    """
    root = project_dir / agents_dir
    routes = routed_diagnostics(config, project_dir, "agent") if config is not None else ()
    if not root.is_dir():
        return (), DiagnosticBag(routes)
    agents: list[AgentModel] = []
    diagnostics: list[Diagnostic] = list(routes)
    for path in sorted(root.glob("*/**/agent.y*ml")):
        if refused(project_dir, path, diagnostics):
            continue
        relative = path.relative_to(project_dir).as_posix()
        try:
            raw = path.read_bytes()
            parsed = parse_yaml_bytes(raw, relative)
        except ProjectError as exc:
            diagnostics.extend(exc.diagnostics)
            continue
        except OSError:
            diagnostics.append(D("SST-LOD018", file=relative, path=relative, origin=Origin(relative)))
            continue
        agent, problems = _parse_agent(project_dir, relative, dict(parsed.tree), parsed.line_index)
        diagnostics.extend((*parsed.diagnostics, *problems))
        if agent is not None:
            documented = _comment_mentions(raw, parsed.line_index.get(_TOKENS), "orchestration")
            folder = path.parent.relative_to(root).parts
            block = config.get("agents") if config is not None else None
            settings = resolve_settings(block, folder, parsed.tree.get("config"))
            enabled = setting_value(settings, "enabled") is not False
            agents.append(
                replace(agent, budget_tokens_documented=documented, folder=folder, enabled=enabled, settings=settings)
            )
    return tuple(agents), DiagnosticBag(diagnostics)


_TOKENS: NodePath = ("config", "budget_tokens")

# The keys each block of an agent file models, by the block's path; `*` stands for any list index.
# `meta` and the passthrough blocks are free-form, and a tool's own mapping fields are its type's.
_TOOL_KEYS = frozenset(key for keys in TOOL_KEYS.values() for key in keys)
_AGENT_KEYS: Mapping[NodePath, frozenset[str]] = MappingProxyType(
    {
        (): frozenset(("name", "comment", "profile", "spec", "config", "deprecated", "meta", "evals")),
        ("profile",): frozenset(("display_name", "avatar", "color")),
        ("spec",): frozenset(("instructions", "tools", "skills", "passthrough")),
        ("spec", "instructions"): frozenset(("orchestration", "response", "sample_questions")),
        ("spec", "tools", "*"): _TOOL_KEYS,
        ("spec", "skills", "*"): frozenset(("name", "source")),
        ("spec", "skills", "*", "source"): frozenset(("type", "path", "version")),
        # `config:`'s own keys are the `agents:` settings, which `item_config_diagnostics` checks.
        ("config", "tags", "*"): frozenset(("name", "value")),
        ("evals",): frozenset(("dataset", "config")),
    }
)
# Settings an earlier draft read outside `config:`, by the block that held them, with where each
# is set now. Each is an error, so a file still writing one learns it no longer does anything.
_MOVED: Mapping[tuple[NodePath, str], str] = MappingProxyType(
    {
        ((), "alias"): "an agent sets it under config:, as config.alias",
        ((), "secure"): "an agent sets it under config:, as config.secure",
        ((), "enabled"): "an agent sets it under config:, as config.enabled",
        ((), "tags"): "an agent sets it under config:, as config.tags",
        (("spec",), "models"): "an agent sets its model under config:, as config.orchestration_model",
        (("spec",), "orchestration"): (
            "an agent sets these under config:, as config.budget_seconds, config.budget_tokens, "
            "config.tool_not_accessible and config.analytical_search"
        ),
    }
)
# Keys a `spec.tools` entry wrote before columns became references, with where each is written now.
_MOVED_TOOL_KEYS: Mapping[str, str] = MappingProxyType(
    {
        "columns_and_descriptions": (
            "a search tool lists its columns under columns:, each column as its member names its own"
        ),
    }
)


def _unknown_keys(
    relative: str, tree: Mapping[str, Any], lines: Mapping[NodePath, SourcePosition], subject: str
) -> list[Diagnostic]:
    """Report each key an agent file writes in a block SST models and that the block does not model.

    A key is placed where it is written, else at the file's start. A tool's key is checked
    against every key some tool type takes; one of another type's is SST-VAL516's to report.

    Diagnostics:
        SST-PRS126: the key is a setting that moved under `config:`, which the message says.
        SST-PRS022: the key is within edit distance of one the block models, which it names.
        SST-PRS004: otherwise.
    """
    found: list[Diagnostic] = []
    for pattern, known in _AGENT_KEYS.items():
        for path, node in _nodes(tree, pattern):
            for key in unknown_keys(node, known):
                position = lines.get((*path, key))
                origin = Origin(relative, position.line, position.col) if position else Origin(relative, 1, 1)
                label = _label((*path, str(key)))
                moved = _MOVED.get((path, str(key)))
                if len(path) == 3 and path[:2] == ("spec", "tools"):
                    moved = _MOVED_TOOL_KEYS.get(str(key))
                found.append(
                    D("SST-PRS126", origin=origin, subject=subject, artifact=subject, field=label, reason=moved)
                    if moved is not None
                    else unknown_field(str(key), known, label=label, artifact=relative, origin=origin, subject=subject)
                )
    return found


def _nodes(tree: Mapping[str, Any], pattern: NodePath) -> list[tuple[NodePath, Mapping[Any, Any]]]:
    """Return each mapping at `pattern` in `tree`, with its path; `*` matches every index of a list."""
    matched: list[tuple[NodePath, object]] = [((), tree)]
    for part in pattern:
        step: list[tuple[NodePath, object]] = []
        for path, node in matched:
            if part == "*" and isinstance(node, list):
                step.extend(((*path, index), item) for index, item in enumerate(node))
            elif part != "*" and isinstance(node, Mapping) and part in node:
                step.append(((*path, part), node[part]))
        matched = step
    return [(path, node) for path, node in matched if isinstance(node, Mapping)]


def _label(path: NodePath) -> str:
    """Write a node path as a diagnostic names a field: `spec.tools[0].warehouse`."""
    text = ""
    for part in path:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else part)
    return text


def _placed(relative: str, lines: Mapping[NodePath, SourcePosition]) -> Callable[[str], Origin]:
    """Place a key of the agent's `config:` block where it is written, else at the file's start."""

    def origin_of(key: str) -> Origin:
        position = lines.get(("config", key) if key else ("config",))
        return Origin(relative, position.line, position.col) if position is not None else Origin(relative, 1, 1)

    return origin_of


def _comment_mentions(raw: bytes, position: SourcePosition | None, word: str) -> bool:
    """Report whether the comment ending the key's line, or the comment lines just above it, say `word`.

    Comments are not part of the parsed tree, so they are read from the file's text.
    """
    if position is None:
        return False
    lines = raw.decode("utf-8", errors="replace").splitlines()
    index = position.line - 1
    comments = [lines[index].partition("#")[2]] if 0 <= index < len(lines) else []
    while index > 0 and lines[index - 1].strip().startswith("#"):
        index -= 1
        comments.append(lines[index])
    return any(word in comment.casefold() for comment in comments)


def _parse_agent(
    project_dir: Path,
    relative: str,
    tree: Mapping[str, Any],
    lines: Mapping[NodePath, SourcePosition],
) -> tuple[AgentModel | None, tuple[Diagnostic, ...]]:
    """Build one agent, reporting each problem; None, with SST-PRS002, only when it has no name.

    Args:
        lines: Where each node of the agent file starts, which places each tool.
    """
    origin = Origin(relative, 1, 1)
    diagnostics: list[Diagnostic] = []
    name = tree.get("name")
    if not isinstance(name, str) or not name:
        return None, (D("SST-PRS002", artifact=relative, field="name", origin=origin),)
    subject = artifact_key("agent", name.casefold())

    def listed(value: object, field: str) -> list[Any]:
        return checked_list(value, diagnostics, field=field, artifact=relative, origin=origin, subject=subject)

    def mapped(value: object, field: str) -> dict[Any, Any]:
        return checked_mapping(value, diagnostics, field=field, artifact=relative, origin=origin, subject=subject)

    profile_node = mapping(tree.get("profile"))
    spec = mapping(tree.get("spec"))
    instructions = mapped(spec.get("instructions"), "spec.instructions")
    source_files = [relative]
    orchestration_text, response_text = (
        _instruction(
            project_dir,
            relative,
            instructions.get(key),
            source_files,
            diagnostics,
            field=key,
            subject=subject,
        )
        for key in ("orchestration", "response")
    )
    tools = _parse_tools(relative, listed(spec.get("tools"), "spec.tools"), lines, diagnostics, subject)
    skills = tuple(
        skill
        for index, value in enumerate(listed(spec.get("skills"), "spec.skills"))
        if (skill := _parse_skill(relative, index, value, diagnostics, subject)) is not None
    )
    questions = listed(instructions.get("sample_questions"), "spec.instructions.sample_questions")
    sample_questions = _sample_questions(questions, name, origin, diagnostics)
    meta = mapped(tree.get("meta"), "meta")
    passthrough = mapped(spec.get("passthrough"), "spec.passthrough")
    diagnostics.extend(
        passthrough_diagnostics(f"{name}.spec", passthrough, AGENT_SPEC_KEYS, subject=subject, origin=origin)
    )
    eval_files = _parse_eval_files(relative, tree.get("evals"), diagnostics)
    diagnostics.extend(_unknown_keys(relative, tree, lines, subject))
    diagnostics.extend(
        item_config_diagnostics("agents", tree.get("config"), subject=subject, origin_of=_placed(relative, lines))
    )
    return (
        AgentModel(
            name=name,
            origin=origin,
            source_files=tuple(dict.fromkeys(source_files)),
            comment=optional_string(tree.get("comment")),
            profile=AgentProfile(
                optional_string(profile_node.get("display_name")),
                optional_string(profile_node.get("avatar")),
                optional_string(profile_node.get("color")),
            ),
            orchestration_instructions=orchestration_text,
            response_instructions=response_text,
            sample_questions=tuple(sample_questions),
            tools=tools,
            skills=skills,
            meta=MappingProxyType(meta),
            passthrough=MappingProxyType(passthrough),
            evals=eval_files,
            deprecated=bool(tree.get("deprecated", False)),
        ),
        tuple(diagnostics),
    )


def _sample_questions(values: Any, name: str, origin: Origin, diagnostics: list[Diagnostic]) -> list[str]:
    """Each `question` of `instructions.sample_questions`, reporting SST-PRS118 for an entry without one."""
    questions: list[str] = []
    for index, value in enumerate(values or []):
        if not isinstance(value, dict) or not isinstance(value.get("question"), str):
            diagnostics.append(
                D(
                    "SST-PRS118",
                    artifact=artifact_key("agent", name),
                    index=index,
                    origin=origin,
                    subject=artifact_key("agent", name.casefold()),
                )
            )
            continue
        questions.append(str(value["question"]))
    return questions


def _parse_eval_files(source_file: str, value: object, diagnostics: list[Diagnostic]) -> AgentEvalFiles | None:
    """Read an agent's `evals:` block: the dataset and the config it publishes, each by name.

    `dataset:` is one `{{ eval_dataset('<name>') }}` call and `config:` one
    `{{ eval_config('<name>') }}` call; the names resolve among the files in the agent's
    `evals/` folder, which is not read here.

    Returns:
        None when the agent declares no `evals:`; otherwise both names, each None when it is
        absent or not that one call, with the agent's `evals/` folder.

    Diagnostics:
        SST-PRS003: when `evals:` is not a mapping.
        SST-PRS002: when `dataset` or `config` is absent.
        Every code `required_call` lists, for a value that is not one `eval_dataset()` or
            `eval_config()` call.
    """
    if value is None:
        return None
    origin = Origin(source_file)
    folder = posixpath.join(posixpath.dirname(source_file), "evals")
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="evals",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return AgentEvalFiles(origin, folder=folder)
    names: dict[str, str | None] = {}
    for field in ("dataset", "config"):
        raw = value.get(field)
        names[field] = None
        if raw is None:
            diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"evals.{field}", origin=origin))
            continue
        call = _reference(
            raw, f"agent.evals.{field}", diagnostics, origin, field=f"evals.{field}", artifact=source_file
        )
        names[field] = _name(call)
    return AgentEvalFiles(origin, names["dataset"], names["config"], folder)


def _instruction(
    project_dir: Path,
    source_file: str,
    value: object,
    source_files: list[str],
    diagnostics: list[Diagnostic],
    *,
    field: str,
    subject: str,
) -> str | None:
    """Read one instruction: its text as written, or the `{{ file('<path>') }}` sidecar it names.

    Text with no template is the instruction. A sidecar is read by `resolve_file`, relative to
    the agent file's folder, without trailing whitespace, and is appended to `source_files`.

    Args:
        field: The instruction's key under `spec.instructions`, which a value of the wrong
            type is reported under.
        subject: The agent's key, which every problem is reported against.

    Returns:
        The instruction; None when `value` is not a string or its sidecar was refused.

    Diagnostics:
        SST-PRS003: when the value is present and is not a string.
        Every code `read_sidecar` lists, for text that holds a template: it must be one
            `file()` call naming a sidecar `resolve_file` reads.
    """
    origin = Origin(source_file)
    if not isinstance(value, str):
        if value is not None:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"spec.instructions.{field}",
                    expected="string",
                    found=type(value).__name__,
                    origin=origin,
                    subject=subject,
                )
            )
        return None
    if "{{" not in value:
        return value
    sidecar = read_sidecar(
        project_dir,
        source_file,
        value,
        origin=origin,
        field=f"spec.instructions.{field}",
        artifact=subject,
        subject=subject,
    )
    if isinstance(sidecar, Diagnostic):
        diagnostics.append(sidecar)
        return None
    source_files.append(sidecar.path)
    return sidecar.text.rstrip()


def _parse_tools(
    relative: str,
    entries: list[Any],
    lines: Mapping[NodePath, SourcePosition],
    diagnostics: list[Diagnostic],
    subject: str,
) -> tuple[AgentTool, ...]:
    """Read the `spec.tools` entries in order, each placed where it starts; an unreadable one is left out."""
    tools: list[AgentTool] = []
    for index, value in enumerate(entries):
        position = lines.get(("spec", "tools", index))
        origin = Origin(relative, position.line, position.col) if position is not None else Origin(relative, 1, 1)
        tool = _parse_tool(relative, index, value, diagnostics, subject, origin)
        if tool is not None:
            tools.append(tool)
    return tuple(tools)


def _parse_tool(
    source_file: str,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
    subject: str,
    origin: Origin,
) -> AgentTool | None:
    """Read one `spec.tools` entry; None when it is not a mapping or declares no string `type`.

    `search_service`, else `identifier`, is read as the one name of a `{{ tool('<name>') }}`
    call, and `semantic_view` and `agent` as the one argument of a call of that name; each is
    reported and read as empty when it is anything else. A search tool's columns are kept as
    written, since what they must be depends on the member they resolve against.

    Args:
        subject: The agent's key, which a mapping field of the wrong type reports against.
        origin: Where the entry starts in the agent file, which the tool and its reports name.

    Diagnostics:
        SST-PRS018: when the entry is not a mapping.
        SST-PRS002: when it declares no string `type`.
        SST-PRS003: when a mapping field, such as `filter` or `input_schema`, holds another type,
            or `columns` is not a list.
        Every code `required_call` lists, for a reference field that is not one call of its
            function.
        SST-PRS023, SST-PRS024: as `passthrough_diagnostics` reports `tool_spec_passthrough`.
    """
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                origin=origin,
                artifact=source_file,
                field="tools",
                index=index,
                expected="mapping",
                found=type(value).__name__,
            )
        )
        return None
    type_name = value.get("type")
    if not isinstance(type_name, str):
        diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"tools[{index}].type", origin=origin))
        return None
    entry: Mapping[str, Any] = value

    def mapped(key: str) -> Mapping[Any, Any]:
        field = f"tools[{index}].{key}"
        read = checked_mapping(
            entry.get(key), diagnostics, field=field, artifact=source_file, origin=origin, subject=subject
        )
        return MappingProxyType(read)

    def called(key: str) -> str | None:
        field = f"tools[{index}].{key}"
        reference = f"agent.spec.tools.{key}"
        return _name(
            _reference(entry.get(key), reference, diagnostics, origin, field=field, artifact=subject, subject=subject)
        )

    backing_key = "search_service" if "search_service" in value else "identifier"
    field = f"tools[{index}].columns"
    columns = checked_list(
        value.get("columns"), diagnostics, field=field, artifact=source_file, origin=origin, subject=subject
    )
    backing = called(backing_key)
    semantic = called("semantic_view")
    agent_ref = called("agent")
    return AgentTool(
        type=type_name,
        origin=origin,
        name=optional_string(value.get("name")),
        description=optional_string(value.get("description")),
        semantic_view=semantic,
        backing=backing,
        agent_ref=agent_ref,
        warehouse=optional_string(value.get("warehouse")),
        query_timeout=optional_int(value.get("query_timeout")),
        max_results=optional_int(value.get("max_results")),
        title_column=optional_string(value.get("title_column")),
        id_column=optional_string(value.get("id_column")),
        stage_path=optional_string(value.get("stage_path")),
        relative_path_column=optional_string(value.get("relative_path_column")),
        filter=mapped("filter"),
        columns=tuple(columns),
        input_schema=mapped("input_schema"),
        passthrough=mapped("passthrough"),
        tool_spec_passthrough=_checked_passthrough(
            mapped("tool_spec_passthrough"),
            f"{subject}.tools[{index}].tool_spec_passthrough",
            TOOL_SPEC_KEYS,
            subject,
            origin,
            diagnostics,
        ),
        declared_keys=tuple(sorted(str(key) for key in value)),
    )


def _checked_passthrough(
    block: _Block,
    artifact: str,
    rendered: frozenset[str],
    subject: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> _Block:
    """Return a passthrough block, reporting its keys as `passthrough_diagnostics` does."""
    diagnostics.extend(passthrough_diagnostics(artifact, block, rendered, subject=subject, origin=origin))
    return block


def _parse_skill(
    source_file: str,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
    subject: str,
) -> AgentSkill | None:
    """Read one `spec.skills` entry; None when it cannot hold a skill.

    `source.path` is one call naming a project skill (`{{ skill() }}`), a project plugin
    (`{{ plugin() }}`) or a consumed extension (`{{ extension() }}`), and `ref` records which.
    A plugin entry's `name` is the `{{ skill('<member>') }}` call naming one of its members, and
    is kept as the member's name; any other entry's `name` is kept as written. A version holding
    a template is one `{{ var() }}` call, kept as the variable's name; any other is kept as
    written. An absent name, path or version, or one that is refused, reads as `""`.

    Diagnostics:
        SST-PRS018: when the entry is not a mapping with a `source:` mapping.
        SST-PRS002: when `source.type` is not a string, or `name` is present and not one.
        Every code `required_call` lists, for a `source.path` that is not one `skill()`,
            `plugin()` or `extension()` call, a plugin entry's `name` that is not one `skill()`
            call, or a templated `source.version` that is not one `var()` call.
        SST-REF008: when a consumed extension's `name` holds a template.
    """
    if not isinstance(value, dict) or not isinstance(value.get("source"), dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field="skills",
                index=index,
                expected="name/source mapping",
                found=type(value).__name__,
            )
        )
        return None
    source = value["source"]
    assert isinstance(source, dict)
    name = value.get("name")
    source_type = source.get("type")
    if not isinstance(source_type, str) or (name is not None and not isinstance(name, str)):
        diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"skills[{index}].name/source.type"))
        return None
    origin = Origin(source_file)
    field = f"spec.skills[{index}]"
    located: dict[str, Any] = {"artifact": subject, "subject": subject}
    path_call = _reference(
        source.get("path"),
        "agent.spec.skills.source.path",
        diagnostics,
        origin,
        field=f"{field}.source.path",
        **located,
    )
    ref = path_call.function if path_call is not None else "extension"
    path = path_call.args[0] if path_call is not None else ""
    if name is not None and ref == "plugin":
        name = (
            _name(_reference(name, "agent.spec.skills.name", diagnostics, origin, field=f"{field}.name", **located))
            or ""
        )
    elif (
        name is not None
        and ref == "extension"
        and (problem := literal_problem(name, origin, field=f"{field}.name", **located))
    ):
        diagnostics.append(problem)
        name = ""
    version = source.get("version")
    version_var = None
    if isinstance(version, str) and "{{" in version:
        reference = "agent.spec.skills.source.version"
        version_var = _name(
            _reference(version, reference, diagnostics, origin, field=f"{field}.source.version", **located)
        )
        version = None
    literal = optional_string(version)
    return AgentSkill(name or "", source_type, path, literal or "", ref=ref, version_var=version_var)


def _reference(
    value: object,
    reference: str,
    diagnostics: list[Diagnostic],
    origin: Origin,
    *,
    field: str,
    artifact: str,
    subject: str | None = None,
) -> TemplateCall | None:
    """Return the one call a reference field's `value` must be; None when it is absent or refused.

    Args:
        reference: The field's entry in `REFERENCE_FIELDS`, which names the functions it takes.

    A value that is not text is read as the text it prints as, so it is refused as a bare value.

    Diagnostics:
        Every code `required_call` lists.
    """
    if value is None:
        return None
    functions = reference_functions(reference)
    found = required_call(str(value), functions, origin, field=field, artifact=artifact, subject=subject)
    if isinstance(found, Diagnostic):
        diagnostics.append(found)
        return None
    return found


def _name(call: TemplateCall | None) -> str | None:
    """Return the one name a reference call gives; None for no call."""
    return call.args[0] if call is not None else None

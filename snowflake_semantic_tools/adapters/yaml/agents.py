"""Parse complete agent documents and resolve authored sidecars."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import resolve_within
from snowflake_semantic_tools.adapters.yaml.documents import NodePath, SourcePosition
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_list,
    checked_mapping,
    mapping,
    optional_int,
    optional_string,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentModel, AgentProfile, AgentSkill, AgentTool
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.parse.passthrough import AGENT_SPEC_KEYS, TOOL_SPEC_KEYS, passthrough_diagnostics
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import call_problem, malformed_field, syntax_problem

_Block = TypeVar("_Block", bound=Mapping[Any, Any])


def load_agents(project_dir: Path, *, agents_dir: str = "agents") -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
    """Read each `agent.yml` or `agent.yaml` one folder below `agents_dir` into an agent, in path order.

    A missing `agents_dir` holds no agents. A file that cannot be read or parsed, or names no
    agent, contributes its diagnostics and no agent; any other problem is reported and the
    agent kept without what could not be read. Sidecars and eval files resolve against the
    agent's own folder.

    Diagnostics:
        SST-LOD004: when a file cannot be read, or a template in it is malformed.
        SST-LOD006: when a file or an instruction sidecar is not UTF-8.
        SST-LOD001: when a file is not valid YAML.
        SST-LOD005: when a file writes a key twice in one mapping.
        SST-LOD003: when a file holds only whitespace or comments.
        SST-LOD008: when a file holds more than one document.
        SST-LOD002: when a file's root is not a mapping.
        SST-PRS002: when the agent has no `name`, a tool no `type`, a skill no `source.type`, or
            `evals:` no `dataset` or `config`.
        SST-PRS003: when `evals:` is not a mapping, or one of its paths not a non-empty string;
            when `spec.tools`, `spec.skills`, `tags` or the sample questions are not a list; or
            when `spec.instructions`, `meta`, `spec.passthrough` or a tool's mapping field, such
            as `filter`, is not a mapping. The agent is kept back, and the field reads as empty.
        SST-PRS018: when a `spec.tools` or `spec.skills` entry has the wrong shape.
        SST-PRS118: when a sample question is not a mapping with a string `question`.
        SST-REF003: when an instruction holds templates other than one whole `{{ file() }}` call.
        SST-REF014: when an instruction's `{{ file() }}` names no file.
        SST-REF027: when a sidecar or an eval file resolves outside the project root.
        SST-LOD018: when a sidecar cannot be read.
        SST-LOD019: when a sidecar holds only whitespace.
    """
    root = project_dir / agents_dir
    if not root.is_dir():
        return (), DiagnosticBag()
    agents: list[AgentModel] = []
    diagnostics: list[Diagnostic] = []
    for path in sorted(root.glob("*/agent.y*ml")):
        relative = path.relative_to(project_dir).as_posix()
        try:
            raw = path.read_bytes()
            parsed = parse_yaml_bytes(raw, relative)
        except ProjectError as exc:
            diagnostics.extend(exc.diagnostics)
            continue
        except OSError as exc:
            diagnostics.append(D("SST-LOD004", file=relative, line=1, col=1, reason=str(exc)))
            continue
        agent, problems = _parse_agent(project_dir, path.parent, relative, dict(parsed.tree), parsed.line_index)
        diagnostics.extend((*parsed.diagnostics, *problems))
        if agent is not None:
            documented = _comment_mentions(raw, parsed.line_index.get(_TOKENS), "orchestration")
            agents.append(replace(agent, budget_tokens_documented=documented))
    return tuple(agents), DiagnosticBag(diagnostics)


_TOKENS: NodePath = ("spec", "orchestration", "budget", "tokens")


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
    agent_dir: Path,
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
    models = mapping(spec.get("models"))
    orchestration = mapping(spec.get("orchestration"))
    budget = mapping(orchestration.get("budget"))
    capabilities = mapping(orchestration.get("capabilities"))
    instructions = mapped(spec.get("instructions"), "spec.instructions")
    source_files = [relative]
    orchestration_text, response_text = (
        _instruction(project_dir, agent_dir, relative, instructions.get(key), source_files, diagnostics)
        for key in ("orchestration", "response")
    )
    tools = _parse_tools(relative, listed(spec.get("tools"), "spec.tools"), lines, diagnostics, subject)
    skills = tuple(
        skill
        for index, value in enumerate(listed(spec.get("skills"), "spec.skills"))
        if (skill := _parse_skill(relative, index, value, diagnostics)) is not None
    )
    questions = listed(instructions.get("sample_questions"), "spec.instructions.sample_questions")
    sample_questions = _sample_questions(questions, name, origin, diagnostics)
    tags = tuple(
        (str(value.get("name")), str(value.get("value")))
        for value in listed(tree.get("tags"), "tags")
        if isinstance(value, dict) and value.get("name") is not None and value.get("value") is not None
    )
    meta = mapped(tree.get("meta"), "meta")
    passthrough = mapped(spec.get("passthrough"), "spec.passthrough")
    diagnostics.extend(
        passthrough_diagnostics(f"{name}.spec", passthrough, AGENT_SPEC_KEYS, subject=subject, origin=origin)
    )
    eval_files = _parse_eval_files(project_dir, agent_dir, relative, tree.get("evals"), diagnostics)
    return (
        AgentModel(
            name=name,
            origin=origin,
            source_files=tuple(dict.fromkeys(source_files)),
            comment=optional_string(tree.get("comment")),
            secure=bool(tree.get("secure", False)),
            profile=AgentProfile(
                optional_string(profile_node.get("display_name")),
                optional_string(profile_node.get("avatar")),
                optional_string(profile_node.get("color")),
            ),
            orchestration_model=str(models.get("orchestration") or "auto"),
            budget_seconds=optional_int(budget.get("seconds")),
            budget_tokens=optional_int(budget.get("tokens")),
            tool_not_accessible=optional_string(orchestration.get("tool_not_accessible")),
            analytical_search=(
                bool(capabilities.get("analytical_search")) if "analytical_search" in capabilities else None
            ),
            orchestration_instructions=orchestration_text,
            response_instructions=response_text,
            sample_questions=tuple(sample_questions),
            tools=tools,
            skills=skills,
            alias=optional_string(tree.get("alias")),
            enabled=bool(tree.get("enabled", True)),
            meta=MappingProxyType(meta),
            tags=tags,
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


def _parse_eval_files(
    project_dir: Path,
    agent_dir: Path,
    source_file: str,
    value: object,
    diagnostics: list[Diagnostic],
) -> AgentEvalFiles | None:
    """Resolve an agent's `evals:` dataset and config against its folder, as project-relative paths.

    Whether the files exist is not checked here.

    Returns:
        None when the agent declares no `evals:`; otherwise both paths, each None when it is
        absent, of the wrong type, or outside the project root.

    Diagnostics:
        SST-PRS003: when `evals:` is not a mapping, or a path is not a non-empty string.
        SST-PRS002: when `dataset` or `config` is absent.
        SST-REF027: when a path resolves outside the project root.
    """
    if value is None:
        return None
    origin = Origin(source_file)
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
        return AgentEvalFiles(origin)
    resolved: dict[str, str | None] = {"dataset": None, "config": None}
    for field in resolved:
        raw = value.get(field)
        if raw is None:
            diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"evals.{field}", origin=origin))
            continue
        if not isinstance(raw, str) or not raw.strip():
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"evals.{field}",
                    expected="non-empty string",
                    found=type(raw).__name__,
                    origin=origin,
                )
            )
            continue
        path = resolve_within(project_dir, agent_dir / raw)
        if path is None:
            diagnostics.append(D("SST-REF027", path=raw, origin=origin))
            continue
        resolved[field] = path.relative_to(project_dir.resolve()).as_posix()
    return AgentEvalFiles(origin, resolved["dataset"], resolved["config"])


def _instruction(
    project_dir: Path,
    agent_dir: Path,
    source_file: str,
    value: object,
    source_files: list[str],
    diagnostics: list[Diagnostic],
) -> str | None:
    """Read one instruction: its text as written, or the `{{ file('<path>') }}` sidecar it names.

    A sidecar resolves against the agent's folder, is read as UTF-8 without trailing whitespace,
    and is appended to `source_files`. A malformed template is reported and the text kept.

    Returns:
        The instruction; None when `value` is not a string or a sidecar problem was reported.

    Diagnostics:
        SST-LOD004, SST-REF033, SST-REF003: when a template in the text does not parse.
        SST-REF003: when the text holds templates, and they are not one call that is all of it.
        SST-REF004, SST-REF041, SST-REF015: when that call is not one one-path `file()` call.
        SST-REF027: when the sidecar resolves outside the project root.
        SST-REF014: when the sidecar's path names no file.
        SST-LOD018: when the sidecar is there and cannot be read.
        SST-LOD006: when the sidecar is not UTF-8.
        SST-LOD019: when the sidecar holds only whitespace.
    """
    if not isinstance(value, str):
        return None
    try:
        calls = scan_template_calls(value)
    except TemplateSyntaxError as exc:
        diagnostics.append(syntax_problem(exc, source_file))
        return value
    if not calls:
        return value
    origin = Origin(source_file)
    if len(calls) != 1 or calls[0].raw != value:
        diagnostics.append(malformed_field(value, origin))
        return None
    problem = call_problem(calls[0], frozenset(("file",)), origin, field="instructions", artifact=source_file)
    if problem is not None:
        diagnostics.append(problem)
        return None
    requested = str(calls[0].args[0])
    path = resolve_within(project_dir, agent_dir / requested)
    if path is None:
        diagnostics.append(D("SST-REF027", path=requested, origin=origin))
        return None
    if not path.exists():
        diagnostics.append(D("SST-REF014", path=requested, origin=origin))
        return None
    relative = path.relative_to(project_dir.resolve()).as_posix()
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        diagnostics.append(D("SST-LOD018", file=source_file, path=requested, origin=Origin(source_file)))
        return None
    except UnicodeDecodeError as exc:
        diagnostics.append(D("SST-LOD006", origin=Origin(relative), file=relative, offset=exc.start))
        return None
    if not content.strip():
        diagnostics.append(D("SST-LOD019", path=requested, file=source_file, origin=Origin(source_file)))
        return None
    source_files.append(relative)
    return content.rstrip()


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

    `search_service`, else `identifier`, is read as the arguments of one `{{ tool() }}` call,
    and `semantic_view` and `agent` as the one argument of a call of that name; any other value
    of those fields reads as empty.

    Args:
        subject: The agent's key, which a mapping field of the wrong type reports against.
        origin: Where the entry starts in the agent file, which the tool and its reports name.

    Diagnostics:
        SST-PRS018: when the entry is not a mapping.
        SST-PRS002: when it declares no string `type`.
        SST-PRS003: when a mapping field, such as `filter` or `input_schema`, holds another type.
        SST-LOD004, SST-REF033, SST-REF003: when a template in a reference field does not parse.
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

    backing_value = value.get("search_service", value.get("identifier"))
    backing = _template_args(backing_value, "tool", diagnostics, origin)
    semantic = _single_template_arg(value.get("semantic_view"), "semantic_view", diagnostics, origin)
    agent_ref = _single_template_arg(value.get("agent"), "agent", diagnostics, origin)
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
        columns_and_descriptions=mapped("columns_and_descriptions"),
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
) -> AgentSkill | None:
    """Read one `spec.skills` entry; None when it cannot hold a skill.

    `source.path` names a project skill (`{{ skill() }}`), a project plugin (`{{ plugin() }}`)
    or a consumed extension (`{{ extension() }}`), tried in that order, and `ref` records which.
    A `{{ var() }}` version is kept as the variable's name, else the version as written. An
    absent name, path or version reads as `""`.

    Diagnostics:
        SST-PRS018: when the entry is not a mapping with a `source:` mapping.
        SST-PRS002: when `source.type` is not a string, or `name` is present and not one.
        SST-LOD004, SST-REF033, SST-REF003: when a template in `source.path` or `source.version`
            does not parse, once each.
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
    ref = "extension"
    path: str | None = None
    for function in ("skill", "plugin"):
        # Syntax errors are reported once, by the final extension() attempt.
        path = _single_template_arg(source.get("path"), function, [], Origin(source_file))
        if path is not None:
            ref = function
            break
    else:
        path = _single_template_arg(source.get("path"), "extension", diagnostics, Origin(source_file))
    version_var = _single_template_arg(source.get("version"), "var", diagnostics, Origin(source_file))
    literal = None if version_var is not None else optional_string(source.get("version"))
    return AgentSkill(name or "", source_type, path or "", literal or "", ref=ref, version_var=version_var)


def _template_args(
    value: object,
    function: str,
    diagnostics: list[Diagnostic],
    origin: Origin,
) -> tuple[str, ...]:
    if not isinstance(value, str):
        return ()
    value = re.sub(r"''([^']+)''", r"'\1'", value)
    try:
        calls = scan_template_calls(value)
    except TemplateSyntaxError as exc:
        diagnostics.append(syntax_problem(exc, origin.file))
        return ()
    if len(calls) != 1 or calls[0].function != function or calls[0].raw != value:
        return ()
    return tuple(str(value) for value in calls[0].args)


def _single_template_arg(
    value: object,
    function: str,
    diagnostics: list[Diagnostic],
    origin: Origin,
) -> str | None:
    args = _template_args(value, function, diagnostics, origin)
    return args[0] if len(args) == 1 else None

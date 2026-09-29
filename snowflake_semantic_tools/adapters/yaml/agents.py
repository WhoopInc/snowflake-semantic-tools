"""Parse complete agent documents and resolve authored sidecars."""

from __future__ import annotations

import re
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from ...domain.model.agent import AgentEvalFiles, AgentModel, AgentProfile, AgentSkill, AgentTool
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.reference import TemplateSyntaxError, scan_template_calls
from ..project import ProjectError
from .loader import _parse_yaml_bytes


def load_agents(project_dir: Path, *, agents_dir: str = "agents") -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
    root = project_dir / agents_dir
    if not root.is_dir():
        return (), DiagnosticBag()
    agents: list[AgentModel] = []
    diagnostics: list[Diagnostic] = []
    for path in sorted(root.glob("*/agent.y*ml")):
        relative = path.relative_to(project_dir).as_posix()
        try:
            tree = dict(_parse_yaml_bytes(path.read_bytes(), relative).tree)
        except ProjectError as exc:
            diagnostics.extend(exc.diagnostics)
            continue
        except OSError as exc:
            diagnostics.append(D("SST-LOD004", file=relative, line=1, col=1, reason=str(exc)))
            continue
        agent, problems = _parse_agent(project_dir, path.parent, relative, tree)
        diagnostics.extend(problems)
        if agent is not None:
            agents.append(agent)
    return tuple(agents), DiagnosticBag(diagnostics)


def _parse_agent(
    project_dir: Path,
    agent_dir: Path,
    relative: str,
    tree: Mapping[str, Any],
) -> tuple[AgentModel | None, tuple[Diagnostic, ...]]:
    origin = Origin(relative, 1, 1)
    diagnostics: list[Diagnostic] = []
    name = tree.get("name")
    if not isinstance(name, str) or not name:
        return None, (D("SST-PRS002", artifact=relative, field="name", origin=origin),)
    profile_node = _mapping(tree.get("profile"))
    spec = _mapping(tree.get("spec"))
    models = _mapping(spec.get("models"))
    orchestration = _mapping(spec.get("orchestration"))
    budget = _mapping(orchestration.get("budget"))
    capabilities = _mapping(orchestration.get("capabilities"))
    instructions = _mapping(spec.get("instructions"))
    source_files = [relative]
    orchestration_text = _instruction(
        project_dir,
        agent_dir,
        relative,
        instructions.get("orchestration"),
        source_files,
        diagnostics,
    )
    response_text = _instruction(
        project_dir,
        agent_dir,
        relative,
        instructions.get("response"),
        source_files,
        diagnostics,
    )
    tools = tuple(
        tool
        for index, value in enumerate(spec.get("tools") or [])
        if (tool := _parse_tool(relative, index, value, diagnostics)) is not None
    )
    skills = tuple(
        skill
        for index, value in enumerate(spec.get("skills") or [])
        if (skill := _parse_skill(relative, index, value, diagnostics)) is not None
    )
    sample_questions: list[str] = []
    for index, value in enumerate(instructions.get("sample_questions") or []):
        if not isinstance(value, dict) or not isinstance(value.get("question"), str):
            diagnostics.append(
                D(
                    "SST-PRS118",
                    artifact=f"agent:{name}",
                    index=index,
                    origin=origin,
                    subject=f"agent:{name.casefold()}",
                )
            )
            continue
        sample_questions.append(str(value["question"]))
    tags = tuple(
        (str(value.get("name")), str(value.get("value")))
        for value in tree.get("tags") or []
        if isinstance(value, dict) and value.get("name") is not None and value.get("value") is not None
    )
    eval_files = _parse_eval_files(project_dir, agent_dir, relative, tree.get("evals"), diagnostics)
    return (
        AgentModel(
            name=name,
            origin=origin,
            source_files=tuple(dict.fromkeys(source_files)),
            comment=_optional_string(tree.get("comment")),
            secure=bool(tree.get("secure", False)),
            profile=AgentProfile(
                _optional_string(profile_node.get("display_name")),
                _optional_string(profile_node.get("avatar")),
                _optional_string(profile_node.get("color")),
            ),
            orchestration_model=str(models.get("orchestration") or "auto"),
            budget_seconds=_optional_int(budget.get("seconds")),
            budget_tokens=_optional_int(budget.get("tokens")),
            tool_not_accessible=_optional_string(orchestration.get("tool_not_accessible")),
            analytical_search=(
                bool(capabilities.get("analytical_search")) if "analytical_search" in capabilities else None
            ),
            orchestration_instructions=orchestration_text,
            response_instructions=response_text,
            sample_questions=tuple(sample_questions),
            tools=tools,
            skills=skills,
            alias=_optional_string(tree.get("alias")),
            enabled=bool(tree.get("enabled", True)),
            meta=MappingProxyType(dict(tree.get("meta") or {})),
            tags=tags,
            passthrough=MappingProxyType(dict(spec.get("passthrough") or {})),
            evals=eval_files,
        ),
        tuple(diagnostics),
    )


def _parse_eval_files(
    project_dir: Path,
    agent_dir: Path,
    source_file: str,
    value: object,
    diagnostics: list[Diagnostic],
) -> AgentEvalFiles | None:
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
        path = (agent_dir / raw).resolve()
        try:
            resolved[field] = path.relative_to(project_dir.resolve()).as_posix()
        except ValueError:
            diagnostics.append(D("SST-REF027", path=raw, origin=origin))
    return AgentEvalFiles(origin, resolved["dataset"], resolved["config"])


def _instruction(
    project_dir: Path,
    agent_dir: Path,
    source_file: str,
    value: object,
    source_files: list[str],
    diagnostics: list[Diagnostic],
) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        calls = scan_template_calls(value)
    except TemplateSyntaxError as exc:
        diagnostics.append(D("SST-LOD004", file=source_file, line=exc.line, col=exc.col, reason=exc.reason))
        return value
    if not calls:
        return value
    if len(calls) != 1 or calls[0].function != "file" or len(calls[0].args) != 1 or calls[0].raw != value:
        diagnostics.append(D("SST-REF014", path=value, origin=Origin(source_file)))
        return None
    requested = str(calls[0].args[0])
    path = (agent_dir / requested).resolve()
    try:
        relative = path.relative_to(project_dir.resolve()).as_posix()
    except ValueError:
        diagnostics.append(D("SST-REF027", path=requested, origin=Origin(source_file)))
        return None
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        diagnostics.append(D("SST-LOD018", file=source_file, path=requested, origin=Origin(source_file)))
        return None
    if not content.strip():
        diagnostics.append(D("SST-LOD019", path=requested, file=source_file, origin=Origin(source_file)))
        return None
    source_files.append(relative)
    return content.rstrip()


def _parse_tool(
    source_file: str,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> AgentTool | None:
    origin = Origin(source_file, index + 1, 1)
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS018",
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
    backing_value = value.get("search_service", value.get("identifier"))
    backing = _template_args(backing_value, "tool", diagnostics, origin)
    semantic = _single_template_arg(value.get("semantic_view"), "semantic_view", diagnostics, origin)
    agent_ref = _single_template_arg(value.get("agent"), "agent", diagnostics, origin)
    return AgentTool(
        type=type_name,
        origin=origin,
        name=_optional_string(value.get("name")),
        description=_optional_string(value.get("description")),
        semantic_view=semantic,
        backing=backing,
        agent_ref=agent_ref,
        warehouse=_optional_string(value.get("warehouse")),
        query_timeout=_optional_int(value.get("query_timeout")),
        max_results=_optional_int(value.get("max_results")),
        title_column=_optional_string(value.get("title_column")),
        id_column=_optional_string(value.get("id_column")),
        stage_path=_optional_string(value.get("stage_path")),
        relative_path_column=_optional_string(value.get("relative_path_column")),
        filter=MappingProxyType(dict(value.get("filter") or {})),
        columns_and_descriptions=MappingProxyType(dict(value.get("columns_and_descriptions") or {})),
        input_schema=MappingProxyType(dict(value.get("input_schema") or {})),
        passthrough=MappingProxyType(dict(value.get("passthrough") or {})),
        tool_spec_passthrough=MappingProxyType(dict(value.get("tool_spec_passthrough") or {})),
    )


def _parse_skill(
    source_file: str,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
) -> AgentSkill | None:
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
    literal = None if version_var is not None else _optional_string(source.get("version"))
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
        diagnostics.append(D("SST-LOD004", file=origin.file, line=exc.line, col=exc.col, reason=exc.reason))
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


def _optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _mapping(value: object) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}

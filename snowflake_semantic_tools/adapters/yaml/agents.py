"""Parse complete agent documents and resolve authored sidecars."""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from snowflake_semantic_tools.adapters.yaml.agent_fields import (
    call_name,
    config_origins,
    folder_files,
    instruction,
    parse_skill,
    published_text,
    read_document,
    reference_call,
    sample_questions,
    unknown_block_keys,
)
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_list,
    checked_mapping,
    mapping,
    optional_int,
    optional_string,
)
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentModel, AgentProfile, AgentTool
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings, setting_value
from snowflake_semantic_tools.domain.parse.names import name_warnings
from snowflake_semantic_tools.domain.parse.passthrough import AGENT_SPEC_KEYS, TOOL_SPEC_KEYS, passthrough_diagnostics
from snowflake_semantic_tools.domain.resolve.calls import literal_problem
from snowflake_semantic_tools.domain.validate.agent_tool import TOOL_KEYS
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics
from snowflake_semantic_tools.domain.validate.sql import name_problem

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
        SST-PRS005: when the agent's `name` is not a Snowflake identifier; the agent is left out.
        SST-PRS012, SST-PRS031, SST-PRS100: when the agent's `name` holds a character outside
            ASCII, is a reserved word, or starts with a reserved prefix.
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
        SST-REF008: when a consumed extension's skill `name` holds a template, or a field the
            agent publishes as written does: its `comment`, `profile.display_name`, a sample
            question, or a tool's `description`.
        SST-LOD018: when an agent file cannot be read.
        SST-VAL801: when a folder holds both `agent.yml` and `agent.yaml`; neither is read.
    """
    root = project_dir / agents_dir
    routes = routed_diagnostics(config, project_dir, "agent") if config is not None else ()
    if not root.is_dir():
        return (), DiagnosticBag(routes)
    agents: list[AgentModel] = []
    diagnostics: list[Diagnostic] = list(routes)
    detail = "agent folder has both agent.yml and agent.yaml"
    for path in folder_files(project_dir, root, _AGENT_FILES, diagnostics, detail=detail):
        read = read_document(project_dir, path, diagnostics)
        if read is None:
            continue
        relative, raw, parsed = read
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
# The spellings of an agent file; a folder holds one of them.
_AGENT_FILES = ("agent.yml", "agent.yaml")


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


def _moved(path: NodePath, key: str) -> str | None:
    """Return where a key an agent file used to write is written now; None for any other key.

    A tool's key is checked against every key some tool type takes; one of another type's is
    SST-VAL516's to report.
    """
    if len(path) == 3 and path[:2] == ("spec", "tools"):
        return _MOVED_TOOL_KEYS.get(key)
    return _MOVED.get((path, key))


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
    """Build one agent, reporting each problem; None only when it has no name, or one SST cannot render.

    A name SST can render but that is risky to publish is reported, and the agent kept.

    Args:
        lines: Where each node of the agent file starts, which places each tool.

    Diagnostics:
        SST-PRS002: the agent has no `name`; it is not built.
        SST-PRS005: the name is not a Snowflake identifier; it is not built.
        Every code `name_warnings` lists, at the name.
    """
    origin = Origin(relative, 1, 1)
    name = tree.get("name")
    if not isinstance(name, str) or not name:
        return None, (D("SST-PRS002", artifact=relative, field="name", origin=origin),)
    subject = artifact_key("agent", name.casefold())
    named = lines.get(("name",))
    at = Origin(relative, named.line, named.col) if named is not None else origin
    unusable = name_problem(name, artifact=subject, subject=subject, origin=at)
    if unusable is not None:
        return None, (unusable,)
    diagnostics: list[Diagnostic] = list(name_warnings(name, subject=subject, origin=at))

    def listed(value: object, field: str) -> list[Any]:
        return checked_list(value, diagnostics, field=field, artifact=relative, origin=origin, subject=subject)

    def mapped(value: object, field: str) -> dict[Any, Any]:
        return checked_mapping(value, diagnostics, field=field, artifact=relative, origin=origin, subject=subject)

    profile_node = mapping(tree.get("profile"))
    spec = mapping(tree.get("spec"))
    instructions = mapped(spec.get("instructions"), "spec.instructions")
    source_files = [relative]
    orchestration_text, response_text = (
        instruction(
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
        if (skill := parse_skill(relative, index, value, diagnostics, subject, kind="agent")) is not None
    )
    questions = listed(instructions.get("sample_questions"), "spec.instructions.sample_questions")
    questions_read = sample_questions(questions, "agent", name, origin, diagnostics)

    def published(value: object, path: NodePath) -> str | None:
        return published_text(value, path, relative, lines, diagnostics, subject)

    meta = mapped(tree.get("meta"), "meta")
    passthrough = mapped(spec.get("passthrough"), "spec.passthrough")
    diagnostics.extend(
        passthrough_diagnostics(f"{name}.spec", passthrough, AGENT_SPEC_KEYS, subject=subject, origin=origin)
    )
    eval_files = _parse_eval_files(relative, tree.get("evals"), diagnostics)
    diagnostics.extend(unknown_block_keys(relative, tree, lines, subject, _AGENT_KEYS, _moved))
    origin_of = config_origins(relative, lines)
    diagnostics.extend(item_config_diagnostics("agents", tree.get("config"), subject=subject, origin_of=origin_of))
    return (
        AgentModel(
            name=name,
            origin=origin,
            source_files=tuple(dict.fromkeys(source_files)),
            comment=published(tree.get("comment"), ("comment",)),
            profile=AgentProfile(
                published(profile_node.get("display_name"), ("profile", "display_name")),
                optional_string(profile_node.get("avatar")),
                optional_string(profile_node.get("color")),
            ),
            orchestration_instructions=orchestration_text,
            response_instructions=response_text,
            sample_questions=tuple(questions_read),
            tools=tools,
            skills=skills,
            meta=MappingProxyType(meta),
            passthrough=MappingProxyType(passthrough),
            evals=eval_files,
            deprecated=bool(tree.get("deprecated", False)),
        ),
        tuple(diagnostics),
    )


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
        call = reference_call(
            raw, f"agent.evals.{field}", diagnostics, origin, field=f"evals.{field}", artifact=source_file
        )
        names[field] = call_name(call)
    return AgentEvalFiles(origin, names["dataset"], names["config"], folder)


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
        SST-REF008: the `description`, published as written, holds a template.
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
        ref = f"agent.spec.tools.{key}"
        call = reference_call(entry.get(key), ref, diagnostics, origin, field=field, artifact=subject, subject=subject)
        return call_name(call)

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
        description=_described(value.get("description"), index, subject, origin, diagnostics),
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


def _described(value: object, index: int, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    """Read a tool's `description`, published as written: a template in it is SST-REF008."""
    text = optional_string(value)
    problem = literal_problem(
        text or "", origin, field=f"spec.tools[{index}].description", artifact=subject, subject=subject
    )
    if problem is not None:
        diagnostics.append(problem)
    return text


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

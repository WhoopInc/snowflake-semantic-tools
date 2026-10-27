"""`sst migrate refs` for agents and eval files.

Agent files: a tag's bare `name:` becomes `tag()`; an agent toolset written by bare `name:` gains
`identifier: "{{ tool('<name>') }}"`, which is how SST resolved it; a Cortex Search tool's own
columns are written as its member writes them (refs of a defined member's `on:` model, bare for a
referenced one) and its `columns_and_descriptions` as a `columns:` list; a `stage_path` on a
declared stage opens with that stage's `tool()`; a plugin skill's `name:` becomes `skill()`. The
settings `alias`, `secure`, `enabled`, `tags`, `spec.models.orchestration` and the
`spec.orchestration` budget, access and capability keys move under `config:`.

Eval files lose `agent:`: the `evals/` folder says whose they are. A config's `agent_version` and
`run.tier`, `run.retry`, `run.concurrency` and `run.baseline_runs` move under `config:`. Its
`run.retention` map of classes by variant becomes the one class its `run.variant` is listed under;
the map's `decision_window_days` has no successor and goes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.domain.migrate.calls import bare, quoted_call
from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.edits import Edit, double_quoted, entry_span
from snowflake_semantic_tools.domain.migrate.outline import Node
from snowflake_semantic_tools.domain.migrate.semantic import tag_names
from snowflake_semantic_tools.domain.migrate.settings import Move, delete_key, move_settings
from snowflake_semantic_tools.domain.migrate.tools import ToolInfo, columns_list, stage_path
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, single_template_call

AGENT_MOVES = (
    Move(("alias",), "alias"),
    Move(("secure",), "secure"),
    Move(("enabled",), "enabled"),
    Move(("tags",), "tags"),
    Move(("spec", "models", "orchestration"), "orchestration_model"),
    Move(("spec", "orchestration", "budget", "seconds"), "budget_seconds"),
    Move(("spec", "orchestration", "budget", "tokens"), "budget_tokens"),
    Move(("spec", "orchestration", "tool_not_accessible"), "tool_not_accessible"),
    Move(("spec", "orchestration", "capabilities", "analytical_search"), "analytical_search"),
)
EVAL_MOVES = (
    Move(("agent_version",), "agent_version"),
    Move(("run", "tier"), "eval_tier"),
    Move(("run", "retry"), "retry"),
    Move(("run", "concurrency"), "concurrency"),
    Move(("run", "baseline_runs"), "baseline_runs"),
)
_SEARCH_COLUMNS = ("id_column", "title_column", "relative_path_column")


def agent_references(root: Node, lines: Sequence[str], index: Mapping[str, ToolInfo]) -> Changes:
    """Record the reference pass's rewrites for one agent file."""
    changes = Changes()
    tag_names(root.get("tags"), changes)
    tag_names(root.find("config", "tags"), changes)
    tools = root.find("spec", "tools")
    for tool in tools.maps() if tools is not None and tools.is_seq else ():
        kind = tool.get("type")
        if kind is not None and kind.text == "agent_toolset":
            _toolset(lines, tool, changes)
        elif kind is not None and kind.text == "cortex_search":
            _search_tool(lines, tool, index, changes)
    skills = root.find("spec", "skills")
    for skill in skills.maps() if skills is not None and skills.is_seq else ():
        name = skill.get("name")
        path = skill.find("source", "path")
        written = bare(name.text) if name is not None else None
        if name is not None and written is not None and _calls(path, "plugin"):
            changes.replace(name, quoted_call("skill", written), "reference")
    return changes


def agent_structure(lines: Sequence[str], root: Node) -> Changes:
    """Record the structure pass's rewrites for one agent file."""
    changes = Changes()
    move_settings(lines, root, AGENT_MOVES, changes, block="config")
    return changes


def eval_structure(lines: Sequence[str], root: Node) -> Changes:
    """Record the structure pass's rewrites for one eval dataset or config file."""
    changes = Changes()
    agent = root.entry("agent")
    if agent is not None:
        delete_key(lines, agent, changes)
    retention = root.find("run", "retention")
    moves = list(EVAL_MOVES)
    if retention is not None and retention.is_map:
        chosen = _retention_class(retention, root.find("run", "variant"))
        moves.append(Move(("run", "retention"), "retention", chosen, drop=chosen is None))
    move_settings(lines, root, moves, changes, block="config")
    return changes


def _calls(node: Node | None, function: str) -> bool:
    """Whether a scalar is exactly one call of `function`."""
    try:
        return node is not None and single_template_call(node.text or "", function) is not None
    except TemplateSyntaxError:
        return False


def _toolset(lines: Sequence[str], tool: Node, changes: Changes) -> None:
    """Give a toolset written by bare `name:` the `identifier:` its name resolved through."""
    name = tool.entry("name")
    written = bare(name.value.text) if name is not None else None
    if name is None or written is None or tool.get("agent") is not None or tool.get("identifier") is not None:
        return
    if tool.flow:
        changes.leave(name.line, name.col, written, "a flow mapping: add identifier: by hand")
        return
    after = " " * name.col + "identifier: " + quoted_call("tool", written) + "\n"
    end = entry_span(lines, name)[1]
    changes.add(Edit(end, 0, end, 0, after), written, after.strip(), "reference")


def _search_tool(lines: Sequence[str], tool: Node, index: Mapping[str, ToolInfo], changes: Changes) -> None:
    """Write a search tool's own columns as its member's are written, and its stage path."""
    service = tool.get("search_service")
    member = None
    try:
        found = single_template_call(service.text or "", "tool") if service is not None else None
    except TemplateSyntaxError:
        found = None
    if found is not None and found.args:
        member = index.get(found.args[-1].casefold())
    columns = tool.entry("columns_and_descriptions")
    written = [node for key in _SEARCH_COLUMNS if (node := tool.get(key)) is not None and bare(node.text)]
    if member is None and (columns is not None or written):
        site = columns or tool.entry(_SEARCH_COLUMNS[0]) or tool.entries[0]
        changes.leave(site.line, site.col, site.key, "the tool's search_service names no tool member")
    elif member is not None:
        model = member.on_model if member.defined else None
        if columns is not None:
            columns_list(lines, columns, model, changes, member.defined)
        for node in written:
            column = bare(node.text) or ""
            if not member.defined:
                continue
            if model is None:
                changes.leave(node.line, node.col, column, "the member's on: names no model to ref the column of")
            else:
                changes.replace(node, quoted_call("ref", model, column), "tool_column")
    stage = tool.get("stage_path")
    converted = stage_path(stage.text if stage is not None else None, index)
    if stage is not None and converted is not None:
        changes.replace(stage, double_quoted(converted), "reference")


def _retention_class(retention: Node, variant: Node | None) -> str | None:
    """Return the class a `run.retention` map lists the run's variant under; None for none."""
    name = variant.text if variant is not None else None
    for cls in ("audit", "decision"):
        listed = retention.get(cls)
        if name is not None and listed is not None and any(item.text == name for item in listed.items):
            return cls
    return None
